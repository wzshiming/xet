package server_test

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/auth"
	xetclient "github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

// The hub serves its files on a branch, which revalidation applies to, at
// one commit.
const (
	routeRepo   = "bench/route"
	routeRev    = "main"
	routeCommit = "0123456789abcdef0123456789abcdef01234567"
)

// revalidations are the mirror revalidation intervals of the route stacks.
var revalidations = []struct {
	name     string
	interval time.Duration
}{
	{"fresh", -1}, // never probes, as xetd within 5 minutes of the last probe
	{"stale", 0},  // probes on every resolve, as xetd past those 5 minutes
}

// routeClasses are the classes of the route files: random and model weights.
var routeClasses = []storagetest.ChunkClass{storagetest.ChunkClasses[0], storagetest.ChunkClassBF16}

// routeStage is the request a route row times; do checks its answer, drains
// any body into body and returns its time to first byte.
type routeStage struct {
	name   string
	bodied bool
	do     func(tb testing.TB, st *routeStack, f *routeFile, body io.Writer, buf []byte) time.Duration
}

var routeStages = []routeStage{
	{"head", false, headResolve},
	{"get302", false, getResolve},
	{"bridge", true, func(tb testing.TB, st *routeStack, f *routeFile, body io.Writer, buf []byte) time.Duration {
		return getBody(tb, client, st.srv.URL+"/xet-bridge/"+f.sha256, f, body, buf)
	}},
	{"chain", true, func(tb testing.TB, st *routeStack, f *routeFile, body io.Writer, buf []byte) time.Duration {
		return getBody(tb, client, st.srv.URL+f.path, f, body, buf)
	}},
}

// noRedirect shares client's connections and returns redirects.
var noRedirect = &http.Client{
	Transport: client.Transport,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// routeHub is a fake upstream hub: a resolve answers the file's metadata
// headers and a redirect to /cdn<path>, which serves the bytes.
type routeHub struct {
	delay   time.Duration // before answering each resolve
	mu      sync.Mutex
	files   map[string]hubFile // by resolve path
	heads   atomic.Int64       // resolve HEADs: the mirror's probes
	gets    atomic.Int64       // resolve GETs
	cdnGets atomic.Int64
}

type hubFile struct {
	data []byte
	etag string // quoted SHA-256 hex of data
}

func (h *routeHub) file(path string) (hubFile, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f, ok := h.files[path]
	return f, ok
}

func (h *routeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if path, ok := strings.CutPrefix(r.URL.Path, "/cdn"); ok {
		h.serveData(w, r, path)
		return
	}
	if r.Method == http.MethodHead {
		h.heads.Add(1)
	} else {
		h.gets.Add(1)
	}
	time.Sleep(h.delay)
	f, ok := h.file(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("ETag", f.etag)
	w.Header().Set("X-Linked-Etag", f.etag)
	w.Header().Set("X-Linked-Size", strconv.Itoa(len(f.data)))
	w.Header().Set("X-Repo-Commit", routeCommit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

func (h *routeHub) serveData(w http.ResponseWriter, r *http.Request, path string) {
	f, ok := h.file(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.Itoa(len(f.data)))
		return
	}
	h.cdnGets.Add(1)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(f.data))
}

// routeStack is an in-process xetd in mirror mode in front of its own hub.
type routeStack struct {
	hub         *routeHub
	hubSrv, srv *httptest.Server
	conns       *connStats // of srv
	files       map[string]*routeFile
}

var routeStacks = map[string]*routeStack{} // by revalidation name

// routeStackFor returns the stack of revalidation name, building it on first use.
func routeStackFor(tb testing.TB, name string, interval time.Duration) *routeStack {
	tb.Helper()
	if routeStacks[name] == nil {
		routeStacks[name] = newRouteStack(tb, name, interval)
	}
	return routeStacks[name]
}

// newRouteStack composes the handlers as cmd/xetd does with -upstream and
// neither -signing-key nor -base-url, without its request log or term split.
func newRouteStack(tb testing.TB, name string, interval time.Duration) *routeStack {
	tb.Helper()
	delay, err := time.ParseDuration(cmp.Or(os.Getenv("XET_ROUTE_BENCH_UPSTREAM_DELAY"), "0"))
	if err != nil {
		tb.Fatalf("XET_ROUTE_BENCH_UPSTREAM_DELAY: %v", err)
	}
	st := &routeStack{hub: &routeHub{delay: delay, files: map[string]hubFile{}}, files: map[string]*routeFile{}}
	st.hubSrv = httptest.NewServer(st.hub)
	dir := filepath.Join(benchRoot(tb), "route", name)
	stor, err := local.NewStorage(local.WithBasePath(dir))
	if err != nil {
		tb.Fatal(err)
	}
	queue, err := spool.NewSpool(filepath.Join(dir, "spool"), stor)
	if err != nil {
		tb.Fatal(err)
	}
	issuer, err := auth.NewIssuer(nil, 15*time.Minute, time.Now)
	if err != nil {
		tb.Fatal(err)
	}
	up := xetclient.StaticUpstreamProvider(st.hubSrv.URL, "")
	m, err := mirror.NewMirror(
		mirror.WithStorage(stor),
		mirror.WithCacheDir(filepath.Join(dir, "mirror")),
		mirror.WithSpool(queue),
		mirror.WithRevalidateInterval(interval),
	)
	if err != nil {
		tb.Fatal(err)
	}
	st.srv, st.conns = newCountingServer(server.NewHandler(
		server.WithStorage(stor),
		server.WithNext(hf.NewHandler(
			hf.WithMirror(m),
			hf.WithUpstreamProvider(up),
			hf.WithMinter(hf.MinterFunc(func(r *http.Request, req hf.TokenRequest) (string, int64, error) {
				if req.Permission != auth.Read {
					return "", 0, hf.ErrNotHandled
				}
				return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
			})),
			hf.WithNext(hf.NewUpstreamProxy(up)),
		)),
	))
	return st
}

// routeFile is a hub file the stack's mirror has ingested.
type routeFile struct {
	path   string // the resolve path
	size   int64
	sha256 string
}

// fileFor returns the file of s and c, putting it on the hub and waiting for
// the mirror to ingest it on first use.
func (st *routeStack) fileFor(tb testing.TB, s shape, c storagetest.ChunkClass) *routeFile {
	tb.Helper()
	key := s.name + "/" + c.Name
	if st.files[key] == nil {
		st.files[key] = st.ingest(tb, s, c)
	}
	return st.files[key]
}

func (st *routeStack) ingest(tb testing.TB, s shape, c storagetest.ChunkClass) *routeFile {
	tb.Helper()
	data := bytes.Join(slices.Concat(c.Xorbs(s.xorbs, s.chunksPerXorb, chunkSize)...), nil)
	sum := sha256.Sum256(data)
	name := c.Name + "-" + s.name + ".bin"
	f := &routeFile{
		path:   "/" + routeRepo + "/resolve/" + routeRev + "/" + name,
		size:   int64(len(data)),
		sha256: hex.EncodeToString(sum[:]),
	}
	st.hub.mu.Lock()
	// The mirror fetches the file at the commit its probe pinned the branch to.
	for _, rev := range []string{routeRev, routeCommit} {
		st.hub.files["/"+routeRepo+"/resolve/"+rev+"/"+name] = hubFile{data: data, etag: `"` + f.sha256 + `"`}
	}
	st.hub.mu.Unlock()

	url := st.srv.URL + f.path
	deadline := time.Now().Add(60*time.Second + time.Duration(f.size>>20)*time.Second)
	for {
		resp, err := noRedirect.Get(url)
		if err != nil {
			tb.Fatal(err)
		}
		var msg []byte
		if resp.StatusCode != http.StatusOK {
			msg, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		}
		resp.Body.Close() // an ingest in flight streams the file; the poll only needs the status
		if resp.StatusCode == http.StatusFound {
			break
		}
		if resp.StatusCode != http.StatusOK || time.Now().After(deadline) {
			tb.Fatalf("GET %s: %s %q; want 302 once ingested", url, resp.Status, msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
	h := sha256.New()
	getBody(tb, client, url, f, h, make([]byte, drainBufSize))
	if got := hex.EncodeToString(h.Sum(nil)); got != f.sha256 {
		tb.Fatalf("GET %s: body SHA-256 %s, want %s", url, got, f.sha256)
	}
	return f
}

func headResolve(tb testing.TB, st *routeStack, f *routeFile, _ io.Writer, _ []byte) time.Duration {
	tb.Helper()
	url := st.srv.URL + f.path
	start := time.Now()
	resp, err := noRedirect.Head(url)
	ttfb := time.Since(start)
	if err != nil {
		tb.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength != f.size || resp.Header.Get("X-Repo-Commit") != routeCommit {
		tb.Fatalf("HEAD %s: %s, Content-Length %d, X-Repo-Commit %q; want 200, %d, %s",
			url, resp.Status, resp.ContentLength, resp.Header.Get("X-Repo-Commit"), f.size, routeCommit)
	}
	return ttfb
}

func getResolve(tb testing.TB, st *routeStack, f *routeFile, _ io.Writer, buf []byte) time.Duration {
	tb.Helper()
	url := st.srv.URL + f.path
	start := time.Now()
	resp, err := noRedirect.Get(url)
	ttfb := time.Since(start)
	if err != nil {
		tb.Fatal(err)
	}
	_, err = io.CopyBuffer(io.Discard, resp.Body, buf)
	resp.Body.Close()
	if loc := resp.Header.Get("Location"); err != nil || resp.StatusCode != http.StatusFound || !strings.HasSuffix(loc, "/xet-bridge/"+f.sha256) {
		tb.Fatalf("GET %s: %s to %q, %v; want 302 to /xet-bridge/%s", url, resp.Status, loc, err, f.sha256)
	}
	return ttfb
}

// getBody GETs url with c, which must end at the bridge of f, drains the
// body into body and returns the time to its first byte.
func getBody(tb testing.TB, c *http.Client, url string, f *routeFile, body io.Writer, buf []byte) time.Duration {
	tb.Helper()
	start := time.Now()
	resp, err := c.Get(url)
	if err != nil {
		tb.Fatal(err)
	}
	defer resp.Body.Close()
	r := &firstByte{r: resp.Body}
	n, err := io.CopyBuffer(body, r, buf)
	if err != nil || resp.StatusCode != http.StatusOK || resp.Request.URL.Path != "/xet-bridge/"+f.sha256 || n != f.size {
		tb.Fatalf("GET %s: %s from %s, %d bytes, %v; want 200 from /xet-bridge/%s and %d bytes",
			url, resp.Status, resp.Request.URL.Path, n, err, f.sha256, f.size)
	}
	return r.at.Sub(start)
}

// firstByte records when a Read first returns data.
type firstByte struct {
	r  io.Reader
	at time.Time
}

func (f *firstByte) Read(p []byte) (int, error) {
	n, err := f.r.Read(p)
	if n > 0 && f.at.IsZero() {
		f.at = time.Now()
	}
	return n, err
}

// TestRoute checks each stage on a 1 MiB file under both revalidations and
// that only stale ones probe the hub, once per resolving stage.
func TestRoute(t *testing.T) {
	buf := make([]byte, drainBufSize)
	for _, v := range revalidations {
		t.Run("revalidate="+v.name, func(t *testing.T) {
			st := routeStackFor(t, v.name, v.interval)
			f := st.fileFor(t, shapes[0], storagetest.ChunkClasses[0])
			gets, cdnGets := st.hub.gets.Load(), st.hub.cdnGets.Load()
			for _, s := range routeStages {
				heads := st.hub.heads.Load()
				h := sha256.New()
				s.do(t, st, f, h, buf)
				if sum := hex.EncodeToString(h.Sum(nil)); s.bodied && sum != f.sha256 {
					t.Errorf("stage=%s: body SHA-256 %s, want %s", s.name, sum, f.sha256)
				}
				// A stale stack probes once per resolve; every stage but bridge resolves.
				want := int64(0)
				if v.name == "stale" && s.name != "bridge" {
					want = 1
				}
				if probes := st.hub.heads.Load() - heads; probes != want {
					t.Errorf("stage=%s: %d hub probes; want %d", s.name, probes, want)
				}
			}
			if refetches := st.hub.gets.Load() - gets + st.hub.cdnGets.Load() - cdnGets; refetches != 0 {
				t.Errorf("%d hub GETs after the ingest; want 0", refetches)
			}
		})
	}
}

// BenchmarkRoute times what a client without xet gets from an in-process
// xetd mirroring a fake hub, through the handler chain of cmd/xetd: rows
// BenchmarkRoute/size=<1MiB|64MiB>/class=<none|bf16>/stage=<stage>/revalidate=<fresh|stale>
// with stages
//   - head: HEAD of the resolve URL, answered 200 with the size;
//   - get302: GET of the resolve URL, answered 302 to /xet-bridge/<sha256>;
//   - bridge: GET of /xet-bridge/<sha256>, drained;
//   - chain: GET of the resolve URL following its redirect, drained.
//
// The file content is that of BenchmarkServe's fixtures of the same size and
// class, ingested by the mirror into its own store, so the bridge rows read
// the mirror's chunking of it. revalidate=fresh never probes the hub, as xetd
// within 5 minutes of a file's last probe; stale probes it synchronously on
// every resolve, as xetd past them, so only stage=bridge, which never
// resolves, goes without. Besides BenchmarkServe's metrics, rows report
// ttfb-ms, the mean time from request to first body byte (to response
// headers on head and get302), and upstream-heads/op and upstream-gets/op,
// the resolve HEADs (probes) and GETs (ingests) the hub answered.
// XET_ROUTE_BENCH_UPSTREAM_DELAY=<duration> delays every resolve answer of the
// hub, adding that much to each probe. The two stacks are built once per
// process and ingest each file on its first row.
func BenchmarkRoute(b *testing.B) {
	for _, s := range shapes[:2] {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range routeClasses {
				b.Run("class="+c.Name, func(b *testing.B) {
					for _, stage := range routeStages {
						b.Run("stage="+stage.name, func(b *testing.B) {
							for _, v := range revalidations {
								b.Run("revalidate="+v.name, func(b *testing.B) {
									st := routeStackFor(b, v.name, v.interval)
									benchmarkStage(b, st, st.fileFor(b, s, c), stage)
								})
							}
						})
					}
				})
			}
		})
	}
}

// benchmarkStage's checking request also warms the connection.
func benchmarkStage(b *testing.B, st *routeStack, f *routeFile, stage routeStage) {
	buf := make([]byte, drainBufSize)
	var body countWriter
	stage.do(b, st, f, &body, buf)
	if stage.bodied {
		b.SetBytes(f.size)
	}
	b.ReportAllocs()
	heads, gets := st.hub.heads.Load(), st.hub.gets.Load()
	var ttfb time.Duration
	n := 0
	w := startWindow(b, st.conns)
	for b.Loop() {
		ttfb += stage.do(b, st, f, &body, buf)
		n++
	}
	w.stop(b, f.size, n)
	b.ReportMetric(ttfb.Seconds()*1e3/float64(n), "ttfb-ms")
	b.ReportMetric(float64(st.hub.heads.Load()-heads)/float64(n), "upstream-heads/op")
	b.ReportMetric(float64(st.hub.gets.Load()-gets)/float64(n), "upstream-gets/op")
}
