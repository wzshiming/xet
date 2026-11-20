package bench_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	hfclient "github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/mirror/spool"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/hf"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/xorb"
)

const (
	benchRepo    = "bench/files"
	unitSize     = 64 << 10 // generator seed granularity
	readyTimeout = 5 * time.Minute

	skipNoHF    = `hf CLI not found on PATH; install with: pip install -U "huggingface_hub[cli,hf_xet]"`
	skipNoHFXet = "hf CLI has no hf_xet; xet downstream path unavailable"
)

var benchCommit = strings.Repeat("b", 40)

type benchSize struct {
	name string
	size int64
}

var benchSizes = []benchSize{{"1MiB", 1 << 20}, {"64MiB", 64 << 20}, {"256MiB", 256 << 20}, {"1GiB", 1 << 30}}

// benchClass pairs a generator with the compression the encoder must pick for every chunk it yields.
type benchClass struct {
	name string
	ct   xorb.CompressionType
	gen  func(src *rand.ChaCha8, p []byte)
}

var benchClasses = []benchClass{
	{name: "none", ct: xorb.CompressionNone, gen: genNone},
	{name: "lz4", ct: xorb.CompressionLZ4, gen: genLZ4},
}

// row is one cell of the matrix: a client downloading from a server.
type row struct{ server, client string }

var (
	servers = []string{"plain", "assembled", "xet", "xet-split"}
	clients = []string{"go-http", "go-xet", "hf-http", "hf-xet"}
)

var rows = func() []row {
	var rs []row
	for _, s := range servers {
		for _, c := range clients {
			rs = append(rs, row{s, c})
		}
	}
	return rs
}()

func (r row) name() string { return "server=" + r.server + "/client=" + r.client }

// unitSource seeds a stream unique to one unit of one class and size, so no two files share chunks.
func unitSource(class string, size, unit int64) *rand.ChaCha8 {
	var seed [32]byte
	copy(seed[:16], class)
	binary.LittleEndian.PutUint64(seed[16:], uint64(size))
	binary.LittleEndian.PutUint64(seed[24:], uint64(unit))
	return rand.NewChaCha8(seed)
}

func genNone(src *rand.ChaCha8, p []byte) {
	_, _ = src.Read(p)
}

var loremWords = strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")

// genLZ4 writes random words, whose repeats byte grouping would split.
func genLZ4(src *rand.ChaCha8, p []byte) {
	r := rand.New(src)
	for i := 0; i < len(p); {
		i += copy(p[i:], loremWords[r.IntN(len(loremWords))])
		if i < len(p) {
			p[i] = ' '
			i++
		}
	}
}

// upstreamFile is a fixture the fake upstream serves, with its request counters.
type upstreamFile struct {
	name, path string
	size       int64
	sum        [32]byte
	digest     string
	probes     atomic.Int64 // HEAD requests on its resolve paths
	gets       atomic.Int64 // GET requests on its /cdn path
}

func (f *upstreamFile) counts() [2]int64 { return [2]int64{f.probes.Load(), f.gets.Load()} }

// fakeUpstream is a plain hub: resolve answers metadata and redirects to /cdn, which serves the bytes.
type fakeUpstream struct {
	mu    sync.Mutex
	files map[string]*upstreamFile
}

func (u *fakeUpstream) add(f *upstreamFile) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[f.name] = f
}

func (u *fakeUpstream) file(name string) *upstreamFile {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.files[name]
}

func (u *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if name, ok := strings.CutPrefix(r.URL.Path, "/cdn/"); ok {
		if f := u.file(name); f != nil {
			f.serveCDN(w, r)
			return
		}
	} else if rest, ok := strings.CutPrefix(r.URL.Path, "/"+benchRepo+"/resolve/"); ok {
		rev, name, _ := strings.Cut(rest, "/")
		if f := u.file(name); f != nil && (rev == "main" || rev == benchCommit) {
			f.serveResolve(w, r)
			return
		}
	}
	http.NotFound(w, r)
}

func (f *upstreamFile) serveResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		f.probes.Add(1)
	}
	w.Header().Set("ETag", `"`+f.digest+`"`)
	w.Header().Set("X-Linked-Etag", `"`+f.digest+`"`)
	w.Header().Set("X-Linked-Size", strconv.FormatInt(f.size, 10))
	w.Header().Set("X-Repo-Commit", benchCommit)
	http.Redirect(w, r, "/cdn/"+f.name, http.StatusFound)
}

func (f *upstreamFile) serveCDN(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		f.gets.Add(1)
	}
	data, err := os.Open(f.path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer data.Close()
	// huggingface_hub takes the commit and etag of a redirected resolve from this final hop.
	w.Header().Set("ETag", `"`+f.digest+`"`)
	w.Header().Set("X-Repo-Commit", benchCommit)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, data)
}

// harness is the topology every row runs on: the fake upstream and the three mirror stacks in front of it.
type harness struct {
	dir                      string
	hub                      *fakeUpstream
	upstreamURL              string
	xet, assembled, xetSplit *mirrorStack
	hfBin                    string // "" when hf is not on PATH
	hfXet                    bool
}

func newHarness(tb testing.TB) *harness {
	tb.Helper()
	h := &harness{dir: tb.TempDir(), hub: &fakeUpstream{files: map[string]*upstreamFile{}}}
	upstream := httptest.NewServer(h.hub)
	tb.Cleanup(upstream.Close)
	h.upstreamURL = upstream.URL
	h.xet = newMirrorStack(tb, upstream.URL, filepath.Join(h.dir, "xet"), false)
	h.assembled = newMirrorStack(tb, upstream.URL, filepath.Join(h.dir, "assembled"), true)
	h.xetSplit = newMirrorStack(tb, upstream.URL, filepath.Join(h.dir, "xet-split"), false, server.WithTermSplit(16, 1<<20))
	h.hfBin, h.hfXet = detectHF(tb, upstream.URL, filepath.Join(h.dir, "hf-env"))
	return h
}

// mirrorStack is one xet mirror in front of the fake upstream, counting the requests that show who assembled a download.
type mirrorStack struct {
	url      string
	stor     *local.Storage
	stripXet bool         // its resolve answers lose their xet headers
	bridge   atomic.Int64 // GET /xet-bridge/*: the server assembles the file
	recon    atomic.Int64 // GET reconstruction queries: the client assembles it
}

func (s *mirrorStack) counts() [2]int64 { return [2]int64{s.bridge.Load(), s.recon.Load()} }

// newMirrorStack wires test/e2e's newMirrorServer composition under dir, never revalidating against the upstream.
func newMirrorStack(tb testing.TB, upstreamURL, dir string, stripXet bool, opts ...server.Option) *mirrorStack {
	tb.Helper()
	s := &mirrorStack{stripXet: stripXet}
	var inner atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.count(r)
		if stripXet && (r.Method == http.MethodGet || r.Method == http.MethodHead) && strings.HasPrefix(r.URL.Path, "/"+benchRepo+"/resolve/") {
			w = &xetStripper{ResponseWriter: w}
		}
		inner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	tb.Cleanup(srv.Close)
	s.url = srv.URL

	storageDir := filepath.Join(dir, "storage")
	stor, err := local.NewStorage(
		local.WithBasePath(storageDir),
		local.WithBaseURL(srv.URL),
	)
	if err != nil {
		tb.Fatal(err)
	}
	s.stor = stor
	queue, err := spool.NewSpool(filepath.Join(storageDir, "spool"), stor)
	if err != nil {
		tb.Fatal(err)
	}
	issuer, err := auth.NewIssuer(nil, 15*time.Minute, time.Now)
	if err != nil {
		tb.Fatal(err)
	}
	upstreamProvider := client.StaticUpstreamProvider(upstreamURL, "")
	m, err := mirror.NewMirror(
		mirror.WithStorage(stor),
		mirror.WithCacheDir(filepath.Join(dir, "cache")),
		mirror.WithSpool(queue),
		mirror.WithRevalidateInterval(-1),
	)
	if err != nil {
		tb.Fatal(err)
	}
	inner.Store(http.Handler(server.NewHandler(append([]server.Option{
		server.WithStorage(stor),
		server.WithAuthorizer(issuer),
		server.WithNext(hf.NewHandler(
			hf.WithMirror(m),
			hf.WithUpstreamProvider(upstreamProvider),
			hf.WithExternalURL(srv.URL),
			hf.WithMinter(hf.MinterFunc(func(r *http.Request, req hf.TokenRequest) (string, int64, error) {
				if req.Permission != auth.Read {
					return "", 0, hf.ErrNotHandled
				}
				return issuer.Sign(auth.Grant{Permission: auth.Read, File: req.File})
			})),
			hf.WithNext(hf.NewUpstreamProxy(upstreamProvider)),
		)),
	}, opts...)...)))
	return s
}

// count tallies the requests that show which side assembles a download.
func (s *mirrorStack) count(r *http.Request) {
	if r.Method != http.MethodGet {
		return
	}
	switch p := r.URL.Path; {
	case strings.HasPrefix(p, "/xet-bridge/"):
		s.bridge.Add(1)
	case strings.HasPrefix(p, "/v1/reconstructions/"), strings.HasPrefix(p, "/v2/reconstructions/"), p == "/reconstructions":
		s.recon.Add(1)
	}
}

// xetStripper deletes the xet headers right before the wrapped writer sends the header block.
type xetStripper struct {
	http.ResponseWriter
	stripped bool
}

func (w *xetStripper) strip() {
	if !w.stripped {
		w.stripped = true
		w.Header().Del("Link")
		w.Header().Del("X-Xet-Hash")
	}
}

func (w *xetStripper) WriteHeader(code int) {
	w.strip()
	w.ResponseWriter.WriteHeader(code)
}

func (w *xetStripper) Write(p []byte) (int, error) {
	w.strip()
	return w.ResponseWriter.Write(p)
}

func (w *xetStripper) Flush() {
	w.strip()
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *xetStripper) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// detectHF finds the hf CLI and whether its Python environment ships hf_xet, logging the versions in use.
func detectHF(tb testing.TB, endpoint, home string) (string, bool) {
	tb.Helper()
	bin, err := exec.LookPath("hf")
	if err != nil {
		return "", false
	}
	ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "env")
	cmd.Env = hfEnv(endpoint, home, true)
	out, err := cmd.Output()
	if err != nil {
		tb.Logf("%s env: %v", bin, err)
		return bin, false
	}
	hasXet := false
	var versions []string
	for line := range strings.Lines(string(out)) {
		key, value, _ := strings.Cut(line, ":")
		if strings.Contains(key, "hf_xet") {
			hasXet = !strings.Contains(value, "not installed") && !strings.Contains(value, "N/A")
		} else if !strings.Contains(key, "huggingface_hub version") && !strings.Contains(key, "Python version") {
			continue
		}
		versions = append(versions, strings.TrimPrefix(strings.TrimSpace(line), "- "))
	}
	tb.Logf("%s: %s", bin, strings.Join(versions, ", "))
	return bin, hasXet
}

// hfEnv pins the CLI to endpoint with an isolated home and no proxy.
func hfEnv(endpoint, home string, xet bool) []string {
	var env []string
	for _, kv := range os.Environ() {
		name, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(name) {
		case "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY":
			continue
		}
		if strings.HasPrefix(name, "HF_") || strings.HasPrefix(name, "HUGGING") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env,
		"HF_ENDPOINT="+endpoint,
		"HF_HOME="+home,
		"HF_XET_CACHE="+filepath.Join(home, "xet"),
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_HUB_DISABLE_PROGRESS_BARS=1",
		// A fresh HF_HOME would otherwise query the package registry on every run.
		"HF_HUB_DISABLE_UPDATE_CHECK=1",
		"HF_HUB_ETAG_TIMEOUT=60",
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	)
	if !xet {
		env = append(env, "HF_HUB_DISABLE_XET=1")
	}
	return env
}

func (h *harness) skipUnavailable(tb testing.TB, r row) {
	tb.Helper()
	if strings.HasPrefix(r.client, "hf-") && h.hfBin == "" {
		tb.Skip(skipNoHF)
	}
	if r.client == "hf-xet" && !h.hfXet {
		tb.Skip(skipNoHFXet)
	}
}

// fixture writes the class's file of size s for the upstream, then ingests it through every stack.
func (h *harness) fixture(tb testing.TB, c benchClass, s benchSize) *upstreamFile {
	tb.Helper()
	dir := filepath.Join(h.dir, "upstream")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatal(err)
	}
	f := &upstreamFile{name: c.name + "-" + s.name + ".bin", size: s.size}
	f.path = filepath.Join(dir, f.name)
	out, err := os.Create(f.path)
	if err != nil {
		tb.Fatal(err)
	}
	defer out.Close()
	sum := sha256.New()
	w := io.MultiWriter(out, sum)
	buf := make([]byte, unitSize)
	for unit := range s.size / unitSize {
		c.gen(unitSource(c.name, s.size, unit), buf)
		if _, err := w.Write(buf); err != nil {
			tb.Fatal(err)
		}
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}
	f.sum = [32]byte(sum.Sum(nil))
	f.digest = hex.EncodeToString(f.sum[:])
	h.hub.add(f)
	for _, stack := range []*mirrorStack{h.xet, h.assembled, h.xetSplit} {
		stack.ingest(tb, f)
		assertXorbClass(tb, stack.stor, f, c)
	}
	return f
}

var noRedirect = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func resolvePath(name string) string { return "/" + benchRepo + "/resolve/main/" + name }

// ingest pulls f through the stack once, waits for the ready redirect, then checks the ready metadata and xet headers.
func (s *mirrorStack) ingest(tb testing.TB, f *upstreamFile) {
	tb.Helper()
	resolveURL := s.url + resolvePath(f.name)
	resp, err := http.Get(resolveURL)
	if err != nil {
		tb.Fatal(err)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK {
		tb.Fatalf("ingest GET %s: %s, %v", resolveURL, resp.Status, err)
	}

	deadline := time.Now().Add(readyTimeout + time.Duration(f.size>>20)*time.Second)
	for {
		poll, err := noRedirect.Get(resolveURL)
		if err != nil {
			tb.Fatal(err)
		}
		poll.Body.Close() // an in-flight answer streams the whole spool; only the status matters
		if poll.StatusCode == http.StatusFound {
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("mirror never published %s, last status %s", resolveURL, poll.Status)
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err = noRedirect.Head(resolveURL)
	if err != nil {
		tb.Fatal(err)
	}
	resp.Body.Close()
	xetHash, links := resp.Header.Get("X-Xet-Hash"), resp.Header.Values("Link")
	etag, size := strings.Trim(resp.Header.Get("X-Linked-Etag"), `"`), resp.Header.Get("X-Linked-Size")
	xetOK, wantXet := xetHash != "" && hfclient.ParseLinkHeaders(links)["xet-reconstruction-info"] != "", "a xet hash and reconstruction link"
	if s.stripXet {
		xetOK, wantXet = xetHash == "" && len(links) == 0, "no xet headers"
	}
	if resp.StatusCode != http.StatusOK || !xetOK || etag != f.digest || size != strconv.FormatInt(f.size, 10) {
		tb.Fatalf("ready HEAD %s: %s, X-Xet-Hash %q, Link %q, X-Linked-Etag %q, X-Linked-Size %q; want 200, %s, %s, %d",
			resolveURL, resp.Status, xetHash, links, etag, size, wantXet, f.digest, f.size)
	}
}

// assertXorbClass fails unless every chunk of every xorb holding f carries the class's compression.
func assertXorbClass(tb testing.TB, stor storage.Storage, f *upstreamFile, c benchClass) {
	tb.Helper()
	ctx := tb.Context()
	fileHash, err := stor.GetFileHashBySHA256(ctx, f.sum)
	if err != nil {
		tb.Fatal(err)
	}
	sh, err := stor.GetShard(ctx, fileHash)
	if err != nil {
		tb.Fatal(err)
	}
	file := storage.FindFileBySHA256(sh, f.sum)
	if file == nil {
		tb.Fatalf("%s: shard of %s lacks its SHA-256", f.name, fileHash)
	}
	seen := map[xet.XorbHash]bool{}
	for _, e := range file.Entries {
		if seen[e.CASHash] {
			continue
		}
		seen[e.CASHash] = true
		ends, err := stor.GetXorbChunkOffsets(ctx, e.CASHash)
		if err != nil {
			tb.Fatal(err)
		}
		rc, err := stor.GetXorbReadSeekCloser(ctx, e.CASHash)
		if err != nil {
			tb.Fatal(err)
		}
		var hdr [xorb.ChunkHeaderSize]byte
		var start uint64
		for i, end := range ends {
			if _, err := rc.Seek(int64(start), io.SeekStart); err != nil {
				tb.Fatal(err)
			}
			if _, err := io.ReadFull(rc, hdr[:]); err != nil {
				tb.Fatalf("xorb %s chunk %d header: %v", e.CASHash, i, err)
			}
			ch, err := xorb.ParseChunkHeader(hdr[:])
			if err != nil {
				tb.Fatalf("xorb %s chunk %d: %v", e.CASHash, i, err)
			}
			if ch.Compression != c.ct {
				tb.Fatalf("xorb %s chunk %d: compression %d, want %d for class %s", e.CASHash, i, ch.Compression, c.ct, c.name)
			}
			start = end
		}
		rc.Close()
	}
}

// outDir creates the fresh output dir of a row's nth download of f.
func (h *harness) outDir(tb testing.TB, r row, f *upstreamFile, n int) string {
	tb.Helper()
	dir := filepath.Join(h.dir, "out", strings.TrimSuffix(f.name, ".bin")+"-"+r.server+"-"+r.client, strconv.Itoa(n))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatal(err)
	}
	return dir
}

// download fetches f from the row's server with its client into dir, reporting whether go-xet fell back to plain HTTP.
func (h *harness) download(tb testing.TB, r row, f *upstreamFile, dir string) bool {
	tb.Helper()
	endpoint := h.upstreamURL
	switch r.server {
	case "assembled":
		endpoint = h.assembled.url
	case "xet":
		endpoint = h.xet.url
	case "xet-split":
		endpoint = h.xetSplit.url
	}
	switch r.client {
	case "go-http":
		hc := coldClient()
		defer hc.CloseIdleConnections()
		fetch(tb, hc, endpoint+resolvePath(f.name), filepath.Join(dir, f.name))
	case "go-xet":
		return fetchXet(tb, endpoint, f.name, dir)
	default:
		h.runHF(tb, endpoint, filepath.Join(dir, ".hf"), r.client == "hf-xet", "download", benchRepo, f.name, "--local-dir", dir)
	}
	return false
}

// coldClient has a fresh transport, so its first request opens a fresh connection.
func coldClient() *http.Client {
	return &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
}

// fetch downloads url to path with hc, following redirects.
func fetch(tb testing.TB, hc *http.Client, url, path string) {
	tb.Helper()
	resp, err := hc.Get(url)
	if err != nil {
		tb.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("GET %s: %s", url, resp.Status)
	}
	out, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		out.Close()
		tb.Fatal(err)
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}
}

// fetchXet reports whether it fell back to plain HTTP.
func fetchXet(tb testing.TB, endpoint, name, dir string) bool {
	tb.Helper()
	ctx := tb.Context()
	hc := coldClient()
	defer hc.CloseIdleConnections()
	url, path := endpoint+resolvePath(name), filepath.Join(dir, name)
	// client/hf never falls back itself: its Resolve fails on an answer without the link.
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		tb.Fatal(err)
	}
	resp, err := (&http.Client{Transport: hc.Transport, CheckRedirect: noRedirect.CheckRedirect}).Do(req)
	if err != nil {
		tb.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		tb.Fatalf("HEAD %s: %s", url, resp.Status)
	}
	if hfclient.ParseLinkHeaders(resp.Header.Values("Link"))["xet-reconstruction-info"] == "" {
		fetch(tb, hc, url, path)
		return true
	}
	c, err := hfclient.NewClient(hfclient.Repo{Endpoint: endpoint, RepoID: benchRepo}, hfclient.WithHTTPClient(hc),
		hfclient.WithClientOptions(client.WithCache(client.NewCache(filepath.Join(dir, "xet-cache"), 0, 0))))
	if err != nil {
		tb.Fatal(err)
	}
	res, err := c.Resolve(ctx, name)
	if err != nil {
		tb.Fatal(err)
	}
	out, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	if err := c.DownloadFile(ctx, res.Hash, out); err != nil {
		out.Close()
		tb.Fatal(err)
	}
	if err := out.Close(); err != nil {
		tb.Fatal(err)
	}
	return false
}

func (h *harness) runHF(tb testing.TB, endpoint, home string, xet bool, args ...string) {
	tb.Helper()
	ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, h.hfBin, args...)
	cmd.Env = hfEnv(endpoint, home, xet)
	if out, err := cmd.CombinedOutput(); err != nil {
		tb.Fatalf("hf %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func checkSize(tb testing.TB, path string, size int64) {
	tb.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		tb.Fatal(err)
	}
	if fi.Size() != size {
		tb.Fatalf("%s: %d bytes, want %d", path, fi.Size(), size)
	}
}

// counts snapshots each server's request counters.
func (h *harness) counts(f *upstreamFile) map[string][2]int64 {
	return map[string][2]int64{"plain": f.counts(), "assembled": h.assembled.counts(), "xet": h.xet.counts(), "xet-split": h.xetSplit.counts()}
}

// finishRow checks the bytes of the last of n downloads, the go-xet fallbacks and the path the downloads took.
func (h *harness) finishRow(tb testing.TB, r row, f *upstreamFile, dir string, before map[string][2]int64, n, fellBack int) {
	tb.Helper()
	if got := sha256File(tb, filepath.Join(dir, f.name)); got != f.digest {
		tb.Fatalf("%s: sha256 %s, want %s", f.name, got, f.digest)
	}
	if r.client == "go-xet" {
		want := n
		if r.server == "xet" || r.server == "xet-split" {
			want = 0
		}
		if fellBack != want {
			tb.Fatalf("go-xet fell back to plain HTTP on %d of %d downloads from server=%s, want %d", fellBack, n, r.server, want)
		}
	}
	after := h.counts(f)
	for _, s := range servers {
		d := [2]int64{after[s][0] - before[s][0], after[s][1] - before[s][1]}
		var ok bool
		switch {
		case s != r.server:
			ok = d == [2]int64{}
		case s == "plain":
			ok = true
		case (s == "xet" || s == "xet-split") && (r.client == "go-xet" || r.client == "hf-xet"):
			ok = d[0] == 0 && d[1] > 0
		default:
			ok = d[0] > 0 && d[1] == 0
		}
		if !ok {
			what := s + " stack [bridge GETs, reconstruction queries]"
			if s == "plain" {
				what = "upstream [resolve HEADs, cdn GETs]"
			}
			tb.Fatalf("%s went from %v to %v during a %s row", what, before[s], after[s], r.name())
		}
	}
	if err := os.RemoveAll(dir); err != nil {
		tb.Fatal(err)
	}
}

func sha256File(tb testing.TB, path string) string {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		tb.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
