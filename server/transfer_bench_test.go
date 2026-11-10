package server_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/internal/benchprof"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

// transfer moves a fixture's file once per run between a client and a server
// of its own on the in-memory network; before readies a run and after removes
// what the run left, checking it first when asked.
type transfer interface {
	before(tb testing.TB)
	run(tb testing.TB)
	after(tb testing.TB, check bool)
}

// transferPath makes a row's transfer of fx by one path.
type transferPath struct {
	name string
	new  func(tb testing.TB, fx *fixture) transfer
}

// transferOps are each op's paths: the whole file over plain HTTP, or the xet
// client protocol.
var transferOps = []struct {
	name  string
	paths []transferPath
}{
	{"upload", []transferPath{{"plain", newPlainUpload}, {"xet", newXetUpload}}},
	{"download", []transferPath{{"plain", newPlainDownload}, {"xet", newXetDownload}}},
}

// plainUpload PUTs the file from disk to a handler that copies the body into
// a file, without fsync.
type plainUpload struct {
	fx     *fixture
	client *http.Client
	url    string
	stored string // the handler's copy
	body   *os.File
}

func newPlainUpload(tb testing.TB, fx *fixture) transfer {
	dir := tb.TempDir()
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /upload/{name}", func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Create(filepath.Join(dir, r.PathValue("name")))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, err = io.Copy(f, r.Body)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
	})
	srv := serve(tb, mux)
	return &plainUpload{fx: fx, client: srv.Client(), url: srv.URL + "/upload/" + fx.sha256, stored: filepath.Join(dir, fx.sha256)}
}

func (u *plainUpload) before(tb testing.TB) {
	f, err := os.Open(u.fx.plain)
	if err != nil {
		tb.Fatal(err)
	}
	u.body = f
}

func (u *plainUpload) run(tb testing.TB) {
	req, err := http.NewRequestWithContext(tb.Context(), http.MethodPut, u.url, u.body)
	if err != nil {
		tb.Fatal(err)
	}
	req.ContentLength = u.fx.size
	resp, err := u.client.Do(req) // closes u.body
	if err != nil {
		tb.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		tb.Fatalf("PUT %s: %s", u.url, resp.Status)
	}
}

func (u *plainUpload) after(tb testing.TB, check bool) {
	if check {
		checkFile(tb, u.stored, u.fx)
	}
	if err := os.Remove(u.stored); err != nil {
		tb.Fatal(err)
	}
}

// xetUpload has the xet client chunk, hash, compress and upload the file into
// local storage made for the run, as are the client and its cache, so that no
// run dedups against an earlier one; the row's server swaps to the run's
// handler, keeping its connections.
type xetUpload struct {
	fx    *fixture
	srv   *httptest.Server
	h     atomic.Pointer[server.Handler]
	posts atomic.Int64 // POST /v1/xorbs/ requests of the run
	root  string
	src   *os.File
	dir   string // the run's storage and cache
	st    *local.Storage
	c     *client.Client
	got   xet.FileHash
}

func newXetUpload(tb testing.TB, fx *fixture) transfer {
	src, err := os.Open(fx.plain)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { src.Close() })
	u := &xetUpload{fx: fx, root: tb.TempDir(), src: src}
	u.srv = serve(tb, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			u.posts.Add(1)
		}
		u.h.Load().ServeHTTP(w, r)
	}))
	return u
}

func (u *xetUpload) before(tb testing.TB) {
	u.dir = runDir(tb, u.root)
	st, err := local.NewStorage(local.WithBasePath(filepath.Join(u.dir, "store")))
	if err != nil {
		tb.Fatal(err)
	}
	u.st = st
	u.h.Store(server.NewHandler(server.WithStorage(st)))
	u.c = newXetClient(tb, u.srv, filepath.Join(u.dir, "cache"))
	if _, err := u.src.Seek(0, io.SeekStart); err != nil {
		tb.Fatal(err)
	}
	u.posts.Store(0)
}

func (u *xetUpload) run(tb testing.TB) {
	h, err := u.c.UploadFile(tb.Context(), u.src)
	if err != nil {
		tb.Fatal(err)
	}
	u.got = h
}

func (u *xetUpload) after(tb testing.TB, check bool) {
	if n := u.posts.Load(); n < 1 {
		tb.Fatalf("upload POSTed %d xorbs, want every run to store its xorbs anew", n)
	}
	if check {
		// The client chunks by content, not as the fixture does, so the hashes differ: read the upload back instead.
		back := &xetDownload{fx: u.fx, root: u.dir, srv: u.srv, hash: u.got}
		back.before(tb)
		back.run(tb)
		back.after(tb, true)
	}
	// The storage keeps the xorbs it verified mapped and has no Close; deleting them unmaps them.
	ctx := tb.Context()
	if err := u.st.WalkXorbs(ctx, func(hash string, _ int64, _ time.Time) error {
		h, err := xet.ParseXorbHash(hash)
		if err != nil {
			return err
		}
		return u.st.DeleteXorb(ctx, h)
	}); err != nil {
		tb.Fatal(err)
	}
	if err := os.RemoveAll(u.dir); err != nil {
		tb.Fatal(err)
	}
}

// sink is the file a download writes, in a directory made for the run.
type sink struct {
	fx   *fixture
	root string
	dir  string
	f    *os.File
}

func (s *sink) before(tb testing.TB) {
	s.dir = runDir(tb, s.root)
	f, err := os.Create(filepath.Join(s.dir, "file"))
	if err != nil {
		tb.Fatal(err)
	}
	s.f = f
}

func (s *sink) after(tb testing.TB, check bool) {
	if err := s.f.Close(); err != nil {
		tb.Fatal(err)
	}
	if check {
		checkFile(tb, s.f.Name(), s.fx)
	}
	if err := os.RemoveAll(s.dir); err != nil {
		tb.Fatal(err)
	}
}

// plainDownload GETs the file as BenchmarkServe's plain path serves it and
// copies the body into a file.
type plainDownload struct {
	sink
	client *http.Client
	url    string
}

func newPlainDownload(tb testing.TB, fx *fixture) transfer {
	srv := serve(tb, fx.handler)
	return &plainDownload{fx: fx, root: tb.TempDir(), client: srv.Client(), url: srv.URL + "/plain/" + fx.sha256}
}

func (d *plainDownload) run(tb testing.TB) {
	resp, err := d.client.Get(d.url)
	if err != nil {
		tb.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		tb.Fatalf("GET %s: %s", d.url, resp.Status)
	}
	if n, err := io.Copy(d.f, resp.Body); err != nil || n != d.fx.size {
		tb.Fatalf("GET %s: copied %d bytes, %v; want %d", d.url, n, err, d.fx.size)
	}
}

// xetDownload has a xet client made for the run, whose chunk cache is empty,
// reconstruct the file by its xet hash into a file.
type xetDownload struct {
	sink
	srv  *httptest.Server
	hash xet.FileHash
	c    *client.Client
}

func (d *xetDownload) before(tb testing.TB) {
	d.sink.before(tb)
	d.c = newXetClient(tb, d.srv, filepath.Join(d.dir, "cache"))
}

func (d *xetDownload) run(tb testing.TB) {
	if err := d.c.DownloadFile(tb.Context(), d.hash, d.f); err != nil {
		tb.Fatal(err)
	}
}

// fetchingXetDownload is download/xet's xetDownload, whose server counts the
// run's xorb GETs: a client finding the chunks cached by an earlier run would
// make none.
type fetchingXetDownload struct {
	xetDownload
	gets atomic.Int64 // GET /v1/xorbs/ requests of the run
}

func newXetDownload(tb testing.TB, fx *fixture) transfer {
	d := &fetchingXetDownload{fx: fx, root: tb.TempDir(), hash: fx.fileHash}
	d.srv = serve(tb, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
			d.gets.Add(1)
		}
		fx.handler.ServeHTTP(w, r)
	}))
	return d
}

func (d *fetchingXetDownload) before(tb testing.TB) {
	d.xetDownload.before(tb)
	d.gets.Store(0)
}

func (d *fetchingXetDownload) after(tb testing.TB, check bool) {
	if n := d.gets.Load(); n < 1 {
		tb.Fatalf("download made %d xorb GETs, want every run to fetch its chunks anew", n)
	}
	d.xetDownload.after(tb, check)
}

// runDir makes a directory under root for one run's files.
func runDir(tb testing.TB, root string) string {
	tb.Helper()
	dir, err := os.MkdirTemp(root, "run-*")
	if err != nil {
		tb.Fatal(err)
	}
	return dir
}

// newXetClient returns a xet client of srv on its in-memory network, caching in dir.
func newXetClient(tb testing.TB, srv *httptest.Server, dir string) *client.Client {
	tb.Helper()
	c, err := client.NewClient(
		client.WithHTTPClient(srv.Client()),
		client.WithCache(client.NewCache(dir, 0, 0)),
		client.WithUpstreamProvider(client.StaticUpstreamProvider(srv.URL, "")),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return c
}

// checkFile fails tb unless the file at path holds fx's bytes.
func checkFile(tb testing.TB, path string, fx *fixture) {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		tb.Fatal(err)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); n != fx.size || sum != fx.sha256 {
		tb.Fatalf("%s holds %d bytes with SHA-256 %s, want %d with %s", path, n, sum, fx.size, fx.sha256)
	}
}

// sentReader counts the bytes of every Read but the latest, which the
// transport may still be writing: those it has sent.
type sentReader struct {
	r    io.Reader
	last int64
	sent atomic.Int64
}

func (s *sentReader) Read(p []byte) (int, error) {
	s.sent.Add(s.last)
	n, err := s.r.Read(p)
	s.last = int64(n)
	return n, err
}

// TestTransfer checks every op and path on each class's 1 MiB file twice in a
// row, as a benchmark loop runs them: a second run must redo all the work of
// the first, not find its result in place.
func TestTransfer(t *testing.T) {
	for _, c := range storagetest.ChunkClasses {
		fx := fixtureFor(t, shapes[0], c) // here, as fixtureFor must not run in parallel
		t.Run("class="+c.Name, func(t *testing.T) {
			t.Parallel()
			for _, op := range transferOps {
				t.Run("op="+op.name, func(t *testing.T) {
					for _, p := range op.paths {
						t.Run("path="+p.name, func(t *testing.T) {
							tr := p.new(t, fx)
							for range 2 {
								tr.before(t)
								tr.run(t)
								tr.after(t, true)
							}
						})
					}
				})
			}
		})
	}
}

// TestTransferBlocksUploadsPastReadBuffer checks that a server connection of
// serve holds at most readBufSize unread, blocking a client that sends a
// larger body until the handler reads it.
func TestTransferBlocksUploadsPastReadBuffer(t *testing.T) {
	const size, slack = 4 << 20, 32 << 10 // slack: io.Copy's buffer
	unblock := make(chan struct{})
	var received atomic.Int64
	srv := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			return
		}
		<-unblock
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			t.Error(err)
		}
		received.Store(n)
	}))
	release := sync.OnceFunc(func() { close(unblock) })
	t.Cleanup(release) // runs before serve's, whose Close waits for the handler
	hc := srv.Client()
	// The server bounds a connection once it accepts it, which this GET ensures before the upload reuses it.
	resp, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	body := &sentReader{r: bytes.NewReader(make([]byte, size))}
	wrote := make(chan error, 1)
	trace := &httptrace.ClientTrace{WroteRequest: func(info httptrace.WroteRequestInfo) { wrote <- info.Err }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(t.Context(), trace), http.MethodPut, srv.URL, body)
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = size
	done := make(chan error, 1)
	go func() {
		resp, err := hc.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-wrote:
		t.Fatalf("client finished the body (%v) with %d bytes sent while it went unread, want it blocked near %d", err, body.sent.Load(), readBufSize)
	case <-time.After(300 * time.Millisecond): // an unbounded connection takes the whole body far sooner
	}
	if n := body.sent.Load(); n < readBufSize-slack || n > readBufSize+slack {
		t.Errorf("client sent %d bytes while the body went unread, want %d ± %d", n, readBufSize, slack)
	}
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := received.Load(); n != size {
		t.Errorf("handler read %d bytes once unblocked, want %d", n, size)
	}
}

// BenchmarkTransfer times moving each fixture's file whole between a client
// and a server of the row's own, per op, over plain HTTP or the xet client
// protocol. upload/plain PUTs the file from disk to a handler copying the body
// into a file; upload/xet has the client chunk, hash, compress and upload it
// into new local storage. download/plain GETs the file as BenchmarkServe's
// plain path serves it into a file; download/xet has the client reconstruct it
// from the fixture's storage into a file. A run's files, storage, client and
// cache are made and removed with the timer stopped, so no xet run dedups
// against an earlier one or finds its chunks cached; allocs include the
// in-process server. Compare the paths with
//
//	go test ./server -run '^$' -bench Transfer -benchtime 3x -count 3 >transfer.txt
//	benchstat -row /op,/size,/class -col /path transfer.txt
func BenchmarkTransfer(b *testing.B) {
	for _, s := range shapes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range storagetest.ChunkClasses {
				b.Run("class="+c.Name, func(b *testing.B) {
					fx := fixtureFor(b, s, c)
					for _, op := range transferOps {
						b.Run("op="+op.name, func(b *testing.B) {
							for _, p := range op.paths {
								b.Run("path="+p.name, func(b *testing.B) { benchmarkTransfer(b, fx, p.new(b, fx)) })
							}
						})
					}
				})
			}
		})
	}
}

// benchmarkTransfer times tr's runs once a checked one passes.
func benchmarkTransfer(b *testing.B, fx *fixture, tr transfer) {
	tr.before(b)
	tr.run(b)
	tr.after(b, true)
	b.SetBytes(fx.size)
	b.ReportAllocs()
	stop := benchprof.Start(b)
	for b.Loop() {
		b.StopTimer()
		tr.before(b)
		b.StartTimer()
		tr.run(b)
		b.StopTimer()
		tr.after(b, false)
		b.StartTimer()
	}
	stop()
}
