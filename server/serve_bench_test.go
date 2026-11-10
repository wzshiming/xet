// Package server_test times one process serving a stored file on the
// in-memory network of httptest.NewTestServer, which leaves only user-space
// work to measure: decoding, copies and net/http's connection layer, no kernel
// sockets. path=bridge is GET /xet-bridge/<sha256>, which serves
// storage.NewReconstructedFile through http.ServeContent; path=plain is
// http.ServeContent of the same bytes from an *os.File, which net/http copies
// through its generic 32 KiB loop as the in-memory conn has no sendfile, so
// plain is the user-space read + copy floor. Each connection end holds at most
// 1 MiB unread, as a socket buffer would (see serve). Files are 64 KiB chunks
// in xorbs of at most xet.MaxXorbSize: 1 MiB = 1 xorb × 16 chunks,
// 64 MiB = 1 × 1024, 256 MiB = 4 × 1024; the 64 MiB row differs from the
// storage bench's 4 × 256 shape. Each (size, class) fixture is built once per
// process, so -count repeats rows only; the full matrix takes about 1.6 GiB of
// temp disk, which TestMain removes. Each row starts a server of its own, whose
// client keeps its connection warm and drains bodies through a 1 MiB buffer
// into a writer without ReadFrom; allocs include the in-process server, and
// XET_BENCH_PROFILE_DIR adds a CPU profile per row (see internal/benchprof).
// Numbers are indicative and, leaving out the kernel, not comparable with TCP
// loopback ones; compare the paths with
//
//	go test ./server -run '^$' -bench Serve -benchtime 3x -count 3 >serve.txt
//	benchstat -row /size,/class -col /path serve.txt
package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/benchprof"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

const (
	chunkSize    = xet.TargetChunkSize
	drainBufSize = 1 << 20
	readBufSize  = 1 << 20 // what each end of a connection holds unread, at socket-buffer scale
)

// shape lays a served file out as xorbs of chunkSize chunks.
type shape struct {
	name                 string
	xorbs, chunksPerXorb int
}

var shapes = []shape{
	{"1MiB", 1, 16},
	{"64MiB", 1, xet.MaxXorbSize / chunkSize},
	{"256MiB", 4, xet.MaxXorbSize / chunkSize},
}

// paths are the routes a fixture serves its file on, by its SHA-256 hex.
var paths = []struct{ name, route string }{
	{"plain", "/plain/"},
	{"bridge", "/xet-bridge/"},
}

// fixture is what a (size, class) cell keeps once its payloads are dropped.
type fixture struct {
	handler  http.Handler // serves both paths, on a server per row
	plain    string       // the file on disk that /plain/ serves
	size     int64
	sha256   string
	fileHash xet.FileHash // the file's xet hash in the storage the handler serves
}

var (
	fixtureRoot string                  // made by the first fixture
	fixtures    = map[string]*fixture{} // by "<size>/<class>"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.RemoveAll(fixtureRoot)
	os.Exit(code)
}

// fixtureFor returns the fixture of s and c, building it on first use.
func fixtureFor(tb testing.TB, s shape, c storagetest.ChunkClass) *fixture {
	tb.Helper()
	key := s.name + "/" + c.Name
	if fixtures[key] == nil {
		fixtures[key] = buildFixture(tb, s, c)
	}
	return fixtures[key]
}

func buildFixture(tb testing.TB, s shape, c storagetest.ChunkClass) *fixture {
	tb.Helper()
	if fixtureRoot == "" {
		root, err := os.MkdirTemp("", "xet-serve-bench-*")
		if err != nil {
			tb.Fatal(err)
		}
		fixtureRoot = root
	}
	dir := filepath.Join(fixtureRoot, s.name, c.Name)
	st, err := local.NewStorage(local.WithBasePath(filepath.Join(dir, "store")))
	if err != nil {
		tb.Fatal(err)
	}
	f := storagetest.PutXorbFile(tb, tb.Context(), st, c.Xorbs(s.xorbs, s.chunksPerXorb, chunkSize))
	storagetest.AssertFileCompression(tb, tb.Context(), st, f, c.Compression)
	plain := filepath.Join(dir, "plain")
	if err := os.WriteFile(plain, f.Content, 0o644); err != nil {
		tb.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/plain/"+f.SHA256Hex, func(w http.ResponseWriter, r *http.Request) {
		file, err := os.Open(plain)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()
		// Set as the bridge does; it spares ServeContent a sniffing Read and Seek.
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, plain, time.Time{}, file)
	})
	mux.Handle("/", server.NewHandler(server.WithStorage(st)))
	return &fixture{handler: mux, plain: plain, size: int64(len(f.Content)), sha256: f.SHA256Hex, fileHash: f.FileHash}
}

// readBufferSizer stands for the in-memory conn, an internal/nettest type tests cannot name.
type readBufferSizer interface{ SetReadBufferSize(size int) }

// serve starts h for tb on the in-memory network, to which srv.Client() sends
// every request. That network buffers without bound, letting a server run up
// to a whole response ahead of a slower client, or a client a whole request
// body ahead of a slower handler; capping what each end of a connection holds
// unread at readBufSize blocks the other end's writes while it is full, as a
// socket buffer does.
func serve(tb testing.TB, h http.Handler) *httptest.Server {
	tb.Helper()
	srv := httptest.NewTestServer(tb, h)
	// Set before srv starts; it runs on each accepted connection before the server reads it.
	srv.Config.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
		if bc, ok := c.(readBufferSizer); ok {
			bc.SetReadBufferSize(readBufSize)
		} else {
			tb.Errorf("server accepted %T, whose read buffer cannot be bounded", c)
			c.Close()
		}
		return ctx
	}
	tr := srv.Client().Transport.(*http.Transport) // starts srv, setting srv.URL
	dial := tr.DialContext
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := dial(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		bc, ok := c.(readBufferSizer)
		if !ok {
			c.Close()
			// An error, as tb.Fatal on this transport goroutine would leave the GET waiting forever.
			return nil, fmt.Errorf("dialed %T, whose read buffer cannot be bounded", c)
		}
		bc.SetReadBufferSize(readBufSize)
		return c, nil
	}
	return srv
}

// checkGet fails tb unless client's GET of url answers 200 with fx's length and
// SHA-256, and returns the answer's header and the connection it came on.
func checkGet(tb testing.TB, client *http.Client, fx *fixture, url string, buf []byte) (http.Header, net.Conn) {
	tb.Helper()
	var conn net.Conn
	trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) { conn = info.Conn }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(tb.Context(), trace), http.MethodGet, url, nil)
	if err != nil {
		tb.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		tb.Fatal(err)
	}
	defer resp.Body.Close()
	h := sha256.New()
	n, err := io.CopyBuffer(h, resp.Body, buf)
	if err != nil {
		tb.Fatal(err)
	}
	if sum := hex.EncodeToString(h.Sum(nil)); resp.StatusCode != http.StatusOK || resp.ContentLength != fx.size || n != fx.size || sum != fx.sha256 {
		tb.Fatalf("GET %s: %s, Content-Length %d, %d bytes with SHA-256 %s; want 200 and %d bytes with %s",
			url, resp.Status, resp.ContentLength, n, sum, fx.size, fx.sha256)
	}
	return resp.Header, conn
}

// countWriter lacks io.Discard's ReadFrom, which would bypass the drain buffer.
type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

// TestServe checks that both paths serve every class's 1 MiB file whole on the
// in-memory network, over connections whose read buffer can be bounded, and
// that each path reaches its own handler: only the bridge sets an ETag.
func TestServe(t *testing.T) {
	buf := make([]byte, drainBufSize)
	for _, c := range storagetest.ChunkClasses {
		t.Run("class="+c.Name, func(t *testing.T) {
			fx := fixtureFor(t, shapes[0], c)
			for _, p := range paths {
				t.Run("path="+p.name, func(t *testing.T) {
					srv := serve(t, fx.handler)
					if srv.Listener != nil {
						t.Errorf("server listens on %s, want the in-memory network", srv.Listener.Addr())
					}
					if srv.URL != "http://example.com" {
						t.Errorf("server URL is %s, want http://example.com", srv.URL)
					}
					hdr, conn := checkGet(t, srv.Client(), fx, srv.URL+p.route+fx.sha256, buf)
					if _, ok := conn.(readBufferSizer); !ok {
						t.Errorf("GET came on a %T, whose read buffer cannot be bounded", conn)
					}
					wantETag := ""
					if p.name == "bridge" {
						wantETag = `"` + fx.sha256 + `"`
					}
					if etag := hdr.Get("ETag"); etag != wantETag {
						t.Errorf("GET %s answered with ETag %q, want %q", p.route, etag, wantETag)
					}
				})
			}
		})
	}
}

// TestServeBlocksWritesPastReadBuffer checks that a client connection of serve
// holds at most readBufSize unread, blocking a handler that writes more until
// the body is read.
func TestServeBlocksWritesPastReadBuffer(t *testing.T) {
	const size, writeSize = 4 << 20, 32 << 10
	var written atomic.Int64 // bytes of the handler's Write calls that returned
	done := make(chan struct{})
	srv := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		p := make([]byte, writeSize)
		for range size / writeSize {
			if _, err := w.Write(p); err != nil {
				return
			}
			written.Add(writeSize)
		}
	}))
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	select {
	case <-done:
		t.Fatalf("handler completed all %d bytes while the body went unread, want it blocked near %d", written.Load(), readBufSize)
	case <-time.After(300 * time.Millisecond): // an unbounded connection takes the whole body far sooner
	}
	if n := written.Load(); n < readBufSize-writeSize || n > readBufSize+writeSize {
		t.Errorf("handler completed %d bytes while the body went unread, want %d ± %d", n, readBufSize, writeSize)
	}
	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil || n != size {
		t.Fatalf("read %d bytes of the body, %v; want %d", n, err, size)
	}
	<-done
	if n := written.Load(); n != size {
		t.Errorf("handler completed %d bytes once the body was read, want %d", n, size)
	}
}

// BenchmarkServe times whole-file GETs by both paths, per size and class.
func BenchmarkServe(b *testing.B) {
	for _, s := range shapes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range storagetest.ChunkClasses {
				b.Run("class="+c.Name, func(b *testing.B) {
					fx := fixtureFor(b, s, c)
					for _, p := range paths {
						b.Run("path="+p.name, func(b *testing.B) { benchmarkGet(b, fx, p.route) })
					}
				})
			}
		})
	}
}

// benchmarkGet times GETs of fx's file by route from a server of the row's
// own; its checking GET also warms the connection.
func benchmarkGet(b *testing.B, fx *fixture, route string) {
	srv := serve(b, fx.handler)
	client := srv.Client()
	url := srv.URL + route + fx.sha256
	buf := make([]byte, drainBufSize)
	checkGet(b, client, fx, url, buf)
	var body countWriter
	b.SetBytes(fx.size)
	b.ReportAllocs()
	stop := benchprof.Start(b)
	for b.Loop() {
		resp, err := client.Get(url)
		if err != nil {
			b.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("GET %s: %s", url, resp.Status)
		}
		body.n = 0
		_, err = io.CopyBuffer(&body, resp.Body, buf)
		resp.Body.Close()
		if err != nil || body.n != fx.size {
			b.Fatalf("GET %s: drained %d bytes, %v; want %d", url, body.n, err, fx.size)
		}
	}
	stop()
}
