// Package server_test times one process serving a stored file over real TCP
// loopback by path=bridge (GET /xet-bridge/<sha256>, which serves
// storage.NewReconstructedFile through http.ServeContent) and by path=plain
// (http.ServeContent of the same bytes from an *os.File, which net sends with
// sendfile). Files are 64 KiB chunks in xorbs of at most xet.MaxXorbSize:
// 1 MiB = 1 xorb × 16 chunks, 64 MiB = 1 × 1024, 256 MiB = 4 × 1024; the
// 64 MiB row differs from the storage bench's 4 × 256 shape. Each (size, class)
// fixture is built once per process, so -count repeats rows only; the full
// matrix takes about 1.6 GiB of temp disk, which TestMain removes. One client
// keeps its connections warm and drains bodies through a 1 MiB buffer into a
// writer without ReadFrom; allocs include the in-process server. Numbers are
// indicative; compare the paths with
//
//	go test ./server -run '^$' -bench Serve -benchtime 3x -count 3 >serve.txt
//	benchstat -row .name -col /path serve.txt
package server_test

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

const (
	chunkSize    = xet.TargetChunkSize
	drainBufSize = 1 << 20
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

// classes are the storage classes plus bf16 model weights.
var classes = append(slices.Clone(storagetest.ChunkClasses), storagetest.ChunkClassBF16)

// paths are the routes a fixture serves its file on, by its SHA-256 hex.
var paths = []struct{ name, route string }{
	{"plain", "/plain/"},
	{"bridge", "/xet-bridge/"},
}

var client = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 4
	return &http.Client{Transport: t}
}()

// fixture is what a (size, class) cell keeps once its payloads are dropped.
type fixture struct {
	srv    *httptest.Server
	size   int64
	sha256 string
}

func (fx *fixture) url(route string) string {
	return fx.srv.URL + route + fx.sha256
}

var (
	fixtureRoot string                  // made by the first fixture
	fixtures    = map[string]*fixture{} // by "<size>/<class>"
)

func TestMain(m *testing.M) {
	code := m.Run()
	for _, fx := range fixtures {
		fx.srv.Close()
	}
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
	if c.Name == storagetest.ChunkClassBF16.Name {
		storagetest.AssertFileStoredRatio(tb, tb.Context(), st, f, 0.85, 0.92)
	}
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
	return &fixture{srv: httptest.NewServer(mux), size: int64(len(f.Content)), sha256: f.SHA256Hex}
}

// checkGet fails tb unless url answers 200 with fx's length and SHA-256.
func checkGet(tb testing.TB, fx *fixture, url string, buf []byte) {
	tb.Helper()
	resp, err := client.Get(url)
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
}

// countWriter lacks io.Discard's ReadFrom, which would bypass the drain buffer.
type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

// TestServe checks that both paths serve every class's 1 MiB file whole.
func TestServe(t *testing.T) {
	buf := make([]byte, drainBufSize)
	for _, c := range classes {
		t.Run("class="+c.Name, func(t *testing.T) {
			fx := fixtureFor(t, shapes[0], c)
			for _, p := range paths {
				t.Run("path="+p.name, func(t *testing.T) { checkGet(t, fx, fx.url(p.route), buf) })
			}
		})
	}
}

// BenchmarkServe times whole-file GETs by both paths, per size and class.
func BenchmarkServe(b *testing.B) {
	for _, s := range shapes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range classes {
				b.Run("class="+c.Name, func(b *testing.B) {
					fx := fixtureFor(b, s, c)
					for _, p := range paths {
						b.Run("path="+p.name, func(b *testing.B) { benchmarkGet(b, fx, fx.url(p.route)) })
					}
				})
			}
		})
	}
}

// benchmarkGet's checking GET also warms the connection.
func benchmarkGet(b *testing.B, fx *fixture, url string) {
	buf := make([]byte, drainBufSize)
	checkGet(b, fx, url, buf)
	var body countWriter
	b.SetBytes(fx.size)
	b.ReportAllocs()
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
}
