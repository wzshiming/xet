//go:build unix

package server_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	xetclient "github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
)

// BenchmarkClient times whole-file transfers by the Go client over TCP
// loopback, per size and class: op=upload into an empty in-process server, so
// every chunk is compressed, uploaded and validated, and op=download from the
// fixture's server, each through version=v1 or v2 of the protocol. An upload
// posts its shard to /v1/shards or /v2/shards and a download queries
// /v1/reconstructions or /v2/reconstructions; with the timer stopped, the row
// fails if a transfer skipped its version's route or used the other's. Each
// transfer gets a new client with its own connections and cache. cpu-s/GiB is
// this process's user and system CPU time, client and server together, per GiB
// transferred.
func BenchmarkClient(b *testing.B) {
	for _, s := range shapes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range storagetest.ChunkClasses {
				b.Run("class="+c.Name, func(b *testing.B) {
					fx := fixtureFor(b, s, c)
					b.Run("op=upload", func(b *testing.B) {
						for v := range versions {
							b.Run("version="+versions[v].name, func(b *testing.B) { benchmarkUpload(b, fx, v) })
						}
					})
					b.Run("op=download", func(b *testing.B) {
						for v := range versions {
							b.Run("version="+versions[v].name, func(b *testing.B) { benchmarkDownload(b, fx, v) })
						}
					})
				})
			}
		})
	}
}

// versions pin a transfer to one protocol version by the client calls it makes.
var versions = [2]struct {
	name     string
	upload   func(*xetclient.Client, context.Context, io.ReadSeeker) (xet.FileHash, error)
	download func(*xetclient.Client, context.Context, xet.FileHash, io.WriteSeeker) error
}{
	{"v1", (*xetclient.Client).UploadFileV1, (*xetclient.Client).DownloadFileV1},
	{"v2", (*xetclient.Client).UploadFileV2, (*xetclient.Client).DownloadFileV2},
}

func benchmarkUpload(b *testing.B, fx *fixture, v int) {
	file, err := os.Open(fx.plain)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	want := chunkedFileHash(b, file)
	var cpu time.Duration
	n := 0
	b.SetBytes(fx.size)
	for b.Loop() {
		b.StopTimer()
		dir, err := os.MkdirTemp(fixtureRoot, "upload-")
		if err != nil {
			b.Fatal(err)
		}
		st, err := local.NewStorage(local.WithBasePath(filepath.Join(dir, "store")))
		if err != nil {
			b.Fatal(err)
		}
		srv := httptest.NewServer(server.NewHandler(server.WithStorage(st)))
		cli, transport := newTransferClient(b, srv.URL, filepath.Join(dir, "cache"), shardRoute)
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		from := cpuTime(b)
		b.StartTimer()
		got, err := versions[v].upload(cli, b.Context(), file)
		b.StopTimer()
		cpu += cpuTime(b) - from
		n++
		transport.CloseIdleConnections()
		srv.Close()
		if err != nil || got != want {
			b.Fatalf("upload: file hash %s, %v; want %s", got, err, want)
		}
		transport.checkVersion(b, v)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		// B.Loop fails when called with the timer stopped.
		b.StartTimer()
	}
	reportCPU(b, cpu, n, fx.size)
}

func benchmarkDownload(b *testing.B, fx *fixture, v int) {
	var cpu time.Duration
	n := 0
	b.SetBytes(fx.size)
	for b.Loop() {
		b.StopTimer()
		dir, err := os.MkdirTemp(fixtureRoot, "download-")
		if err != nil {
			b.Fatal(err)
		}
		cli, transport := newTransferClient(b, fx.srv.URL, filepath.Join(dir, "cache"), reconstructionRoute)
		name := filepath.Join(dir, "file")
		out, err := os.Create(name)
		if err != nil {
			b.Fatal(err)
		}
		from := cpuTime(b)
		b.StartTimer()
		err = versions[v].download(cli, b.Context(), fx.hash, out)
		b.StopTimer()
		cpu += cpuTime(b) - from
		n++
		transport.CloseIdleConnections()
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
		if err != nil {
			b.Fatal(err)
		}
		transport.checkVersion(b, v)
		checkFile(b, name, fx)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	reportCPU(b, cpu, n, fx.size)
}

// TestClient transfers every class's 1 MiB file once per op and version of
// BenchmarkClient: each transfer must take its version's route and never the
// other's, a download must write the file and an upload must leave the file
// retrievable from the server.
func TestClient(t *testing.T) {
	for _, c := range storagetest.ChunkClasses {
		t.Run("class="+c.Name, func(t *testing.T) {
			fx := fixtureFor(t, shapes[0], c)
			t.Run("op=upload", func(t *testing.T) {
				for v := range versions {
					t.Run("version="+versions[v].name, func(t *testing.T) { testUpload(t, fx, v) })
				}
			})
			t.Run("op=download", func(t *testing.T) {
				for v := range versions {
					t.Run("version="+versions[v].name, func(t *testing.T) { testDownload(t, fx, v) })
				}
			})
		})
	}
}

func testUpload(t *testing.T, fx *fixture, v int) {
	file, err := os.Open(fx.plain)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	want := chunkedFileHash(t, file)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	st, err := local.NewStorage(local.WithBasePath(filepath.Join(dir, "store")))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(server.NewHandler(server.WithStorage(st)))
	defer srv.Close()
	cli, transport := newTransferClient(t, srv.URL, filepath.Join(dir, "cache"), shardRoute)
	defer transport.CloseIdleConnections()
	got, err := versions[v].upload(cli, t.Context(), file)
	if err != nil || got != want {
		t.Fatalf("upload: file hash %s, %v; want %s", got, err, want)
	}
	transport.checkVersion(t, v)
	checkGet(t, fx, srv.URL+"/xet-bridge/"+fx.sha256, make([]byte, drainBufSize))
}

func testDownload(t *testing.T, fx *fixture, v int) {
	dir := t.TempDir()
	cli, transport := newTransferClient(t, fx.srv.URL, filepath.Join(dir, "cache"), reconstructionRoute)
	defer transport.CloseIdleConnections()
	name := filepath.Join(dir, "file")
	out, err := os.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	err = versions[v].download(cli, t.Context(), fx.hash, out)
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	transport.checkVersion(t, v)
	checkFile(t, name, fx)
}

// route is the request of a transfer whose path names the protocol version:
// its method and its path prefix, indexed like versions.
type route struct {
	method string
	prefix [2]string
}

var (
	shardRoute          = route{http.MethodPost, [2]string{"/v1/shards", "/v2/shards"}}
	reconstructionRoute = route{http.MethodGet, [2]string{"/v1/reconstructions/", "/v2/reconstructions/"}}
)

// routeTransport counts the requests it sends on its route, per version.
type routeTransport struct {
	*http.Transport
	route route
	sent  [2]atomic.Int64
}

func (t *routeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == t.route.method {
		for v, prefix := range t.route.prefix {
			if strings.HasPrefix(req.URL.Path, prefix) {
				t.sent[v].Add(1)
			}
		}
	}
	return t.Transport.RoundTrip(req)
}

// checkVersion fails tb unless t sent its route in version v and never in the other.
func (t *routeTransport) checkVersion(tb testing.TB, v int) {
	tb.Helper()
	if own, other := t.sent[v].Load(), t.sent[1-v].Load(); own == 0 || other != 0 {
		tb.Fatalf("sent %s %s %d times and %s %s %d times; want at least once and never",
			t.route.method, t.route.prefix[v], own, t.route.method, t.route.prefix[1-v], other)
	}
}

// newTransferClient returns a client of the server at url with its own
// connections, counting its requests on r, and its cache in dir.
func newTransferClient(tb testing.TB, url, dir string, r route) (*xetclient.Client, *routeTransport) {
	tb.Helper()
	transport := &routeTransport{Transport: http.DefaultTransport.(*http.Transport).Clone(), route: r}
	cli, err := xetclient.NewClient(
		xetclient.WithHTTPClient(&http.Client{Transport: transport}),
		xetclient.WithCache(xetclient.NewCache(dir, 0, 0)),
		xetclient.WithUpstreamProvider(xetclient.StaticUpstreamProvider(url, "")),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return cli, transport
}

// chunkedFileHash is the xet hash of r's bytes as the client chunks them.
func chunkedFileHash(tb testing.TB, r io.Reader) xet.FileHash {
	tb.Helper()
	var hashes []xet.ChunkHash
	var sizes []uint64
	err := xet.ChunkData(r, func(_ int64, chunk []byte) error {
		hashes = append(hashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, uint64(len(chunk)))
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return xet.ComputeFileHash(hashes, sizes)
}

// checkFile fails tb unless the file at name holds fx's bytes.
func checkFile(tb testing.TB, name string, fx *fixture) {
	tb.Helper()
	f, err := os.Open(name)
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
		tb.Fatalf("downloaded %d bytes with SHA-256 %s, want %d bytes with %s", n, sum, fx.size, fx.sha256)
	}
}

// cpuTime is the user and system CPU time this process has used so far.
func cpuTime(tb testing.TB) time.Duration {
	tb.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		tb.Fatal(err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func reportCPU(b *testing.B, cpu time.Duration, n int, size int64) {
	b.ReportMetric(cpu.Seconds()/float64(n)/(float64(size)/(1<<30)), "cpu-s/GiB")
}
