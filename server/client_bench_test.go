//go:build unix

package server_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
// fixture's server. Each transfer gets a new client with its own connections
// and cache. cpu-s/GiB is this process's user and system CPU time, client and
// server together, per GiB transferred.
func BenchmarkClient(b *testing.B) {
	for _, s := range shapes {
		b.Run("size="+s.name, func(b *testing.B) {
			for _, c := range storagetest.ChunkClasses {
				b.Run("class="+c.Name, func(b *testing.B) {
					fx := fixtureFor(b, s, c)
					b.Run("op=upload", func(b *testing.B) { benchmarkUpload(b, fx) })
					b.Run("op=download", func(b *testing.B) { benchmarkDownload(b, fx) })
				})
			}
		})
	}
}

func benchmarkUpload(b *testing.B, fx *fixture) {
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
		cli, transport := newTransferClient(b, srv.URL, filepath.Join(dir, "cache"))
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		from := cpuTime(b)
		b.StartTimer()
		got, err := cli.UploadFile(b.Context(), file)
		b.StopTimer()
		cpu += cpuTime(b) - from
		n++
		transport.CloseIdleConnections()
		srv.Close()
		if err != nil || got != want {
			b.Fatalf("upload: file hash %s, %v; want %s", got, err, want)
		}
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		// B.Loop fails when called with the timer stopped.
		b.StartTimer()
	}
	reportCPU(b, cpu, n, fx.size)
}

func benchmarkDownload(b *testing.B, fx *fixture) {
	srv := httptest.NewServer(fx.handler)
	defer srv.Close()
	var cpu time.Duration
	n := 0
	b.SetBytes(fx.size)
	for b.Loop() {
		b.StopTimer()
		dir, err := os.MkdirTemp(fixtureRoot, "download-")
		if err != nil {
			b.Fatal(err)
		}
		cli, transport := newTransferClient(b, srv.URL, filepath.Join(dir, "cache"))
		name := filepath.Join(dir, "file")
		out, err := os.Create(name)
		if err != nil {
			b.Fatal(err)
		}
		from := cpuTime(b)
		b.StartTimer()
		err = cli.DownloadFile(b.Context(), fx.fileHash, out)
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
		checkFile(b, name, fx)
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	reportCPU(b, cpu, n, fx.size)
}

// newTransferClient returns a client of the server at url with its own connections and its cache in dir.
func newTransferClient(tb testing.TB, url, dir string) (*xetclient.Client, *http.Transport) {
	tb.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
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
