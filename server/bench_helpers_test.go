package server_test

import (
	"fmt"
	"io"
	"maps"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// connStats counts what the accepted connections of one server write.
type connStats struct {
	writes atomic.Int64 // Write calls, plus one per sendfile
	bytes  atomic.Int64
}

// newCountingServer starts an httptest server for h whose connections count
// their writes into the returned stats.
func newCountingServer(h http.Handler) (*httptest.Server, *connStats) {
	stats := &connStats{}
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = countingListener{Listener: srv.Listener, stats: stats}
	srv.Start()
	return srv, stats
}

type countingListener struct {
	net.Listener
	stats *connStats
}

func (l countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: c, stats: l.stats}, nil
}

// countingConn counts each write before making it, so a client that has read
// the last byte of a response finds all of its writes counted.
type countingConn struct {
	net.Conn
	stats *connStats
}

func (c *countingConn) Write(p []byte) (int, error) {
	c.stats.writes.Add(1)
	c.stats.bytes.Add(int64(len(p)))
	n, err := c.Conn.Write(p)
	c.stats.bytes.Add(int64(n - len(p)))
	return n, err
}

// ReadFrom keeps what net does without the wrapper: net/http's response
// ReadFrom hands it the *io.LimitedReader of http.ServeContent after a
// 512-byte sniff copy; an *os.File below it goes to the inner conn's sendfile
// as one write, anything else is copied in io.Copy's 32 KiB writes.
func (c *countingConn) ReadFrom(src io.Reader) (int64, error) {
	r, want := src, int64(0)
	if lr, ok := src.(*io.LimitedReader); ok {
		r, want = lr.R, lr.N
	}
	rf, ok := c.Conn.(io.ReaderFrom)
	if _, file := r.(*os.File); !file || !ok {
		return io.Copy(connWriter{c}, src)
	}
	c.stats.writes.Add(1)
	c.stats.bytes.Add(want)
	n, err := rf.ReadFrom(src)
	c.stats.bytes.Add(n - want)
	return n, err
}

// connWriter hides countingConn's ReadFrom from io.Copy; a single pointer, it
// converts to io.Writer without allocating, as net's own wrapper does.
type connWriter struct{ c *countingConn }

func (w connWriter) Write(p []byte) (int, error) { return w.c.Write(p) }

// rusage is this process's CPU time and page faults so far.
type rusage struct {
	usr, sys       time.Duration
	minflt, majflt int64
}

// window measures a row's timed loop: this process's rusage, the writes of
// the server's connections and, with XET_BENCH_PROFILE_DIR, a CPU profile.
type window struct {
	conns         *connStats
	from          rusage
	writes, bytes int64
	stopProfile   func()
}

// startWindow opens b's window just before its timed loop.
func startWindow(b *testing.B, conns *connStats) *window {
	w := &window{conns: conns, stopProfile: startProfile(b)}
	w.writes, w.bytes = conns.writes.Load(), conns.bytes.Load()
	w.from = readRusage(b)
	return w
}

// stop closes w just after a timed loop of n requests for a file of size
// bytes and reports the window per GiB of file requested.
func (w *window) stop(b *testing.B, size int64, n int) {
	to := readRusage(b)
	writes, bytes := w.conns.writes.Load()-w.writes, w.conns.bytes.Load()-w.bytes
	w.stopProfile()
	for unit, v := range windowMetrics(w.from, to, writes, bytes, size, n) {
		b.ReportMetric(v, unit)
	}
}

// windowMetrics turns a window over n requests for a file of size bytes,
// its rusage from and to and its connections' writes and bytes, into metrics
// by unit, per GiB of file requested.
func windowMetrics(from, to rusage, writes, bytes, size int64, n int) map[string]float64 {
	file := float64(size) * float64(n)
	gib := file / (1 << 30)
	usr, sys := (to.usr - from.usr).Seconds(), (to.sys - from.sys).Seconds()
	return map[string]float64{
		"cpu-s/GiB":   (usr + sys) / gib,
		"usr-s/GiB":   usr / gib,
		"sys-s/GiB":   sys / gib,
		"minflt/GiB":  float64(to.minflt-from.minflt) / gib,
		"majflt/GiB":  float64(to.majflt-from.majflt) / gib,
		"writes/GiB":  float64(writes) / gib,
		"served/file": float64(bytes) / file,
	}
}

// TestWindowMetrics requests a 256 MiB file 3 times, 0.75 GiB in all.
func TestWindowMetrics(t *testing.T) {
	from := rusage{usr: time.Second, sys: 250 * time.Millisecond, minflt: 100, majflt: 1}
	to := rusage{usr: 2500 * time.Millisecond, sys: 750 * time.Millisecond, minflt: 1600, majflt: 7}
	got := windowMetrics(from, to, 6147, 3<<28, 1<<28, 3)
	want := map[string]float64{
		"cpu-s/GiB":   8.0 / 3,
		"usr-s/GiB":   2,
		"sys-s/GiB":   2.0 / 3,
		"minflt/GiB":  2000,
		"majflt/GiB":  8,
		"writes/GiB":  8196,
		"served/file": 1,
	}
	if !maps.EqualFunc(got, want, func(g, w float64) bool { return math.Abs(g-w) <= 1e-9*w }) {
		t.Errorf("windowMetrics = %v\nwant %v", got, want)
	}
}

// profiles numbers the CPU profiles of this process, so -count keeps each.
var profiles int

// startProfile writes b's CPU profile into XET_BENCH_PROFILE_DIR, if set,
// until stop.
func startProfile(b *testing.B) (stop func()) {
	dir := os.Getenv("XET_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if !filepath.IsAbs(dir) {
		b.Fatalf("XET_BENCH_PROFILE_DIR=%q is not absolute", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.Fatal(err)
	}
	profiles++
	name := filepath.Join(dir, fmt.Sprintf("%s-%d.pprof", strings.ReplaceAll(b.Name(), "/", "_"), profiles))
	f, err := os.Create(name)
	if err != nil {
		b.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		b.Logf("no CPU profile: %v", err)
		f.Close()
		os.Remove(name)
		return func() {}
	}
	return func() {
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			b.Error(err)
		}
	}
}
