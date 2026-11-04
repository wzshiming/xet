package server_test

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
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

// window measures a row's timed loop: this process's rusage and the writes
// of the server's connections.
type window struct {
	conns         *connStats
	from          rusage
	writes, bytes int64
}

// startWindow opens b's window just before its timed loop.
func startWindow(b *testing.B, conns *connStats) *window {
	w := &window{conns: conns}
	w.writes, w.bytes = conns.writes.Load(), conns.bytes.Load()
	w.from = readRusage(b)
	return w
}

// stop closes w just after a timed loop of n requests for a file of size
// bytes and reports the window per GiB of file requested.
func (w *window) stop(b *testing.B, size int64, n int) {
	to := readRusage(b)
	writes, bytes := w.conns.writes.Load()-w.writes, w.conns.bytes.Load()-w.bytes
	file := float64(size) * float64(n)
	gib := file / (1 << 30)
	usr, sys := (to.usr - w.from.usr).Seconds(), (to.sys - w.from.sys).Seconds()
	b.ReportMetric((usr+sys)/gib, "cpu-s/GiB")
	b.ReportMetric(usr/gib, "usr-s/GiB")
	b.ReportMetric(sys/gib, "sys-s/GiB")
	b.ReportMetric(float64(to.minflt-w.from.minflt)/gib, "minflt/GiB")
	b.ReportMetric(float64(to.majflt-w.from.majflt)/gib, "majflt/GiB")
	b.ReportMetric(float64(writes)/gib, "writes/GiB")
	b.ReportMetric(float64(bytes)/file, "served/file")
}
