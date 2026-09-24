package chaos_test

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitTimeout bounds every wait on proxy state.
const waitTimeout = 10 * time.Second

type faultKind int

const (
	passThrough    faultKind = iota
	stallHeaders             // send nothing until the client gives up
	stallBody                // relay prefix bytes, then hang
	abortBody                // relay prefix bytes, then close the connection mid-body
	resetBody                // relay prefix bytes, then reset the connection
	shortBody                // relay prefix bytes under the declared Content-Length, then end the response
	shortChunked             // relay prefix bytes without Content-Length, then end the response cleanly
	trickle                  // relay piece bytes every gap on a live connection
	injectStatus             // answer status without forwarding
	shiftRange               // forward with the Range start moved one byte later, so the 206 describes another range
	ignoreRange              // forward without the Range header, so the backend answers 200 with the whole resource
	noContentRange           // relay the response without its Content-Range header
	tagURLs                  // relay a reconstruction answer with ?gen=<tag> appended to every xorb URL
)

var faultNames = [...]string{"pass", "stallHeaders", "stallBody", "abortBody", "resetBody", "shortBody", "shortChunked", "trickle", "injectStatus", "shiftRange", "ignoreRange", "noContentRange", "tagURLs"}

var xorbURLs = regexp.MustCompile(`(/v1/xorbs/[^"?]*)"`)

func (k faultKind) String() string { return faultNames[k] }

// fault is the action applied to one proxied request.
type fault struct {
	kind   faultKind
	prefix int64         // body bytes relayed before the fault takes effect
	piece  int64         // trickle write size
	gap    time.Duration // trickle pause between writes
	status int           // injectStatus code
	tag    int           // tagURLs generation
}

// record is one client request as seen by the proxy: Seq counts earlier
// requests in the phase with the same method, path and Range, Status stays 0
// while headers are withheld, and Bytes counts body bytes written to the client.
type record struct {
	Method, Path, Query, Range string
	Seq                        int
	Fault                      faultKind
	Status                     int
	Bytes                      int64
	Start, End                 time.Time
}

func (r record) xorbGet() bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/xorbs/")
}

func (r record) reconstruction() bool {
	return strings.Contains(r.Path, "reconstructions")
}

// rule picks the fault for a request before it is forwarded.
type rule func(record) fault

// faultProxy is an HTTP/1.1 reverse proxy in front of the CAS backend that
// applies one fault per request and records every request it serves.
type faultProxy struct {
	srv       *httptest.Server
	transport *http.Transport
	stop      chan struct{}
	closeOnce sync.Once

	mu       sync.Mutex
	cond     *sync.Cond
	target   string
	rule     rule
	seq      map[string]int
	log      []*record
	inflight int
}

func newFaultProxy() *faultProxy {
	p := &faultProxy{transport: &http.Transport{}, stop: make(chan struct{}), seq: make(map[string]int)}
	p.cond = sync.NewCond(&p.mu)
	p.srv = httptest.NewUnstartedServer(http.HandlerFunc(p.serve))
	p.srv.Start() // plain HTTP/1.1; TLS/h2 variants start here
	return p
}

// Close releases stalled handlers before draining the server.
func (p *faultProxy) Close() {
	p.closeOnce.Do(func() {
		close(p.stop)
		p.srv.Close()
		p.transport.CloseIdleConnections()
	})
}

func (p *faultProxy) setTarget(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = url
}

// arm starts a new phase: r decides the faults of the requests that follow
// while the log and occurrence counters restart.
func (p *faultProxy) arm(r rule) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rule = r
	p.seq = make(map[string]int)
	p.log = nil
}

func (p *faultProxy) records() []record {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

func (p *faultProxy) snapshotLocked() []record {
	out := make([]record, 0, len(p.log))
	for _, rec := range p.log {
		out = append(out, *rec)
	}
	return out
}

// wait blocks until pred holds for the current log and in-flight count.
func (p *faultProxy) wait(t *testing.T, pred func(recs []record, inflight int) bool) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	timer := time.AfterFunc(waitTimeout, func() {
		p.mu.Lock()
		p.cond.Broadcast()
		p.mu.Unlock()
	})
	defer timer.Stop()
	p.mu.Lock()
	defer p.mu.Unlock()
	for !pred(p.snapshotLocked(), p.inflight) {
		if !time.Now().Before(deadline) {
			t.Fatalf("proxy: waited %v with %d requests in flight and %d recorded", waitTimeout, p.inflight, len(p.log))
		}
		p.cond.Wait()
	}
}

// settle waits for every handler to finish and returns the phase's records.
func (p *faultProxy) settle(t *testing.T) []record {
	t.Helper()
	p.wait(t, func(_ []record, inflight int) bool { return inflight == 0 })
	return p.records()
}

func (p *faultProxy) serve(w http.ResponseWriter, r *http.Request) {
	rec, f, target := p.begin(r)
	defer p.finish(rec)

	switch f.kind {
	case injectStatus:
		p.setStatus(rec, f.status)
		http.Error(w, http.StatusText(f.status), f.status)
		return
	case stallHeaders:
		p.block(r.Context())
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target+r.URL.RequestURI(), r.Body)
	if err != nil {
		p.setStatus(rec, http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	req.ContentLength = r.ContentLength
	switch f.kind {
	case shiftRange:
		var start, end int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err == nil {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start+1, end))
		}
	case ignoreRange:
		req.Header.Del("Range")
	}
	resp, err := p.transport.RoundTrip(req)
	if err != nil {
		p.setStatus(rec, http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	maps.Copy(w.Header(), resp.Header)
	switch f.kind {
	case shortChunked, tagURLs:
		w.Header().Del("Content-Length")
	case noContentRange:
		w.Header().Del("Content-Range")
	}
	w.WriteHeader(resp.StatusCode)
	p.setStatus(rec, resp.StatusCode)

	body := &countingWriter{w: w, p: p, rec: rec}
	switch f.kind {
	case passThrough, shiftRange, ignoreRange, noContentRange:
		_, _ = io.Copy(body, resp.Body)
	case tagURLs:
		answer, _ := io.ReadAll(resp.Body)
		_, _ = body.Write(xorbURLs.ReplaceAll(answer, fmt.Appendf(nil, "${1}?gen=%d\"", f.tag)))
	case trickle:
		for {
			if _, err := io.CopyN(body, resp.Body, f.piece); err != nil {
				return
			}
			flush(w)
			if !p.pause(r.Context(), f.gap) {
				return
			}
		}
	default:
		if _, err := io.CopyN(body, resp.Body, f.prefix); err != nil {
			return
		}
		flush(w)
		switch f.kind {
		case stallBody:
			p.block(r.Context())
		case abortBody:
			panic(http.ErrAbortHandler)
		case resetBody:
			resetConn(w)
		}
	}
}

// begin records the request, picks its fault, and returns the backend base URL.
func (p *faultProxy) begin(r *http.Request) (*record, fault, string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	key := r.Method + " " + r.URL.Path + " " + r.Header.Get("Range")
	rec := &record{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Range: r.Header.Get("Range"), Seq: p.seq[key], Start: time.Now()}
	p.seq[key]++
	var f fault
	if p.rule != nil {
		f = p.rule(*rec)
	}
	rec.Fault = f.kind
	p.log = append(p.log, rec)
	p.inflight++
	p.cond.Broadcast()
	return rec, f, p.target
}

func (p *faultProxy) finish(rec *record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rec.End = time.Now()
	p.inflight--
	p.cond.Broadcast()
}

func (p *faultProxy) setStatus(rec *record, status int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	rec.Status = status
	p.cond.Broadcast()
}

// block holds the handler until the client leaves or the proxy closes.
func (p *faultProxy) block(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-p.stop:
	}
}

// pause sleeps for d and reports whether the handler should keep going.
func (p *faultProxy) pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
	case <-p.stop:
	}
	return false
}

// countingWriter tracks body bytes as they reach the client.
type countingWriter struct {
	w   io.Writer
	p   *faultProxy
	rec *record
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	c.p.mu.Lock()
	c.rec.Bytes += int64(n)
	c.p.cond.Broadcast()
	c.p.mu.Unlock()
	return n, err
}

func flush(w http.ResponseWriter) {
	_ = http.NewResponseController(w).Flush()
}

// resetConn drops the connection with RST instead of FIN.
func resetConn(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	conn.Close()
}
