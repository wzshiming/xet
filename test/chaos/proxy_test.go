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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitTimeout bounds every wait on proxy state.
const waitTimeout = 10 * time.Second

type faultKind int

const (
	passThrough      faultKind = iota
	stallHeaders               // drain the request body, then send nothing until the client gives up
	stallBody                  // relay prefix bytes, then hang
	abortBody                  // relay prefix bytes, then close the connection mid-body
	resetBody                  // relay prefix bytes, then reset the connection
	shortBody                  // relay prefix bytes under the declared Content-Length, then end the response
	shortChunked               // relay prefix bytes without Content-Length, then end the response cleanly
	trickle                    // relay piece bytes every gap on a live connection
	injectStatus               // drain the request body, then answer status without forwarding
	shiftRange                 // forward with the Range start moved one byte later, so the 206 describes another range
	ignoreRange                // forward without the Range header, so the backend answers 200 with the whole resource
	noContentRange             // relay the response without its Content-Range header
	tagURLs                    // relay a reconstruction answer with ?gen=<tag> appended to every xorb URL
	injectBody                 // drain the request body, then answer status with contentType and body without forwarding
	cutRequestBody             // relay the request head and prefix body bytes, then close the backend side and reset the client
	stallRequestBody           // relay the request head and prefix body bytes, then hold both sides until the client leaves
)

var faultNames = [...]string{"pass", "stallHeaders", "stallBody", "abortBody", "resetBody", "shortBody", "shortChunked", "trickle", "injectStatus", "shiftRange", "ignoreRange", "noContentRange", "tagURLs", "injectBody", "cutRequestBody", "stallRequestBody"}

var xorbURLs = regexp.MustCompile(`(/v1/xorbs/[^"?]*)"`)

// hopHeaders describe a single connection, so the proxy forwards none of them.
var hopHeaders = []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func (k faultKind) String() string { return faultNames[k] }

// fault is the action applied to one proxied request.
type fault struct {
	kind        faultKind
	prefix      int64           // body bytes relayed before the fault takes effect
	piece       int64           // trickle write size
	gap         time.Duration   // trickle pause between writes
	status      int             // injectStatus and injectBody code
	tag         int             // tagURLs generation
	gate        <-chan struct{} // injectStatus answers only once this closes
	contentType string          // injectBody Content-Type
	body        []byte          // injectBody answer
}

// record is one client request as seen by the proxy: Seq counts earlier
// requests in the phase with the same method, path and Range, Status stays 0
// while headers are withheld, Bytes counts body bytes written to the client,
// and ReqBytes counts request-body bytes relayed to the backend or drained.
type record struct {
	Method, Path, Query, Range string
	Seq                        int
	Fault                      faultKind
	Status                     int
	Bytes                      int64
	ReqBytes                   int64
	Start, End                 time.Time
}

func (r record) xorbGet() bool {
	return r.Method == http.MethodGet && strings.HasPrefix(r.Path, "/v1/xorbs/")
}

func (r record) reconstruction() bool {
	return strings.Contains(r.Path, "reconstructions")
}

func filter(recs []record, match func(record) bool) []record {
	var out []record
	for _, r := range recs {
		if match(r) {
			out = append(out, r)
		}
	}
	return out
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

func (p *faultProxy) URL() string { return p.srv.URL }

func (p *faultProxy) Addr() string { return p.srv.Listener.Addr().String() }

// Close releases stalled handlers before draining the server.
func (p *faultProxy) Close() {
	p.closeOnce.Do(func() {
		close(p.stop)
		// Handlers reading a stalled request body only return once its connection is gone.
		p.srv.CloseClientConnections()
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
	if err := p.waitErr(pred); err != nil {
		t.Fatal(err)
	}
}

// waitErr blocks until pred holds for the current log and in-flight count, or reports the state after waitTimeout.
func (p *faultProxy) waitErr(pred func(recs []record, inflight int) bool) error {
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
			return fmt.Errorf("proxy: waited %v with %d requests in flight and %d recorded", waitTimeout, p.inflight, len(p.log))
		}
		p.cond.Wait()
	}
	return nil
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
	// A response may stream while the request body still arrives, as /v2/shards does.
	_ = http.NewResponseController(w).EnableFullDuplex()
	reqBody := &countingReader{r: r.Body, p: p, rec: rec}

	// Answers drain the request body first: Go's server closes the connection
	// on a large unread body, which can cost the client the answer.
	switch f.kind {
	case injectStatus:
		_, _ = io.Copy(io.Discard, reqBody)
		if f.gate != nil && !p.await(r.Context(), f.gate) {
			return
		}
		p.setStatus(rec, f.status)
		http.Error(w, http.StatusText(f.status), f.status)
		return
	case injectBody:
		_, _ = io.Copy(io.Discard, reqBody)
		w.Header().Set("Content-Type", f.contentType)
		w.Header().Set("Content-Length", strconv.Itoa(len(f.body)))
		w.WriteHeader(f.status)
		p.setStatus(rec, f.status)
		_, _ = (&countingWriter{w: w, p: p, rec: rec}).Write(f.body)
		return
	case stallHeaders:
		_, _ = io.Copy(io.Discard, reqBody)
		p.block(r.Context())
		return
	case cutRequestBody, stallRequestBody:
		p.relayPrefix(w, r, reqBody, rec, f, target)
		return
	}

	var forward io.Reader = http.NoBody
	if r.Body != http.NoBody {
		forward = reqBody
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target+r.URL.RequestURI(), forward)
	if err != nil {
		p.setStatus(rec, http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	req.Header = r.Header.Clone()
	dropHopByHop(req.Header)
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
	dropHopByHop(w.Header())
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

// relayPrefix hands the backend the request head and prefix body bytes over a
// connection of its own, as http.Transport may leave the tail of a failed body
// unflushed, then cuts or stalls both sides. A body of unknown length cannot
// be declared to the backend, so it is drained and answered with 502.
func (p *faultProxy) relayPrefix(w http.ResponseWriter, r *http.Request, body io.Reader, rec *record, f fault, target string) {
	if r.ContentLength < 0 {
		_, _ = io.Copy(io.Discard, body)
		p.setStatus(rec, http.StatusBadGateway)
		http.Error(w, "request body of unknown length", http.StatusBadGateway)
		return
	}
	host := strings.TrimPrefix(target, "http://")
	var dialer net.Dialer
	conn, err := dialer.DialContext(r.Context(), "tcp", host)
	if err != nil {
		p.setStatus(rec, http.StatusBadGateway)
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	backend := conn.(*net.TCPConn)
	header := r.Header.Clone()
	dropHopByHop(header)
	header.Del("Content-Length")
	var head strings.Builder
	fmt.Fprintf(&head, "%s %s HTTP/1.1\r\nHost: %s\r\n", r.Method, r.URL.RequestURI(), host)
	_ = header.Write(&head)
	fmt.Fprintf(&head, "Content-Length: %d\r\n\r\n", r.ContentLength)
	if _, err := io.WriteString(backend, head.String()); err == nil {
		_, _ = io.CopyN(backend, body, f.prefix)
	}

	client, buf, err := http.NewResponseController(w).Hijack()
	if err != nil {
		rstClose(backend)
		panic(http.ErrAbortHandler)
	}
	var left chan struct{}
	if f.kind == stallRequestBody {
		left = make(chan struct{})
		go func() {
			// Only reading notices the client leave; what it still sends is dropped.
			_, _ = io.Copy(io.Discard, buf)
			close(left)
		}()
		select {
		case <-left:
		case <-p.stop:
		}
	}
	// FIN first: closing with unread backend output would send a reset instead.
	_ = backend.CloseWrite()
	_ = backend.Close()
	rstClose(client)
	if left != nil {
		<-left
	}
}

// begin records the request, picks its fault, and returns the backend base URL.
func (p *faultProxy) begin(r *http.Request) (*record, fault, string) {
	key := r.Method + " " + r.URL.Path + " " + r.Header.Get("Range")
	p.mu.Lock()
	rec := &record{Method: r.Method, Path: r.URL.Path, Query: r.URL.RawQuery, Range: r.Header.Get("Range"), Seq: p.seq[key], Start: time.Now()}
	p.seq[key]++
	p.inflight++
	rule, target := p.rule, p.target
	p.cond.Broadcast()
	p.mu.Unlock()
	// The rule runs unlocked, so it may read records(), which hold the earlier requests.
	var f fault
	if rule != nil {
		f = rule(*rec)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	rec.Fault = f.kind
	p.log = append(p.log, rec)
	p.cond.Broadcast()
	return rec, f, target
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
	p.await(ctx, nil)
}

// await reports whether ch closed before the client left or the proxy closed.
func (p *faultProxy) await(ctx context.Context, ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-ctx.Done():
	case <-p.stop:
	}
	return false
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

// countingReader tracks request-body bytes as they are relayed or drained.
type countingReader struct {
	r   io.ReadCloser
	p   *faultProxy
	rec *record
}

func (c *countingReader) Read(b []byte) (int, error) {
	n, err := c.r.Read(b)
	c.p.mu.Lock()
	c.rec.ReqBytes += int64(n)
	c.p.cond.Broadcast()
	c.p.mu.Unlock()
	return n, err
}

func (c *countingReader) Close() error { return c.r.Close() }

// countingWriter flushes body bytes to the client as they come and tracks them.
type countingWriter struct {
	w   http.ResponseWriter
	p   *faultProxy
	rec *record
}

func (c *countingWriter) Write(b []byte) (int, error) {
	n, err := c.w.Write(b)
	flush(c.w)
	c.p.mu.Lock()
	c.rec.Bytes += int64(n)
	c.p.cond.Broadcast()
	c.p.mu.Unlock()
	return n, err
}

func flush(w http.ResponseWriter) {
	_ = http.NewResponseController(w).Flush()
}

// dropHopByHop removes the headers that describe a single connection.
func dropHopByHop(h http.Header) {
	for _, field := range h["Connection"] {
		for name := range strings.SplitSeq(field, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopHeaders {
		h.Del(name)
	}
}

// resetConn drops the connection with RST instead of FIN.
func resetConn(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	rstClose(conn)
}

// rstClose closes conn with RST instead of FIN.
func rstClose(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}
