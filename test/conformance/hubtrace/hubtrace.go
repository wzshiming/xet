// Package hubtrace records a client's traffic with a Hugging Face hub, and with the CAS the hub names, as redacted hftest traces: a hub reverse proxy that keeps the xet links on its own origin and hands out a second recording proxy in place of the CAS.
package hubtrace

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/wzshiming/xet/client/hftest"
)

// maxText bounds the body text kept in a trace.
const maxText = 1 << 20

// Recorder proxies one hub and records every exchange with it and with the CAS proxies it hands out.
type Recorder struct {
	origin    string // upstream scheme://host
	transport *http.Transport
	hub       *httptest.Server

	mu        sync.Mutex
	cas       map[string]*httptest.Server // real CAS origin -> recording proxy
	exchanges []*exchange
}

// exchange is one recorded request/response pair before redaction.
type exchange struct {
	origin, method, path string
	reqHeader            http.Header
	reqBody              *hftest.Body
	status               int
	respHeader           http.Header
	respBody             *hftest.Body
}

// Start serves a recording proxy for the hub at upstream, an absolute http(s) URL; the environment's proxy settings apply to upstream traffic.
func Start(upstream string) (*Recorder, error) {
	u, err := url.Parse(upstream)
	if err != nil {
		return nil, fmt.Errorf("parse upstream: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("upstream %q: want an absolute http(s) URL", upstream)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 2 * time.Minute
	r := &Recorder{
		origin:    u.Scheme + "://" + u.Host,
		transport: transport,
		cas:       map[string]*httptest.Server{},
	}
	r.hub = httptest.NewServer(r.proxy("hub", u, r.rewriteHubResponse))
	return r, nil
}

// HubURL returns the origin clients use as their hub endpoint.
func (r *Recorder) HubURL() string {
	return r.hub.URL
}

// Close stops the hub proxy, then the CAS proxies, once their in-flight requests complete.
func (r *Recorder) Close() {
	r.hub.Close()
	r.mu.Lock()
	servers := make([]*httptest.Server, 0, len(r.cas))
	for _, srv := range r.cas {
		servers = append(servers, srv)
	}
	r.mu.Unlock()
	for _, srv := range servers {
		srv.Close()
	}
	r.transport.CloseIdleConnections()
}

// Reset forgets the exchanges recorded so far, keeping the proxies.
func (r *Recorder) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exchanges = nil
}

// Trace snapshots the exchanges recorded so far, redacted, as scenario of tool against repo.
func (r *Recorder) Trace(scenario, repo, tool string) *hftest.Trace {
	r.mu.Lock()
	defer r.mu.Unlock()
	t := &hftest.Trace{
		Scenario:  scenario,
		Recorded:  time.Now().UTC().Format(time.RFC3339),
		Upstream:  r.origin,
		Repo:      repo,
		Tool:      Redact(tool),
		Exchanges: make([]hftest.Exchange, 0, len(r.exchanges)),
	}
	for i, ex := range r.exchanges {
		t.Exchanges = append(t.Exchanges, ex.redacted(i+1))
	}
	return t
}

// proxy forwards to target, uncompressed, recording each request as received and each response as sent under origin.
func (r *Recorder) proxy(origin string, target *url.URL, modify func(*http.Response) error) http.Handler {
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Header.Set("Accept-Encoding", "identity")
		},
		Transport:      r.transport,
		ModifyResponse: modify,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "read request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		ex := r.begin(origin, req, body)
		rw := &recordingWriter{ResponseWriter: w}
		rp.ServeHTTP(rw, req)
		r.finish(ex, rw)
	})
}

// begin appends the request half of an exchange, fixing its arrival order.
func (r *Recorder) begin(origin string, req *http.Request, body []byte) *exchange {
	ex := &exchange{
		origin:    origin,
		method:    req.Method,
		path:      req.URL.RequestURI(),
		reqHeader: req.Header.Clone(),
		reqBody:   newBody(req.Header, body),
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exchanges = append(r.exchanges, ex)
	return ex
}

func (r *Recorder) finish(ex *exchange, rw *recordingWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ex.status = rw.status
	ex.respHeader = rw.header
	if ex.respHeader == nil {
		ex.respHeader = http.Header{}
	}
	ex.respBody = newBody(ex.respHeader, rw.body.Bytes())
}

// rewriteHubResponse keeps xet links on the recorder's origin and points the CAS the hub names at a recording proxy.
func (r *Recorder) rewriteHubResponse(resp *http.Response) error {
	links := resp.Header["Link"]
	for i, v := range links {
		links[i] = strings.ReplaceAll(v, r.origin, r.hub.URL)
	}
	if v := resp.Header.Get("X-Xet-Cas-Url"); v != "" {
		resp.Header.Set("X-Xet-Cas-Url", r.rewriteCASURL(v))
	}
	if !isJSON(resp.Header.Get("Content-Type")) {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("read upstream body: %w", err)
	}
	if rewritten, ok := r.rewriteCASBody(body); ok {
		body = rewritten
		resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		resp.ContentLength = int64(len(body))
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	return nil
}

// rewriteCASBody replaces every casUrl value of the JSON body, reporting whether one changed.
func (r *Recorder) rewriteCASBody(body []byte) ([]byte, bool) {
	changed := false
	out, ok := rewriteJSON(string(body), func(key, s string) string {
		if key != "casUrl" {
			return s
		}
		rewritten := r.rewriteCASURL(s)
		changed = changed || rewritten != s
		return rewritten
	})
	if !ok || !changed {
		return body, false
	}
	return []byte(out), true
}

// rewriteCASURL swaps the origin of raw for its recording proxy, leaving anything but an absolute http(s) URL alone.
func (r *Recorder) rewriteCASURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return raw
	}
	rest := u.EscapedPath()
	if u.RawQuery != "" {
		rest += "?" + u.RawQuery
	}
	return r.casProxy(&url.URL{Scheme: u.Scheme, Host: u.Host}) + rest
}

// casProxy returns the recording proxy for the CAS at origin, starting it on first use.
func (r *Recorder) casProxy(origin *url.URL) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := origin.String()
	if srv, ok := r.cas[key]; ok {
		return srv.URL
	}
	srv := httptest.NewServer(r.proxy("cas", origin, nil))
	r.cas[key] = srv
	return srv.URL
}

// newBody describes data, keeping its text only for text-like content types that fit the trace.
func newBody(h http.Header, data []byte) *hftest.Body {
	if len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(data)
	b := &hftest.Body{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	if len(data) <= maxText && isText(h.Get("Content-Type")) && utf8.Valid(data) {
		b.Text = string(data)
	}
	return b
}

func mediaType(contentType string) string {
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return ""
	}
	return mt
}

func isJSON(contentType string) bool {
	mt := mediaType(contentType)
	return mt == "application/json" || mt == "application/x-ndjson" || strings.HasSuffix(mt, "+json")
}

func isText(contentType string) bool {
	return isJSON(contentType) || strings.HasPrefix(mediaType(contentType), "text/")
}

// recordingWriter captures the status, the headers as of WriteHeader and the body written through it.
type recordingWriter struct {
	http.ResponseWriter
	status int
	header http.Header
	body   bytes.Buffer
}

func (w *recordingWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
		w.header = w.Header().Clone()
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	w.body.Write(p)
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the server's flusher.
func (w *recordingWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
