package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// blockingBody blocks every read until ctx ends, like a stalled connection.
type blockingBody struct{ ctx context.Context }

func (b blockingBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (blockingBody) Close() error               { return nil }

// gatedBody delivers one byte plus tail only once ctx ends, like data landing as the deadline fires.
type gatedBody struct {
	ctx    context.Context
	tail   error
	closed bool
}

func (b *gatedBody) Read(p []byte) (int, error) { <-b.ctx.Done(); p[0] = 'x'; return 1, b.tail }
func (b *gatedBody) Close() error               { b.closed = true; return nil }

func isIdleTimeout(err error) bool {
	var timeout interface{ Timeout() bool }
	return errors.Is(err, context.DeadlineExceeded) && errors.As(err, &timeout) && timeout.Timeout()
}

func stringResponse(body string) *http.Response {
	return &http.Response{StatusCode: http.StatusOK, ContentLength: int64(len(body)), Body: io.NopCloser(strings.NewReader(body))}
}

func TestIdleTimeoutTransportReleasesRequestContext(t *testing.T) {
	errBase := errors.New("base failure")
	okBody := func(*http.Request) (*http.Response, error) { return stringResponse("ok"), nil }
	noBody := func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}
	fail := func(*http.Request) (*http.Response, error) { return nil, errBase }
	readAll := func(r *http.Response) { _, _ = io.ReadAll(r.Body) }
	closeBody := func(r *http.Response) { _ = r.Body.Close() }
	nothing := func(*http.Response) {}
	cases := map[string]struct {
		method string
		resp   func(*http.Request) (*http.Response, error)
		use    func(*http.Response)
	}{
		"body EOF":   {http.MethodGet, okBody, readAll},
		"body Close": {http.MethodGet, okBody, closeBody},
		"HEAD":       {http.MethodHead, noBody, nothing},
		"NoBody":     {http.MethodGet, noBody, nothing},
		"error":      {http.MethodGet, fail, nothing},
	}
	for name, tc := range cases {
		var seen context.Context
		rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
			seen = r.Context()
			return tc.resp(r)
		}), time.Second)
		req, _ := http.NewRequestWithContext(t.Context(), tc.method, "http://example/x", nil)
		resp, err := rt.RoundTrip(req)
		if err != nil && !errors.Is(err, errBase) {
			t.Fatalf("%s: RoundTrip: %v", name, err)
		}
		if err == nil {
			tc.use(resp)
		}
		if seen == req.Context() || seen.Err() == nil {
			t.Errorf("%s: request context not released after use", name)
		}
		if t.Context().Err() != nil {
			t.Fatalf("%s: parent context ended", name)
		}
	}
}

func TestIdleTimeoutTransportLeavesOtherMethodsAlone(t *testing.T) {
	body := io.NopCloser(strings.NewReader("x"))
	var seen context.Context
	rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.Context()
		return &http.Response{StatusCode: 200, Body: body}, nil
	}), time.Millisecond)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example/x", strings.NewReader("payload"))
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if seen != req.Context() || resp.Body != body {
		t.Fatal("POST request context or body was wrapped")
	}
	if NewIdleTimeoutTransport(http.DefaultTransport, 0) != http.DefaultTransport {
		t.Fatal("timeout <= 0 must return the base transport")
	}
}

func TestIdleTimeoutTransportZeroLengthReadDoesNotArm(t *testing.T) {
	var seen context.Context
	rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = r.Context()
		return stringResponse("data"), nil
	}), 20*time.Millisecond)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example/x", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := resp.Body.Read(nil); n != 0 || err != nil {
		t.Fatalf("zero-length read = %d, %v", n, err)
	}
	time.Sleep(60 * time.Millisecond)
	if seen.Err() != nil {
		t.Fatalf("request canceled after a zero-length read and a pause: %v", seen.Err())
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil || string(got) != "data" {
		t.Fatalf("read after zero-length read = %q, %v", got, err)
	}
}

func TestIdleTimeoutTransportParentCancelPassesThrough(t *testing.T) {
	rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, ContentLength: -1, Body: blockingBody{r.Context()}}, nil
	}), time.Second)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://example/x", nil)
	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(20*time.Millisecond, cancel)
	start := time.Now()
	_, err = resp.Body.Read(make([]byte, 1))
	if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read after parent cancel = %v, want plain context.Canceled", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("read did not fail promptly after parent cancel")
	}
}

func TestIdleTimeoutTransportStalledHeadersError(t *testing.T) {
	errBase := errors.New("base failure")
	rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, errBase
	}), 20*time.Millisecond)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example/x", nil)
	_, err := rt.RoundTrip(req)
	var timeout interface{ Timeout() bool }
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if !errors.Is(err, errBase) {
		t.Fatalf("underlying error dropped: %v", err)
	}
	if t.Context().Err() != nil {
		t.Fatal("parent context ended")
	}
}

func TestIdleTimeoutTransportLateBodyBytes(t *testing.T) {
	cases := map[string]struct {
		tail    error
		wantEOF bool
	}{
		"nil becomes terminal timeout": {nil, false},
		"EOF stays EOF":                {io.EOF, true},
	}
	for name, tc := range cases {
		body := &gatedBody{tail: tc.tail}
		rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
			body.ctx = r.Context()
			return &http.Response{StatusCode: 200, ContentLength: -1, Body: body}, nil
		}), 20*time.Millisecond)
		req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example/x", nil)
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("%s: RoundTrip: %v", name, err)
		}
		buf := make([]byte, 4)
		n, err := resp.Body.Read(buf)
		if n != 1 || buf[0] != 'x' {
			t.Fatalf("%s: late bytes dropped: n=%d", name, n)
		}
		if tc.wantEOF {
			if err != io.EOF {
				t.Fatalf("%s: read = %v, want io.EOF", name, err)
			}
			continue
		}
		if !isIdleTimeout(err) {
			t.Fatalf("%s: read = %v, want idle timeout", name, err)
		}
		if n, err := resp.Body.Read(buf); n != 0 || !isIdleTimeout(err) {
			t.Fatalf("%s: read after timeout = %d, %v; want terminal timeout", name, n, err)
		}
		if t.Context().Err() != nil {
			t.Fatalf("%s: parent context ended", name)
		}
	}
}

func TestIdleTimeoutTransportLateHeadersAreBounded(t *testing.T) {
	body := &gatedBody{}
	rt := NewIdleTimeoutTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return &http.Response{StatusCode: 200, ContentLength: 1, Body: body}, nil
	}), 20*time.Millisecond)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://example/x", nil)
	resp, err := rt.RoundTrip(req)
	if resp != nil || !isIdleTimeout(err) {
		t.Fatalf("late headers = %v, %v; want idle timeout without a response", resp, err)
	}
	if !body.closed {
		t.Fatal("late response body leaked")
	}
	if t.Context().Err() != nil {
		t.Fatal("parent context ended")
	}
}

func TestIdleTimeoutTransportNilBaseUsesDefaultTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Method)
	}))
	defer srv.Close()
	hc := &http.Client{}
	hc.Transport = NewIdleTimeoutTransport(hc.Transport, time.Second)
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		req, _ := http.NewRequestWithContext(t.Context(), method, srv.URL, nil)
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		got, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK || (method == http.MethodGet && string(got) != "GET") {
			t.Fatalf("%s: status %d body %q err %v", method, resp.StatusCode, got, err)
		}
	}
}

func TestNewClientKeepsHTTPClientSettings(t *testing.T) {
	tr := &http.Transport{}
	hc := &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		Timeout:       9 * time.Second,
	}
	c, err := NewClient(WithHTTPClient(hc))
	if err != nil {
		t.Fatal(err)
	}
	if c.httpClient != hc || c.httpClient.Transport != tr {
		t.Fatal("raw http client or its transport was replaced")
	}
	if c.getHttpClient.Timeout != hc.Timeout || c.getHttpClient.CheckRedirect == nil {
		t.Fatal("getHttpClient lost the caller's Timeout or CheckRedirect")
	}
	if c.idleTimeout != DefaultIdleTimeout {
		t.Fatalf("idleTimeout = %v, want %v", c.idleTimeout, DefaultIdleTimeout)
	}
}
