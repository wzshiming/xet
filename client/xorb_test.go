package client

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
)

// connCountingServer reports how many TCP connections the server accepted.
func connCountingServer(t *testing.T, handler http.Handler) (*httptest.Server, *atomic.Int32) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv, &conns
}

func TestNewClientReusesConnections(t *testing.T) {
	srv, conns := connCountingServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "xorb")
	}))

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if _, err := c.HasXorb(t.Context(), xet.XorbHash{}); err != nil {
			t.Fatal(err)
		}
		r, err := c.DownloadXorbWithURL(t.Context(), srv.URL+"/xorb", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(r); err != nil {
			t.Fatal(err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
		if got := conns.Load(); got != 1 {
			t.Fatalf("round %d: %d connections, want 1", i, got)
		}
	}
}

func TestNewClientKeepsSuppliedTransport(t *testing.T) {
	transport := &http.Transport{}
	c, err := NewClient(WithHTTPClient(&http.Client{Transport: transport}))
	if err != nil {
		t.Fatal(err)
	}
	if c.httpClient.Transport != transport || transport.DisableKeepAlives || transport.MaxIdleConnsPerHost != 0 || transport.MaxIdleConns != 0 {
		t.Fatalf("supplied transport replaced or mutated: %T DisableKeepAlives=%v MaxIdleConnsPerHost=%d MaxIdleConns=%d",
			c.httpClient.Transport, transport.DisableKeepAlives, transport.MaxIdleConnsPerHost, transport.MaxIdleConns)
	}

	srv, conns := connCountingServer(t, http.NotFoundHandler())
	c, err = NewClient(WithBaseURL(srv.URL), WithHTTPClient(&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}))
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.HasXorb(t.Context(), xet.XorbHash{}); err != nil {
			t.Fatal(err)
		}
	}
	if got := conns.Load(); got != 2 {
		t.Fatalf("%d connections with keep-alives disabled, want 2", got)
	}
}

func TestDownloadXorbWithURLAcceptsExactRangeAsOK(t *testing.T) {
	const body = "term"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Range"); got != "bytes=0-3" {
			t.Errorf("Range = %q, want bytes=0-3", got)
		}
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestDownloadXorbWithURLAcceptsPartialWithoutContentRange(t *testing.T) {
	const body = "term"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestDownloadXorbWithURLRejectsWrongLengthOKResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "whole xorb")
	}))
	defer srv.Close()

	c, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
	if err == nil || !strings.Contains(err.Error(), "status 200 OK") {
		t.Fatalf("expected range response error, got %v", err)
	}
}
