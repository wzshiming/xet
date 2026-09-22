package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

// stallServer serves body: the first request stops after prefix bytes until
// the client drops the connection, later requests honor a bytes=N- Range.
func stallServer(t *testing.T, body []byte, prefix int, requests *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			_, _ = w.Write(body[:prefix])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		var start int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err != nil || start != prefix {
			t.Errorf("resume Range = %q, want bytes=%d-", r.Header.Get("Range"), prefix)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(body)-1, len(body)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(body[start:])
	}))
}

// countingClient counts attempts client-side; a server-side count races with a handler that has not started yet.
func countingClient(attempts *atomic.Int32) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return http.DefaultTransport.RoundTrip(r)
	})}
}

func TestDownloadXorbWithURLStalledHeadersFailAfterRetries(t *testing.T) {
	var requests, observedCancel, attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		<-r.Context().Done()
		observedCancel.Add(1)
	}))

	c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithIdleTimeout(50*time.Millisecond), WithRetries(1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = c.DownloadXorbWithURL(ctx, srv.URL, nil)
	var timeout interface{ Timeout() bool }
	if !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected a timeout error, got %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("parent context ended: %v", ctx.Err())
	}
	srv.Close() // returns once every stalled handler has seen its connection dropped
	// retries=1 lets the httpseek handler pass retry indices 0..1 before failing.
	if n := attempts.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if seen, got := requests.Load(), observedCancel.Load(); seen == 0 || got != seen {
		t.Fatalf("handlers observing cancel = %d of %d requests", got, seen)
	}
}

func TestHasXorbStalledHeadersFailAfterRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithBaseURL(srv.URL), WithIdleTimeout(50*time.Millisecond), WithRetries(1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = c.HasXorb(ctx, xet.XorbHash{})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected idle timeout error, got %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("parent context ended: %v", ctx.Err())
	}
	if n := attempts.Load(); n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
}

func TestDownloadXorbWithURLResumesAfterBodyStall(t *testing.T) {
	body := bytes.Repeat([]byte("0123456789abcdef"), 256)
	var requests atomic.Int32
	srv := stallServer(t, body, 1000, &requests)
	defer srv.Close()

	c, err := NewClient(WithIdleTimeout(100 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r, err := c.DownloadXorbWithURL(ctx, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	if ctx.Err() != nil {
		t.Fatalf("parent context ended: %v", ctx.Err())
	}
	if n := requests.Load(); n != 2 {
		t.Fatalf("requests = %d, want 2", n)
	}
}

// pacedServer streams body in pieces separated by gap on a live connection.
func pacedServer(body []byte, piece int, gap time.Duration, requests *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		for i := 0; i < len(body); i += piece {
			time.Sleep(gap)
			_, _ = w.Write(body[i:min(i+piece, len(body))])
			w.(http.Flusher).Flush()
		}
	}))
}

func TestDownloadXorbWithURLSlowBodyOutlivesIdleTimeout(t *testing.T) {
	body := bytes.Repeat([]byte("slow"), 128)
	var requests atomic.Int32
	srv := pacedServer(body, 64, 40*time.Millisecond, &requests) // 320ms total
	defer srv.Close()

	c, err := NewClient(WithIdleTimeout(200 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("transfer took %v, expected it to outlive the idle timeout", elapsed)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

func TestDownloadXorbWithURLIdleTimeoutIsPerRequest(t *testing.T) {
	stalled := bytes.Repeat([]byte("a"), 2048)
	paced := bytes.Repeat([]byte("b"), 512)
	var stallRequests, pacedRequests atomic.Int32
	stallSrv := stallServer(t, stalled, 512, &stallRequests)
	defer stallSrv.Close()
	pacedSrv := pacedServer(paced, 32, 20*time.Millisecond, &pacedRequests) // 320ms total
	defer pacedSrv.Close()

	c, err := NewClient(WithIdleTimeout(100 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	download := func(url string) ([]byte, time.Time, error) {
		r, err := c.DownloadXorbWithURL(ctx, url, nil)
		if err != nil {
			return nil, time.Time{}, err
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		return got, time.Now(), err
	}

	type result struct {
		got  []byte
		done time.Time
		err  error
	}
	pacedResult := make(chan result, 1)
	go func() {
		got, done, err := download(pacedSrv.URL)
		pacedResult <- result{got, done, err}
	}()
	gotStalled, stalledDone, err := download(stallSrv.URL)
	if err != nil {
		t.Fatalf("stalled download: %v", err)
	}
	pr := <-pacedResult
	if pr.err != nil {
		t.Fatalf("paced download: %v", pr.err)
	}
	if !bytes.Equal(gotStalled, stalled) || !bytes.Equal(pr.got, paced) {
		t.Fatalf("bodies: stalled %d/%d bytes, paced %d/%d bytes", len(gotStalled), len(stalled), len(pr.got), len(paced))
	}
	if !stalledDone.Before(pr.done) {
		t.Fatal("stalled download finished after the paced one; its idle window was masked")
	}
	if a, b := stallRequests.Load(), pacedRequests.Load(); a != 2 || b != 1 {
		t.Fatalf("requests: stalled %d (want 2), paced %d (want 1)", a, b)
	}
}

func TestDownloadXorbWithURLPausedReaderDoesNotTimeOut(t *testing.T) {
	body := bytes.Repeat([]byte("p"), 4096)
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c, err := NewClient(WithIdleTimeout(50 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got := make([]byte, 1)
	if _, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("first read: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // consumer pause longer than the idle timeout
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read after pause: %v", err)
	}
	if got = append(got, rest...); !bytes.Equal(got, body) {
		t.Fatalf("got %d bytes, want %d", len(got), len(body))
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

func TestDownloadXorbWithURLIdleTimeoutDisabledHonorsParentDeadline(t *testing.T) {
	body := bytes.Repeat([]byte("d"), 1024)
	var requests atomic.Int32
	srv := stallServer(t, body, 100, &requests)
	defer srv.Close()

	c, err := NewClient(WithIdleTimeout(-1))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	r, err := c.DownloadXorbWithURL(ctx, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, err = io.ReadAll(r)
	var idle *idleTimeoutError
	if !errors.Is(err, context.DeadlineExceeded) || errors.As(err, &idle) {
		t.Fatalf("expected only the parent deadline to end the read, got %v", err)
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

// Raw GET routes keep status and headers verbatim and never retry, but still time out when idle.
func TestRawGETIdleTimeout(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/body" {
			w.Header().Set("Content-Range", "bytes 0-9/10")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("abc"))
			w.(http.Flusher).Flush()
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithIdleTimeout(50*time.Millisecond), WithRetries(3))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	calls := map[string]func() error{
		"range headers": func() error {
			_, err := c.FetchXorbRangeWithURL(ctx, srv.URL+"/headers", nil)
			return err
		},
		"range body": func() error {
			resp, err := c.FetchXorbRangeWithURL(ctx, srv.URL+"/body", http.Header{"Range": {"bytes=0-9"}})
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 0-9/10" {
				t.Fatalf("status %d, Content-Range %q not preserved", resp.StatusCode, resp.Header.Get("Content-Range"))
			}
			got, err := io.ReadAll(resp.Body)
			if string(got) != "abc" {
				t.Fatalf("body prefix = %q, want abc", got)
			}
			return err
		},
		"dedup shard": func() error {
			_, err := c.QueryDedupShard(ctx, xet.ChunkHash{})
			return err
		},
	}
	for name, call := range calls {
		before := requests.Load()
		err := call()
		var idle *idleTimeoutError
		if !errors.As(err, &idle) {
			t.Fatalf("%s: err = %v, want idle timeout", name, err)
		}
		if ctx.Err() != nil {
			t.Fatalf("%s: parent context ended", name)
		}
		if n := requests.Load() - before; n != 1 {
			t.Fatalf("%s: requests = %d, want 1", name, n)
		}
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
