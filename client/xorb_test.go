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
	"testing/iotest"
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

	c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithIdleTimeout(50*time.Millisecond), WithRetries(1), WithRetryBackoff(0))
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
	// retries=1 allows one reopen after the first stalled attempt.
	if n := attempts.Load(); n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
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

	c, err := NewClient(WithHTTPClient(countingClient(&attempts)), WithBaseURL(srv.URL), WithIdleTimeout(50*time.Millisecond), WithRetries(1), WithRetryBackoff(0))
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

	c, err := NewClient(WithIdleTimeout(100*time.Millisecond), WithRetryBackoff(0))
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

	// No backoff: the resume must follow the idle timeout, not a retry wait.
	c, err := NewClient(WithIdleTimeout(100*time.Millisecond), WithRetryBackoff(0))
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

// stalledBody reports that a Read started, then blocks until ctx ends like a
// connection that stopped delivering bytes.
type stalledBody struct {
	ctx     context.Context
	reading chan struct{}
}

func (b stalledBody) Read([]byte) (int, error) {
	select {
	case b.reading <- struct{}{}:
	default:
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (stalledBody) Close() error { return nil }

// TestDownloadXorbWithURLCloseUnblocksStalledRead closes the body while a Read
// is blocked on it: Close must return promptly and end that Read instead of
// letting it reopen the range, without touching the reader concurrently.
func TestDownloadXorbWithURLCloseUnblocksStalledRead(t *testing.T) {
	reading := make(chan struct{}, 1)
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, ContentLength: 64, Body: stalledBody{ctx: r.Context(), reading: reading}}, nil
	})
	c, err := NewClient(WithHTTPClient(&http.Client{Transport: transport}), WithIdleTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	r, err := c.DownloadXorbWithURL(t.Context(), "http://example/xorb", nil)
	if err != nil {
		t.Fatal(err)
	}
	readErr := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 16))
		readErr <- err
	}()
	<-reading
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return while a Read was blocked")
	}
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("blocked Read returned no error after Close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked Read did not return after Close")
	}
	if t.Context().Err() != nil {
		t.Fatal("parent context ended")
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

// TestDownloadXorbWithURLStatusBudget pins the requests a ranged GET spends on
// statuses: retryable ones up to retries+1 times, terminal ones once.
func TestDownloadXorbWithURLStatusBudget(t *testing.T) {
	const body = "term"
	serve := func(statuses ...int) (*httptest.Server, *atomic.Int32) {
		var requests atomic.Int32
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if n := int(requests.Add(1)); n <= len(statuses) {
				http.Error(w, http.StatusText(statuses[n-1]), statuses[n-1])
				return
			}
			w.Header().Set("Content-Range", "bytes 0-3/4")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = io.WriteString(w, body)
		})), &requests
	}
	for _, tc := range []struct {
		name     string
		statuses []int
		want     int32
		fails    bool
	}{
		{"503 twice then served", []int{503, 503}, 3, false},
		{"429 then served", []int{429}, 2, false},
		{"503 always", []int{503, 503, 503, 503}, 3, true},
		{"404", []int{404}, 1, true},
		{"401", []int{401}, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, requests := serve(tc.statuses...)
			defer srv.Close()
			c, err := NewClient(WithRetries(2), WithRetryBackoff(0))
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=0-3"}})
			if tc.fails {
				if err == nil || !strings.Contains(err.Error(), strconv.Itoa(tc.statuses[0])) {
					t.Fatalf("err = %v, want status %d reported", err, tc.statuses[0])
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				r.Close()
				if err != nil || string(got) != body {
					t.Fatalf("body = %q, %v", got, err)
				}
			}
			if n := requests.Load(); n != tc.want {
				t.Fatalf("requests = %d, want %d", n, tc.want)
			}
		})
	}
}

// TestDownloadXorbWithURLRangeAnswers pins how a ranged GET treats answers
// httpseek rejects: the exact term bytes as 200 are still accepted through one
// plain request, anything describing other bytes fails without a second try.
func TestDownloadXorbWithURLRangeAnswers(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		contentRange string
		body         string
		wantBody     string // "" means the call must fail with wantErr
		wantErr      string
		wantRequests int32
	}{
		{"exact bytes as 200 from an offset", http.StatusOK, "", "term", "term", "", 2},
		{"whole resource as 200 from an offset", http.StatusOK, "", "whole xorb", "", "status 200 OK", 2},
		{"206 for another range", http.StatusPartialContent, "bytes 5-8/10", "erm?", "", "Content-Range", 1},
		{"206 without Content-Range", http.StatusPartialContent, "", "term", "term", "", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if got := r.Header.Get("Range"); got != "bytes=4-7" {
					t.Errorf("Range = %q, want bytes=4-7", got)
				}
				if tc.contentRange != "" {
					w.Header().Set("Content-Range", tc.contentRange)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			c, err := NewClient(WithRetryBackoff(0))
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=4-7"}})
			if tc.wantBody == "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				r.Close()
				if err != nil || string(got) != tc.wantBody {
					t.Fatalf("body = %q, %v; want %q", got, err, tc.wantBody)
				}
			}
			if n := requests.Load(); n != tc.wantRequests {
				t.Fatalf("requests = %d, want %d", n, tc.wantRequests)
			}
		})
	}
}

// scriptResource is the ranged resource behind the scripted servers below; bytes=4-7 reads "term".
const scriptResource = "0123term89"

// TestDownloadXorbWithURLFallbackChecksContentRange answers the resumable open
// with a 206 lacking Content-Range and the one-shot fallback with a 206 that
// carries one: its bytes are taken only when it describes the requested range.
func TestDownloadXorbWithURLFallbackChecksContentRange(t *testing.T) {
	for _, tc := range []struct {
		name         string
		contentRange string
		start, end   int // bytes the fallback serves
		ok           bool
	}{
		{"requested range", "bytes 4-7/10", 4, 7, true},
		{"unknown total", "bytes 4-7/*", 4, 7, true},
		{"shifted range", "bytes 5-8/10", 5, 8, false},
		{"shorter range", "bytes 4-6/10", 4, 6, false},
		{"missing total", "bytes 4-7/", 4, 7, false},
		{"invalid total", "bytes 4-7/garbage", 4, 7, false},
		{"trailing total data", "bytes 4-7/10junk", 4, 7, false},
		{"impossible total", "bytes 4-7/7", 4, 7, false},
		{"signed total", "bytes 4-7/+10", 4, 7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.Header.Get("Range"); got != "bytes=4-7" {
					t.Errorf("Range = %q, want bytes=4-7", got)
				}
				if requests.Add(1) == 1 {
					w.WriteHeader(http.StatusPartialContent)
					_, _ = io.WriteString(w, scriptResource[4:8])
					return
				}
				w.Header().Set("Content-Range", tc.contentRange)
				w.WriteHeader(http.StatusPartialContent)
				_, _ = io.WriteString(w, scriptResource[tc.start:tc.end+1])
			}))
			defer srv.Close()

			c, err := NewClient(WithRetryBackoff(0))
			if err != nil {
				t.Fatal(err)
			}
			r, err := c.DownloadXorbWithURL(t.Context(), srv.URL, http.Header{"Range": {"bytes=4-7"}})
			if !tc.ok {
				if err == nil || !strings.Contains(err.Error(), "Content-Range") {
					t.Fatalf("err = %v, want the Content-Range mismatch reported", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(r)
				r.Close()
				if err != nil || string(body) != "term" {
					t.Fatalf("body = %q, %v; want term", body, err)
				}
			}
			if n := requests.Load(); n != 2 {
				t.Fatalf("requests = %d, want 2", n)
			}
		})
	}
}

// answer is one scripted reply to a ranged GET for scriptResource.
type answer int

const (
	answer503   answer = iota // retryable status
	answerDrop                // transport error before headers
	answerServe               // 206 for the requested range
	answerCut                 // 206 whose body delivers two bytes, then fails
)

var errDropped = errors.New("connection dropped")

// scriptTransport replies to each request with the next answer in script,
// cycling through it, and counts attempts.
func scriptTransport(attempts *atomic.Int32, script ...answer) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		n := int(attempts.Add(1))
		var start, end int64
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
			return nil, fmt.Errorf("unexpected Range %q", r.Header.Get("Range"))
		}
		a := script[(n-1)%len(script)]
		switch a {
		case answer503:
			return &http.Response{StatusCode: 503, Status: "503 Service Unavailable", Header: http.Header{}, Body: http.NoBody, Request: r}, nil
		case answerDrop:
			return nil, errDropped
		}
		body := io.Reader(strings.NewReader(scriptResource[start : end+1]))
		if a == answerCut {
			body = io.MultiReader(strings.NewReader(scriptResource[start:start+2]), iotest.ErrReader(errDropped))
		}
		header := http.Header{"Content-Range": {fmt.Sprintf("bytes %d-%d/%d", start, end, len(scriptResource))}}
		return &http.Response{StatusCode: 206, Status: "206 Partial Content", Header: header, ContentLength: end - start + 1, Body: io.NopCloser(body), Request: r}, nil
	})
}

// readXorb downloads url and drains it, reporting the first failure of either step.
func readXorb(ctx context.Context, c *Client, url string) ([]byte, error) {
	r, err := c.DownloadXorbWithURL(ctx, url, http.Header{"Range": {"bytes=4-7"}})
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

// TestDownloadXorbWithURLMixedFaultBudget interleaves retryable statuses,
// transport drops and a mid-body cut on one ranged GET with WithRetries(2):
// all failures share one budget, so three attempts without received bytes end
// the download, and bytes received in between start the count over.
func TestDownloadXorbWithURLMixedFaultBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		script  []answer
		wantErr string // "" means the download must yield term
		want    int32  // attempts
	}{
		{"status then drop then served", []answer{answer503, answerDrop, answerServe}, "", 3},
		{"drop then status then served", []answer{answerDrop, answer503, answerServe}, "", 3},
		{"status and drop alternate", []answer{answer503, answerDrop}, "503", 3},
		{"drop and status alternate", []answer{answerDrop, answer503}, "connection dropped", 3},
		{"served one attempt too late", []answer{answer503, answerDrop, answer503, answerServe}, "503", 3},
		{"cut body then status then served", []answer{answerCut, answer503, answerServe}, "", 3},
		{"cut body then status then drop", []answer{answerCut, answer503, answerDrop, answerServe}, "connection dropped", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			c, err := NewClient(WithHTTPClient(&http.Client{Transport: scriptTransport(&attempts, tc.script...)}), WithRetries(2), WithRetryBackoff(0))
			if err != nil {
				t.Fatal(err)
			}
			got, err := readXorb(t.Context(), c, "http://cas/xorb")
			if tc.wantErr == "" && (err != nil || string(got) != "term") {
				t.Errorf("body = %q, %v; want term", got, err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Errorf("err = %v, want %q reported", err, tc.wantErr)
			}
			if n := attempts.Load(); n != tc.want {
				t.Errorf("attempts = %d, want %d", n, tc.want)
			}
		})
	}
}

// TestDownloadXorbWithURLBudgetIsPerDownload runs two downloads on one client
// that each need two retries, holding the first download's serving attempt
// until the second has failed once: a budget shared across downloads would
// run out, separate budgets let both finish.
func TestDownloadXorbWithURLBudgetIsPerDownload(t *testing.T) {
	var attemptsA, attemptsB atomic.Int32
	scriptA := scriptTransport(&attemptsA, answer503, answer503, answerServe)
	scriptB := scriptTransport(&attemptsB, answer503, answer503, answerServe)
	bFailed := make(chan struct{})
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/b" {
			resp, err := scriptB.RoundTrip(r)
			if attemptsB.Load() == 1 {
				close(bFailed)
			}
			return resp, err
		}
		if attemptsA.Load() == 2 {
			<-bFailed
		}
		return scriptA.RoundTrip(r)
	})
	c, err := NewClient(WithHTTPClient(&http.Client{Transport: transport}), WithRetries(2), WithRetryBackoff(0))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type result struct {
		got []byte
		err error
	}
	results := make(chan result, 2)
	for _, path := range []string{"/a", "/b"} {
		go func() {
			got, err := readXorb(ctx, c, "http://cas"+path)
			results <- result{got, err}
		}()
	}
	for range 2 {
		if r := <-results; r.err != nil || string(r.got) != "term" {
			t.Fatalf("body = %q, %v; want term", r.got, r.err)
		}
	}
	if a, b := attemptsA.Load(), attemptsB.Load(); a != 3 || b != 3 {
		t.Fatalf("attempts: a %d, b %d; want 3 each", a, b)
	}
}
