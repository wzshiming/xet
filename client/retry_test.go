package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/shard"
)

// A transient status gets one more attempt, resending the body byte for byte; any other status reaches the caller after one request.
func TestRetryOnlyTransientStatuses(t *testing.T) {
	payload := []byte("xorb payload")
	sites := []struct {
		name string
		body []byte
		call func(context.Context, *Client) error
	}{
		{"GET reconstruction", nil, func(ctx context.Context, c *Client) error {
			_, err := c.GetReconstructionV1(ctx, xet.FileHash{}, nil)
			return err
		}},
		{"POST xorb", payload, func(ctx context.Context, c *Client) error {
			_, err := c.UploadXorb(ctx, xet.XorbHash{}, bytes.NewReader(payload))
			return err
		}},
	}
	for _, tc := range []struct {
		status    int
		transient bool
	}{
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
		{http.StatusGatewayTimeout, true},
		{http.StatusInsufficientStorage, true},
		{http.StatusBadRequest, false},
		{http.StatusNotFound, false},
		{http.StatusConflict, false},
		{http.StatusRequestedRangeNotSatisfiable, false},
		{http.StatusNotImplemented, false},
	} {
		for _, site := range sites {
			t.Run(fmt.Sprintf("%s %d", site.name, tc.status), func(t *testing.T) {
				var mu sync.Mutex
				var bodies [][]byte
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					mu.Lock()
					bodies = append(bodies, body)
					first := len(bodies) == 1
					mu.Unlock()
					if first {
						// On a reused connection net/http would resend the body through GetBody itself.
						w.Header().Set("Connection", "close")
						w.WriteHeader(tc.status)
						return
					}
					_, _ = io.WriteString(w, "{}")
				}))
				defer srv.Close()
				c, err := NewClient(WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
				if err != nil {
					t.Fatal(err)
				}
				err = site.call(t.Context(), c)
				mu.Lock()
				defer mu.Unlock()
				if !tc.transient {
					if len(bodies) != 1 {
						t.Fatalf("requests = %d, want 1", len(bodies))
					}
					if tc.status == http.StatusNotFound {
						if !errors.Is(err, errNotFound) {
							t.Fatalf("err = %v, want errNotFound", err)
						}
					} else if got, ok := errors.AsType[statusCodeError](err); !ok || got.code != tc.status {
						t.Fatalf("err = %v, want status %d", err, tc.status)
					}
					return
				}
				if err != nil || len(bodies) != 2 {
					t.Fatalf("err = %v after %d requests, want success on the second", err, len(bodies))
				}
				if !bytes.Equal(bodies[0], site.body) || !bytes.Equal(bodies[1], site.body) {
					t.Fatalf("bodies = %q, want %q twice", bodies, site.body)
				}
			})
		}
	}
}

// A connection dropped before any response is retried like any other network error.
func TestRetryDroppedConnection(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			panic(http.ErrAbortHandler)
		}
		_, _ = io.WriteString(w, "{}")
	}))
	defer srv.Close()
	c, err := NewClient(WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetReconstructionV1(t.Context(), xet.FileHash{}, nil); err != nil {
		t.Fatalf("GetReconstructionV1: %v", err)
	}
	if n := requests.Load(); n != 2 {
		t.Fatalf("requests = %d, want 2", n)
	}
}

// recordRetryWaits skips retry sleeps, recording each nominal delay and the wait drawn from it.
func recordRetryWaits(t *testing.T) (nominal, drawn *[]time.Duration) {
	t.Helper()
	jitter, sleep := retryJitter, retrySleep
	t.Cleanup(func() { retryJitter, retrySleep = jitter, sleep })
	nominal, drawn = new([]time.Duration), new([]time.Duration)
	retryJitter = func(d time.Duration) time.Duration {
		*nominal = append(*nominal, d)
		return jitter(d)
	}
	retrySleep = func(_ context.Context, d time.Duration) error {
		*drawn = append(*drawn, d)
		return nil
	}
	return nominal, drawn
}

// Retry n waits a random part of base·3^(n-1), at most 6 minutes; base defaults to 3s and <= 0 disables waiting.
func TestRetryWaitSchedule(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	const s, m = time.Second, time.Minute
	for _, tc := range []struct {
		name string
		opts []Options
		want []time.Duration
	}{
		{"default", nil, []time.Duration{3 * s, 9 * s, 27 * s, 81 * s, 243 * s}},
		{"base 200s", []Options{WithRetryBaseDelay(200 * s)}, []time.Duration{200 * s, 6 * m, 6 * m, 6 * m, 6 * m}},
		{"base 0", []Options{WithRetryBaseDelay(0)}, nil},
		{"negative base", []Options{WithRetryBaseDelay(-s)}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nominal, drawn := recordRetryWaits(t)
			requests.Store(0)
			c, err := NewClient(append(tc.opts, WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.GetReconstructionV1(t.Context(), xet.FileHash{}, nil); err == nil {
				t.Fatal("want the retries exhausted")
			}
			if n := requests.Load(); n != 6 {
				t.Fatalf("requests = %d, want 6", n)
			}
			if !slices.Equal(*nominal, tc.want) {
				t.Fatalf("nominal waits = %v, want %v", *nominal, tc.want)
			}
			if len(*drawn) != len(tc.want) {
				t.Fatalf("waits = %v, want one per nominal wait %v", *drawn, tc.want)
			}
			for i, d := range *drawn {
				if d < 0 || d >= tc.want[i] {
					t.Fatalf("wait %d = %v, want within [0, %v)", i+1, d, tc.want[i])
				}
			}
		})
	}
}

// Cancelling the context while a retry waits ends the request at once with the context's error and no further attempt.
func TestRetryWaitEndsWithContext(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	jitter, sleep := retryJitter, retrySleep
	t.Cleanup(func() { retryJitter, retrySleep = jitter, sleep })
	retryJitter = func(d time.Duration) time.Duration { return d }
	retrySleep = func(ctx context.Context, d time.Duration) error {
		time.AfterFunc(10*time.Millisecond, cancel)
		return sleep(ctx, d)
	}
	c, err := NewClient(WithRetryBaseDelay(time.Hour), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.GetReconstructionV1(ctx, xet.FileHash{}, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting 5s after the context was cancelled")
	}
	if n := requests.Load(); n != 1 {
		t.Fatalf("requests = %d, want 1", n)
	}
}

// ndjsonFrames answers with the given NDJSON lines.
func ndjsonFrames(lines ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, line := range lines {
			_, _ = io.WriteString(w, line+"\n")
		}
	}
}

// A fatal, malformed or oversized frame, a stream ending without a terminal frame, a 404, a 501 or another 4xx fails a v2 shard upload after one request.
func TestUploadShardV2FailsAtOnce(t *testing.T) {
	status := func(code int) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
	}
	for _, tc := range []struct {
		name  string
		serve http.HandlerFunc
	}{
		{"fatal frame", ndjsonFrames(`{"type":"error","message":"rejected","retryable":false}`)},
		{"malformed frame", ndjsonFrames(`{"type":`)},
		{"oversized frame", ndjsonFrames(strings.Repeat("x", maxShardUploadEventSize+1))},
		{"no terminal frame", ndjsonFrames(`{"type":"validating","verified":1,"total":1}`)},
		{"400", status(http.StatusBadRequest)},
		{"404", status(http.StatusNotFound)},
		{"501", status(http.StatusNotImplemented)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				tc.serve(w, r)
			}))
			defer srv.Close()
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.UploadShardV2(t.Context(), shard.NewShard()); err == nil {
				t.Fatal("UploadShardV2 succeeded, want it to fail")
			}
			if n := requests.Load(); n != 1 {
				t.Fatalf("requests = %d, want 1", n)
			}
		})
	}
}

// A v2 shard upload whose response stream breaks before a terminal frame, between frames or inside one, is tried again.
func TestUploadShardV2RetriesBrokenStream(t *testing.T) {
	for _, tc := range []struct{ name, tail string }{
		{"between frames", ""},
		{"inside a frame", `{"type":"valid`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				if requests.Add(1) == 1 {
					ndjsonFrames(`{"type":"validating","verified":0,"total":1}`)(w, r)
					_, _ = io.WriteString(w, tc.tail)
					w.(http.Flusher).Flush()
					panic(http.ErrAbortHandler)
				}
				ndjsonFrames(`{"type":"result"}`)(w, r)
			}))
			defer srv.Close()
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.UploadShardV2(t.Context(), shard.NewShard()); err != nil {
				t.Fatalf("UploadShardV2: %v", err)
			}
			if n := requests.Load(); n != 2 {
				t.Fatalf("requests = %d, want 2", n)
			}
		})
	}
}

// cyclingServer answers request n with responses[n % len(responses)], counting the requests.
func cyclingServer(requests *atomic.Int32, responses ...http.HandlerFunc) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		n := requests.Add(1) - 1
		responses[int(n)%len(responses)](w, r)
	}))
}

func unavailable(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusServiceUnavailable)
}

// A v2 shard upload's request and stream are one attempt, so a 503 and a retryable frame take the first two waits of one schedule.
func TestUploadShardV2SharesRetryWaits(t *testing.T) {
	nominal, _ := recordRetryWaits(t)
	var requests atomic.Int32
	srv := cyclingServer(&requests, unavailable, ndjsonFrames(`{"type":"error","message":"transient","retryable":true}`), ndjsonFrames(`{"type":"result"}`))
	defer srv.Close()
	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetries(2), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadShardV2(t.Context(), shard.NewShard()); err != nil {
		t.Fatalf("UploadShardV2: %v", err)
	}
	if n := requests.Load(); n != 3 {
		t.Fatalf("requests = %d, want 3", n)
	}
	if want := []time.Duration{3 * time.Second, 9 * time.Second}; !slices.Equal(*nominal, want) {
		t.Fatalf("nominal waits = %v, want %v", *nominal, want)
	}
}

// 503s and retryable frames in turn exhaust one count of retries.
func TestUploadShardV2SharesRetryCount(t *testing.T) {
	var requests atomic.Int32
	srv := cyclingServer(&requests, unavailable, ndjsonFrames(`{"type":"error","message":"transient","retryable":true}`))
	defer srv.Close()
	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetries(2), WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UploadShardV2(t.Context(), shard.NewShard()); err == nil {
		t.Fatal("UploadShardV2 succeeded, want the retries exhausted")
	}
	if n := requests.Load(); n != 3 {
		t.Fatalf("requests = %d, want 3", n)
	}
}

// A resumed download whose reconstruction query keeps failing transiently stops after 1 + retries requests instead of retrying them all without Range.
func TestResumedDownloadKeepsRangeAfterRetries(t *testing.T) {
	entryPoints := map[string]func(*Client, context.Context, xet.FileHash, io.WriteSeeker) error{
		"v1": (*Client).DownloadFileV1,
		"v2": (*Client).DownloadFileV2,
	}
	for name, downloadFile := range entryPoints {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			srv := cyclingServer(&requests, unavailable)
			defer srv.Close()
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetries(2), WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
			if err != nil {
				t.Fatal(err)
			}
			_, err = downloadInto(t, []byte("partial download"), func(w io.WriteSeeker) error {
				return downloadFile(c, t.Context(), xet.FileHash{}, w)
			})
			if err == nil {
				t.Fatal("download succeeded, want the retries exhausted")
			}
			if n := requests.Load(); n != 3 {
				t.Fatalf("requests = %d, want 3", n)
			}
		})
	}
}

// A resumed download whose context ends while a retry waits keeps its offset, sends nothing more and returns the context's error.
func TestResumedDownloadKeepsOffsetWhenCancelledDuringWait(t *testing.T) {
	entryPoints := map[string]func(*Client, context.Context, xet.FileHash, io.WriteSeeker) error{
		"v1": (*Client).DownloadFileV1,
		"v2": (*Client).DownloadFileV2,
	}
	for name, downloadFile := range entryPoints {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			srv := cyclingServer(&requests, unavailable)
			defer srv.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			sleep := retrySleep
			t.Cleanup(func() { retrySleep = sleep })
			retrySleep = func(ctx context.Context, d time.Duration) error {
				cancel()
				return sleep(ctx, d)
			}
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetryBaseDelay(time.Hour), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
			if err != nil {
				t.Fatal(err)
			}
			prefix := []byte("partial download")
			var pos int64
			got, err := downloadInto(t, prefix, func(w io.WriteSeeker) error {
				err := downloadFile(c, ctx, xet.FileHash{}, w)
				pos, _ = w.Seek(0, io.SeekCurrent)
				return err
			})
			if pos != int64(len(prefix)) || !bytes.Equal(got, prefix) {
				t.Fatalf("destination at offset %d with %d bytes, want untouched at %d", pos, len(got), len(prefix))
			}
			if n := requests.Load(); n != 1 {
				t.Fatalf("requests = %d, want 1", n)
			}
			if !errors.Is(err, context.Canceled) || errors.Is(err, errRetriesExhausted) {
				t.Fatalf("err = %v, want the context's error from the wait", err)
			}
		})
	}
}

// providerFunc adapts a function to UpstreamProvider.
type providerFunc func(context.Context, auth.Permission) (string, string, error)

func (f providerFunc) Resolve(ctx context.Context, perm auth.Permission) (string, string, error) {
	return f(ctx, perm)
}

// A retry resolves its request's permission again after the wait and carries the new token; each request costs one Resolve.
func TestRetryResolvesCredentialAgain(t *testing.T) {
	emptyJSON := func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") }
	uploadShardV2 := func(ctx context.Context, c *Client) error {
		_, err := c.UploadShardV2(ctx, shard.NewShard())
		return err
	}
	for _, tc := range []struct {
		name          string
		perm          auth.Permission
		first, second http.HandlerFunc
		call          func(context.Context, *Client) error
	}{
		{"GET reconstruction", auth.Read, unavailable, emptyJSON, func(ctx context.Context, c *Client) error {
			_, err := c.GetReconstructionV1(ctx, xet.FileHash{}, nil)
			return err
		}},
		{"POST xorb", auth.Write, unavailable, emptyJSON, func(ctx context.Context, c *Client) error {
			_, err := c.UploadXorb(ctx, xet.XorbHash{}, bytes.NewReader([]byte("payload")))
			return err
		}},
		{"v2 shard after a 503", auth.Write, unavailable, ndjsonFrames(`{"type":"result"}`), uploadShardV2},
		{"v2 shard after a retryable frame", auth.Write, ndjsonFrames(`{"type":"error","message":"transient","retryable":true}`), ndjsonFrames(`{"type":"result"}`), uploadShardV2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var auths []string
			var perms []auth.Permission
			token := "A"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				mu.Lock()
				auths = append(auths, r.Header.Get("Authorization"))
				first := len(auths) == 1
				mu.Unlock()
				if first {
					tc.first(w, r)
					return
				}
				tc.second(w, r)
			}))
			defer srv.Close()
			p := providerFunc(func(_ context.Context, perm auth.Permission) (string, string, error) {
				mu.Lock()
				defer mu.Unlock()
				perms = append(perms, perm)
				return srv.URL, token, nil
			})
			sleep := retrySleep
			t.Cleanup(func() { retrySleep = sleep })
			retrySleep = func(context.Context, time.Duration) error {
				mu.Lock()
				defer mu.Unlock()
				token = "B"
				return nil
			}
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(p))
			if err != nil {
				t.Fatal(err)
			}
			if err := tc.call(t.Context(), c); err != nil {
				t.Fatalf("call: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			if want := []string{"Bearer A", "Bearer B"}; !slices.Equal(auths, want) {
				t.Fatalf("authorization per request = %q, want %q", auths, want)
			}
			if len(perms) != len(auths) {
				t.Fatalf("resolves = %d, want one per request (%d)", len(perms), len(auths))
			}
			if want := []auth.Permission{tc.perm, tc.perm}; !slices.Equal(perms, want) {
				t.Fatalf("permission per resolve = %q, want %q", perms, want)
			}
		})
	}
}

// A failed re-resolution sends nothing more and ends the call like a failed first resolution: a resumed download keeps its offset.
func TestRetryStopsWhenResolvingAgainFails(t *testing.T) {
	errBoom := errors.New("boom")
	entryPoints := map[string]func(*Client, context.Context, xet.FileHash, io.WriteSeeker) error{
		"v1": (*Client).DownloadFileV1,
		"v2": (*Client).DownloadFileV2,
	}
	for name, downloadFile := range entryPoints {
		t.Run(name, func(t *testing.T) {
			var requests, resolves atomic.Int32
			srv := cyclingServer(&requests, unavailable)
			defer srv.Close()
			p := providerFunc(func(context.Context, auth.Permission) (string, string, error) {
				if resolves.Add(1) > 1 {
					return "", "", errBoom
				}
				return srv.URL, "A", nil
			})
			c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(p))
			if err != nil {
				t.Fatal(err)
			}
			prefix := []byte("partial download")
			var pos int64
			got, err := downloadInto(t, prefix, func(w io.WriteSeeker) error {
				err := downloadFile(c, t.Context(), xet.FileHash{}, w)
				pos, _ = w.Seek(0, io.SeekCurrent)
				return err
			})
			if !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want errBoom", err)
			}
			if n := requests.Load(); n != 1 {
				t.Fatalf("requests = %d, want 1", n)
			}
			if pos != int64(len(prefix)) || !bytes.Equal(got, prefix) {
				t.Fatalf("destination at offset %d with %d bytes, want untouched at %d", pos, len(got), len(prefix))
			}
		})
	}
}

// A retry is not sent once the provider names another endpoint: the request was built for the first, and the new token belongs to the other.
func TestRetryRefusesChangedEndpoint(t *testing.T) {
	var before, after atomic.Int32
	first := cyclingServer(&before, unavailable)
	defer first.Close()
	moved := cyclingServer(&after, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "{}") })
	defer moved.Close()
	var resolves atomic.Int32
	p := providerFunc(func(context.Context, auth.Permission) (string, string, error) {
		if resolves.Add(1) > 1 {
			return moved.URL, "B", nil
		}
		return first.URL, "A", nil
	})
	c, err := NewClient(WithRetryBaseDelay(time.Millisecond), WithUpstreamProvider(p))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.GetReconstructionV1(t.Context(), xet.FileHash{}, nil)
	if !isAuthError(err) || !strings.Contains(err.Error(), moved.URL) {
		t.Fatalf("err = %v, want an auth error naming %s", err, moved.URL)
	}
	if a, b := before.Load(), after.Load(); a != 1 || b != 0 {
		t.Fatalf("requests: %d to the first endpoint (want 1), %d to the new one (want 0)", a, b)
	}
}
