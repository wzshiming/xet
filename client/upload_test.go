package client

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

const (
	postV1Shards = "POST /v1/shards"
	postV2Shards = "POST /v2/shards"
)

// shardAPICAS stores nothing, so dedup queries and xorb HEADs miss; uploads succeed unless a route's answer is replaced.
type shardAPICAS struct {
	mu      sync.Mutex
	shards  []string
	answers map[string]http.HandlerFunc
}

func newShardAPICAS(t *testing.T) (*shardAPICAS, *httptest.Server) {
	t.Helper()
	cas := &shardAPICAS{answers: map[string]http.HandlerFunc{}}
	srv := httptest.NewServer(cas)
	t.Cleanup(srv.Close)
	return cas, srv
}

// answer replaces the answer to route ("METHOD path"); nil restores success.
func (f *shardAPICAS) answer(route string, h http.HandlerFunc) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.answers[route] = h
}

func (f *shardAPICAS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	route := r.Method + " " + r.URL.Path
	f.mu.Lock()
	if route == postV1Shards || route == postV2Shards {
		f.shards = append(f.shards, route)
	}
	answer := f.answers[route]
	f.mu.Unlock()
	switch {
	case answer != nil:
		answer(w, r)
	case route == postV1Shards:
		_, _ = io.WriteString(w, `{"result":1}`)
	case route == postV2Shards:
		_, _ = io.WriteString(w, `{"type":"result"}`+"\n")
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/xorbs/"):
		_, _ = io.WriteString(w, `{"was_inserted":true}`)
	default:
		http.NotFound(w, r)
	}
}

func answerStatus(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

// wantShards checks the shard uploads cas saw since the previous check.
func wantShards(t *testing.T, cas *shardAPICAS, step string, want ...string) {
	t.Helper()
	cas.mu.Lock()
	got := cas.shards
	cas.shards = nil
	cas.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: shard uploads = %q, want %q", step, got, want)
	}
}

// newShardAPIClient never retries, so every failing request is sent once.
func newShardAPIClient(t *testing.T, baseURL string, cache *Cache) *Client {
	t.Helper()
	c, err := NewClient(WithCache(cache), WithRetries(0), WithUpstreamProvider(StaticUpstreamProvider(baseURL, "")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// A /v2/shards 404 or 501 sends the same upload to /v1/shards, where that Client and its copies then stay.
func TestUploadFileFallsBackToV1Shards(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusNotImplemented} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			cas, srv := newShardAPICAS(t)
			cas.answer(postV2Shards, answerStatus(code))
			shared := NewCache(t.TempDir(), 0, 0)
			c := newShardAPIClient(t, srv.URL, shared)

			_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
			wantShards(t, cas, "first UploadFile", postV2Shards, postV1Shards)
			if err != nil {
				t.Fatalf("first UploadFile: %v", err)
			}
			_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
			wantShards(t, cas, "second UploadFile on the same Client", postV1Shards)
			if err != nil {
				t.Fatalf("second UploadFile: %v", err)
			}
			_, err = newShardAPIClient(t, srv.URL, shared).UploadFile(t.Context(), strings.NewReader("third"))
			wantShards(t, cas, "UploadFile on a new Client sharing the Cache", postV2Shards, postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile on a new Client: %v", err)
			}
		})
	}
}

func TestUploadFilesFallsBackToV1Shards(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.answer(postV2Shards, answerStatus(http.StatusNotFound))
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFiles(t.Context(), []io.ReadSeeker{strings.NewReader("first"), strings.NewReader("second")})
	wantShards(t, cas, "first UploadFiles", postV2Shards, postV1Shards)
	if err != nil {
		t.Fatalf("first UploadFiles: %v", err)
	}
	_, err = c.UploadFiles(t.Context(), []io.ReadSeeker{strings.NewReader("third")})
	wantShards(t, cas, "second UploadFiles on the same Client", postV1Shards)
	if err != nil {
		t.Fatalf("second UploadFiles: %v", err)
	}
}

// A /v2/shards failure other than 404 or 501 comes back as is, with no fallback and nothing remembered.
func TestUploadFileReturnsOtherV2Failures(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.answer(postV2Shards, answerStatus(http.StatusInternalServerError))
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShards(t, cas, "UploadFile while /v2/shards answers 500", postV2Shards)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("UploadFile error = %v, want the /v2/shards 500", err)
	}
	cas.answer(postV2Shards, nil)
	_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
	wantShards(t, cas, "UploadFile after /v2/shards recovered", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile after /v2/shards recovered: %v", err)
	}
}

func TestUploadFileReturnsFatalV2ErrorFrame(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.answer(postV2Shards, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"type":"error","message":"shard rejected","retryable":false}`+"\n")
	})
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShards(t, cas, "UploadFile against a fatal error frame", postV2Shards)
	if err == nil || !strings.Contains(err.Error(), "shard rejected") {
		t.Fatalf("UploadFile error = %v, want the error frame's message", err)
	}
}

// A failed /v1/shards fallback comes back and is not remembered.
func TestUploadFileForgetsFailedV1Fallback(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.answer(postV2Shards, answerStatus(http.StatusNotFound))
	cas.answer(postV1Shards, answerStatus(http.StatusBadRequest))
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShards(t, cas, "UploadFile while /v1/shards answers 400", postV2Shards, postV1Shards)
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("UploadFile error = %v, want the /v1/shards 400", err)
	}
	cas.answer(postV1Shards, nil)
	_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
	wantShards(t, cas, "UploadFile after /v1/shards recovered", postV2Shards, postV1Shards)
	if err != nil {
		t.Fatalf("UploadFile after /v1/shards recovered: %v", err)
	}
}

// An automatic /v2/shards success puts the Client back on /v2/shards, even after a concurrent upload fell back.
func TestUploadFileRemembersLateV2Success(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	held, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	cas.answer(postV2Shards, func(w http.ResponseWriter, r *http.Request) {
		if !first.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		close(held)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, `{"type":"result"}`+"\n")
	})
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	accepted := make(chan error, 1)
	go func() {
		_, err := c.UploadFile(t.Context(), strings.NewReader("accepted late"))
		accepted <- err
	}()
	select {
	case <-held:
	case err := <-accepted:
		t.Fatalf("UploadFile returned %v before its /v2/shards was answered", err)
	}
	if _, err := c.UploadFile(t.Context(), strings.NewReader("fell back")); err != nil {
		t.Fatalf("UploadFile falling back: %v", err)
	}
	close(release)
	if err := <-accepted; err != nil {
		t.Fatalf("UploadFile accepted late: %v", err)
	}
	wantShards(t, cas, "concurrent UploadFile calls", postV2Shards, postV2Shards, postV1Shards)
	cas.answer(postV2Shards, nil)
	_, err := c.UploadFile(t.Context(), strings.NewReader("after"))
	wantShards(t, cas, "UploadFile after the late /v2/shards success", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile after the late /v2/shards success: %v", err)
	}
}

// Concurrent automatic uploads on one Client all fall back, and the Client then stays on /v1/shards.
func TestUploadFileConcurrentFallback(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.answer(postV2Shards, answerStatus(http.StatusNotFound))
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			if _, err := c.UploadFile(t.Context(), strings.NewReader("concurrent "+strconv.Itoa(i))); err != nil {
				t.Errorf("UploadFile %d: %v", i, err)
			}
		})
	}
	wg.Wait()
	cas.mu.Lock()
	cas.shards = nil
	cas.mu.Unlock()
	_, err := c.UploadFile(t.Context(), strings.NewReader("after"))
	wantShards(t, cas, "UploadFile after the concurrent fallbacks", postV1Shards)
	if err != nil {
		t.Fatalf("UploadFile after the concurrent fallbacks: %v", err)
	}
}

// A Client copied by value and its original share the version automatic uploads settled on, either way round.
func TestUploadFileCopiesShareNegotiatedVersion(t *testing.T) {
	for _, tc := range []struct {
		name          string
		copyFallsBack bool
	}{{"copy falls back", true}, {"original falls back", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cas, srv := newShardAPICAS(t)
			cas.answer(postV2Shards, answerStatus(http.StatusNotFound))
			original := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))
			copied := *original
			fallsBack, other := original, &copied
			if tc.copyFallsBack {
				fallsBack, other = &copied, original
			}

			_, err := fallsBack.UploadFile(t.Context(), strings.NewReader("first"))
			wantShards(t, cas, "UploadFile that falls back", postV2Shards, postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile that falls back: %v", err)
			}
			_, err = other.UploadFile(t.Context(), strings.NewReader("second"))
			wantShards(t, cas, "UploadFile on the other Client", postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile on the other Client: %v", err)
			}
		})
	}
}

// A copy rebound to a CAS without /v2/shards falls back from a remembered 2 and moves its original to /v1/shards.
func TestUploadFileReboundCopySharesNegotiatedVersion(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	v1Only, v1OnlySrv := newShardAPICAS(t)
	v1Only.answer(postV2Shards, answerStatus(http.StatusNotFound))
	original := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))
	rebound := *original
	WithUpstreamProvider(StaticUpstreamProvider(v1OnlySrv.URL, ""))(&rebound)

	_, err := original.UploadFile(t.Context(), strings.NewReader("first"))
	wantShards(t, cas, "UploadFile on the original", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile on the original: %v", err)
	}
	_, err = rebound.UploadFile(t.Context(), strings.NewReader("second"))
	wantShards(t, v1Only, "UploadFile on the copy rebound to a CAS without /v2/shards", postV2Shards, postV1Shards)
	if err != nil {
		t.Fatalf("UploadFile on the rebound copy: %v", err)
	}
	_, err = original.UploadFile(t.Context(), strings.NewReader("third"))
	wantShards(t, cas, "UploadFile on the original after its copy fell back", postV1Shards)
	if err != nil {
		t.Fatalf("UploadFile on the original after its copy fell back: %v", err)
	}
}

// Copying a Client by value while it uploads without a forced version is no data race under -race.
func TestUploadFileWhileClientIsCopied(t *testing.T) {
	_, srv := newShardAPICAS(t)
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	var copied Client
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				copied = *c
				// Spinning would starve the uploads at GOMAXPROCS=1.
				time.Sleep(50 * time.Microsecond)
			}
		}
	})
	for i := range 20 {
		if _, err := c.UploadFile(t.Context(), strings.NewReader("copied meanwhile "+strconv.Itoa(i))); err != nil {
			t.Errorf("UploadFile %d: %v", i, err)
		}
	}
	close(stop)
	wg.Wait()
	_ = copied
}

// forcedUpload is a method that forces a shard API version, given one file.
type forcedUpload struct {
	name   string
	upload func(*Client, context.Context, io.ReadSeeker) (xet.FileHash, error)
}

// oneFile hands a multi-file upload method a single file.
func oneFile(upload func(*Client, context.Context, []io.ReadSeeker) ([]xet.FileHash, error)) func(*Client, context.Context, io.ReadSeeker) (xet.FileHash, error) {
	return func(c *Client, ctx context.Context, r io.ReadSeeker) (xet.FileHash, error) {
		hashes, err := upload(c, ctx, []io.ReadSeeker{r})
		if err != nil {
			return xet.FileHash{}, err
		}
		return hashes[0], nil
	}
}

// UploadFileV1 and UploadFilesV1 send /v1/shards alone and leave an automatic Client on /v2/shards.
func TestUploadFileV1KeepsNegotiatedVersion(t *testing.T) {
	for _, forced := range []forcedUpload{{"UploadFileV1", (*Client).UploadFileV1}, {"UploadFilesV1", oneFile((*Client).UploadFilesV1)}} {
		t.Run(forced.name, func(t *testing.T) {
			cas, srv := newShardAPICAS(t)
			c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

			_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
			wantShards(t, cas, "UploadFile", postV2Shards)
			if err != nil {
				t.Fatalf("UploadFile: %v", err)
			}
			_, err = forced.upload(c, t.Context(), strings.NewReader("second"))
			wantShards(t, cas, forced.name, postV1Shards)
			if err != nil {
				t.Fatalf("%s: %v", forced.name, err)
			}
			_, err = c.UploadFile(t.Context(), strings.NewReader("third"))
			wantShards(t, cas, "UploadFile after "+forced.name, postV2Shards)
			if err != nil {
				t.Fatalf("UploadFile after %s: %v", forced.name, err)
			}
		})
	}
}

// UploadFileV2 and UploadFilesV2 send /v2/shards alone, even on a 404, and leave a Client settled on /v1/shards there.
func TestUploadFileV2KeepsNegotiatedVersion(t *testing.T) {
	for _, forced := range []forcedUpload{{"UploadFileV2", (*Client).UploadFileV2}, {"UploadFilesV2", oneFile((*Client).UploadFilesV2)}} {
		t.Run(forced.name, func(t *testing.T) {
			for _, tc := range []struct {
				name    string
				answer  http.HandlerFunc
				wantErr error
			}{{"404", answerStatus(http.StatusNotFound), errNotFound}, {"accepted", nil, nil}} {
				t.Run(tc.name, func(t *testing.T) {
					cas, srv := newShardAPICAS(t)
					cas.answer(postV2Shards, answerStatus(http.StatusNotFound))
					c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

					_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
					wantShards(t, cas, "UploadFile", postV2Shards, postV1Shards)
					if err != nil {
						t.Fatalf("UploadFile: %v", err)
					}
					cas.answer(postV2Shards, tc.answer)
					_, err = forced.upload(c, t.Context(), strings.NewReader("second"))
					wantShards(t, cas, forced.name, postV2Shards)
					if !errors.Is(err, tc.wantErr) {
						t.Fatalf("%s error = %v, want %v", forced.name, err, tc.wantErr)
					}
					_, err = c.UploadFile(t.Context(), strings.NewReader("third"))
					wantShards(t, cas, "UploadFile after "+forced.name, postV1Shards)
					if err != nil {
						t.Fatalf("UploadFile after %s: %v", forced.name, err)
					}
				})
			}
		})
	}
}
