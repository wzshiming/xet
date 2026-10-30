package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
)

const (
	postV1Shards = "POST /v1/shards"
	postV2Shards = "POST /v2/shards"
)

// shardAPICAS stores nothing, so every dedup query and xorb HEAD misses; uploads succeed unless respond overrides them.
type shardAPICAS struct {
	mu       sync.Mutex
	requests []string
	status   map[string]int
}

func newShardAPICAS(t *testing.T) (*shardAPICAS, *httptest.Server) {
	t.Helper()
	cas := &shardAPICAS{status: map[string]int{}}
	srv := httptest.NewServer(cas)
	t.Cleanup(srv.Close)
	return cas, srv
}

// respond answers request ("METHOD path") with status from now on; 0 restores the success reply.
func (f *shardAPICAS) respond(request string, status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[request] = status
}

func (f *shardAPICAS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	request := r.Method + " " + r.URL.Path
	f.mu.Lock()
	f.requests = append(f.requests, request)
	status := f.status[request]
	f.mu.Unlock()
	switch {
	case status != 0:
		w.WriteHeader(status)
	case request == postV1Shards:
		_, _ = io.WriteString(w, `{"result":1}`)
	case request == postV2Shards:
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n{\"type\":\"result\"}\n")
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/xorbs/"):
		_, _ = io.WriteString(w, `{"was_inserted":true}`)
	default:
		http.NotFound(w, r)
	}
}

// wantShardRequests checks the shard uploads cas saw since the previous check.
func wantShardRequests(t *testing.T, cas *shardAPICAS, step string, want ...string) {
	t.Helper()
	cas.mu.Lock()
	var got []string
	for _, request := range cas.requests {
		if strings.HasSuffix(request, "/shards") {
			got = append(got, request)
		}
	}
	cas.requests = nil
	cas.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("%s: shard requests = %q, want %q", step, got, want)
	}
}

func newShardAPIClient(t *testing.T, baseURL string, cache *Cache) *Client {
	t.Helper()
	c, err := NewClient(WithCache(cache), WithRetries(0), WithUpstreamProvider(StaticUpstreamProvider(baseURL, "")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestUploadFileFallsBackToV1ShardAPI(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusNotImplemented} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			cas, srv := newShardAPICAS(t)
			cas.respond(postV2Shards, status)
			shared := NewCache(t.TempDir(), 0, 0)
			c := newShardAPIClient(t, srv.URL, shared)

			hash, err := c.UploadFile(t.Context(), strings.NewReader("first"))
			wantShardRequests(t, cas, "first UploadFile", postV2Shards, postV1Shards)
			if err != nil || hash == (xet.FileHash{}) {
				t.Fatalf("first UploadFile = %s, %v; want a file hash", hash, err)
			}
			_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
			wantShardRequests(t, cas, "second UploadFile on the same client", postV1Shards)
			if err != nil {
				t.Fatalf("second UploadFile: %v", err)
			}
			_, err = newShardAPIClient(t, srv.URL, shared).UploadFile(t.Context(), strings.NewReader("third"))
			wantShardRequests(t, cas, "UploadFile on a new client sharing the cache", postV2Shards, postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile on a new client: %v", err)
			}
		})
	}
}

func TestUploadFilesFallsBackToV1ShardAPI(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.respond(postV2Shards, http.StatusNotFound)
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	hashes, err := c.UploadFiles(t.Context(), []io.ReadSeeker{strings.NewReader("first"), strings.NewReader("second")})
	wantShardRequests(t, cas, "first UploadFiles", postV2Shards, postV1Shards)
	if err != nil || len(hashes) != 2 {
		t.Fatalf("first UploadFiles = %v, %v; want two file hashes", hashes, err)
	}
	_, err = c.UploadFiles(t.Context(), []io.ReadSeeker{strings.NewReader("third")})
	wantShardRequests(t, cas, "second UploadFiles on the same client", postV1Shards)
	if err != nil {
		t.Fatalf("second UploadFiles: %v", err)
	}
}

func TestUploadFileDoesNotFallBackOnV2ServerError(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.respond(postV2Shards, http.StatusInternalServerError)
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShardRequests(t, cas, "UploadFile against a failing /v2/shards", postV2Shards)
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("UploadFile error = %v, want the /v2/shards 500", err)
	}
	cas.respond(postV2Shards, 0)
	_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
	wantShardRequests(t, cas, "UploadFile after /v2/shards recovered", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile after /v2/shards recovered: %v", err)
	}
}

func TestUploadFileFailedV1FallbackIsNotRemembered(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	cas.respond(postV2Shards, http.StatusNotFound)
	cas.respond(postV1Shards, http.StatusInternalServerError)
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShardRequests(t, cas, "UploadFile against a failing /v1/shards", postV2Shards, postV1Shards)
	if err == nil || !strings.Contains(err.Error(), "v1 shard upload after v2 fallback") {
		t.Fatalf("UploadFile error = %v, want the v1 shard upload after v2 fallback failure", err)
	}
	cas.respond(postV1Shards, 0)
	_, err = c.UploadFile(t.Context(), strings.NewReader("second"))
	wantShardRequests(t, cas, "UploadFile after /v1/shards recovered", postV2Shards, postV1Shards)
	if err != nil {
		t.Fatalf("UploadFile after /v1/shards recovered: %v", err)
	}
}

func TestUploadFileV1BypassesNegotiatedShardAPI(t *testing.T) {
	cas, srv := newShardAPICAS(t)
	c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

	_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
	wantShardRequests(t, cas, "UploadFile", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	_, err = c.UploadFileV1(t.Context(), strings.NewReader("second"))
	wantShardRequests(t, cas, "UploadFileV1", postV1Shards)
	if err != nil {
		t.Fatalf("UploadFileV1: %v", err)
	}
	_, err = c.UploadFile(t.Context(), strings.NewReader("third"))
	wantShardRequests(t, cas, "UploadFile after UploadFileV1", postV2Shards)
	if err != nil {
		t.Fatalf("UploadFile after UploadFileV1: %v", err)
	}
}

func TestUploadFileV2BypassesNegotiatedShardAPI(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		wantErr bool
	}{{"rejected", http.StatusNotFound, true}, {"accepted", 0, false}} {
		t.Run(tc.name, func(t *testing.T) {
			cas, srv := newShardAPICAS(t)
			cas.respond(postV2Shards, http.StatusNotFound)
			c := newShardAPIClient(t, srv.URL, NewCache(t.TempDir(), 0, 0))

			_, err := c.UploadFile(t.Context(), strings.NewReader("first"))
			wantShardRequests(t, cas, "UploadFile", postV2Shards, postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile: %v", err)
			}
			cas.respond(postV2Shards, tc.status)
			_, err = c.UploadFileV2(t.Context(), strings.NewReader("second"))
			wantShardRequests(t, cas, "UploadFileV2", postV2Shards)
			if (err != nil) != tc.wantErr {
				t.Fatalf("UploadFileV2 error = %v, want error %t", err, tc.wantErr)
			}
			_, err = c.UploadFile(t.Context(), strings.NewReader("third"))
			wantShardRequests(t, cas, "UploadFile after UploadFileV2", postV1Shards)
			if err != nil {
				t.Fatalf("UploadFile after UploadFileV2: %v", err)
			}
		})
	}
}
