package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet/shard"
)

func TestUploadShardV2ReadsNDJSONUntilResult(t *testing.T) {
	var requestBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method: got %s want POST", r.Method)
		}
		if r.URL.Path != "/v2/shards" {
			t.Errorf("path: got %s want /v2/shards", r.URL.Path)
		}
		if r.ContentLength <= 0 {
			t.Errorf("expected a positive Content-Length, got %d", r.ContentLength)
		}
		var err error
		requestBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}

		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"validating\",\"verified\":1,\"total\":1}\n")
		_, _ = io.WriteString(w, "{\"type\":\"committing\",\"stage\":\"syncing\"}\n")
		_, _ = io.WriteString(w, "{\"type\":\"result\"}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	response, err := c.UploadShardV2(t.Context(), shard.NewShard())
	if err != nil {
		t.Fatalf("UploadShardV2: %v", err)
	}
	if response.Result != 1 {
		t.Fatalf("result: got %d want 1", response.Result)
	}
	if len(requestBody) == 0 {
		t.Fatal("expected a shard request body")
	}
}

func TestUploadShardV2ReportsTerminalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"error\",\"message\":\"rejected\",\"retryable\":false}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.UploadShardV2(t.Context(), shard.NewShard())
	if err == nil || !strings.Contains(err.Error(), "rejected") {
		t.Fatalf("expected terminal error, got %v", err)
	}
}

func TestUploadShardV2RequiresResultEvent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"validating\",\"verified\":1,\"total\":1}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.UploadShardV2(t.Context(), shard.NewShard())
	if err == nil || !strings.Contains(err.Error(), "without a result event") {
		t.Fatalf("expected missing-result error, got %v", err)
	}
}

func TestUploadShardV2RetriesRetryableError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		if attempts.Add(1) == 1 {
			_, _ = io.WriteString(w, "{\"type\":\"error\",\"message\":\"transient\",\"retryable\":true}\n")
			return
		}
		_, _ = io.WriteString(w, "{\"type\":\"result\"}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()), WithRetries(1))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	response, err := c.UploadShardV2(t.Context(), shard.NewShard())
	if err != nil {
		t.Fatalf("UploadShardV2: %v", err)
	}
	if response.Result != 1 {
		t.Fatalf("result: got %d want 1", response.Result)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts: got %d want 2", got)
	}
}

func TestUploadShardV2ExhaustsRetryableRetries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		attempts.Add(1)
		_, _ = io.WriteString(w, "{\"type\":\"error\",\"message\":\"transient\",\"retryable\":true}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()), WithRetries(1))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.UploadShardV2(t.Context(), shard.NewShard())
	if err == nil || !strings.Contains(err.Error(), "after 2 attempts") {
		t.Fatalf("expected retry exhaustion error, got %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts: got %d want 2", got)
	}
}

func TestUploadShardV2SkipsUnknownFrames(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"validating\",\"verified\":0,\"total\":1}\n")
		_, _ = io.WriteString(w, "{\"type\":\"heartbeat\"}\n")
		_, _ = io.WriteString(w, "{\"type\":\"result\"}\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	response, err := c.UploadShardV2(t.Context(), shard.NewShard())
	if err != nil {
		t.Fatalf("UploadShardV2: %v", err)
	}
	if response.Result != 1 {
		t.Fatalf("result: got %d want 1", response.Result)
	}
}

func TestUploadShardV2RejectsOversizedFrame(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, strings.Repeat("x", maxShardUploadEventSize+1)+"\n")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	_, err = c.UploadShardV2(t.Context(), shard.NewShard())
	if err == nil {
		t.Fatal("expected oversized frame error, got nil")
	}
}

func TestUploadShardV2HandlesTrailingFrameWithoutNewline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = io.WriteString(w, "{\"type\":\"validating\",\"verified\":1,\"total\":1}\n")
		_, _ = io.WriteString(w, "{\"type\":\"result\"}")
	}))
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	response, err := c.UploadShardV2(t.Context(), shard.NewShard())
	if err != nil {
		t.Fatalf("UploadShardV2: %v", err)
	}
	if response.Result != 1 {
		t.Fatalf("result: got %d want 1", response.Result)
	}
}
