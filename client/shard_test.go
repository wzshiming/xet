package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

func TestQueryChunkDeduplicationNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))

	hash := xet.Hash{}
	result, err := client.QueryChunkDeduplication(context.Background(), hash)
	if err != nil {
		t.Fatalf("QueryChunkDeduplication failed: %v", err)
	}

	if result != nil {
		t.Error("Expected nil result for 404")
	}
}

func TestQueryChunkDeduplicationFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return a minimal valid shard
		shardObj := shard.NewShard()
		reader, _ := shard.Encode(shardObj, false)
		if _, err := io.Copy(w, reader); err != nil {
			t.Fatalf("copy shard response: %v", err)
		}
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))

	hash := xet.Hash{}
	result, err := client.QueryChunkDeduplication(context.Background(), hash)
	if err != nil {
		t.Fatalf("QueryChunkDeduplication failed: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}
}

func TestQueryChunkDeduplicationUsesPersistentCache(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		shardObj := shard.NewShard()
		reader, _ := shard.Encode(shardObj, false)
		if _, err := io.Copy(w, reader); err != nil {
			t.Fatalf("copy shard response: %v", err)
		}
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))

	hash := xet.Hash{1, 2, 3}
	if _, err := client.QueryChunkDeduplication(context.Background(), hash); err != nil {
		t.Fatalf("first query failed: %v", err)
	}
	if _, err := client.QueryChunkDeduplication(context.Background(), hash); err != nil {
		t.Fatalf("second query failed: %v", err)
	}

	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("expected one network hit due to cache reuse, got %d", got)
	}
}
