package client

import (
	"context"
	"encoding/json"
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

func TestQueryChunksDeduplicationBatchEndpoint(t *testing.T) {
	chunkFound := xet.Hash{1, 2, 3}
	chunkMissing := xet.Hash{4, 5, 6}
	xorbHash := xet.Hash{9, 9, 9}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chunks/default:query" {
			http.NotFound(w, r)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{
					"chunk_hash":  chunkFound.String(),
					"found":       true,
					"xorb_hash":   xorbHash.String(),
					"chunk_index": uint32(0),
				},
				{
					"chunk_hash": chunkMissing.String(),
					"found":      false,
				},
			},
		})
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))

	results, err := client.QueryChunksDeduplication(context.Background(), []xet.Hash{chunkFound, chunkMissing})
	if err != nil {
		t.Fatalf("QueryChunksDeduplication failed: %v", err)
	}

	found, ok := results[chunkFound]
	if !ok || found == nil {
		t.Fatal("missing dedup result for found chunk")
	}
	if found.IsNew {
		t.Fatal("expected found chunk to be marked as existing")
	}
	if found.XorbHash != xorbHash {
		t.Fatalf("unexpected xorb hash: got %s want %s", found.XorbHash.String(), xorbHash.String())
	}

	missing, ok := results[chunkMissing]
	if !ok || missing == nil {
		t.Fatal("missing dedup result for missing chunk")
	}
	if !missing.IsNew {
		t.Fatal("expected missing chunk to be marked as new")
	}
}

func TestQueryChunksDeduplicationFallsBackToSingleQuery(t *testing.T) {
	var postCalls atomic.Int32
	var getCalls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/chunks/default:query" {
			postCalls.Add(1)
			http.NotFound(w, r)
			return
		}

		if r.Method == http.MethodGet {
			getCalls.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))
	chunks := []xet.Hash{{1}, {2}, {3}}

	results, err := client.QueryChunksDeduplication(context.Background(), chunks)
	if err != nil {
		t.Fatalf("QueryChunksDeduplication failed: %v", err)
	}

	if postCalls.Load() != 1 {
		t.Fatalf("expected exactly one batch POST call, got %d", postCalls.Load())
	}
	if getCalls.Load() != int32(len(chunks)) {
		t.Fatalf("expected fallback GET calls=%d, got %d", len(chunks), getCalls.Load())
	}

	for _, chunk := range chunks {
		result, ok := results[chunk]
		if !ok || result == nil {
			t.Fatalf("missing result for chunk %s", chunk.String())
		}
		if !result.IsNew {
			t.Fatalf("expected chunk %s to be marked as new", chunk.String())
		}
	}
}
