package client

import (
	"bytes"
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
		reader, _ := shardObj.Encode(false)
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
		reader, _ := shardObj.Encode(false)
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

func TestQueryChunksDeduplicationBatchIgnoresIncompleteHits(t *testing.T) {
	chunk := xet.Hash{7, 7, 7}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chunks/default:query" {
			http.NotFound(w, r)
			return
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{
					"chunk_hash": chunk.String(),
					"found":      true,
					// Missing xorb_hash should not be treated as dedup hit.
					"chunk_index": uint32(42),
				},
			},
		})
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))
	results, err := client.QueryChunksDeduplication(context.Background(), []xet.Hash{chunk})
	if err != nil {
		t.Fatalf("QueryChunksDeduplication failed: %v", err)
	}

	result, ok := results[chunk]
	if !ok || result == nil {
		t.Fatal("missing dedup result")
	}
	if !result.IsNew {
		t.Fatal("expected incomplete hit to be treated as new")
	}
	if result.XorbHash != (xet.Hash{}) {
		t.Fatalf("expected empty xorb hash, got %s", result.XorbHash.String())
	}
}

func TestQueryChunksDeduplicationFallsBackToSingleQuery(t *testing.T) {
	var postCalls atomic.Int32
	var getCalls atomic.Int32
	chunk := xet.Hash{1}
	xorbHash := xet.Hash{9}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/chunks/default:query" {
			postCalls.Add(1)
			http.NotFound(w, r)
			return
		}

		if r.Method == http.MethodGet {
			getCalls.Add(1)
			if r.URL.Path == "/v1/chunks/default/"+chunk.String() {
				sh := shard.NewShard()
				sh.CASInfos = append(sh.CASInfos, shard.CASBlock{
					CASHash:       xorbHash,
					NumBytesInCAS: 2,
					Chunks: []shard.CASChunkSequenceEntry{
						{ChunkHash: xet.Hash{88}, ByteRangeStart: 0, UnpackedSegBytes: 1},
						{ChunkHash: chunk, ByteRangeStart: 1, UnpackedSegBytes: 1},
					},
				})

				reader, err := sh.Encode(false)
				if err != nil {
					t.Fatalf("encode shard response: %v", err)
				}
				if _, err := io.Copy(w, reader); err != nil {
					t.Fatalf("write shard response: %v", err)
				}
				return
			}

			w.WriteHeader(http.StatusNotFound)
			return
		}

		http.NotFound(w, r)
	}))
	defer server.Close()

	client := NewClient(WithBaseURL(server.URL), WithCacheDir(t.TempDir()))
	chunks := []xet.Hash{chunk, {2}, {3}}

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
		if chunk != chunks[0] && !result.IsNew {
			t.Fatalf("expected chunk %s to be marked as new", chunk.String())
		}
	}

	if results[chunk].IsNew {
		t.Fatalf("expected chunk %s to be marked as deduplicated", chunk.String())
	}
	if results[chunk].XorbHash != xorbHash {
		t.Fatalf("unexpected xorb hash: got %s want %s", results[chunk].XorbHash, xorbHash)
	}
	if results[chunk].ChunkIndex != 1 {
		t.Fatalf("unexpected chunk index: got %d want 1", results[chunk].ChunkIndex)
	}
}

func TestFindChunkLocationInDedupShard(t *testing.T) {
	chunk := xet.Hash{7}
	xorbHash := xet.Hash{8}

	sh := shard.NewShard()
	sh.CASInfos = append(sh.CASInfos, shard.CASBlock{
		CASHash: xorbHash,
		Chunks: []shard.CASChunkSequenceEntry{
			{ChunkHash: xet.Hash{1}},
			{ChunkHash: chunk},
		},
	})

	reader, err := sh.Encode(false)
	if err != nil {
		t.Fatalf("encode shard: %v", err)
	}
	decoded := shard.NewShard()
	if err := decoded.Decode(bytes.NewReader(func() []byte {
		b, _ := io.ReadAll(reader)
		return b
	}()), false); err != nil {
		t.Fatalf("decode shard: %v", err)
	}

	h, idx, ok := findChunkLocationInDedupShard(decoded, chunk)
	if !ok {
		t.Fatal("expected chunk match")
	}
	if h != xorbHash || idx != 1 {
		t.Fatalf("unexpected match result: hash=%s idx=%d", h, idx)
	}
}
