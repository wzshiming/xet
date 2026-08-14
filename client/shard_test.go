package client

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
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

// buildDedupShardBytes serializes a single-xorb dedup shard whose stored
// chunk hashes are given verbatim (pre-keyed by the caller when simulating a
// keyed CAS response).
func buildDedupShardBytes(t *testing.T, key [32]byte, keyExpiry uint64, xorbHash xet.XorbHash, storedChunks []xet.ChunkHash) []byte {
	t.Helper()

	s := shard.NewShard()
	entries := make([]shard.CASChunkSequenceEntry, len(storedChunks))
	var offset uint32
	for i, storedChunk := range storedChunks {
		entries[i] = shard.CASChunkSequenceEntry{
			ChunkHash:        storedChunk,
			ByteRangeStart:   offset,
			UnpackedSegBytes: 100,
		}
		offset += 100
	}
	s.AddCASBlock(shard.CASBlock{
		CASHash:        xorbHash,
		Chunks:         entries,
		NumBytesInCAS:  offset,
		NumBytesOnDisk: offset,
	})
	s.SetFooter()
	s.Footer.ChunkHashKey = key
	if keyExpiry != 0 {
		s.Footer.ShardKeyExpiry = keyExpiry
	}

	reader, err := s.Encode(true)
	if err != nil {
		t.Fatalf("encode shard: %v", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read shard: %v", err)
	}
	return data
}

func serveDedupShard(t *testing.T, shardBytes []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ":query") {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/v1/chunks/default/") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(shardBytes)
	}))
}

func TestQueryDedupShardKeyedShard(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	other := xet.ComputeChunkHash([]byte("chunk-1"))
	missing := xet.ComputeChunkHash([]byte("chunk-2"))
	unrelated := xet.ComputeChunkHash([]byte("chunk-3"))
	xorbHash := xet.XorbHash{0xAA}

	shardBytes := buildDedupShardBytes(t, key, 0, xorbHash, []xet.ChunkHash{
		probe.HMAC(key),
		other.HMAC(key),
		unrelated.HMAC(key),
	})
	srv := serveDedupShard(t, shardBytes)
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := c.QueryDedupShard(t.Context(), probe, other, missing)
	if err != nil {
		t.Fatalf("QueryDedupShard: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("results: got %d entries want 2 (%v)", len(results), results)
	}
	probeResult := results[probe]
	if probeResult == nil || probeResult.IsNew || probeResult.XorbHash != xorbHash || probeResult.ChunkIndex != 0 {
		t.Fatalf("probe result: got %+v", probeResult)
	}
	otherResult := results[other]
	if otherResult == nil || otherResult.IsNew || otherResult.XorbHash != xorbHash || otherResult.ChunkIndex != 1 {
		t.Fatalf("candidate result: got %+v", otherResult)
	}
	if _, ok := results[missing]; ok {
		t.Fatal("missing candidate must not appear in results")
	}
}

func TestQueryDedupShardExpiredKeyIgnoresShard(t *testing.T) {
	key := [32]byte{7}
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	xorbHash := xet.XorbHash{0xAB}

	shardBytes := buildDedupShardBytes(t, key, 1000, xorbHash, []xet.ChunkHash{probe.HMAC(key)})
	srv := serveDedupShard(t, shardBytes)
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := c.QueryDedupShard(t.Context(), probe)
	if err != nil {
		t.Fatalf("QueryDedupShard: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results: got %d entries want 1", len(results))
	}
	if probeResult := results[probe]; probeResult == nil || !probeResult.IsNew {
		t.Fatalf("probe from expired shard must be treated as new, got %+v", results[probe])
	}
}

func TestQueryDedupShardUnkeyedShardIndexesRawHashes(t *testing.T) {
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	other := xet.ComputeChunkHash([]byte("chunk-1"))
	xorbHash := xet.XorbHash{0xAC}

	shardBytes := buildDedupShardBytes(t, [32]byte{}, 0, xorbHash, []xet.ChunkHash{probe, other})
	srv := serveDedupShard(t, shardBytes)
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := c.QueryDedupShard(t.Context(), probe)
	if err != nil {
		t.Fatalf("QueryDedupShard: %v", err)
	}
	probeResult := results[probe]
	if probeResult == nil || probeResult.IsNew || probeResult.ChunkIndex != 0 {
		t.Fatalf("probe result: got %+v", probeResult)
	}
	otherResult := results[other]
	if otherResult == nil || otherResult.IsNew || otherResult.ChunkIndex != 1 {
		t.Fatalf("unkeyed shard must index all raw hashes, got %+v", otherResult)
	}
}

func TestQueryDedupShardsFallbackMatchesKeyedCandidates(t *testing.T) {
	key := [32]byte{9}
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	candidate := xet.ComputeChunkHash([]byte("chunk-1"))
	xorbHash := xet.XorbHash{0xAD}

	shardBytes := buildDedupShardBytes(t, key, 0, xorbHash, []xet.ChunkHash{
		probe.HMAC(key),
		candidate.HMAC(key),
	})
	srv := serveDedupShard(t, shardBytes)
	defer srv.Close()

	c, err := NewClient(WithBaseURL(srv.URL), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// The batch :query endpoint 404s, forcing the single-shard fallback that
	// must carry candidates through to keyed matching.
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe}, candidate)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if probeResult := results[probe]; probeResult == nil || probeResult.IsNew {
		t.Fatalf("probe result: got %+v", results[probe])
	}
	candidateResult := results[candidate]
	if candidateResult == nil || candidateResult.IsNew || candidateResult.XorbHash != xorbHash || candidateResult.ChunkIndex != 1 {
		t.Fatalf("candidate result: got %+v", candidateResult)
	}
}
