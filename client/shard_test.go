package client

import (
	"bytes"
	"encoding/json"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage/local"
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetries(1), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithRetries(1), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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
	s.SetFooter(time.Now())
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
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
	if loc, ok := results[probe]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash}) {
		t.Fatalf("probe result: got %+v, %v", loc, ok)
	}
	if loc, ok := results[other]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) {
		t.Fatalf("candidate result: got %+v, %v", loc, ok)
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := c.QueryDedupShard(t.Context(), probe)
	if err != nil {
		t.Fatalf("QueryDedupShard: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("probe from expired shard must be treated as new, got %v", results)
	}
}

// An unkeyed shard resolves the requested hashes only: the probe and the
// candidates, never the other chunks it stores.
func TestQueryDedupShardUnkeyedShardResolvesRequestedHashes(t *testing.T) {
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	other := xet.ComputeChunkHash([]byte("chunk-1"))
	unrequested := xet.ComputeChunkHash([]byte("chunk-2"))
	xorbHash := xet.XorbHash{0xAC}

	shardBytes := buildDedupShardBytes(t, [32]byte{}, 0, xorbHash, []xet.ChunkHash{probe, other, unrequested})
	srv := serveDedupShard(t, shardBytes)
	defer srv.Close()

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	results, err := c.QueryDedupShard(t.Context(), probe, other)
	if err != nil {
		t.Fatalf("QueryDedupShard: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("results: got %d entries want the probe and the candidate (%v)", len(results), results)
	}
	if loc, ok := results[probe]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash}) {
		t.Fatalf("probe result: got %+v, %v", loc, ok)
	}
	if loc, ok := results[other]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) {
		t.Fatalf("candidate result: got %+v, %v", loc, ok)
	}
	if loc, ok := results[unrequested]; ok {
		t.Fatalf("unrequested stored hash resolved %+v; want absent", loc)
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

	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// The batch :query endpoint 404s, forcing the single-shard fallback that
	// must carry candidates through to keyed matching.
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe}, candidate)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if loc, ok := results[probe]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash}) {
		t.Fatalf("probe result: got %+v, %v", loc, ok)
	}
	if loc, ok := results[candidate]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) {
		t.Fatalf("candidate result: got %+v, %v", loc, ok)
	}
}

// dedupCAS serves one xorb's unkeyed dedup shard: the batch endpoint reports each stored chunk found at its index, every other request answers the shard bytes (or getStatus).
type dedupCAS struct {
	xorbHash   xet.XorbHash
	stored     []xet.ChunkHash
	shardBytes []byte
	getStatus  int
	posts      atomic.Int32
	gets       atomic.Int32
}

func newDedupCAS(t *testing.T, xorbHash xet.XorbHash, stored ...xet.ChunkHash) (*dedupCAS, *httptest.Server) {
	t.Helper()
	cas := &dedupCAS{xorbHash: xorbHash, stored: stored, shardBytes: buildDedupShardBytes(t, [32]byte{}, 0, xorbHash, stored)}
	srv := httptest.NewServer(cas)
	t.Cleanup(srv.Close)
	return cas, srv
}

func (d *dedupCAS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":query") {
		d.posts.Add(1)
		var req batchChunkDedupQueryRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		var resp batchChunkDedupQueryResponse
		for _, h := range req.ChunkHashes {
			item := batchChunkDedupResult{ChunkHash: h}
			for i, stored := range d.stored {
				if stored.String() == h {
					item = batchChunkDedupResult{ChunkHash: h, Found: true, XorbHash: d.xorbHash.String(), ChunkIndex: uint32(i)}
				}
			}
			resp.Results = append(resp.Results, item)
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	d.gets.Add(1)
	if d.getStatus != 0 {
		w.WriteHeader(d.getStatus)
		return
	}
	_, _ = w.Write(d.shardBytes)
}

func TestQueryDedupShardsFetchesShardOnBatchHit(t *testing.T) {
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	neighbours := []xet.ChunkHash{xet.ComputeChunkHash([]byte("chunk-1")), xet.ComputeChunkHash([]byte("chunk-2")), xet.ComputeChunkHash([]byte("chunk-3"))}
	unrelated := xet.ComputeChunkHash([]byte("chunk-9"))
	xorbHash := xet.XorbHash{0xB0}
	cas, srv := newDedupCAS(t, xorbHash, append(append([]xet.ChunkHash{probe}, neighbours...), unrelated)...)
	cacheDir := t.TempDir()
	c, err := NewClient(WithCache(NewCache(cacheDir, 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe}, neighbours...)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if posts, gets := cas.posts.Load(), cas.gets.Load(); posts != 1 || gets != 1 {
		t.Fatalf("requests = %d POST, %d GET; want one batch query and one shard fetch", posts, gets)
	}
	for i, h := range append([]xet.ChunkHash{probe}, neighbours...) {
		if loc, ok := results[h]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: uint32(i)}) {
			t.Fatalf("chunk %d = %+v, %v; want xorb %s index %d", i, loc, ok, xorbHash, i)
		}
	}
	// The shard also lists a chunk nobody asked about; it is neither resolved nor cached.
	if loc, ok := results[unrelated]; ok {
		t.Fatalf("unrequested chunk resolved %+v; want absent", loc)
	}
	if cached := cachedChunkEntries(cacheDir); len(cached) != len(cas.stored)-1 {
		t.Fatalf("cached chunk locations = %v; want one per requested chunk", cached)
	}
	if got := c.cache.Upload.Lookup(t.Context(), c.dedupScope(srv.URL), []xet.ChunkHash{unrelated}); len(got) != 0 {
		t.Fatalf("unrequested chunk cached: %v", got)
	}
}

// cachedChunkEntries lists the chunk location entries under cacheDir's upload cache.
func cachedChunkEntries(cacheDir string) []string {
	entries, _ := filepath.Glob(filepath.Join(cacheDir, "upload", "chunks", "*", "*", "*", "*"))
	return entries
}

// Two probes of one previous session resolve through a single fetch; without unresolved neighbours the batch locations suffice and nothing is fetched.
func TestQueryDedupShardsSharesOneShardAcrossProbes(t *testing.T) {
	p1, p2, neighbour := xet.ComputeChunkHash([]byte("chunk-0")), xet.ComputeChunkHash([]byte("chunk-1")), xet.ComputeChunkHash([]byte("chunk-2"))
	xorbHash := xet.XorbHash{0xB1}
	cas, srv := newDedupCAS(t, xorbHash, p1, p2, neighbour)
	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{p1, p2}, neighbour)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if gets := cas.gets.Load(); gets != 1 {
		t.Fatalf("shard fetches = %d; want 1 for two probes in one shard", gets)
	}
	l1, ok1 := results[p1]
	l2, ok2 := results[p2]
	l3, ok3 := results[neighbour]
	if !ok1 || l1 != (shard.ChunkLocation{XorbHash: xorbHash}) || !ok2 || l2 != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) || !ok3 || l3 != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 2}) {
		t.Fatalf("results = %+v, %+v, %+v; want all found", results[p1], results[p2], results[neighbour])
	}

	cas.gets.Store(0)
	c, err = NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err = c.QueryDedupShards(t.Context(), []xet.ChunkHash{p1, p2})
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if gets := cas.gets.Load(); gets != 0 {
		t.Fatalf("shard fetches = %d; want none when the batch settles every candidate", gets)
	}
	l1, ok1 = results[p1]
	l2, ok2 = results[p2]
	if !ok1 || l1 != (shard.ChunkLocation{XorbHash: xorbHash}) || !ok2 || l2 != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) {
		t.Fatalf("results = %+v, %+v; want both found from the batch", results[p1], results[p2])
	}
}

func TestQueryDedupShardsSharesOneKeyedShardAcrossProbes(t *testing.T) {
	key := [32]byte{0xEE}
	p1, p2, neighbour := xet.ComputeChunkHash([]byte("chunk-0")), xet.ComputeChunkHash([]byte("chunk-1")), xet.ComputeChunkHash([]byte("chunk-2"))
	xorbHash := xet.XorbHash{0xB2}
	cas := &dedupCAS{xorbHash: xorbHash, stored: []xet.ChunkHash{p1, p2, neighbour}, shardBytes: buildDedupShardBytes(t, key, 0, xorbHash, []xet.ChunkHash{p1.HMAC(key), p2.HMAC(key), neighbour.HMAC(key)})}
	srv := httptest.NewServer(cas)
	t.Cleanup(srv.Close)
	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{p1, p2}, neighbour)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if gets := cas.gets.Load(); gets != 1 {
		t.Fatalf("shard fetches = %d; want 1 for two probes in one keyed shard", gets)
	}
	l1, ok1 := results[p1]
	l2, ok2 := results[p2]
	l3, ok3 := results[neighbour]
	if !ok1 || l1 != (shard.ChunkLocation{XorbHash: xorbHash}) || !ok2 || l2 != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) || !ok3 || l3 != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 2}) {
		t.Fatalf("results = %+v, %+v, %+v; want all found", results[p1], results[p2], results[neighbour])
	}
}

func TestQueryDedupShardsKeepsBatchLocationWhenShardFetchFails(t *testing.T) {
	probe, neighbour := xet.ComputeChunkHash([]byte("chunk-0")), xet.ComputeChunkHash([]byte("chunk-1"))
	xorbHash := xet.XorbHash{0xB2}
	cas, srv := newDedupCAS(t, xorbHash, neighbour, probe)
	cas.getStatus = http.StatusInternalServerError
	cacheDir := t.TempDir()
	c, err := NewClient(WithCache(NewCache(cacheDir, 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe}, neighbour)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if loc, ok := results[probe]; !ok || loc != (shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}) {
		t.Fatalf("probe = %+v, %v; want the batch location (xorb %s index 1)", loc, ok, xorbHash)
	}
	if r, ok := results[neighbour]; ok {
		t.Fatalf("neighbour = %+v; want absent without the shard", r)
	}
	if cached := cachedChunkEntries(cacheDir); len(cached) != 1 {
		t.Fatalf("cached chunk locations = %v; want the probe's batch location only", cached)
	}
}

func TestQueryDedupShardsServesRepeatFromCache(t *testing.T) {
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	neighbours := []xet.ChunkHash{xet.ComputeChunkHash([]byte("chunk-1")), xet.ComputeChunkHash([]byte("chunk-2"))}
	cas, srv := newDedupCAS(t, xet.XorbHash{0xB3}, append([]xet.ChunkHash{probe}, neighbours...)...)
	shared := NewCache(t.TempDir(), 0, 0)
	ctx := t.Context()
	first, err := NewClient(WithCache(shared), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	want, err := first.QueryDedupShards(ctx, []xet.ChunkHash{probe}, neighbours...)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	second, err := NewClient(WithCache(shared), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	cas.posts.Store(0)
	cas.gets.Store(0)
	for i, c := range []*Client{first, second} {
		got, err := c.QueryDedupShards(ctx, []xet.ChunkHash{probe}, neighbours...)
		if err != nil {
			t.Fatalf("client %d: QueryDedupShards: %v", i, err)
		}
		if posts, gets := cas.posts.Load(), cas.gets.Load(); posts != 0 || gets != 0 {
			t.Fatalf("client %d: requests = %d POST, %d GET; want none, served from the cached locations", i, posts, gets)
		}
		if len(got) != len(want) {
			t.Fatalf("client %d: %d results, want %d", i, len(got), len(want))
		}
		for h, w := range want {
			if g, ok := got[h]; !ok || g != w {
				t.Fatalf("client %d: %s = %+v, %v; want %+v", i, h, g, ok, w)
			}
		}
	}
}

// Locations from an expired server shard are not trusted, so the probe is new and nothing is cached.
func TestQueryDedupShardsCachesOnlyUnexpiredLocations(t *testing.T) {
	probe := xet.ComputeChunkHash([]byte("chunk-0"))
	for _, expiry := range []uint64{1000, 0} {
		cacheDir := t.TempDir()
		srv := serveDedupShard(t, buildDedupShardBytes(t, [32]byte{}, expiry, xet.XorbHash{0xB4}, []xet.ChunkHash{probe}))
		t.Cleanup(srv.Close)
		c, err := NewClient(WithCache(NewCache(cacheDir, 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe})
		if err != nil {
			t.Fatalf("expiry %d: QueryDedupShards: %v", expiry, err)
		}
		expired := expiry != 0
		cached := cachedChunkEntries(cacheDir)
		if _, found := results[probe]; found == expired || (len(cached) != 0) == expired {
			t.Fatalf("expiry %d: probe = %+v, cached chunk locations = %v; want found and cached only when unexpired", expiry, results[probe], cached)
		}
	}
}

// A second upload of the same bytes on a shared Cache is answered by the locations cached from the first: no dedup queries, no xorb traffic, one shard.
func TestUploadTwiceSkipsDedupQueriesAndXorbUploads(t *testing.T) {
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	handler := server.NewHandler(server.WithStorage(stor))
	var mu sync.Mutex
	requests := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	count := func(prefixes ...string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for req, c := range requests {
			for _, prefix := range prefixes {
				if strings.HasPrefix(req, prefix) {
					n += c
					break
				}
			}
		}
		return n
	}

	content := make([]byte, 1<<20)
	rand.New(rand.NewSource(1)).Read(content)
	cacheDir := t.TempDir()
	shared := NewCache(cacheDir, 0, 0)
	ctx := t.Context()
	uploadOnce := func() xet.FileHash {
		t.Helper()
		c, err := NewClient(WithCache(shared), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		hash, err := c.UploadFile(ctx, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("UploadFile: %v", err)
		}
		return hash
	}

	first := uploadOnce()
	if xorbs, shards := count("POST /v1/xorbs/"), count("POST /v1/shards"); xorbs == 0 || shards != 1 {
		t.Fatalf("first upload: %d xorb uploads, %d shard uploads; want at least one and exactly one", xorbs, shards)
	}
	cached := cachedChunkEntries(cacheDir)
	if len(cached) == 0 {
		t.Fatal("first upload cached no chunk locations")
	}
	mu.Lock()
	clear(requests)
	mu.Unlock()

	if second := uploadOnce(); second != first {
		t.Fatalf("second upload hash = %s, want %s", second, first)
	}
	dedup, xorbs, shards := count("GET /v1/chunks", "POST /v1/chunks"), count("POST /v1/xorbs/", "HEAD /v1/xorbs/"), count("POST /v1/shards")
	if dedup != 0 || xorbs != 0 || shards != 1 {
		t.Fatalf("second upload: %d dedup queries, %d xorb POST/HEAD, %d shard uploads; want 0, 0, 1", dedup, xorbs, shards)
	}
	// The second shard lists no new chunks, so it adds no locations.
	if after := cachedChunkEntries(cacheDir); len(after) != len(cached) {
		t.Fatalf("cached chunk locations = %d; want the first upload's %d", len(after), len(cached))
	}
}

// A single-chunk file's only chunk is cached by its shard upload like any other, so uploading it again asks the CAS nothing.
func TestUploadSingleChunkTwiceSkipsDedupQueries(t *testing.T) {
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	srv, count, reset := countingServer(t, server.NewHandler(server.WithStorage(stor)))
	content := make([]byte, 4<<10)
	rand.New(rand.NewSource(5)).Read(content)
	shared := NewCache(t.TempDir(), 0, 0)
	upload := func() {
		t.Helper()
		c, err := NewClient(WithCache(shared), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.UploadFile(t.Context(), bytes.NewReader(content)); err != nil {
			t.Fatalf("UploadFile: %v", err)
		}
	}
	upload()
	if xorbs := count("POST /v1/xorbs/"); xorbs != 1 {
		t.Fatalf("first upload: %d xorb uploads; want one", xorbs)
	}
	reset()
	upload()
	dedup, xorbs, shards := count("GET /v1/chunks", "POST /v1/chunks"), count("POST /v1/xorbs/", "HEAD /v1/xorbs/"), count("POST /v1/shards")
	if dedup != 0 || xorbs != 0 || shards != 1 {
		t.Fatalf("second upload: %d dedup queries, %d xorb POST/HEAD, %d shard uploads; want 0, 0, 1", dedup, xorbs, shards)
	}
}

// Cached locations belong to one CAS: the same bytes uploaded to a second server through the same cache root are deduplicated against that server, not the first.
func TestUploadToSecondEndpointIgnoresFirstEndpointsCache(t *testing.T) {
	content := make([]byte, 1<<20)
	rand.New(rand.NewSource(2)).Read(content)
	shared := NewCache(t.TempDir(), 0, 0)
	ctx := t.Context()
	var servers []*httptest.Server
	var requests []map[string]int
	for range 2 {
		stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
		if err != nil {
			t.Fatal(err)
		}
		handler := server.NewHandler(server.WithStorage(stor))
		seen := map[string]int{}
		var mu sync.Mutex
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen[r.Method+" "+r.URL.Path]++
			mu.Unlock()
			handler.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		servers, requests = append(servers, srv), append(requests, seen)
	}
	for i, srv := range servers {
		c, err := NewClient(WithCache(shared), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.UploadFile(ctx, bytes.NewReader(content)); err != nil {
			t.Fatalf("upload to server %d: %v", i, err)
		}
		xorbs, shards := 0, requests[i]["POST /v1/shards"]
		for req, n := range requests[i] {
			if strings.HasPrefix(req, "POST /v1/xorbs/") {
				xorbs += n
			}
		}
		if xorbs != 1 || shards != 1 {
			t.Fatalf("server %d saw %d xorb uploads and %d shard uploads; want one each", i, xorbs, shards)
		}
	}
}

// countingServer serves handler and tallies requests as "METHOD path"; count sums the tallies starting with any prefix, reset clears them.
func countingServer(t *testing.T, handler http.Handler) (srv *httptest.Server, count func(prefixes ...string) int, reset func()) {
	t.Helper()
	var mu sync.Mutex
	requests := map[string]int{}
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests[r.Method+" "+r.URL.Path]++
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	count = func(prefixes ...string) int {
		mu.Lock()
		defer mu.Unlock()
		n := 0
		for req, c := range requests {
			if slices.ContainsFunc(prefixes, func(prefix string) bool { return strings.HasPrefix(req, prefix) }) {
				n += c
			}
		}
		return n
	}
	reset = func() {
		mu.Lock()
		clear(requests)
		mu.Unlock()
	}
	return srv, count, reset
}

// A cached probe whose neighbours are not cached still fetches the shard behind it, with or without the batch endpoint, so a partly warm cache never re-uploads chunks the CAS holds.
func TestUploadWithCachedProbeFetchesShardForUncachedNeighbours(t *testing.T) {
	content := make([]byte, 2<<20)
	rand.New(rand.NewSource(6)).Read(content)
	for _, tc := range []struct {
		name    string
		noBatch bool
	}{{"batch", false}, {"fallback", true}} {
		t.Run(tc.name, func(t *testing.T) {
			stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			handler := server.NewHandler(server.WithStorage(stor))
			srv, count, reset := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.noBatch && strings.Contains(r.URL.Path, ":query") {
					http.NotFound(w, r)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			upload := func(cache *Cache, data []byte) {
				t.Helper()
				c, err := NewClient(WithCache(cache), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.UploadFile(t.Context(), bytes.NewReader(data)); err != nil {
					t.Fatalf("UploadFile: %v", err)
				}
			}
			upload(NewCache(t.TempDir(), 0, 0), content)
			rootB := NewCache(t.TempDir(), 0, 0)
			upload(rootB, content[:1<<20])
			reset()
			upload(rootB, content)
			if xorbs, gets, shards := count("POST /v1/xorbs/", "HEAD /v1/xorbs/"), count("GET /v1/chunks/"), count("POST /v1/shards"); xorbs != 0 || gets < 1 || shards != 1 {
				t.Fatalf("upload with a cached probe and uncached neighbours: %d xorb POST/HEAD, %d shard fetches, %d shard uploads; want 0, at least 1, 1", xorbs, gets, shards)
			}
		})
	}
}

// Batch results for hashes that were not asked about are dropped: they neither enter the results nor pick the shards fetched.
func TestQueryDedupShardsIgnoresUnrequestedBatchResults(t *testing.T) {
	probe, neighbour := xet.ComputeChunkHash([]byte("chunk-0")), xet.ComputeChunkHash([]byte("chunk-1"))
	unrequested := []xet.ChunkHash{xet.ComputeChunkHash([]byte("chunk-7")), xet.ComputeChunkHash([]byte("chunk-8")), xet.ComputeChunkHash([]byte("chunk-9"))}
	var mu sync.Mutex
	var gets []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, ":query") {
			var resp batchChunkDedupQueryResponse
			for _, h := range append([]xet.ChunkHash{probe}, unrequested...) {
				resp.Results = append(resp.Results, batchChunkDedupResult{ChunkHash: h.String(), Found: true, XorbHash: xet.XorbHash{0xB5}.String()})
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		mu.Lock()
		gets = append(gets, strings.TrimPrefix(r.URL.Path, "/v1/chunks/default/"))
		mu.Unlock()
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)
	c, err := NewClient(WithCache(NewCache(t.TempDir(), 0, 0)), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
	if err != nil {
		t.Fatal(err)
	}
	results, err := c.QueryDedupShards(t.Context(), []xet.ChunkHash{probe}, neighbour)
	if err != nil {
		t.Fatalf("QueryDedupShards: %v", err)
	}
	if loc, ok := results[probe]; !ok || loc != (shard.ChunkLocation{XorbHash: xet.XorbHash{0xB5}}) {
		t.Fatalf("probe = %+v, %v; want the batch location", loc, ok)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gets) > 1 || (len(gets) == 1 && gets[0] != probe.String()) {
		t.Fatalf("shard fetches = %v; want at most the probe's", gets)
	}
	for _, h := range unrequested {
		if r, ok := results[h]; ok {
			t.Fatalf("unrequested %s = %+v; want absent", h, r)
		}
	}
}

// The namespace is part of the cache scope: the same bytes uploaded under another namespace of one server are resolved by the server, not the cache.
func TestUploadToOtherNamespaceIgnoresCache(t *testing.T) {
	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	srv, count, reset := countingServer(t, server.NewHandler(server.WithStorage(stor)))
	content := make([]byte, 1<<20)
	rand.New(rand.NewSource(5)).Read(content)
	shared := NewCache(t.TempDir(), 0, 0)
	for _, tc := range []struct {
		namespace string
		wantDedup bool
	}{{"default", true}, {"default", false}, {"other", true}} {
		reset()
		c, err := NewClient(WithCache(shared), WithNamespace(tc.namespace), WithUpstreamProvider(StaticUpstreamProvider(srv.URL, "")))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.UploadFile(t.Context(), bytes.NewReader(content)); err != nil {
			t.Fatalf("upload to namespace %s: %v", tc.namespace, err)
		}
		if dedup := count("POST /v1/chunks", "GET /v1/chunks"); (dedup > 0) != tc.wantDedup {
			t.Fatalf("upload to namespace %s made %d dedup queries; want some = %v", tc.namespace, dedup, tc.wantDedup)
		}
	}
}
