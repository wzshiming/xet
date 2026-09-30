package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

type batchChunkDedupQueryRequest struct {
	ChunkHashes []string `json:"chunk_hashes"`
}

type batchChunkDedupQueryResponse struct {
	Results []batchChunkDedupResult `json:"results"`
}

type batchChunkDedupResult struct {
	ChunkHash  string `json:"chunk_hash"`
	Found      bool   `json:"found"`
	XorbHash   string `json:"xorb_hash,omitempty"`
	ChunkIndex uint32 `json:"chunk_index,omitempty"`
}

type shardUploadEventV2 struct {
	Type      string `json:"type"`
	Message   string `json:"message,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

// UploadShard uploads a serialized shard through the V1 API and caches its chunk locations for later dedup lookups.
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	req, scope, err := c.newShardUploadRequest(ctx, "v1", shardObj)
	if err != nil {
		return nil, err
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var uploadResp upload.ShardUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	_ = c.cache.Upload.Store(scope, shardObj.ChunkLocations())
	return &uploadResp, nil
}

// UploadShardV2 uploads a serialized shard through the V2 NDJSON streaming
// API. Error frames marked retryable retry the whole upload, mirroring the
// xet-core reference client.
func (c *Client) UploadShardV2(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	attempts := c.retryAttempts()
	var lastErr error
	for i := range attempts {
		if i > 0 && ctx.Err() != nil {
			break
		}
		uploadResp, err := c.uploadShardV2(ctx, shardObj)
		if err == nil {
			return uploadResp, nil
		}
		var retryable *retryableShardUploadError
		if !errors.As(err, &retryable) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("v2 shard upload failed after %d attempts: %w", attempts, lastErr)
}

// newShardUploadRequest builds a POST request carrying the encoded shard for
// the given API version path, and the dedup cache scope of its CAS.
func (c *Client) newShardUploadRequest(ctx context.Context, version string, shardObj *shard.Shard) (*http.Request, string, error) {
	ctx, baseURL, err := c.casContext(ctx, auth.Write)
	if err != nil {
		return nil, "", fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/%s/shards", baseURL, version)

	reader, err := shardObj.Encode(false)
	if err != nil {
		return nil, "", err
	}
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", fmt.Errorf("read shard payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, "", fmt.Errorf("create request: %w", err)
	}
	req.ContentLength = int64(len(bodyBytes))
	req.Header.Set("Content-Type", "application/octet-stream")
	return req, c.dedupScope(baseURL), nil
}

// uploadShardV2 performs a single /v2/shards upload attempt, caching the shard's chunk locations on success.
func (c *Client) uploadShardV2(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	req, scope, err := c.newShardUploadRequest(ctx, "v2", shardObj)
	if err != nil {
		return nil, err
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	uploadResp, err := parseShardUploadNDJSON(resp.Body)
	if err != nil {
		return nil, err
	}
	_ = c.cache.Upload.Store(scope, shardObj.ChunkLocations())
	return uploadResp, nil
}

// retryableShardUploadError reports a terminal /v2/shards error frame marked
// retryable. The whole upload should be attempted again.
type retryableShardUploadError struct {
	message string
}

func (e *retryableShardUploadError) Error() string {
	return e.message
}

// maxShardUploadEventSize caps a single NDJSON frame at 1 MiB, mirroring the
// xet-core reference implementation (larger frames fail rather than allocate
// unboundedly).
const maxShardUploadEventSize = 1 << 20

// parseShardUploadNDJSON reads the /v2/shards NDJSON response stream until a
// terminal result or error frame. Progress frames ("validating", "committing",
// "heartbeat", ...) and unknown frame types are non-terminal and skipped to
// stay forward compatible. An error frame with retryable=true is reported as
// *retryableShardUploadError so callers can retry the upload.
func parseShardUploadNDJSON(r io.Reader) (*upload.ShardUploadResponse, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxShardUploadEventSize)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event shardUploadEventV2
		if err := json.Unmarshal(line, &event); err != nil {
			return nil, fmt.Errorf("decode v2 shard upload event (line=%q): %w", line, err)
		}
		switch event.Type {
		case "result":
			return &upload.ShardUploadResponse{Result: 1}, nil
		case "error":
			if event.Message == "" {
				event.Message = "unknown error"
			}
			if event.Retryable {
				return nil, &retryableShardUploadError{message: event.Message}
			}
			return nil, fmt.Errorf("v2 shard upload failed: %s", event.Message)
		default:
			// Non-terminal progress frame; keep reading.
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read v2 shard upload events: %w", err)
	}
	return nil, fmt.Errorf("v2 shard upload stream ended without a result event")
}

// QueryDedupShard downloads the deduplication shard for chunkHash and
// resolves where it stores chunkHash and candidates; hashes the shard does
// not store are absent from the result.
//
// HMAC-keyed shards (production CAS) store keyed hashes that cannot be
// reversed, so only hashes offered here can be matched.
func (c *Client) QueryDedupShard(ctx context.Context, chunkHash xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	ctx, baseURL, err := c.casContext(ctx, auth.Write)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/chunks/%s/%s", baseURL, c.namespace, chunkHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return map[xet.ChunkHash]shard.ChunkLocation{}, nil
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	shardObj := shard.NewShard()
	if err := shardObj.Decode(resp.Body, true); err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	if shardObj.Footer != nil && shardObj.Footer.IsExpired(time.Now()) {
		// The shard key has expired, so its xorb references can no longer be
		// relied on for dedup; treat the probe as new data.
		return map[xet.ChunkHash]shard.ChunkLocation{}, nil
	}

	return shardObj.ResolveChunks(append([]xet.ChunkHash{chunkHash}, candidates...)...), nil
}

// dedupScope names the CAS whose xorb locations the cache entries describe; the length prefix keeps a namespace with slashes from aliasing another base URL.
func (c *Client) dedupScope(baseURL string) string {
	return fmt.Sprintf("%d:%s/%s", len(baseURL), baseURL, c.namespace)
}

// QueryDedupShards resolves chunkHashes and candidates from the cached chunk locations first, then the batch endpoint (or per-chunk queries when it is unavailable), fetching hit shards while candidates around them are still unresolved; hashes the CAS does not store are absent from the result.
func (c *Client) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, candidates ...xet.ChunkHash) (results map[xet.ChunkHash]shard.ChunkLocation, err error) {
	if len(chunkHashes) == 0 {
		return nil, nil
	}
	ctx, baseURL, err := c.casContext(ctx, auth.Write)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}

	// Probes double as keyed-shard candidates so one fetched shard settles every probe it lists.
	candidates = append(slices.Clone(chunkHashes), candidates...)
	scope := c.dedupScope(baseURL)
	local := c.cache.Upload.Lookup(ctx, scope, candidates)
	results = make(map[xet.ChunkHash]shard.ChunkLocation, len(chunkHashes)+len(local))
	maps.Copy(results, local)
	remaining := make([]xet.ChunkHash, 0, len(chunkHashes))
	requested := make(map[xet.ChunkHash]bool, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; !ok {
			remaining = append(remaining, chunkHash)
			requested[chunkHash] = true
		}
	}
	if len(remaining) == 0 && !slices.ContainsFunc(candidates, unresolvedIn(results)) {
		return results, nil
	}

	var batchResp batchChunkDedupQueryResponse
	if len(remaining) > 0 {
		requestBody := batchChunkDedupQueryRequest{ChunkHashes: make([]string, len(remaining))}
		for i, chunkHash := range remaining {
			requestBody.ChunkHashes[i] = chunkHash.String()
		}
		bodyBytes, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("marshal batch chunk query: %w", err)
		}

		url := fmt.Sprintf("%s/v1/chunks/%s:query", baseURL, c.namespace)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("create batch request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("do batch request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
			fallback, err := c.queryChunksDeduplicationFallback(ctx, remaining, candidates)
			if err != nil {
				return nil, err
			}
			mergeDedupResults(results, fallback)
		} else if err := reqError(req, resp); err != nil {
			return nil, err
		} else if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
			return nil, fmt.Errorf("decode batch chunk query response: %w", err)
		}
	}

	var hits []xet.ChunkHash
	for _, item := range batchResp.Results {
		chunkHash, err := xet.ParseChunkHash(item.ChunkHash)
		if err != nil || !item.Found || !requested[chunkHash] {
			continue
		}
		xorbHash, err := xet.ParseXorbHash(item.XorbHash)
		if err != nil {
			continue
		}
		if _, ok := results[chunkHash]; ok {
			continue
		}
		results[chunkHash] = shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: item.ChunkIndex}
		hits = append(hits, chunkHash)
	}
	// The shard behind a cached probe may list the neighbours the cache does not know.
	for _, chunkHash := range chunkHashes {
		if _, ok := local[chunkHash]; ok {
			hits = append(hits, chunkHash)
		}
	}
	c.fetchHitShards(ctx, results, hits, candidates)

	// Only the requested hashes are cached, never the unrelated chunks a fetched shard lists.
	learned := make(map[xet.ChunkHash]shard.ChunkLocation)
	for _, h := range candidates {
		if _, cached := local[h]; cached {
			continue
		}
		if loc, ok := results[h]; ok {
			learned[h] = loc
		}
	}
	_ = c.cache.Upload.Store(scope, learned)
	return results, nil
}

// unresolvedIn reports which hashes results holds no location for.
func unresolvedIn(results map[xet.ChunkHash]shard.ChunkLocation) func(xet.ChunkHash) bool {
	return func(h xet.ChunkHash) bool { _, ok := results[h]; return !ok }
}

// fetchHitShards fetches the shards around batch hits while some candidate is still unresolved: one first, since a previous session's shard often lists every hit, then concurrency at a time, skipping hits a fetched shard already listed.
func (c *Client) fetchHitShards(ctx context.Context, results map[xet.ChunkHash]shard.ChunkLocation, hits, candidates []xet.ChunkHash) {
	listed := make(map[xet.ChunkHash]bool)
	unresolved := unresolvedIn(results)
	for width := 1; len(hits) > 0 && slices.ContainsFunc(candidates, unresolved); width = max(c.concurrency, 1) {
		var wave []xet.ChunkHash
		for ; len(hits) > 0 && len(wave) < width; hits = hits[1:] {
			if !listed[hits[0]] {
				wave = append(wave, hits[0])
			}
		}
		fetched := make([]map[xet.ChunkHash]shard.ChunkLocation, len(wave))
		var wg sync.WaitGroup
		for i, probe := range wave {
			wg.Go(func() { fetched[i], _ = c.QueryDedupShard(ctx, probe, candidates...) })
		}
		wg.Wait()
		for _, shardResults := range fetched {
			for h := range shardResults {
				listed[h] = true
			}
			mergeDedupResults(results, shardResults)
		}
	}
}

// mergeDedupResults copies src into dst; a location already in dst is never overwritten.
func mergeDedupResults(dst, src map[xet.ChunkHash]shard.ChunkLocation) {
	for chunkHash, loc := range src {
		if _, ok := dst[chunkHash]; ok {
			continue
		}
		dst[chunkHash] = loc
	}
}

func (c *Client) queryChunksDeduplicationFallback(ctx context.Context, chunkHashes []xet.ChunkHash, candidates []xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	results := make(map[xet.ChunkHash]shard.ChunkLocation, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; ok {
			continue // listed by an earlier probe's shard
		}
		found, err := c.QueryDedupShard(ctx, chunkHash, candidates...)
		if err != nil {
			return nil, err
		}
		mergeDedupResults(results, found)
	}
	return results, nil
}
