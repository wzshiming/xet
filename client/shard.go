package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/wzshiming/xet"
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

// UploadShard uploads a serialized shard through the V1 API.
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return c.UploadShardWithAuthProvider(ctx, nil, shardObj)
}

// UploadShardWithAuthProvider uploads a serialized shard through the V1 API
// with a per-call auth provider.
func (c *Client) UploadShardWithAuthProvider(ctx context.Context, provider AuthProvider, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	req, err := c.newShardUploadRequest(ctx, provider, "v1", shardObj)
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

	return &uploadResp, nil
}

// UploadShardV2 uploads a serialized shard through the V2 NDJSON streaming API.
func (c *Client) UploadShardV2(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return c.UploadShardV2WithAuthProvider(ctx, nil, shardObj)
}

// UploadShardV2WithAuthProvider uploads a serialized shard through the V2
// NDJSON streaming API with a per-call auth provider. Error frames marked
// retryable retry the whole upload, mirroring the xet-core reference client.
func (c *Client) UploadShardV2WithAuthProvider(ctx context.Context, provider AuthProvider, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	attempts := c.retryAttempts()
	var lastErr error
	for i := range attempts {
		if i > 0 && ctx.Err() != nil {
			break
		}
		uploadResp, err := c.uploadShardV2(ctx, provider, shardObj)
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
// the given API version path, applying auth from the provider.
func (c *Client) newShardUploadRequest(ctx context.Context, provider AuthProvider, version string, shardObj *shard.Shard) (*http.Request, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/%s/shards", baseURL, version)

	reader, err := shardObj.Encode(false)
	if err != nil {
		return nil, err
	}
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read shard payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.ContentLength = int64(len(bodyBytes))
	req.Header.Set("Content-Type", "application/octet-stream")
	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req, nil
}

// uploadShardV2 performs a single /v2/shards upload attempt.
func (c *Client) uploadShardV2(ctx context.Context, provider AuthProvider, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	req, err := c.newShardUploadRequest(ctx, provider, "v2", shardObj)
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

	return parseShardUploadNDJSON(resp.Body)
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

// QueryDedupShard downloads the deduplication shard for the given chunk hash and
// returns all chunk locations indexed by that shard, enabling local O(1) lookups
// for any chunk that shares the same shard (xet-core style local dedup).
//
// candidates are additional raw chunk hashes the caller wants dedup info for.
// They are needed for HMAC-keyed shards (production CAS): stored hashes are
// keyed and cannot be reversed, so only hashes offered as candidates can be
// matched. Unkeyed shards ignore candidates and index every stored hash.
func (c *Client) QueryDedupShard(ctx context.Context, chunkHash xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	return c.QueryDedupShardWithAuthProvider(ctx, nil, chunkHash, candidates...)
}

// QueryDedupShard downloads the deduplication shard for the given chunk hash
// with a per-call auth provider.
func (c *Client) QueryDedupShardWithAuthProvider(ctx context.Context, provider AuthProvider, chunkHash xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/chunks/%s/%s", baseURL, c.namespace, chunkHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return map[xet.ChunkHash]*upload.DeduplicationResult{
			chunkHash: {
				ChunkHash: chunkHash,
				IsNew:     true,
			},
		}, nil
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	shardObj := shard.NewShard()
	if err := shardObj.Decode(resp.Body, true); err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	results := map[xet.ChunkHash]*upload.DeduplicationResult{
		chunkHash: {
			ChunkHash: chunkHash,
			IsNew:     true,
		},
	}

	if shardObj.Footer != nil && shardObj.Footer.IsExpired(time.Now()) {
		// The shard key has expired, so its xorb references can no longer be
		// relied on for dedup; treat the probe as new data.
		return results, nil
	}

	if shardObj.Footer != nil && shardObj.Footer.IsKeyed() {
		return matchKeyedDedupShard(shardObj, chunkHash, candidates, results), nil
	}

	for _, casBlock := range shardObj.CASInfos {
		for i, casChunk := range casBlock.Chunks {
			results[casChunk.ChunkHash] = &upload.DeduplicationResult{
				ChunkHash:  casChunk.ChunkHash,
				IsNew:      false,
				XorbHash:   casBlock.CASHash,
				ChunkIndex: uint32(i),
			}
		}
	}

	return results, nil
}

// matchKeyedDedupShard resolves raw candidate hashes against a shard whose
// stored chunk hashes are HMAC-keyed with the footer's ChunkHashKey
// (xet-core MDBShardInfo::keyed_chunk_hash semantics).
func matchKeyedDedupShard(shardObj *shard.Shard, chunkHash xet.ChunkHash, candidates []xet.ChunkHash, results map[xet.ChunkHash]*upload.DeduplicationResult) map[xet.ChunkHash]*upload.DeduplicationResult {
	type chunkLocation struct {
		xorbHash   xet.XorbHash
		chunkIndex uint32
	}

	keyed := make(map[xet.ChunkHash]chunkLocation)
	for _, casBlock := range shardObj.CASInfos {
		for i, casChunk := range casBlock.Chunks {
			keyed[casChunk.ChunkHash] = chunkLocation{
				xorbHash:   casBlock.CASHash,
				chunkIndex: uint32(i),
			}
		}
	}

	key := shardObj.Footer.ChunkHashKey
	for _, candidate := range append([]xet.ChunkHash{chunkHash}, candidates...) {
		if existing, ok := results[candidate]; ok && !existing.IsNew {
			continue
		}
		location, ok := keyed[candidate.HMAC(key)]
		if !ok {
			continue
		}
		results[candidate] = &upload.DeduplicationResult{
			ChunkHash:  candidate,
			IsNew:      false,
			XorbHash:   location.xorbHash,
			ChunkIndex: location.chunkIndex,
		}
	}

	return results
}

// QueryDedupShards checks multiple chunk hashes against the global
// deduplication index. It prefers the batch endpoint and falls back to single
// chunk queries when the batch endpoint is unavailable. candidates are the
// raw chunk hashes matched against HMAC-keyed shards on the fallback path;
// see QueryDedupShard.
func (c *Client) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	return c.QueryDedupShardsWithAuthProvider(ctx, nil, chunkHashes, candidates...)
}

// QueryDedupShards checks multiple chunk hashes against the global
// deduplication index with a per-call auth provider.
func (c *Client) QueryDedupShardsWithAuthProvider(ctx context.Context, provider AuthProvider, chunkHashes []xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	if len(chunkHashes) == 0 {
		return nil, nil
	}

	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	requestBody := batchChunkDedupQueryRequest{ChunkHashes: make([]string, len(chunkHashes))}
	for i, chunkHash := range chunkHashes {
		requestBody.ChunkHashes[i] = chunkHash.String()
	}
	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal batch chunk query: %w", err)
	}

	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/chunks/%s:query", baseURL, c.namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create batch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do batch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return c.queryChunksDeduplicationFallback(ctx, provider, chunkHashes, candidates)
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var batchResp batchChunkDedupQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("decode batch chunk query response: %w", err)
	}

	for _, item := range batchResp.Results {
		chunkHash, err := xet.ParseChunkHash(item.ChunkHash)
		if err != nil {
			continue
		}

		result := &upload.DeduplicationResult{
			ChunkHash: chunkHash,
			IsNew:     true,
		}
		if item.Found {
			if item.XorbHash != "" {
				if xorbHash, err := xet.ParseXorbHash(item.XorbHash); err == nil {
					result.IsNew = false
					result.XorbHash = xorbHash
					result.ChunkIndex = item.ChunkIndex
				}
			}
		}
		results[chunkHash] = result
	}

	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; !ok {
			results[chunkHash] = &upload.DeduplicationResult{
				ChunkHash: chunkHash,
				IsNew:     true,
			}
		}
	}
	return results, nil
}

func (c *Client) queryChunksDeduplicationFallback(ctx context.Context, provider AuthProvider, chunkHashes []xet.ChunkHash, candidates []xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		result, err := c.QueryDedupShardWithAuthProvider(ctx, provider, chunkHash, candidates...)
		if err != nil {
			return nil, err
		}

		for _, dedupResult := range result {
			// Keep found locations; a hash marked new by one probe's shard
			// may still be found in a later probe's shard.
			if existing, ok := results[dedupResult.ChunkHash]; ok && !existing.IsNew {
				continue
			}
			results[dedupResult.ChunkHash] = dedupResult
		}
	}
	return results, nil
}
