package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

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

// UploadShard uploads a serialized shard to the server
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	url := fmt.Sprintf("%s/shards", c.baseURL)

	reader, encodeErr := shardObj.Encode(false)
	if encodeErr != nil {
		return nil, encodeErr
	}

	contentLength := shardObj.EncodedSize(false)
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read shard payload: %w", err)
	}
	contentLength = int64(len(bodyBytes))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.ContentLength = contentLength
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
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

// QueryDedupShard downloads the deduplication shard for the given chunk hash and
// returns all chunk locations indexed by that shard, enabling local O(1) lookups
// for any chunk that shares the same shard (xet-core style local dedup).
func (c *Client) QueryDedupShard(ctx context.Context, chunkHash xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	url := fmt.Sprintf("%s/v1/chunks/%s/%s", c.baseURL, c.namespace, chunkHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return map[xet.Hash]*upload.DeduplicationResult{
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
	if err := shardObj.Decode(resp.Body, false); err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	results := map[xet.Hash]*upload.DeduplicationResult{
		chunkHash: {
			ChunkHash: chunkHash,
			IsNew:     true,
		},
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

// QueryDedupShards checks multiple chunk hashes against the global
// deduplication index. It prefers the batch endpoint and falls back to single
// chunk queries when the batch endpoint is unavailable.
func (c *Client) QueryDedupShards(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	if len(chunkHashes) == 0 {
		return nil, nil
	}

	results := make(map[xet.Hash]*upload.DeduplicationResult, len(chunkHashes))
	requestBody := batchChunkDedupQueryRequest{ChunkHashes: make([]string, len(chunkHashes))}
	for i, chunkHash := range chunkHashes {
		requestBody.ChunkHashes[i] = chunkHash.String()
	}
	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal batch chunk query: %w", err)
	}

	url := fmt.Sprintf("%s/v1/chunks/%s:query", c.baseURL, c.namespace)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create batch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do batch request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed {
		return c.queryChunksDeduplicationFallback(ctx, chunkHashes)
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var batchResp batchChunkDedupQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&batchResp); err != nil {
		return nil, fmt.Errorf("decode batch chunk query response: %w", err)
	}

	for _, item := range batchResp.Results {
		chunkHash, err := xet.ParseHash(item.ChunkHash)
		if err != nil {
			continue
		}

		result := &upload.DeduplicationResult{
			ChunkHash: chunkHash,
			IsNew:     true,
		}
		if item.Found {
			if item.XorbHash != "" {
				if xorbHash, err := xet.ParseHash(item.XorbHash); err == nil {
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

func (c *Client) queryChunksDeduplicationFallback(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	results := make(map[xet.Hash]*upload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		result, err := c.QueryDedupShard(ctx, chunkHash)
		if err != nil {
			return nil, err
		}

		for _, dedupResult := range result {
			if _, ok := results[dedupResult.ChunkHash]; ok {
				continue
			}
			results[dedupResult.ChunkHash] = dedupResult
		}
	}
	return results, nil
}
