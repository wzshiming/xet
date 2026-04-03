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

	var body io.Reader = reader

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.ContentLength = contentLength
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
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

// QueryChunkDeduplication checks if a chunk exists in the global deduplication index
func (c *Client) QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*upload.DeduplicationResult, error) {
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
		// Chunk not found - this is expected for new chunks
		return nil, nil
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	// Deserialize shard from cached response
	shardObj := shard.NewShard()
	if err := shardObj.Decode(resp.Body, false); err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	xorbHash, chunkIndex, ok := findChunkLocationInDedupShard(shardObj, chunkHash)
	if !ok {
		return nil, nil
	}
	return &upload.DeduplicationResult{
		ChunkHash:  chunkHash,
		IsNew:      false,
		XorbHash:   xorbHash,
		ChunkIndex: chunkIndex,
	}, nil
}

// QueryChunksDeduplication checks multiple chunk hashes against the global
// deduplication index. It prefers the batch endpoint and falls back to single
// chunk queries when the batch endpoint is unavailable.
func (c *Client) QueryChunksDeduplication(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	results := make(map[xet.Hash]*upload.DeduplicationResult, len(chunkHashes))
	if len(chunkHashes) == 0 {
		return results, nil
	}

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

		result := &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
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
			results[chunkHash] = &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
		}
	}

	return results, nil
}

func (c *Client) queryChunksDeduplicationFallback(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	results := make(map[xet.Hash]*upload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		result, err := c.QueryChunkDeduplication(ctx, chunkHash)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
		}
		results[chunkHash] = result
	}
	return results, nil
}

func findChunkLocationInDedupShard(shardData *shard.Shard, chunkHash xet.Hash) (xet.Hash, uint32, bool) {
	for _, casBlock := range shardData.CASInfos {
		for i, casChunk := range casBlock.Chunks {
			if casChunk.ChunkHash == chunkHash {
				return casBlock.CASHash, uint32(i), true
			}
		}
	}

	return xet.Hash{}, 0, false
}
