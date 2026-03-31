package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

// UploadShard uploads a serialized shard to the server
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	url := fmt.Sprintf("%s/shards", c.baseURL)

	r, encodeErr := upload.EncodeShard(shardObj)
	if encodeErr != nil {
		return nil, encodeErr
	}
	cacheFile, contentLength, err := c.spoolReaderToCache(r, "upload-shard")
	if err != nil {
		return nil, fmt.Errorf("cache serialized shard: %w", err)
	}

	var body io.Reader = cacheFile
	if onUpload := getUploadProgress(ctx); onUpload != nil {
		body = wrapReaderWithReadProgress(body, onUpload)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = contentLength
	req.Header.Set("Content-Length", fmt.Sprintf("%d", contentLength))
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer closeAndIgnoreError(resp.Body)

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
func (c *Client) QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*shard.Shard, error) {
	url := fmt.Sprintf("%s/v1/chunks/%s/%s", c.baseURL, c.namespace, chunkHash.String())
	cacheKey := stableCacheKey(c.namespace, chunkHash.String())
	cacheFile, _, hit, err := c.openPersistentCache("query-chunk", cacheKey, ".bin")
	if err != nil {
		return nil, fmt.Errorf("open chunk query cache: %w", err)
	}
	if hit {
		defer closeAndIgnoreError(cacheFile)
		shardObj, decodeErr := upload.DecodeChunkQueryResponse(cacheFile)
		if decodeErr == nil {
			return shardObj, nil
		}
	}

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
	defer closeAndIgnoreError(resp.Body)

	if resp.StatusCode == http.StatusNotFound {
		// Chunk not found - this is expected for new chunks
		return nil, nil
	}

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	cacheFile, _, err = c.writePersistentCache("query-chunk", cacheKey, ".bin", resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cache chunk query response: %w", err)
	}
	defer closeAndIgnoreError(cacheFile)

	// Deserialize shard from cached response
	shardObj, err := upload.DecodeChunkQueryResponse(cacheFile)
	if err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	return shardObj, nil
}
