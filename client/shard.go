package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

// UploadShard uploads a serialized shard to the server
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	url := fmt.Sprintf("%s/shards", c.baseURL)

	r, err := upload.EncodeShard(shardObj)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

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
func (c *Client) QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*shard.Shard, error) {
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

	// Deserialize shard directly from response body
	shardObj, err := upload.DecodeChunkQueryResponse(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	return shardObj, nil
}
