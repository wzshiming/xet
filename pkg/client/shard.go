package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/upload"
)

// UploadShard uploads a serialized shard to the server
func (c *Client) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return upload.EncodeAndUploadShard(ctx, c, shardObj)
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
	shard, err := shard.Decode(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	return shard, nil
}
