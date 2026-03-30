package upload

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// ClientAdapter provides access to client operations needed for upload encoding
type ClientAdapter interface {
	// GetBaseURL returns the base URL of the server
	GetBaseURL() string
	// GetNamespace returns the namespace for uploads
	GetNamespace() string
	// GetToken returns the authentication token
	GetToken() string
	// GetHTTPClient returns the HTTP client to use for requests
	GetHTTPClient() *http.Client
}

// EncodeAndUploadXorb serializes and uploads a xorb to the server
func EncodeAndUploadXorb(ctx context.Context, client ClientAdapter, xorbObj *xorb.Xorb) (*XorbUploadResponse, error) {
	// Serialize the xorb with full format (including footer)
	reader, err := xorb.Encode(xorbObj, false)
	if err != nil {
		return nil, fmt.Errorf("serialize xorb: %w", err)
	}

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", client.GetBaseURL(), client.GetNamespace(), xorbObj.Hash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	token := client.GetToken()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.GetHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp XorbUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

// EncodeAndUploadShard serializes and uploads a shard to the server
func EncodeAndUploadShard(ctx context.Context, client ClientAdapter, shardObj *shard.Shard) (*ShardUploadResponse, error) {
	url := fmt.Sprintf("%s/shards", client.GetBaseURL())

	r, err := shard.Encode(shardObj, false)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	token := client.GetToken()
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.GetHTTPClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp ShardUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}
