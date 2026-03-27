package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
)

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
	namespace  string
}

// ClientOptions configures the API client
type ClientOptions struct {
	BaseURL   string
	Token     string
	Namespace string
	Timeout   time.Duration
}

// NewClient creates a new API client
func NewClient(opts ClientOptions) *Client {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}

	return &Client{
		baseURL: strings.TrimSuffix(opts.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: opts.Timeout,
		},
		token:     opts.Token,
		namespace: opts.Namespace,
	}
}

type ReqOpt func(req *http.Request)

func WithRange(start, end int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
}

func WithRangeStart(start int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}
}

func WithRangeEnd(end int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=-%d", end))
	}
}

// GetReconstruction retrieves reconstruction information for a file
func (c *Client) GetReconstruction(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (*ReconstructionResponse, error) {
	url := fmt.Sprintf("%s/v1/reconstructions/%s", c.baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var reconstruction ReconstructionResponse
	if err := json.NewDecoder(resp.Body).Decode(&reconstruction); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &reconstruction, nil
}

var errNotFound = fmt.Errorf("404 not found")

func reqError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}

	hasRange := req.Header.Get("Range") != ""

	if hasRange {
		if resp.StatusCode != http.StatusPartialContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}
	} else {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}
	}
	return nil
}

// UploadXorb uploads a serialized xorb to the server
func (c *Client) UploadXorb(ctx context.Context, xorbHash xet.Hash, xorbData []byte) (*XorbUploadResponse, error) {
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(xorbData))
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

	var uploadResp XorbUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

// UploadShard uploads a serialized shard to the server
func (c *Client) UploadShard(ctx context.Context, shardData []byte) (*ShardUploadResponse, error) {
	url := fmt.Sprintf("%s/shards", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(shardData))
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

	var uploadResp ShardUploadResponse
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

	// Read shard data
	shardData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	// Deserialize shard
	shard, err := shard.Deserialize(shardData)
	if err != nil {
		return nil, fmt.Errorf("deserialize shard: %w", err)
	}

	return shard, nil
}

// GetReconstructionV2 retrieves V2 reconstruction information for a file
func (c *Client) GetReconstructionV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (*ReconstructionResponseV2, error) {
	url := fmt.Sprintf("%s/v2/reconstructions/%s", c.baseURL, fileHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	var reconstruction ReconstructionResponseV2
	if err := json.NewDecoder(resp.Body).Decode(&reconstruction); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &reconstruction, nil
}

// DownloadXorbData downloads xorb data from a URL with optional byte range
func (c *Client) DownloadXorbData(ctx context.Context, url string, opts ...ReqOpt) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for _, opt := range opts {
		opt(req)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if err := reqError(req, resp); err != nil {
		return nil, err
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	return data, nil
}
