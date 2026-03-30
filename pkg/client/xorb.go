package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// DownloadXorb downloads and deserializes a xorb from a URL
// This is a high-level method that handles downloading raw bytes and deserializing into a Xorb object.
// The chunkOnly parameter is automatically determined based on whether a Range request is being made.
func (c *Client) DownloadXorb(ctx context.Context, url string, opts ...ReqOpt) (*xorb.Xorb, error) {
	// Download the raw xorb data
	xorbData, err := c.DownloadXorbData(ctx, url, opts...)
	if err != nil {
		return nil, err
	}

	// Determine if this is a range request (chunks-only format)
	chunkOnly := false
	for _, opt := range opts {
		// Check if any of the opts set a Range header
		// We can detect this by creating a dummy request and checking if Range is set
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		opt(req)
		if req.Header.Get("Range") != "" {
			chunkOnly = true
			break
		}
	}

	// Deserialize the xorb
	xorbObj, err := xorb.DeserializeBytes(xorbData, chunkOnly)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil
}

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*XorbUploadResponse, error) {
	// Serialize the xorb with full format (including footer)
	serialized, err := xorb.SerializeBytes(xorbObj, false)
	if err != nil {
		return nil, fmt.Errorf("serialize xorb: %w", err)
	}

	// Upload using the internal helper
	return c.uploadXorbBytes(ctx, xorbObj.Hash, serialized)
}

// uploadXorbBytes uploads serialized xorb bytes to the server
// This is an internal helper method used by UploadXorb
func (c *Client) uploadXorbBytes(ctx context.Context, xorbHash xet.Hash, xorbData []byte) (*XorbUploadResponse, error) {
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

// DownloadXorbData downloads xorb data from a URL with optional byte range
// This is a low-level method that returns raw bytes. Use DownloadXorb for a higher-level API.
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
