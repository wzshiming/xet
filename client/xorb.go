package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	// Serialize the xorb with full format (including footer)
	reader, err := upload.EncodeXorb(xorbObj)
	if err != nil {
		return nil, fmt.Errorf("serialize xorb: %w", err)
	}

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbObj.Hash.String())

	// TODO: For large xorb uploads, we may want to stream the upload instead of buffering the entire serialized xorb in memory.
	// This would require implementing an io.Reader that can serialize the xorb on-the-fly as it's being read.
	// For now, we buffer it in memory for simplicity.
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read serialized xorb: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewBuffer(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(data)))
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

	var uploadResp upload.XorbUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

// DownloadXorb downloads and deserializes a xorb from a URL
// This is a high-level method that handles downloading and deserialization of a Xorb object from a given URL.
func (c *Client) DownloadXorb(ctx context.Context, url string, opts ...ReqOpt) (*xorb.Xorb, error) {
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

	// Determine if this is a range request (chunks-only format)
	// Check if a Range header was set on the actual request
	chunkOnly := req.Header.Get("Range") != ""

	// Deserialize the xorb
	xorbObj, err := xorb.Decode(resp.Body, chunkOnly)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil
}
