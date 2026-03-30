package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet/pkg/xorb"
)

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*XorbUploadResponse, error) {
	// Serialize the xorb with full format (including footer)
	reader, err := xorb.Serialize(xorbObj, false)
	if err != nil {
		return nil, fmt.Errorf("serialize xorb: %w", err)
	}

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbObj.Hash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
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
	xorbObj, err := xorb.Deserialize(resp.Body, chunkOnly)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil

}
