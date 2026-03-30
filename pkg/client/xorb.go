package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/wzshiming/xet/pkg/upload"
	"github.com/wzshiming/xet/pkg/xorb"
)

// GetBaseURL returns the base URL of the server
func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// GetNamespace returns the namespace for uploads
func (c *Client) GetNamespace() string {
	return c.namespace
}

// GetToken returns the authentication token
func (c *Client) GetToken() string {
	return c.token
}

// GetHTTPClient returns the HTTP client to use for requests
func (c *Client) GetHTTPClient() *http.Client {
	return c.httpClient
}

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	return upload.EncodeAndUploadXorb(ctx, c, xorbObj)
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
	xorbObj, err := xorb.Decode(resp.Body, chunkOnly)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil

}
