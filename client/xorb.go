package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// UploadXorb serializes and uploads a xorb to the server
// This is a high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbHash xet.Hash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	contentLength, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to end: %w", err)
	}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to start: %w", err)
	}

	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbHash.String())

	var body io.Reader = reader
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, url, contentLength, c.progressFunc)
	}

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

	var uploadResp upload.XorbUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &uploadResp, nil
}

// DownloadXorb downloads a xorb from a URL and returns a streaming Decoder.
// The caller must call Decoder.Close() when done to release the underlying HTTP connection.
func (c *Client) DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Decoder, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	for k, v := range header {
		req.Header[k] = v
	}

	withFooter := req.Header.Get("Range") == ""

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	if err := reqError(req, resp); err != nil {
		resp.Body.Close()
		return nil, err
	}

	var body io.Reader = resp.Body
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, url, resp.ContentLength, c.progressFunc)
	}

	// resp.Body is owned by the Decoder; it will be closed via SetCloser.
	return xorb.NewDecoder(body, withFooter), nil
}
