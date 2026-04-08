package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/upload"
)

// HasXorb checks whether a xorb already exists on the server.
func (c *Client) HasXorb(ctx context.Context, xorbHash xet.Hash) (bool, error) {
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", c.baseURL, c.namespace, xorbHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}

	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}

	if err := reqError(req, resp); err != nil {
		return false, err
	}

	return false, nil
}

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

	attempts := max(c.uploadRetries+1, 1)

	var resp *http.Response
	var req *http.Request
	var lastErr error
	for i := 0; i < attempts; i++ {
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to start: %w", err)
		}

		var body io.Reader = reader
		if c.progressFunc != nil {
			body = progress.NewProgressReader(body, url, contentLength, c.progressFunc)
		}

		req, err = http.NewRequestWithContext(ctx, http.MethodPost, url, body)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		req.ContentLength = contentLength
		req.Header.Set("Content-Type", "application/octet-stream")
		if c.token != "" {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}

		resp, err = c.httpClient.Do(req)
		if err == nil {
			break
		}
		if !isNetworkError(err) {
			return nil, fmt.Errorf("do request: %w", err)
		}
		lastErr = err
		if req.Context().Err() != nil {
			break
		}
	}
	if resp == nil {
		return nil, fmt.Errorf("network error after %d attempts: %w", attempts, lastErr)
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
func (c *Client) DownloadXorb(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	maps.Copy(req.Header, header)

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
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
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: body,
		Closer: resp.Body,
	}, nil
}

// DownloadXorbsMultipart sends one multi-range request and returns the multipart reader.
// Caller must close the returned closer.
func (c *Client) DownloadXorbsMultipart(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error) {
	if len(header) == 0 {
		return nil, nil, fmt.Errorf("empty ranges")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}
	maps.Copy(req.Header, header)

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, nil, err
	}

	if err := reqError(req, resp); err != nil {
		resp.Body.Close()
		return nil, nil, err
	}

	mediaType, params, parseErr := parseMediaType(resp.Header.Get("Content-Type"))
	if parseErr != nil {
		mediaType = ""
	}

	if mediaType != "multipart/byteranges" {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("expected multipart/byteranges response for multi-range request, got content-type %q", resp.Header.Get("Content-Type"))
	}

	boundary := params["boundary"]
	if boundary == "" {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("multipart/byteranges response missing boundary")
	}

	var body io.Reader = resp.Body
	if c.progressFunc != nil {
		body = progress.NewProgressReader(body, url, resp.ContentLength, c.progressFunc)
	}

	return multipart.NewReader(body, boundary), resp.Body, nil
}

// parseMediaType is a wrapper around mime.ParseMediaType that can handle some non-compliant content types seen in the wild.
func parseMediaType(contentType string) (mediaType string, params map[string]string, err error) {
	mediaType, params, err = mime.ParseMediaType(contentType)
	if err != nil {
		// Handle cases where the media type is not strictly compliant, e.g. "multipart/byteranges; boundary=CloudFront:9AD1BEA90E67CF6B5180502F7C1727DB"
		if err == mime.ErrInvalidMediaParameter &&
			mediaType == "multipart/byteranges" {
			parts := strings.SplitSeq(contentType, ";")
			for part := range parts {
				part = strings.TrimSpace(part)
				if after, ok := strings.CutPrefix(part, "boundary="); ok {
					params = map[string]string{
						"boundary": after,
					}
					err = nil
					break
				}
			}
		}
	}
	return
}
