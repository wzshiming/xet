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
	"github.com/wzshiming/xet/upload"
)

// HasXorb checks whether a xorb already exists on the server.
func (c *Client) HasXorb(ctx context.Context, xorbHash xet.Hash) (bool, error) {
	return c.HasXorbWithAuthProvider(ctx, nil, xorbHash)
}

// HasXorbWithAuthProvider checks whether a xorb already exists on the server
// with a per-call auth provider.
func (c *Client) HasXorbWithAuthProvider(ctx context.Context, provider AuthProvider, xorbHash xet.Hash) (bool, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return false, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, c.namespace, xorbHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}

	if token, err := c.getToken(ctx, provider); err != nil {
		return false, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
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
	return c.UploadXorbWithAuthProvider(ctx, nil, xorbHash, reader)
}

// UploadXorbWithAuthProvider serializes and uploads a xorb to the server with
// a per-call auth provider.
func (c *Client) UploadXorbWithAuthProvider(ctx context.Context, provider AuthProvider, xorbHash xet.Hash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	startOffset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("seek current: %w", err)
	}

	contentLength, err := reader.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek to end: %w", err)
	}
	if _, err := reader.Seek(startOffset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to start offset: %w", err)
	}

	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, c.namespace, xorbHash.String())

	makeBody := func() (io.ReadCloser, error) {
		if _, err := reader.Seek(startOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to start offset: %w", err)
		}
		return io.NopCloser(reader), nil
	}

	body, err := makeBody()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.GetBody = makeBody
	req.ContentLength = contentLength - startOffset
	req.Header.Set("Content-Type", "application/octet-stream")
	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := c.doWithNetworkRetry(req)
	if err != nil {
		return nil, err
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

// DownloadXorb fetches the raw xorb bytes for the given hash directly
// from the upstream CAS server, including the Authorization header.
// The caller must close the returned ReadCloser.
func (c *Client) DownloadXorb(ctx context.Context, namespace string, xorbHash xet.Hash) (io.ReadCloser, error) {
	return c.DownloadXorbWithAuthProvider(ctx, nil, namespace, xorbHash)
}

// DownloadXorbWithAuthProvider fetches the raw xorb bytes with a per-call auth
// provider.
func (c *Client) DownloadXorbWithAuthProvider(ctx context.Context, provider AuthProvider, namespace string, xorbHash xet.Hash) (io.ReadCloser, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	xorbURL := fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, namespace, xorbHash.String())

	header := http.Header{}
	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}

	return c.DownloadXorbWithURL(ctx, xorbURL, header)
}

// DownloadXorb downloads a xorb from a URL and returns a streaming Decoder.
// The caller must call Decoder.Close() when done to release the underlying HTTP connection.
func (c *Client) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	maps.Copy(req.Header, header)

	// Use the getHttpClient for retry with resume with range requests.
	resp, err := c.getHttpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch xorb: %w", err)
	}

	if err := reqError(req, resp); err != nil {
		resp.Body.Close()
		return nil, err
	}

	return resp.Body, nil
}

// DownloadXorbsMultipart sends one multi-range request and returns the multipart reader.
// Caller must close the returned closer.
func (c *Client) DownloadXorbsMultipartWithURL(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error) {
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
		return nil, nil, fmt.Errorf("fetch xorbs: %w", err)
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

// FetchXorbRangeWithURL issues a GET to rawURL forwarding the provided headers (typically
// a Range header) and returns the raw *http.Response so the caller can proxy the
// status code and response headers (e.g. Content-Range) verbatim.
// The plain httpClient is used so the response is not modified by MustReaderTransport.
// The caller must close the response body.
func (c *Client) FetchXorbRangeWithURL(ctx context.Context, rawURL string, header http.Header) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	maps.Copy(req.Header, header)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch xorb range: %w", err)
	}
	return resp, nil
}

// FetchXorbRange fetches a raw xorb byte range from the upstream CAS
// server endpoint, adding the client's authentication headers. Useful as a
// fallback when no CDN URL is known for a given xorb hash.
func (c *Client) FetchXorbRange(ctx context.Context, namespace string, xorbHash xet.Hash, header http.Header) (*http.Response, error) {
	return c.FetchXorbRangeWithAuthProvider(ctx, nil, namespace, xorbHash, header)
}

// FetchXorbRangeWithAuthProvider fetches a raw xorb byte range with a per-call
// auth provider.
func (c *Client) FetchXorbRangeWithAuthProvider(ctx context.Context, provider AuthProvider, namespace string, xorbHash xet.Hash, header http.Header) (*http.Response, error) {
	baseURL, err := c.getBaseURL(ctx, provider)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, namespace, xorbHash.String())

	if token, err := c.getToken(ctx, provider); err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	} else if token != "" {
		if header == nil {
			header = make(http.Header)
		}
		header.Set("Authorization", "Bearer "+token)
	}

	return c.FetchXorbRangeWithURL(ctx, url, header)
}
