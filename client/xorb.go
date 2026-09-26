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
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/upload"
)

// HasXorb checks whether a xorb already exists on the server.
func (c *Client) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	ctx, baseURL, err := c.casContext(ctx, c.provider, auth.Write)
	if err != nil {
		return false, fmt.Errorf("get base URL: %w", err)
	}
	url := fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, c.namespace, xorbHash.String())

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
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

// UploadXorb serializes and uploads a xorb to the server. This is a
// high-level method that handles serialization and upload of a Xorb object.
func (c *Client) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
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

	ctx, baseURL, err := c.casContext(ctx, c.provider, auth.Write)
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

// DownloadXorb fetches the raw xorb bytes for the given hash directly from
// the bound CAS server. The caller must close the returned ReadCloser.
func (c *Client) DownloadXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadCloser, error) {
	ctx, baseURL, err := c.casContext(ctx, c.provider, auth.Read)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	return c.DownloadXorbWithURL(ctx, fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, namespace, xorbHash.String()), nil)
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
	if err != nil && req.Header.Get("Range") != "" {
		// Some CAS signed-range endpoints return 206 without Content-Range.
		// httpseek correctly rejects that as a resumable HTTP response, but the
		// original one-shot response is still usable, so retry without the
		// resumable transport.
		resp, err = c.doWithNetworkRetry(req)
	}
	if err != nil {
		return nil, fmt.Errorf("fetch xorb: %w", err)
	}

	if req.Header.Get("Range") != "" &&
		(resp.StatusCode == http.StatusPartialContent || isExactWholeRangeResponse(req, resp)) {
		return resp.Body, nil
	}

	if err := reqError(req, resp); err != nil {
		resp.Body.Close()
		return nil, err
	}

	return resp.Body, nil
}

// isExactWholeRangeResponse accepts the behavior used by xet-core's signed
// fetch_term URLs: the URL itself identifies the requested term, so the server
// may return that exact byte range as a 200 response while ignoring Range.
// Requiring the exact Content-Length prevents accidentally treating a full xorb
// response as the requested sub-range.
func isExactWholeRangeResponse(req *http.Request, resp *http.Response) bool {
	if resp.StatusCode != http.StatusOK || resp.ContentLength < 0 {
		return false
	}
	var start, end int64
	n, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end)
	if err != nil || n != 2 || start < 0 || end < start {
		return false
	}
	return resp.ContentLength == end-start+1
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
	resp, err := c.do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch xorb range: %w", err)
	}
	return resp, nil
}

// FetchXorbRange fetches a raw xorb byte range from the bound CAS server
// endpoint. Useful as a fallback when no CDN URL is known for a given xorb hash.
func (c *Client) FetchXorbRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, header http.Header) (*http.Response, error) {
	ctx, baseURL, err := c.casContext(ctx, c.provider, auth.Read)
	if err != nil {
		return nil, fmt.Errorf("get base URL: %w", err)
	}
	return c.FetchXorbRangeWithURL(ctx, fmt.Sprintf("%s/v1/xorbs/%s/%s", baseURL, namespace, xorbHash.String()), header)
}
