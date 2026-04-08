package client

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/wzshiming/xet/progress"
)

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL      string
	httpClient   *http.Client
	token        string
	namespace    string
	cacheDirPath string
	concurrency  int
	retries      int
	progressFunc progress.ProgressFunc
}

type Options func(*Client)

// WithBaseURL sets the base URL for the API endpoints, allowing the client to connect to different servers or environments.
func WithBaseURL(url string) Options {
	return func(c *Client) {
		c.baseURL = url
	}
}

// WithHTTPClient allows users to provide a custom HTTP client, which can be used to configure timeouts, TLS settings, or other HTTP behaviors.
func WithHTTPClient(httpClient *http.Client) Options {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithToken sets the authentication token for the client, which will be included in the Authorization header of API requests.
func WithToken(token string) Options {
	return func(c *Client) {
		c.token = token
	}
}

// WithNamespace sets the namespace for the client, which is used to scope resources on the server.
func WithNamespace(namespace string) Options {
	return func(c *Client) {
		c.namespace = namespace
	}
}

// WithCacheDir sets the directory path for caching API responses and serialized data. If not set, caching is disabled.
func WithCacheDir(dir string) Options {
	return func(c *Client) {
		c.cacheDirPath = dir
	}
}

// WithProgressFunc sets a callback function to receive progress updates for uploads and downloads.
func WithProgressFunc(progressFunc progress.ProgressFunc) Options {
	return func(c *Client) {
		c.progressFunc = progressFunc
	}
}

// WithConcurrency sets the concurrency level for uploads and downloads, allowing multiple parts of a file to be processed in parallel for improved performance.
func WithConcurrency(concurrency int) Options {
	return func(c *Client) {
		c.concurrency = concurrency
	}
}

// WithDownloadRetries sets the number of retries for network requests.
// Kept for backward compatibility; it now shares one unified retry setting.
// Values less than 0 are treated as 0.
func WithDownloadRetries(retries int) Options {
	return func(c *Client) {
		if retries < 0 {
			retries = 0
		}
		c.retries = retries
	}
}

// WithUploadRetries sets the number of retries for network requests.
// Kept for backward compatibility; it now shares one unified retry setting.
// Values less than 0 are treated as 0.
func WithUploadRetries(retries int) Options {
	return func(c *Client) {
		if retries < 0 {
			retries = 0
		}
		c.retries = retries
	}
}

// WithRetries sets the number of retries for all network requests when transient
// network errors occur. Values less than 0 are treated as 0.
func WithRetries(retries int) Options {
	return func(c *Client) {
		if retries < 0 {
			retries = 0
		}
		c.retries = retries
	}
}

// NewClient creates a new API client
func NewClient(opts ...Options) *Client {
	c := &Client{
		httpClient:  &http.Client{},
		namespace:   "default",
		concurrency: 4,
		retries:     5,
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

var errNotFound = fmt.Errorf("404 not found")

func reqError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}

	ranges := req.Header.Get("Range")

	if ranges != "" {
		if resp.StatusCode != http.StatusPartialContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("url %s: range: %s: API error (status %s): %s", req.URL.String(), ranges, resp.Status, string(body))
		}
	} else {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("url %s: API error (status %s): %s", req.URL.String(), resp.Status, string(body))
		}
	}
	return nil
}

func isNetworkError(err error) bool {
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}
	return false
}

func isServerError(statusCode int) bool {
	return statusCode == http.StatusInternalServerError ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}

func (c *Client) retryAttempts() int {
	return max(c.retries+1, 1)
}

func resetRequestBody(req *http.Request) error {
	if req.Body == nil {
		return nil
	}

	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return err
		}
		req.Body = body
		return nil
	}

	return fmt.Errorf("request body is not retryable")
}

func (c *Client) doWithNetworkRetry(req *http.Request) (*http.Response, error) {
	attempts := c.retryAttempts()

	var lastErr error
	for i := range attempts {
		if i > 0 {
			if err := resetRequestBody(req); err != nil {
				return nil, fmt.Errorf("reset request body: %w", err)
			}
		}

		resp, err := c.httpClient.Do(req)
		if err == nil {
			if isServerError(resp.StatusCode) {
				lastErr = fmt.Errorf("server error status %s", resp.Status)
				resp.Body.Close()
				if req.Context().Err() != nil {
					break
				}
				continue
			}
			return resp, nil
		}
		if !isNetworkError(err) {
			return nil, fmt.Errorf("do request: %w", err)
		}
		lastErr = err
		if req.Context().Err() != nil {
			break
		}
	}

	return nil, fmt.Errorf("network error after %d attempts: %w", attempts, lastErr)
}
