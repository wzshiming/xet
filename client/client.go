package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/progress"
)

// AuthProvider provides dynamic base URL and access token values.
type AuthProvider interface {
	BaseURL(context.Context) (string, error)
	Token(context.Context) (string, error)
}

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL       string
	token         string
	httpClient    *http.Client
	getHttpClient *http.Client
	namespace     string
	concurrency   int
	retries       int
	idleTimeout   time.Duration
	progressFunc  progress.ProgressFunc
	cacheDir      string
	cacheSize     int64
	cacheManager  *download.CacheManager
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

// WithToken sets a static authentication token for the client. The token is
// included verbatim in the Authorization header of every request.
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

// WithIdleTimeout sets the GET/HEAD read-idle timeout (default DefaultIdleTimeout; <= 0 disables it).
func WithIdleTimeout(timeout time.Duration) Options {
	return func(c *Client) {
		c.idleTimeout = timeout
	}
}

// WithCacheDir enables the persistent disk chunk cache at the given directory.
func WithCacheDir(cacheDir string) Options {
	return func(c *Client) {
		c.cacheDir = cacheDir
	}
}

// WithCacheSize bounds the total size in bytes of the persistent disk chunk
// cache; least recently used entries are evicted once the limit is exceeded.
// Zero or negative keeps the cache unbounded. Defaults to
// download.DefaultCacheSize (10 GB), matching xet-core.
func WithCacheSize(sizeBytes int64) Options {
	return func(c *Client) {
		c.cacheSize = sizeBytes
	}
}

// NewClient creates a new API client
func NewClient(opts ...Options) (*Client, error) {
	c := &Client{
		httpClient:  &http.Client{},
		namespace:   "default",
		concurrency: 4,
		retries:     5,
		idleTimeout: DefaultIdleTimeout,
		cacheSize:   download.DefaultCacheSize,
	}
	for _, opt := range opts {
		opt(c)
	}

	c.cacheManager = download.NewCacheManager(c.cacheDir, c.cacheSize)

	if c.httpClient.Transport == nil {
		c.httpClient.Transport = http.DefaultTransport.(*http.Transport).Clone()
	}

	c.getHttpClient = &http.Client{
		CheckRedirect: c.httpClient.CheckRedirect,
		Jar:           c.httpClient.Jar,
		Timeout:       c.httpClient.Timeout,
		Transport: httpseek.NewMustReaderTransport(&retryStatusTransport{base: NewIdleTimeoutTransport(c.httpClient.Transport, c.idleTimeout), c: c},
			func(r *http.Request, retry int, err error) error {
				if retry >= c.retryAttempts() {
					var timeout interface{ Timeout() bool }
					if errors.As(err, &timeout) && timeout.Timeout() {
						return err // unwrapped so url.Error.Timeout stays true
					}
					return fmt.Errorf("max retries reached: %w", err)
				}
				return nil
			}),
	}

	return c, nil
}

type Usage struct {
	Download download.CacheUsage
}

// Usage includes temporary and incomplete files in the cache directory.
func (c *Client) Usage(ctx context.Context) (Usage, error) {
	downloadUsage, err := c.cacheManager.Usage(ctx)
	if err != nil {
		return Usage{}, err
	}
	return Usage{Download: downloadUsage}, nil
}

// getToken calls the configured tokenFunc and returns the bearer token string.
// If no tokenFunc is set it returns an empty string.
func (c *Client) getToken(ctx context.Context, provider AuthProvider) (string, error) {
	if provider != nil {
		token, err := provider.Token(ctx)
		if err != nil {
			return "", err
		}
		if token != "" {
			return token, nil
		}
	}
	return c.token, nil
}

// getBaseURL calls the configured baseURLFunc and returns the request base URL.
// If no baseURLFunc is set it returns the static baseURL configured on client.
func (c *Client) getBaseURL(ctx context.Context, provider AuthProvider) (string, error) {
	if provider != nil {
		baseURL, err := provider.BaseURL(ctx)
		if err != nil {
			return "", err
		}
		if baseURL != "" {
			return baseURL, nil
		}
	}
	return c.baseURL, nil
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

// isRetryableStatus matches xet-core's transient set: 408, 429 and the 5xx gateway family.
func isRetryableStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

func (c *Client) retryAttempts() int {
	return max(c.retries+1, 1)
}

// retryStatusTransport re-sends GET/HEAD requests answered with a retryable
// status. It sits below httpseek so a status on a resumed range open is retried
// too, while network failures stay with httpseek's own resume handling.
type retryStatusTransport struct {
	base http.RoundTripper
	c    *Client
}

func (t *retryStatusTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return t.base.RoundTrip(req)
	}
	attempts := t.c.retryAttempts()
	for i := 1; ; i++ {
		resp, err := t.base.RoundTrip(req)
		if err != nil || !isRetryableStatus(resp.StatusCode) || i >= attempts {
			return resp, err
		}
		resp.Body.Close()
		if ctxErr := req.Context().Err(); ctxErr != nil {
			return nil, fmt.Errorf("server error status %s: %w", resp.Status, ctxErr)
		}
	}
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

// do issues one request on the plain httpClient, guarding GET/HEAD with the read-idle timeout.
func (c *Client) do(req *http.Request) (*http.Response, error) {
	httpClient := c.httpClient
	if c.idleTimeout > 0 && (req.Method == http.MethodGet || req.Method == http.MethodHead) {
		guarded := *c.httpClient
		guarded.Transport = NewIdleTimeoutTransport(guarded.Transport, c.idleTimeout)
		httpClient = &guarded
	}
	return httpClient.Do(req)
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

		resp, err := c.do(req)
		if err == nil {
			if isRetryableStatus(resp.StatusCode) {
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
