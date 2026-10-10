package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/upload"
)

// UpstreamProvider selects the CAS endpoint and bearer token for one permission.
type UpstreamProvider interface {
	Resolve(ctx context.Context, perm auth.Permission) (baseURL, token string, err error)
}

// Client represents an HTTP client for the XET protocol
type Client struct {
	provider      UpstreamProvider
	httpClient    *http.Client
	getHttpClient *http.Client
	concurrency   int
	retries       int
	idleTimeout   time.Duration
	progressFunc  progress.ProgressFunc
	cache         *Cache
}

type Options func(*Client)

// WithHTTPClient allows users to provide a custom HTTP client, which can be used to configure timeouts, TLS settings, or other HTTP behaviors.
func WithHTTPClient(httpClient *http.Client) Options {
	return func(c *Client) {
		c.httpClient = httpClient
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

// WithCache shares a cache root; without it the client shares the default NewCache("", download.DefaultCacheSize, upload.DefaultCacheSize).
func WithCache(cache *Cache) Options {
	return func(c *Client) {
		c.cache = cache
	}
}

// defaultCache is the cache root every client built without WithCache shares.
var defaultCache = sync.OnceValue(func() *Cache { return NewCache("", download.DefaultCacheSize, upload.DefaultCacheSize) })

// WithUpstreamProvider binds the CAS endpoint and token source; without it every CAS request fails.
func WithUpstreamProvider(provider UpstreamProvider) Options {
	return func(c *Client) {
		c.provider = provider
	}
}

// NewClient creates a new API client
func NewClient(opts ...Options) (*Client, error) {
	c := &Client{
		httpClient:  &http.Client{},
		concurrency: 4,
		retries:     5,
		idleTimeout: DefaultIdleTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}

	if c.cache == nil {
		c.cache = defaultCache()
	}

	// Copy the caller's client so wrapping its transport never mutates it.
	httpClient := *c.httpClient
	if httpClient.Transport == nil {
		httpClient.Transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	httpClient.Transport = &authTransport{base: httpClient.Transport}
	c.httpClient = &httpClient

	c.getHttpClient = &http.Client{
		CheckRedirect: c.httpClient.CheckRedirect,
		Jar:           c.httpClient.Jar,
		Timeout:       c.httpClient.Timeout,
		Transport: httpseek.NewMustReaderTransport(NewIdleTimeoutTransport(c.httpClient.Transport, c.idleTimeout),
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

// Usage reports the client's cache directory, shared or private.
func (c *Client) Usage(ctx context.Context) (Usage, error) {
	return c.cache.Usage(ctx)
}

var (
	errNotFound     = fmt.Errorf("404 not found")
	errUnauthorized = errors.New("401 Unauthorized")
	errForbidden    = errors.New("403 Forbidden")
)

func reqError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}

	ranges := req.Header.Get("Range")

	if ranges != "" {
		if resp.StatusCode != http.StatusPartialContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("url %s: range: %s: API error (status %w): %s", req.URL.String(), ranges, statusError(resp), string(body))
		}
	} else {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("url %s: API error (status %w): %s", req.URL.String(), statusError(resp), string(body))
		}
	}
	return nil
}

// statusError is resp.Status as an error; a 401 also matches errUnauthorized and a 403 errForbidden so callers can keep them terminal.
func statusError(resp *http.Response) error {
	return statusCodeError{resp.StatusCode, resp.Status}
}

type statusCodeError struct {
	code   int
	status string
}

func (e statusCodeError) Error() string { return e.status }

func (e statusCodeError) Is(target error) bool {
	switch target {
	case errUnauthorized:
		return e.code == http.StatusUnauthorized
	case errForbidden:
		return e.code == http.StatusForbidden
	}
	return false
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
