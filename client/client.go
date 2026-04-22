package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/progress"
)

// AuthProvider provides dynamic base URL and access token values.
type AuthProvider interface {
	BaseURL(context.Context) (string, error)
	Token(context.Context) (string, error)
}

type authProviderFuncs struct {
	baseURL func(ctx context.Context) (string, error)
	token   func(ctx context.Context) (string, error)
}

func (p *authProviderFuncs) BaseURL(ctx context.Context) (string, error) {
	if p == nil || p.baseURL == nil {
		return "", nil
	}
	return p.baseURL(ctx)
}

func (p *authProviderFuncs) Token(ctx context.Context) (string, error) {
	if p == nil || p.token == nil {
		return "", nil
	}
	return p.token(ctx)
}

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL       string
	token         string
	authProvider  AuthProvider
	httpClient    *http.Client
	getHttpClient *http.Client
	namespace     string
	concurrency   int
	retries       int
	progressFunc  progress.ProgressFunc
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

// WithAuthProvider sets a dynamic provider for both base URL and token.
func WithAuthProvider(provider AuthProvider) Options {
	return func(c *Client) {
		c.authProvider = provider
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

	if c.httpClient.Transport == nil {
		c.httpClient.Transport = http.DefaultTransport
	}

	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		transport.DisableKeepAlives = true
		transport.ForceAttemptHTTP2 = false
	}

	c.getHttpClient = &http.Client{
		CheckRedirect: c.httpClient.CheckRedirect,
		Jar:           c.httpClient.Jar,
		Timeout:       c.httpClient.Timeout,
		Transport: httpseek.NewMustReaderTransport(c.httpClient.Transport,
			func(r *http.Request, retry int, err error) error {
				if retry >= c.retryAttempts() {
					return fmt.Errorf("max retries reached: %w", err)
				}
				return nil
			}),
	}

	return c
}

// getToken calls the configured tokenFunc and returns the bearer token string.
// If no tokenFunc is set it returns an empty string.
func (c *Client) getToken(ctx context.Context) (string, error) {
	if c.authProvider != nil {
		token, err := c.authProvider.Token(ctx)
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
func (c *Client) getBaseURL(ctx context.Context) (string, error) {
	if c.authProvider != nil {
		baseURL, err := c.authProvider.BaseURL(ctx)
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
