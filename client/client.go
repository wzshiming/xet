package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
	namespace  string
}

type Options func(*Client)

func WithBaseURL(url string) Options {
	return func(c *Client) {
		c.baseURL = url
	}
}

func WithHTTPClient(httpClient *http.Client) Options {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

func WithToken(token string) Options {
	return func(c *Client) {
		c.token = token
	}
}

func WithNamespace(namespace string) Options {
	return func(c *Client) {
		c.namespace = namespace
	}
}

// NewClient creates a new API client
func NewClient(opts ...Options) *Client {
	c := &Client{
		httpClient: &http.Client{},
		namespace:  "default",
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

type ReqOpt func(req *http.Request)

type downloadProgressContextKey struct{}
type uploadProgressContextKey struct{}

func WithDownloadProgress(progress func(int64)) ReqOpt {
	return func(req *http.Request) {
		ctx := context.WithValue(req.Context(), downloadProgressContextKey{}, progress)
		*req = *req.WithContext(ctx)
	}
}

func withUploadProgressContext(ctx context.Context, progress func(int64)) context.Context {
	if progress == nil {
		return ctx
	}
	return context.WithValue(ctx, uploadProgressContextKey{}, progress)
}

func getUploadProgress(ctx context.Context) func(int64) {
	progress, _ := ctx.Value(uploadProgressContextKey{}).(func(int64))
	return progress
}

func WithRange(start, end int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	}
}

func WithRangeStart(start int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", start))
	}
}

func WithRangeEnd(end int64) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set("Range", fmt.Sprintf("bytes=-%d", end))
	}
}

var errNotFound = fmt.Errorf("404 not found")

func reqError(req *http.Request, resp *http.Response) error {
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}

	hasRange := req.Header.Get("Range") != ""

	if hasRange {
		if resp.StatusCode != http.StatusPartialContent {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}
	} else {
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}
	}
	return nil
}
