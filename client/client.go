package client

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client represents an HTTP client for the XET protocol
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
	namespace  string
}

// ClientOptions configures the API client
type ClientOptions struct {
	BaseURL   string
	Token     string
	Namespace string
	Timeout   time.Duration
}

// NewClient creates a new API client
func NewClient(opts ClientOptions) *Client {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Namespace == "" {
		opts.Namespace = "default"
	}

	return &Client{
		baseURL: strings.TrimSuffix(opts.BaseURL, "/"),
		httpClient: &http.Client{
			Timeout: opts.Timeout,
		},
		token:     opts.Token,
		namespace: opts.Namespace,
	}
}

type ReqOpt func(req *http.Request)

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
