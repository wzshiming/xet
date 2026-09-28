package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wzshiming/xet/client"
)

// Client is a xet client bound to one hub repository revision: CAS operations use its tokens and Commit writes to it.
type Client struct {
	*client.Client
	repo       Repo // normalized
	token      string
	hub        *http.Client // hub requests: no redirects, read-idle guarded
	httpClient *http.Client // as configured (nil: default); serves the hub and the CAS
	clientOpts []client.Options
}

type Options func(*Client)

// WithToken authenticates hub and token requests as the user (empty: anonymous).
func WithToken(token string) Options {
	return func(c *Client) {
		c.token = token
	}
}

// WithHTTPClient serves the hub and the CAS (nil: default).
func WithHTTPClient(httpClient *http.Client) Options {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithClientOptions configures the embedded CAS client.
func WithClientOptions(opts ...client.Options) Options {
	return func(c *Client) {
		c.clientOpts = append(c.clientOpts, opts...)
	}
}

// NewClient binds a client to repo; opts set the token (default anonymous), the HTTP client and the CAS client options.
func NewClient(repo Repo, opts ...Options) (*Client, error) {
	c := &Client{repo: repo.normalized()}
	for _, opt := range opts {
		opt(c)
	}
	c.hub = hubClient(c.httpClient)
	var casOpts []client.Options
	if c.httpClient != nil {
		casOpts = append(casOpts, client.WithHTTPClient(c.httpClient))
	}
	casOpts = append(casOpts, c.clientOpts...)
	// The repo binding wins over any provider in the CAS client options.
	casOpts = append(casOpts, client.WithUpstreamProvider(newTokenProvider(c.hub, c.repo, c.token)))
	cas, err := client.NewClient(casOpts...)
	if err != nil {
		return nil, err
	}
	c.Client = cas
	return c, nil
}

// hubClient copies httpClient (nil: default) so hub requests never follow a redirect and are read-idle guarded.
func hubClient(httpClient *http.Client) *http.Client {
	hub := http.Client{}
	if httpClient != nil {
		hub = *httpClient
	}
	hub.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	hub.Transport = client.NewIdleTimeoutTransport(hub.Transport, client.DefaultIdleTimeout)
	return &hub
}

// newRequest builds a hub request, carrying token as a bearer when set.
func newRequest(ctx context.Context, method, url, token string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req, nil
}

// hubError reports a failed hub response with the hub's explanation when it gives one.
func hubError(req *http.Request, resp *http.Response) error {
	msg := hubMessage(resp)
	if msg != "" {
		msg = ": " + msg
	}
	return fmt.Errorf("%s %s: hub API error (status %s)%s", req.Method, req.URL.Path, resp.Status, msg)
}

// hubMessage returns the hub's explanation of a failed response: its X-Error-Message, else the JSON error field, else the body text.
func hubMessage(resp *http.Response) string {
	if msg := resp.Header.Get("X-Error-Message"); msg != "" {
		return msg
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var payload struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(body, &payload); err == nil && payload.Error != "" {
		return payload.Error
	}
	return strings.TrimSpace(string(body))
}
