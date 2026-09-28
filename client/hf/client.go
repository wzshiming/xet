package hf

import (
	"net/http"

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
	var casOpts []client.Options
	if c.httpClient != nil {
		casOpts = append(casOpts, client.WithHTTPClient(c.httpClient))
	}
	casOpts = append(casOpts, c.clientOpts...)
	// The repo binding wins over any provider in the CAS client options.
	casOpts = append(casOpts, client.WithUpstreamProvider(NewTokenProvider(c.httpClient, c.repo, c.token)))
	cas, err := client.NewClient(casOpts...)
	if err != nil {
		return nil, err
	}
	c.Client = cas
	c.hub = hubClient(c.httpClient)
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
