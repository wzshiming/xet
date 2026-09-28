package hf

import (
	"net/http"

	"github.com/wzshiming/xet/client"
)

// Client is a xet client bound to one hub repository revision: CAS operations use its tokens and Commit writes to it.
type Client struct {
	*client.Client
	repo  Repo // normalized
	token string
	hub   *http.Client // hub requests: no redirects, read-idle guarded
}

// NewClient binds a client to repo with token (empty: anonymous); httpClient (nil: default) serves the hub and the CAS, opts configure the CAS client.
func NewClient(httpClient *http.Client, repo Repo, token string, opts ...client.Options) (*Client, error) {
	repo = repo.normalized()
	var casOpts []client.Options
	if httpClient != nil {
		casOpts = append(casOpts, client.WithHTTPClient(httpClient))
	}
	casOpts = append(casOpts, opts...)
	// The repo binding wins over any provider in opts.
	casOpts = append(casOpts, client.WithUpstreamProvider(NewTokenProvider(httpClient, repo, token)))
	cas, err := client.NewClient(casOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{Client: cas, repo: repo, token: token, hub: hubClient(httpClient)}, nil
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
