package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
)

// ResolvedFile is a hub file resolved to its xet hash and the CAS upstream the hub named for it.
type ResolvedFile struct {
	Hash     xet.FileHash
	upstream UpstreamProvider
}

// Resolve HEADs a hub resolve URL once, never following redirects, and returns the file behind its XET links; token (empty: anonymous) authenticates the HEAD and, on the same origin only, the xet-auth fetches.
func (c *Client) Resolve(ctx context.Context, resolveURL, token string) (*ResolvedFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, resolveURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create resolve request: %w", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	httpClient := c.hubClient()
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("resolve request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return resolveResponse(ctx, httpClient, resp, token)
}

// hubClient copies the client's HTTP client so hub requests never follow a redirect and are read-idle guarded like every GET/HEAD.
func (c *Client) hubClient() *http.Client {
	hub := noRedirect(c.httpClient)
	if c.idleTimeout > 0 {
		hub.Transport = NewIdleTimeoutTransport(hub.Transport, c.idleTimeout)
	}
	return hub
}

// DownloadResolved downloads f into w through the upstream it was resolved from, with DownloadFile's V1 fallback and resume behavior.
func (c *Client) DownloadResolved(ctx context.Context, f *ResolvedFile, w io.WriteSeeker) error {
	return c.download(ctx, f.upstream, f.Hash, w)
}

func resolveResponse(ctx context.Context, httpClient *http.Client, resp *http.Response, token string) (*ResolvedFile, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return nil, fmt.Errorf("unexpected status from resolve: %d", resp.StatusCode)
	}

	linkMap := ParseLinkHeaders(resp.Header.Values("Link"))
	reconURLStr := linkMap["xet-reconstruction-info"]
	if reconURLStr == "" {
		return nil, fmt.Errorf("missing xet-reconstruction-info link (status %d)", resp.StatusCode)
	}

	authURL := linkMap["xet-auth"]
	if authURL == "" {
		return nil, fmt.Errorf("missing xet-auth link")
	}

	reconURL, err := url.Parse(reconURLStr)
	if err != nil {
		return nil, fmt.Errorf("parse reconstruction link: %w", err)
	}
	if reconURL.Scheme == "" || reconURL.Host == "" {
		return nil, fmt.Errorf("invalid reconstruction link: %s", reconURLStr)
	}

	// The link normally ends in the hash; a link without one leaves it to the header.
	hashStr := path.Base(reconURL.Path)
	if _, err := xet.ParseFileHash(hashStr); err != nil {
		if hashStr = resp.Header.Get("X-Xet-Hash"); hashStr == "" {
			return nil, fmt.Errorf("missing X-Xet-Hash header in resolve response")
		}
	}

	fileHash, err := xet.ParseFileHash(hashStr)
	if err != nil {
		return nil, fmt.Errorf("parse X-Xet-Hash: %w", err)
	}

	token = authToken(resp, authURL, token)
	initial, err := fetchXETAuthToken(ctx, httpClient, authURL, token)
	if err != nil {
		return nil, fmt.Errorf("fetch xet auth token: %w", err)
	}

	return &ResolvedFile{Hash: fileHash, upstream: newTokenProviderFromURL(httpClient, authURL, token, initial)}, nil
}

// authToken keeps token only when authURL shares the origin the caller resolved against: the first request of resp's redirect chain.
func authToken(resp *http.Response, authURL, token string) string {
	req := resp.Request
	for req != nil && req.Response != nil {
		req = req.Response.Request
	}
	u, err := url.Parse(authURL)
	if req == nil || req.URL == nil || err != nil || u.Scheme+"://"+u.Host != req.URL.Scheme+"://"+req.URL.Host {
		return ""
	}
	return token
}

// ParseLinkHeaders extracts rel -> URL pairs from Link header values.
func ParseLinkHeaders(values []string) map[string]string {
	result := make(map[string]string)
	for _, value := range values {
		parts := strings.SplitSeq(value, ",")
		for part := range parts {
			part = strings.TrimSpace(part)
			if !strings.HasPrefix(part, "<") {
				continue
			}
			end := strings.Index(part, ">")
			if end == -1 {
				continue
			}
			linkURL := part[1:end]
			params := strings.Split(part[end+1:], ";")
			var rel string
			for _, p := range params {
				p = strings.TrimSpace(p)
				prefix := "rel="
				if len(p) < len(prefix) || !strings.EqualFold(p[:len(prefix)], prefix) {
					continue
				}
				rel = strings.Trim(p[len(prefix):], "\"")
				break
			}
			if rel != "" {
				result[rel] = linkURL
			}
		}
	}
	return result
}

// tokenExpiryMargin renews a token this long before its exp so no request is sent with one about to expire.
const tokenExpiryMargin = 1 * time.Minute

// tokenData holds a fetched CAS access token and its metadata.
type tokenData struct {
	BaseURL string
	Token   string
	Exp     time.Time
}

// tokenProvider caches one CAS token per permission, refreshing each before it expires.
type tokenProvider struct {
	httpClient *http.Client
	tokenURLs  map[auth.Permission]string // token endpoint per permission
	hubToken   string                     // hub access token used in the Authorization header

	mu     sync.Mutex
	tokens map[auth.Permission]*tokenData
}

// NewTokenProvider returns a provider fetching each permission's CAS token from endpoints[perm] with hubToken, renewing it before its exp.
func NewTokenProvider(httpClient *http.Client, hubToken string, endpoints map[auth.Permission]string) UpstreamProvider {
	return &tokenProvider{
		httpClient: httpClient,
		tokenURLs:  endpoints,
		hubToken:   hubToken,
		tokens:     map[auth.Permission]*tokenData{},
	}
}

// newTokenProviderFromURL creates a read-only provider refreshing through authURL with hubToken; initial saves the first round-trip.
func newTokenProviderFromURL(httpClient *http.Client, authURL, hubToken string, initial *tokenData) UpstreamProvider {
	return &tokenProvider{
		httpClient: httpClient,
		tokenURLs:  map[auth.Permission]string{auth.Read: authURL},
		hubToken:   hubToken,
		tokens:     map[auth.Permission]*tokenData{auth.Read: initial},
	}
}

// Resolve returns the CAS base URL and access token for perm, fetching or refreshing the token as needed.
func (p *tokenProvider) Resolve(ctx context.Context, perm auth.Permission) (string, string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	tok := p.tokens[perm]
	if tok == nil || !time.Now().Add(tokenExpiryMargin).Before(tok.Exp) {
		var err error
		if tok, err = p.fetch(ctx, perm); err != nil {
			return "", "", err
		}
	}
	return tok.BaseURL, tok.Token, nil
}

func (p *tokenProvider) fetch(ctx context.Context, perm auth.Permission) (*tokenData, error) {
	tokenURL := p.tokenURLs[perm]
	if tokenURL == "" {
		return nil, fmt.Errorf("no %s token endpoint", perm)
	}
	tok, err := fetchXETAuthToken(ctx, p.httpClient, tokenURL, p.hubToken)
	if err != nil {
		return nil, err
	}
	p.tokens[perm] = tok
	return tok, nil
}

type tokenResp struct {
	CASURL string `json:"casUrl"`
	Token  string `json:"accessToken"`
	Exp    int64  `json:"exp"`
}

// noRedirect copies httpClient (nil: a 30s-timeout client) so a credential never follows a redirect.
func noRedirect(httpClient *http.Client) *http.Client {
	c := http.Client{Timeout: 30 * time.Second}
	if httpClient != nil {
		c = *httpClient
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &c
}

func fetchXETAuthToken(ctx context.Context, httpClient *http.Client, tokenURL string, token string) (*tokenData, error) {
	// Avoid caching issues by adding a timestamp query parameter
	tokenURL += "?" + fmt.Sprint(time.Now().Unix())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create auth request: %w", err)
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := noRedirect(httpClient).Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auth request failed with status %d: %s", resp.StatusCode, hubMessage(resp))
	}

	var respData tokenResp
	err = json.NewDecoder(resp.Body).Decode(&respData)
	if err != nil {
		return nil, fmt.Errorf("decode auth response: %w", err)
	}

	return &tokenData{
		BaseURL: respData.CASURL,
		Token:   respData.Token,
		Exp:     time.Unix(respData.Exp, 0),
	}, nil
}
