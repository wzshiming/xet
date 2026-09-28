package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
)

// NewTokenProvider returns a provider fetching repo's CAS read and write tokens from the hub with token.
func NewTokenProvider(httpClient *http.Client, repo Repo, token string) client.UpstreamProvider {
	repo = repo.normalized()
	base, rev := repo.apiBase(), url.PathEscape(repo.Revision)
	return newTokenProvider(httpClient, token, map[auth.Permission]string{
		auth.Read:  base + "/xet-read-token/" + rev,
		auth.Write: base + "/xet-write-token/" + rev,
	})
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

// newTokenProvider fetches each permission's CAS token from endpoints[perm] with hubToken, renewing it before its exp.
func newTokenProvider(httpClient *http.Client, hubToken string, endpoints map[auth.Permission]string) *tokenProvider {
	return &tokenProvider{
		httpClient: httpClient,
		tokenURLs:  endpoints,
		hubToken:   hubToken,
		tokens:     map[auth.Permission]*tokenData{},
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
