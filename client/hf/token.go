package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
)

// NewTokenProvider returns a provider fetching repo's CAS read and write tokens from the hub with token.
func NewTokenProvider(httpClient *http.Client, repo Repo, token string) client.UpstreamProvider {
	return newTokenProvider(hubClient(httpClient), repo.normalized(), token)
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
	hub       *http.Client
	tokenURLs map[auth.Permission]string // token endpoint per permission
	hubToken  string                     // hub access token used in the Authorization header

	mu     sync.Mutex
	tokens map[auth.Permission]*tokenData
}

// newTokenProvider fetches the normalized repo's CAS tokens through the hub client with hubToken, renewing each before its exp.
func newTokenProvider(hub *http.Client, repo Repo, hubToken string) *tokenProvider {
	return &tokenProvider{
		hub:       hub,
		tokenURLs: map[auth.Permission]string{auth.Read: repo.apiURL("xet-read-token"), auth.Write: repo.apiURL("xet-write-token")},
		hubToken:  hubToken,
		tokens:    map[auth.Permission]*tokenData{},
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
	// The timestamp query defeats caches between the client and the hub.
	req, err := newRequest(ctx, http.MethodGet, tokenURL+"?"+fmt.Sprint(time.Now().Unix()), p.hubToken, nil)
	if err != nil {
		return nil, fmt.Errorf("create auth request: %w", err)
	}
	resp, err := p.hub.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, hubError(req, resp)
	}
	var data tokenResp
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, fmt.Errorf("decode auth response: %w", err)
	}
	tok := &tokenData{BaseURL: data.CASURL, Token: data.Token, Exp: time.Unix(data.Exp, 0)}
	p.tokens[perm] = tok
	return tok, nil
}

type tokenResp struct {
	CASURL string `json:"casUrl"`
	Token  string `json:"accessToken"`
	Exp    int64  `json:"exp"`
}
