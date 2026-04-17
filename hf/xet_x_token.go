package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet/client"
)

// Target identifies a Hugging Face repository.
type Target struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

const tokenExpiryMargin = 1 * time.Minute

// tokenData holds a fetched CAS access token and its metadata.
type tokenData struct {
	BaseURL string
	Token   string
	Exp     time.Time
}

// tokenProvider fetches and caches a CAS access token for a Hugging Face
// repository, automatically refreshing it before it expires. Separate
// providers exist for read (download) and write (upload) operations because
// HF issues different tokens for each mode.
type tokenProvider struct {
	httpClient *http.Client
	tokenURL   string // endpoint to call for a new token
	hfToken    string // HF access token used in the Authorization header

	mu      sync.Mutex
	current *tokenData
}

// NewReadTokenProvider returns a TokenProvider that obtains CAS read tokens
// for the given Hugging Face repository.
func NewReadTokenProvider(httpClient *http.Client, target Target, hfToken string) client.AuthProvider {
	return newTokenProvider(httpClient, target, hfToken, "read")
}

// NewWriteTokenProvider returns a TokenProvider that obtains CAS write tokens
// for the given Hugging Face repository.
func NewWriteTokenProvider(httpClient *http.Client, target Target, hfToken string) client.AuthProvider {
	return newTokenProvider(httpClient, target, hfToken, "write")
}

func newTokenProvider(httpClient *http.Client, target Target, hfToken, mode string) client.AuthProvider {
	target = normalizeTarget(target)
	tokenURL := fmt.Sprintf("%s/api/%ss/%s/xet-%s-token/%s",
		strings.TrimRight(target.Endpoint, "/"),
		target.RepoType,
		target.RepoID,
		mode,
		url.PathEscape(target.Revision),
	)
	return &tokenProvider{
		httpClient: httpClient,
		tokenURL:   tokenURL,
		hfToken:    hfToken,
	}
}

// newTokenProviderFromURL creates a provider that refreshes by calling authURL
// directly. An optional pre-fetched token can be provided to avoid an
// immediate network round-trip.
func newTokenProviderFromURL(httpClient *http.Client, authURL string, initial *tokenData) client.AuthProvider {
	return &tokenProvider{
		httpClient: httpClient,
		tokenURL:   authURL,
		current:    initial,
	}
}

// BaseURL returns the CAS base URL associated with the current token,
// fetching a new token if necessary.
func (p *tokenProvider) BaseURL(ctx context.Context) (string, error) {
	tok, err := p.get(ctx)
	if err != nil {
		return "", err
	}
	return tok.BaseURL, nil
}

// Token returns the CAS access token, fetching or refreshing it as needed.
func (p *tokenProvider) Token(ctx context.Context) (string, error) {
	tok, err := p.get(ctx)
	if err != nil {
		return "", err
	}
	return tok.Token, nil
}

func (p *tokenProvider) get(ctx context.Context) (*tokenData, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.current != nil && time.Now().Add(tokenExpiryMargin).Before(p.current.Exp) {
		return p.current, nil
	}

	tok, err := fetchXETAuthToken(ctx, p.httpClient, p.tokenURL, p.hfToken)
	if err != nil {
		return nil, err
	}
	p.current = tok
	return tok, nil
}

func normalizeTarget(target Target) Target {
	target.Endpoint = strings.TrimRight(target.Endpoint, "/")
	if target.Endpoint == "" {
		target.Endpoint = "https://huggingface.co"
	}

	target.RepoType = normalizeRepoType(target.RepoType)
	if target.RepoType == "" {
		target.RepoType = "model"
	}

	target.RepoID = strings.Trim(target.RepoID, "/")
	if target.Revision == "" {
		target.Revision = "main"
	}

	return target
}
