package hf

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
)

// ResolveDownload resolves a download URL to its corresponding file hash and a
// CAS TokenProvider. The provider pre-populates the first token from the
// response headers so no extra round-trip is needed on first use.
func ResolveDownload(ctx context.Context, httpClient *http.Client, resolveURL string) (xet.Hash, client.AuthProvider, error) {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, resolveURL, nil)
	if err != nil {
		return xet.Hash{}, nil, fmt.Errorf("create resolve request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return xet.Hash{}, nil, fmt.Errorf("resolve request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return ResolveResponse(ctx, httpClient, resp)
}

// ResolveResponse extracts the file hash and a CAS TokenProvider from an
// already-executed HTTP response that contains XET link headers.
func ResolveResponse(ctx context.Context, httpClient *http.Client, resp *http.Response) (xet.Hash, client.AuthProvider, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return xet.Hash{}, nil, fmt.Errorf("unexpected status from resolve: %d", resp.StatusCode)
	}

	linkMap := parseLinkHeaders(resp.Header.Values("Link"))
	reconURLStr := linkMap["xet-reconstruction-info"]
	if reconURLStr == "" {
		return xet.Hash{}, nil, fmt.Errorf("missing xet-reconstruction-info link")
	}

	authURL := linkMap["xet-auth"]
	if authURL == "" {
		return xet.Hash{}, nil, fmt.Errorf("missing xet-auth link")
	}

	reconURL, err := url.Parse(reconURLStr)
	if err != nil {
		return xet.Hash{}, nil, fmt.Errorf("parse reconstruction link: %w", err)
	}
	if reconURL.Scheme == "" || reconURL.Host == "" {
		return xet.Hash{}, nil, fmt.Errorf("invalid reconstruction link: %s", reconURLStr)
	}

	hashStr := path.Base(reconURL.Path)
	if hashStr == "" {
		hashStr := resp.Header.Get("X-Xet-Hash")
		if hashStr == "" {
			return xet.Hash{}, nil, fmt.Errorf("missing X-Xet-Hash header in resolve response")
		}
	}

	fileHash, err := xet.ParseHash(hashStr)
	if err != nil {
		return xet.Hash{}, nil, fmt.Errorf("parse X-Xet-Hash: %w", err)
	}

	initial, err := fetchXETAuthToken(ctx, httpClient, authURL, "")
	if err != nil {
		return xet.Hash{}, nil, fmt.Errorf("fetch xet auth token: %w", err)
	}

	provider := newTokenProviderFromURL(httpClient, authURL, initial)
	return fileHash, provider, nil
}

func normalizeRepoType(repoType string) string {
	switch strings.ToLower(strings.TrimSpace(repoType)) {
	case "model", "models", "":
		if strings.TrimSpace(repoType) == "" {
			return ""
		}
		return "model"
	case "dataset", "datasets":
		return "dataset"
	case "space", "spaces":
		return "space"
	default:
		return ""
	}
}

func parseLinkHeaders(values []string) map[string]string {
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

type tokenResp struct {
	CASURL string `json:"casUrl"`
	Token  string `json:"accessToken"`
	Exp    int64  `json:"exp"`
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

	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("auth request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
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
