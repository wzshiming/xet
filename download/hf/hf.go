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
)

type Resolved struct {
	Hash    xet.Hash
	BaseURL string
	Token   string
}

func Resolve(ctx context.Context, resolveURL string) (Resolved, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, resolveURL, nil)
	if err != nil {
		return Resolved{}, fmt.Errorf("create resolve request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return Resolved{}, fmt.Errorf("resolve request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return Resolved{}, fmt.Errorf("unexpected status from resolve: %d", resp.StatusCode)
	}

	linkMap := parseLinkHeaders(resp.Header.Values("Link"))
	reconURLStr := linkMap["xet-reconstruction-info"]
	if reconURLStr == "" {
		return Resolved{}, fmt.Errorf("missing xet-reconstruction-info link")
	}

	reconURL, err := url.Parse(reconURLStr)
	if err != nil {
		return Resolved{}, fmt.Errorf("parse reconstruction link: %w", err)
	}
	if reconURL.Scheme == "" || reconURL.Host == "" {
		return Resolved{}, fmt.Errorf("invalid reconstruction link: %s", reconURLStr)
	}

	hashStr := path.Base(reconURL.Path)
	if hashStr == "" {
		hashStr := resp.Header.Get("X-Xet-Hash")
		if hashStr == "" {
			return Resolved{}, fmt.Errorf("missing X-Xet-Hash header in resolve response")
		}
	}

	fileHash, err := xet.ParseHash(hashStr)
	if err != nil {
		return Resolved{}, fmt.Errorf("parse X-Xet-Hash: %w", err)
	}

	result := Resolved{
		Hash:    fileHash,
		BaseURL: fmt.Sprintf("%s://%s", reconURL.Scheme, reconURL.Host),
	}

	if authURL := linkMap["xet-auth"]; authURL != "" {
		tokenResp, err := fetchXETAuthToken(ctx, httpClient, authURL)
		if err != nil {
			return Resolved{}, fmt.Errorf("fetch xet auth token: %w", err)
		}
		result.Token = tokenResp.Token
		result.BaseURL = tokenResp.CASURL
	}

	return result, nil
}

func parseLinkHeaders(values []string) map[string]string {
	result := make(map[string]string)
	for _, value := range values {
		parts := strings.Split(value, ",")
		for _, part := range parts {
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

func fetchXETAuthToken(ctx context.Context, httpClient *http.Client, tokenURL string) (*tokenResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create auth request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("auth request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("auth request failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var respData tokenResp
	err = json.NewDecoder(resp.Body).Decode(&respData)
	if err != nil {
		return nil, fmt.Errorf("decode auth response: %w", err)
	}

	return &respData, nil
}
