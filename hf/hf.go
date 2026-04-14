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

type DownloadResolved struct {
	Hash    xet.Hash
	BaseURL string
	Token   string
}

type UploadOptions struct {
	Endpoint string
	RepoType string
	Revision string
}

type XETToken struct {
	BaseURL  string
	Token    string
	Exp      int64
	HubURL   string
	RepoType string
	RepoID   string
	Revision string
}

// ResolveDownload resolves a download URL to its corresponding file hash and CAS endpoint
func ResolveDownload(ctx context.Context, httpClient *http.Client, resolveURL string) (*DownloadResolved, error) {
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
		return nil, fmt.Errorf("create resolve request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("resolve request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return ResolveResponse(ctx, httpClient, resp)
}

// ResolveXETReadToken resolves a Hugging Face repo reference to an XET read token for downloading
func ResolveResponse(ctx context.Context, httpClient *http.Client, resp *http.Response) (*DownloadResolved, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return nil, fmt.Errorf("unexpected status from resolve: %d", resp.StatusCode)
	}

	linkMap := parseLinkHeaders(resp.Header.Values("Link"))
	reconURLStr := linkMap["xet-reconstruction-info"]
	if reconURLStr == "" {
		return nil, fmt.Errorf("missing xet-reconstruction-info link")
	}

	reconURL, err := url.Parse(reconURLStr)
	if err != nil {
		return nil, fmt.Errorf("parse reconstruction link: %w", err)
	}
	if reconURL.Scheme == "" || reconURL.Host == "" {
		return nil, fmt.Errorf("invalid reconstruction link: %s", reconURLStr)
	}

	hashStr := path.Base(reconURL.Path)
	if hashStr == "" {
		hashStr := resp.Header.Get("X-Xet-Hash")
		if hashStr == "" {
			return nil, fmt.Errorf("missing X-Xet-Hash header in resolve response")
		}
	}

	fileHash, err := xet.ParseHash(hashStr)
	if err != nil {
		return nil, fmt.Errorf("parse X-Xet-Hash: %w", err)
	}

	result := DownloadResolved{
		Hash:    fileHash,
		BaseURL: fmt.Sprintf("%s://%s", reconURL.Scheme, reconURL.Host),
	}

	if authURL := linkMap["xet-auth"]; authURL != "" {
		if httpClient == nil {
			httpClient = &http.Client{
				Timeout: 30 * time.Second,
				CheckRedirect: func(req *http.Request, via []*http.Request) error {
					return http.ErrUseLastResponse
				},
			}
		}
		tokenResp, err := fetchXETAuthToken(ctx, httpClient, authURL)
		if err != nil {
			return nil, fmt.Errorf("fetch xet auth token: %w", err)
		}
		result.Token = tokenResp.Token
		result.BaseURL = tokenResp.CASURL
	}

	return &result, nil
}

func ResolveXETWriteToken(ctx context.Context, httpClient *http.Client, repoOrURL, hubToken string, opts UploadOptions) (XETToken, error) {
	return resolveRepoToken(ctx, httpClient, repoOrURL, hubToken, opts, "write")
}

func ResolveXETReadToken(ctx context.Context, httpClient *http.Client, repoOrURL, hubToken string, opts UploadOptions) (XETToken, error) {
	return resolveRepoToken(ctx, httpClient, repoOrURL, hubToken, opts, "read")
}

func resolveRepoToken(ctx context.Context, httpClient *http.Client, repoOrURL, hubToken string, opts UploadOptions, mode string) (XETToken, error) {
	if hubToken == "" {
		return XETToken{}, fmt.Errorf("missing Hugging Face token")
	}

	target, err := parseTarget(repoOrURL, opts)
	if err != nil {
		return XETToken{}, err
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	tokenURL := fmt.Sprintf("%s/api/%ss/%s/xet-%s-token/%s",
		strings.TrimRight(target.Endpoint, "/"),
		target.RepoType,
		target.RepoID,
		mode,
		url.PathEscape(target.Revision),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return XETToken{}, fmt.Errorf("create %s auth request: %w", mode, err)
	}
	req.Header.Set("Authorization", "Bearer "+hubToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return XETToken{}, fmt.Errorf("%s auth request: %w", mode, err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return XETToken{}, fmt.Errorf("%s auth request failed with status %d: %s", mode, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var tokenInfo tokenResp
	if err := json.NewDecoder(resp.Body).Decode(&tokenInfo); err != nil {
		return XETToken{}, fmt.Errorf("decode %s auth response: %w", mode, err)
	}

	return XETToken{
		BaseURL:  tokenInfo.CASURL,
		Token:    tokenInfo.Token,
		Exp:      tokenInfo.Exp,
		HubURL:   target.Endpoint,
		RepoType: target.RepoType,
		RepoID:   target.RepoID,
		Revision: target.Revision,
	}, nil
}

type repoInfo struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

func parseTarget(repoOrURL string, opts UploadOptions) (repoInfo, error) {
	if repoOrURL == "" {
		return repoInfo{}, fmt.Errorf("missing Hugging Face repo")
	}

	if parsed, err := url.Parse(repoOrURL); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		return parseURLTarget(parsed, opts)
	}

	return parseRepoTarget(strings.Trim(repoOrURL, "/"), opts)
}

func parseURLTarget(parsed *url.URL, opts UploadOptions) (repoInfo, error) {
	parts, err := splitPathParts(parsed.EscapedPath())
	if err != nil {
		return repoInfo{}, fmt.Errorf("parse Hugging Face repo URL: %w", err)
	}

	endpoint := strings.TrimRight(opts.Endpoint, "/")
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s://%s", parsed.Scheme, parsed.Host)
	}

	return buildUploadTarget(parts, endpoint, opts)
}

func parseRepoTarget(repo string, opts UploadOptions) (repoInfo, error) {
	parts := strings.Split(strings.Trim(repo, "/"), "/")
	endpoint := strings.TrimRight(opts.Endpoint, "/")
	if endpoint == "" {
		endpoint = "https://huggingface.co"
	}

	return buildUploadTarget(parts, endpoint, opts)
}

func buildUploadTarget(parts []string, endpoint string, opts UploadOptions) (repoInfo, error) {
	parts = compactParts(parts)

	repoType := normalizeRepoType(opts.RepoType)
	revision := opts.Revision
	start := 0

	if len(parts) > 0 {
		if normalized := normalizeRepoType(parts[0]); normalized != "" {
			if repoType == "" {
				repoType = normalized
			}
			start = 1
		}
	}

	if repoType == "" {
		repoType = "model"
	}

	if len(parts) < start+2 {
		return repoInfo{}, fmt.Errorf("invalid Hugging Face repo %q", strings.Join(parts, "/"))
	}

	repoID := parts[start] + "/" + parts[start+1]
	rest := parts[start+2:]
	if revision == "" && len(rest) >= 2 && isRepoActionSegment(rest[0]) {
		revision = rest[1]
	}
	if revision == "" {
		revision = "main"
	}

	return repoInfo{
		Endpoint: endpoint,
		RepoType: repoType,
		RepoID:   repoID,
		Revision: revision,
	}, nil
}

func splitPathParts(escapedPath string) ([]string, error) {
	raw := strings.Split(strings.Trim(escapedPath, "/"), "/")
	parts := make([]string, 0, len(raw))
	for _, part := range raw {
		if part == "" {
			continue
		}
		decoded, err := url.PathUnescape(part)
		if err != nil {
			return nil, err
		}
		parts = append(parts, decoded)
	}
	return parts, nil
}

func compactParts(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	return out
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

func isRepoActionSegment(part string) bool {
	switch part {
	case "blob", "resolve", "tree":
		return true
	default:
		return false
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

func fetchXETAuthToken(ctx context.Context, httpClient *http.Client, tokenURL string) (*tokenResp, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create auth request: %w", err)
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

	return &respData, nil
}
