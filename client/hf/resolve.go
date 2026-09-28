package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/wzshiming/xet"
)

// ResolvedFile is a hub file resolved to its xet hash.
type ResolvedFile struct {
	Hash xet.FileHash
}

// Resolve HEADs path at the bound repository revision once, never following redirects, and returns the file behind its xet reconstruction link.
func (c *Client) Resolve(ctx context.Context, path string) (*ResolvedFile, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.repo.ResolveURL(path), nil)
	if err != nil {
		return nil, fmt.Errorf("create resolve request: %w", err)
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.hub.Do(req)
	if err != nil {
		return nil, fmt.Errorf("resolve request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	return resolveResponse(resp)
}

func resolveResponse(resp *http.Response) (*ResolvedFile, error) {
	if resp.StatusCode < 200 || resp.StatusCode >= 400 {
		return nil, fmt.Errorf("unexpected status from resolve: %d", resp.StatusCode)
	}

	reconURLStr := ParseLinkHeaders(resp.Header.Values("Link"))["xet-reconstruction-info"]
	if reconURLStr == "" {
		return nil, fmt.Errorf("missing xet-reconstruction-info link (status %d)", resp.StatusCode)
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

	return &ResolvedFile{Hash: fileHash}, nil
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
