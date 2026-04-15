package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Target struct {
	Endpoint string
	RepoType string
	RepoID   string
	Revision string
}

type Token struct {
	BaseURL string
	Token   string
	Exp     time.Time
}

func ResolveXETWriteToken(ctx context.Context, httpClient *http.Client, target Target, token string) (*Token, error) {
	return resolveRepoToken(ctx, httpClient, target, token, "write")
}

func ResolveXETReadToken(ctx context.Context, httpClient *http.Client, target Target, token string) (*Token, error) {
	return resolveRepoToken(ctx, httpClient, target, token, "read")
}

func resolveRepoToken(ctx context.Context, httpClient *http.Client, target Target, token string, mode string) (*Token, error) {
	target = normalizeTarget(target)

	if target.RepoID == "" {
		return nil, fmt.Errorf("missing Hugging Face repo")
	}

	tokenURL := fmt.Sprintf("%s/api/%ss/%s/xet-%s-token/%s",
		strings.TrimRight(target.Endpoint, "/"),
		target.RepoType,
		target.RepoID,
		mode,
		url.PathEscape(target.Revision),
	)

	return fetchXETAuthToken(ctx, httpClient, tokenURL, token)
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
