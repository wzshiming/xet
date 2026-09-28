package client

import (
	"context"

	"github.com/wzshiming/xet/auth"
)

type staticUpstreamProvider struct {
	baseURL string
	token   string
}

// StaticUpstreamProvider returns an UpstreamProvider serving the same base URL and token for every permission.
func StaticUpstreamProvider(baseURL, token string) UpstreamProvider {
	return staticUpstreamProvider{baseURL: baseURL, token: token}
}

func (p staticUpstreamProvider) Resolve(context.Context, auth.Permission) (string, string, error) {
	return p.baseURL, p.token, nil
}
