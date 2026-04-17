package client

import (
	"context"
)

type staticAuthProvider struct {
	baseURL string
	token   string
}

// StaticAuthProvider returns an AuthProvider backed by fixed base URL and token values.
func StaticAuthProvider(baseURL, token string) AuthProvider {
	return staticAuthProvider{baseURL: baseURL, token: token}
}

func (p staticAuthProvider) BaseURL(context.Context) (string, error) {
	return p.baseURL, nil
}

func (p staticAuthProvider) Token(context.Context) (string, error) {
	return p.token, nil
}
