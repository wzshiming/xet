package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/wzshiming/xet/auth"
)

var errNoUpstreamProvider = errors.New("no upstream provider bound")

// casAuth is the per-operation snapshot authTransport authenticates CAS-origin requests with.
type casAuth struct {
	origin string
	token  string
}

type casAuthKey struct{}

// casContext resolves the CAS endpoint for perm through upstream and marks ctx so authTransport authenticates requests to that origin.
func (c *Client) casContext(ctx context.Context, upstream UpstreamProvider, perm auth.Permission) (context.Context, string, error) {
	if upstream == nil {
		return nil, "", errNoUpstreamProvider
	}
	// A provider may fetch its token through this client's transport; the mark of an enclosing operation must not authenticate that.
	baseURL, token, err := upstream.Resolve(context.WithValue(ctx, casAuthKey{}, nil), perm)
	if err != nil {
		return nil, "", &authError{err}
	}
	baseURL, origin, err := casOrigin(baseURL)
	if err != nil {
		return nil, "", err
	}
	mark := &casAuth{origin: origin, token: token}
	return context.WithValue(ctx, casAuthKey{}, mark), baseURL, nil
}

// casOrigin returns baseURL without its trailing slash and its scheme://host origin.
func casOrigin(baseURL string) (string, string, error) {
	trimmed := strings.TrimRight(baseURL, "/")
	u, err := url.Parse(trimmed)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", "", fmt.Errorf("invalid base URL %q", baseURL)
	}
	return trimmed, u.Scheme + "://" + u.Host, nil
}

// authError marks provider failures so retry loops never treat them as network errors.
type authError struct{ err error }

func (e *authError) Error() string { return "auth: " + e.err.Error() }

func (e *authError) Unwrap() error { return e.err }

func isAuthError(err error) bool {
	var authErr *authError
	return errors.As(err, &authErr)
}

// authTransport adds the snapshot token to CAS-origin requests; the provider renews tokens before they expire, so a 401 is final.
type authTransport struct {
	base http.RoundTripper
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	mark, _ := req.Context().Value(casAuthKey{}).(*casAuth)
	if mark == nil || mark.origin != req.URL.Scheme+"://"+req.URL.Host || req.Header.Get("Authorization") != "" {
		return t.base.RoundTrip(req)
	}
	return t.base.RoundTrip(withBearer(req, mark.token))
}

// withBearer clones req with the bearer token set; an empty token stays anonymous.
func withBearer(req *http.Request, token string) *http.Request {
	clone := req.Clone(req.Context())
	if token != "" {
		clone.Header.Set("Authorization", "Bearer "+token)
	}
	return clone
}
