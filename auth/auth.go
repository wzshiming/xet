// Package auth defines the server-side authorization hook and a reference HMAC token issuer.
package auth

import (
	"errors"
	"net/http"
	"strings"

	"github.com/wzshiming/xet"
)

// Permission is what a route requires; Read and Write are independent.
type Permission string

const (
	Read  Permission = "read"
	Write Permission = "write"
)

// Authorizer allows with nil, denies with ErrUnauthenticated (401) or another error (403).
type Authorizer interface {
	Authorize(r *http.Request, g Grant) error
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(r *http.Request, g Grant) error

func (f AuthorizerFunc) Authorize(r *http.Request, g Grant) error {
	return f(r, g)
}

// Grant is a permission optionally bound to one file (zero File/SHA256 = unbound); routes pass the grant they require, tokens carry the grant they convey.
type Grant struct {
	Permission Permission
	File       xet.FileHash
	SHA256     [32]byte
	Targeted   bool // Routes set this to distinguish an all-zero target from a permission-only check.
}

var (
	// ErrUnauthenticated indicates a missing or invalid credential.
	ErrUnauthenticated = errors.New("unauthenticated")
	// ErrForbidden indicates a valid credential with access denied.
	ErrForbidden = errors.New("forbidden")
)

// BearerToken returns the nonempty token of an Authorization: Bearer <token> header.
func BearerToken(r *http.Request) (string, bool) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" {
		return "", false
	}
	return token, true
}

// Deny writes err.Error() with 401 and a Bearer challenge for wrapped ErrUnauthenticated, or 403 otherwise.
func Deny(w http.ResponseWriter, err error) {
	status := http.StatusForbidden
	if errors.Is(err, ErrUnauthenticated) {
		status = http.StatusUnauthorized
		w.Header().Set("WWW-Authenticate", "Bearer")
	}
	http.Error(w, err.Error(), status)
}
