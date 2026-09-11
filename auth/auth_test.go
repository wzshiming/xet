package auth

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

var _ Authorizer = AuthorizerFunc(nil)

func TestBearerToken(t *testing.T) {
	for _, test := range []struct {
		name   string
		header string
		token  string
		ok     bool
	}{
		{name: "missing"},
		{name: "basic", header: "Basic abc"},
		{name: "no token", header: "Bearer"},
		{name: "empty token", header: "Bearer "},
		{name: "bearer", header: "Bearer abc", token: "abc", ok: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			if test.header != "" {
				request.Header.Set("Authorization", test.header)
			}
			token, ok := BearerToken(request)
			if token != test.token || ok != test.ok {
				t.Fatalf("BearerToken() = (%q, %v), want (%q, %v)", token, ok, test.token, test.ok)
			}
		})
	}
}

func TestDeny(t *testing.T) {
	for _, test := range []struct {
		name   string
		err    error
		status int
		header string
	}{
		{name: "unauthenticated", err: ErrUnauthenticated, status: http.StatusUnauthorized, header: "Bearer"},
		{name: "wrapped unauthenticated", err: fmt.Errorf("x: %w", ErrUnauthenticated), status: http.StatusUnauthorized, header: "Bearer"},
		{name: "forbidden", err: ErrForbidden, status: http.StatusForbidden},
		{name: "custom", err: errors.New("custom"), status: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			Deny(response, test.err)
			if response.Code != test.status {
				t.Errorf("status = %d, want %d", response.Code, test.status)
			}
			if header := response.Header().Get("WWW-Authenticate"); header != test.header {
				t.Errorf("WWW-Authenticate = %q, want %q", header, test.header)
			}
			if body := strings.TrimSpace(response.Body.String()); body != test.err.Error() {
				t.Errorf("body = %q, want %q", body, test.err.Error())
			}
			if got := response.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
			}
		})
	}
}

func TestAuthorizerFunc(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	grant := Grant{Permission: Read, File: xet.FileHash{1}, SHA256: [32]byte{2}}
	for _, wantErr := range []error{nil, ErrForbidden} {
		called := false
		authorizer := AuthorizerFunc(func(gotRequest *http.Request, gotGrant Grant) error {
			called = true
			if gotRequest != request || gotGrant != grant {
				t.Error("Authorize did not forward its arguments")
			}
			return wantErr
		})
		if err := authorizer.Authorize(request, grant); err != wantErr {
			t.Errorf("Authorize() = %v, want %v", err, wantErr)
		}
		if !called {
			t.Error("Authorize did not call the function")
		}
	}
}
