package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
)

// authRecorder keeps the Authorization header of every request in arrival order per path.
type authRecorder struct {
	mu   sync.Mutex
	seen map[string][]string
}

func (a *authRecorder) record(r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seen == nil {
		a.seen = map[string][]string{}
	}
	a.seen[r.URL.Path] = append(a.seen[r.URL.Path], r.Header.Get("Authorization"))
}

func (a *authRecorder) get(path string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.seen[path]...)
}

// recordingServer serves handler on a real listener, recording every request's Authorization.
func recordingServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *authRecorder) {
	t.Helper()
	rec := &authRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		handler(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

const readTokenPath, writeTokenPath = "/api/models/org/repo/xet-read-token/main", "/api/models/org/repo/xet-write-token/main"

// countingTokenServer answers each token path with a token named after its mode and fetch count.
func countingTokenServer(t *testing.T) (*httptest.Server, func(path string) int) {
	var mu sync.Mutex
	fetches := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fetches[r.URL.Path]++
		n := fetches[r.URL.Path]
		mu.Unlock()
		mode := "read"
		if strings.Contains(r.URL.Path, "xet-write-token") {
			mode = "write"
		}
		_, _ = fmt.Fprintf(w, `{"casUrl":"https://%s.cas.example","accessToken":"%s-%d","exp":%d}`, mode, mode, n, time.Now().Add(time.Hour).Unix())
	}))
	t.Cleanup(srv.Close)
	return srv, func(path string) int {
		mu.Lock()
		defer mu.Unlock()
		return fetches[path]
	}
}

// repoTokenProvider fetches read and write tokens from srv's counting endpoints.
func repoTokenProvider(srv *httptest.Server) *tokenProvider {
	return newTokenProvider(nil, "hf-token", map[auth.Permission]string{
		auth.Read:  srv.URL + readTokenPath,
		auth.Write: srv.URL + writeTokenPath,
	})
}

// resolve returns perm's base URL and token, failing the test on error.
func resolve(t *testing.T, provider client.UpstreamProvider, perm auth.Permission) (string, string) {
	t.Helper()
	baseURL, token, err := provider.Resolve(context.Background(), perm)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", perm, err)
	}
	return baseURL, token
}

func TestTokenProviderCachesPerPermission(t *testing.T) {
	srv, fetches := countingTokenServer(t)
	p := repoTokenProvider(srv)
	for range 2 {
		if baseURL, token := resolve(t, p, auth.Read); baseURL != "https://read.cas.example" || token != "read-1" {
			t.Fatalf("read = %s %s, want the cached read token", baseURL, token)
		}
		if baseURL, token := resolve(t, p, auth.Write); baseURL != "https://write.cas.example" || token != "write-1" {
			t.Fatalf("write = %s %s, want the cached write token", baseURL, token)
		}
	}
	if fetches(readTokenPath) != 1 || fetches(writeTokenPath) != 1 {
		t.Fatalf("fetches = %d read, %d write, want one each", fetches(readTokenPath), fetches(writeTokenPath))
	}
}

func TestTokenProviderRefreshesExpiringToken(t *testing.T) {
	srv, fetches := countingTokenServer(t)
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer down.Close()
	p := repoTokenProvider(srv)
	resolve(t, p, auth.Read)
	resolve(t, p, auth.Write)
	expiring := func() { p.tokens[auth.Write].Exp = time.Now().Add(tokenExpiryMargin / 2) }

	expiring()
	if _, token := resolve(t, p, auth.Write); token != "write-2" {
		t.Fatalf("expiring write token = %q, want write-2", token)
	}
	if _, token := resolve(t, p, auth.Read); token != "read-1" || fetches(readTokenPath) != 1 || fetches(writeTokenPath) != 2 {
		t.Fatalf("read token %q after %d read and %d write fetches; want read-1 after 1 and 2", token, fetches(readTokenPath), fetches(writeTokenPath))
	}

	// A failed refresh reports the error and keeps the expiring token for the next attempt.
	expiring()
	p.tokenURLs[auth.Write] = down.URL + writeTokenPath
	if _, _, err := p.Resolve(context.Background(), auth.Write); err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("Resolve with the token endpoint down: %v", err)
	}
	if tok := p.tokens[auth.Write]; tok == nil || tok.Token != "write-2" {
		t.Fatalf("slot after failed refresh = %+v, want write-2", tok)
	}
	p.tokenURLs[auth.Write] = srv.URL + writeTokenPath
	if _, token := resolve(t, p, auth.Write); token != "write-3" || fetches(writeTokenPath) != 3 {
		t.Fatalf("write token %q after %d fetches, want write-3 after 3", token, fetches(writeTokenPath))
	}
}

// originTransport answers every request in memory through handler, so URLs may name any scheme or host.
type originTransport struct{ handler http.Handler }

func (t originTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.handler.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

// A redirect off the token endpoint is reported, never followed with the hub token, on the first fetch and on renewal alike.
func TestAuthTokenRedirectNotFollowed(t *testing.T) {
	const hubOrigin = "https://hub.example"
	endpoints := map[auth.Permission]string{auth.Read: hubOrigin + readTokenPath}
	scenarios := map[string]struct {
		redirectAt int // the token fetch that answers with the redirect
		run        func(t *testing.T, httpClient *http.Client) error
	}{
		"provider": {1, func(_ *testing.T, httpClient *http.Client) error {
			_, _, err := newTokenProvider(httpClient, "hub-token", endpoints).Resolve(context.Background(), auth.Read)
			return err
		}},
		"provider renewal": {2, func(t *testing.T, httpClient *http.Client) error {
			p := newTokenProvider(httpClient, "hub-token", endpoints)
			if _, _, err := p.Resolve(context.Background(), auth.Read); err != nil {
				t.Fatalf("initial token fetch: %v", err)
			}
			_, _, err := p.Resolve(context.Background(), auth.Read)
			return err
		}},
	}
	for _, target := range []string{"http://hub.example:8080", "https://evil.hub.example", "https://other.example"} {
		for name, sc := range scenarios {
			t.Run(target+" "+name, func(t *testing.T) {
				var fetches atomic.Int32
				leaked := &authRecorder{} // requests the redirect target received
				httpClient := &http.Client{Transport: originTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.URL.Scheme+"://"+r.URL.Host == target:
						leaked.record(r)
						_, _ = fmt.Fprintf(w, `{"casUrl":"https://cas.example","accessToken":"leaked","exp":%d}`, time.Now().Add(time.Hour).Unix())
					case r.URL.Path == readTokenPath:
						if int(fetches.Add(1)) < sc.redirectAt {
							_, _ = fmt.Fprintf(w, `{"casUrl":"https://cas.example","accessToken":"A","exp":%d}`, time.Now().Add(tokenExpiryMargin/2).Unix())
							return
						}
						http.Redirect(w, r, target+readTokenPath, http.StatusFound)
					default:
						http.NotFound(w, r)
					}
				})}}

				err := sc.run(t, httpClient)
				if got := leaked.get(readTokenPath); len(got) != 0 {
					t.Fatalf("redirect target Authorization = %q, want no request", got)
				}
				if err == nil || !strings.Contains(err.Error(), "302") {
					t.Fatalf("err = %v, want the 302 reported", err)
				}
			})
		}
	}
}
