package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
)

const resolvePath, authPath = "/org/repo/resolve/main/f.bin", "/xet-auth"

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

// hubHandler serves resolvePath with xet links (xet-auth at authURL) to casURL and hands out the tokens in turn from authPath, every one but the last inside the expiry margin, recording every request in rec.
func hubHandler(t *testing.T, rec *authRecorder, authURL, casURL, fileHash string, tokens ...string) http.Handler {
	var fetches atomic.Int32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		switch r.URL.Path {
		case authPath:
			n := min(int(fetches.Add(1)), len(tokens))
			ttl := time.Hour
			if n < len(tokens) {
				ttl = tokenExpiryMargin / 2
			}
			_, _ = fmt.Fprintf(w, `{"casUrl":%q,"accessToken":%q,"exp":%d}`, casURL, tokens[n-1], time.Now().Add(ttl).Unix())
		case resolvePath:
			if r.Method != http.MethodHead {
				t.Errorf("resolve method = %s, want HEAD", r.Method)
			}
			w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="xet-auth", <%s/v1/reconstructions/%s>; rel="xet-reconstruction-info"`, authURL, casURL, fileHash))
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	})
}

// newHub serves hubHandler on a real listener with xet-auth on the hub itself.
func newHub(t *testing.T, casURL, fileHash string, tokens ...string) (*httptest.Server, *authRecorder) {
	t.Helper()
	rec := &authRecorder{}
	hub := httptest.NewUnstartedServer(nil)
	hub.Config.Handler = hubHandler(t, rec, "http://"+hub.Listener.Addr().String()+authPath, casURL, fileHash, tokens...)
	hub.Start()
	t.Cleanup(hub.Close)
	return hub, rec
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

// DownloadResolved downloads through the upstream the hub named, falling back to V1 like DownloadFile, while the bound provider stays untouched for the client's own downloads.
func TestDownloadResolvedUsesResolvedUpstream(t *testing.T) {
	fx := newDownloadFixture(t, 1)
	fileHash := fx.hashes[0]
	casX, recX := recordingServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v2/") {
			http.NotFound(w, r)
			return
		}
		fx.serveHTTP(w, r)
	})
	casP, recP := recordingServer(t, fx.serveHTTP)
	hub, hubRec := newHub(t, casX.URL, fileHash.String(), "X")
	p := newFakeProvider(casP.URL, "P")
	c, err := NewClient(WithCacheDir(t.TempDir()), WithUpstreamProvider(p))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()

	f, err := c.Resolve(ctx, hub.URL+resolvePath, "hub-token")
	if err != nil {
		t.Fatal(err)
	}
	if f.Hash != fileHash {
		t.Fatalf("hash = %s, want %s", f.Hash, fileHash)
	}
	got, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadResolved(ctx, f, w) })
	if err != nil || !bytes.Equal(got, fx.data[0]) {
		t.Fatalf("DownloadResolved: %d bytes, %v; want %d", len(got), err, len(fx.data[0]))
	}
	v1, v2 := "/v1/reconstructions/"+fileHash.String(), "/v2/reconstructions/"+fileHash.String()
	if !slices.Equal(recX.get(v2), []string{"Bearer X"}) || !slices.Equal(recX.get(v1), []string{"Bearer X"}) {
		t.Fatalf("resolved CAS saw v2 %q, v1 %q; want the resolved token on the V2 attempt and the V1 fallback", recX.get(v2), recX.get(v1))
	}
	if n := len(p.perms()); n != 0 || len(recP.get(v2)) != 0 {
		t.Fatalf("bound provider resolved %d times and its CAS saw %q; want neither", n, recP.get(v2))
	}
	if got := hubRec.get(resolvePath); !slices.Equal(got, []string{"Bearer hub-token"}) {
		t.Fatalf("resolve HEAD Authorization = %q, want the hub token", got)
	}

	got, err = downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, fileHash, w) })
	if err != nil || !bytes.Equal(got, fx.data[0]) {
		t.Fatalf("DownloadFile: %d bytes, %v; want %d", len(got), err, len(fx.data[0]))
	}
	if !slices.Equal(recP.get(v2), []string{"Bearer P"}) || !slices.Equal(p.perms(), []auth.Permission{auth.Read}) {
		t.Fatalf("bound CAS saw %q after resolves %v; want one V2 query with the bound token after one Read resolve", recP.get(v2), p.perms())
	}
	if len(recX.get(v1))+len(recX.get(v2)) != 2 {
		t.Fatal("the bound download reached the resolved CAS")
	}
}

// An unbound client downloads a hub file through its resolution while its own CAS operations still need a provider.
func TestDownloadResolved(t *testing.T) {
	fx := newDownloadFixture(t, 2)
	hub, rec := newHub(t, fx.srv.URL, fx.hashes[0].String(), "cas-token")
	c, err := NewClient(WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	f, err := c.Resolve(ctx, hub.URL+resolvePath, "hub-token")
	if err != nil {
		t.Fatal(err)
	}
	if f.Hash != fx.hashes[0] {
		t.Fatalf("hash = %s, want %s", f.Hash, fx.hashes[0])
	}
	got, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadResolved(ctx, f, w) })
	if err != nil || !bytes.Equal(got, fx.data[0]) {
		t.Fatalf("downloaded %d bytes, %v; want %d", len(got), err, len(fx.data[0]))
	}
	if got := rec.get(resolvePath); !slices.Equal(got, []string{"Bearer hub-token"}) {
		t.Fatalf("resolve HEAD Authorization = %q, want the hub token", got)
	}
	if _, err := downloadInto(t, nil, func(w io.WriteSeeker) error { return c.DownloadFile(ctx, f.Hash, w) }); !errors.Is(err, errNoUpstreamProvider) {
		t.Fatalf("DownloadFile on the unbound client: %v, want errNoUpstreamProvider", err)
	}
}

// resolveThrough resolves resolveURL with token on a client built from opts, failing the test on error.
func resolveThrough(t *testing.T, resolveURL, token string, opts ...Options) *ResolvedFile {
	t.Helper()
	c, err := NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.Resolve(context.Background(), resolveURL, token)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// resolveErr resolves resolveURL with token on a client built from opts and returns the error.
func resolveErr(t *testing.T, resolveURL, token string, opts ...Options) error {
	t.Helper()
	c, err := NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Resolve(context.Background(), resolveURL, token)
	return err
}

func TestResolveHuggingFace(t *testing.T) {
	const sampleHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"

	var tokenFetches atomic.Int32
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokenFetches.Add(1)
		_, _ = fmt.Fprintf(w, `{"casUrl":"https://override.cas","accessToken":"token-123","exp":%d}`, time.Now().Add(time.Hour).Unix())
	}))
	defer tokenSrv.Close()

	reconURL := "https://cas-server.example.com/v1/reconstructions/" + sampleHash

	resolveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Fatalf("expected HEAD request, got %s", r.Method)
		}
		linkHeader := fmt.Sprintf("<%s>; rel=\"xet-auth\", <%s>; rel=\"xet-reconstruction-info\"", tokenSrv.URL, reconURL)
		w.Header().Set("X-Xet-Hash", sampleHash)
		w.Header().Set("Link", linkHeader)
		w.WriteHeader(http.StatusFound)
	}))
	defer resolveSrv.Close()

	f := resolveThrough(t, resolveSrv.URL, "")
	if got := f.Hash.String(); got != sampleHash {
		t.Fatalf("unexpected hash: %s", got)
	}
	baseURL, token, err := f.upstream.Resolve(context.Background(), auth.Read)
	if err != nil {
		t.Fatal(err)
	}
	if baseURL != "https://override.cas" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if token != "token-123" {
		t.Fatalf("unexpected token: %s", token)
	}
	// The link-based provider serves reads from the pre-fetched token and has no write endpoint.
	if _, _, err := f.upstream.Resolve(context.Background(), auth.Write); err == nil || !strings.Contains(err.Error(), "no write token endpoint") {
		t.Fatalf("Resolve(write) = %v, want no write token endpoint", err)
	}
	if n := tokenFetches.Load(); n != 1 {
		t.Fatalf("token fetches = %d, want only the one during resolve", n)
	}
}

func TestResolveHuggingFaceMissingHeaders(t *testing.T) {
	resolveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusFound)
	}))
	defer resolveSrv.Close()

	if err := resolveErr(t, resolveSrv.URL, ""); err == nil {
		t.Fatalf("expected error due to missing headers")
	}
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
	return NewTokenProvider(nil, "hf-token", map[auth.Permission]string{
		auth.Read:  srv.URL + readTokenPath,
		auth.Write: srv.URL + writeTokenPath,
	}).(*tokenProvider)
}

// resolve returns perm's base URL and token, failing the test on error.
func resolve(t *testing.T, provider UpstreamProvider, perm auth.Permission) (string, string) {
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

// newCAS serves a CAS accepting only token; the returned check reconstructs f through its resolved upstream and asserts the single CAS request carried it.
func newCAS(t *testing.T, token string) (*httptest.Server, func(f *ResolvedFile)) {
	t.Helper()
	var casCalls atomic.Int32
	casSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		casCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "expired", http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(casSrv.Close)
	return casSrv, func(f *ResolvedFile) {
		t.Helper()
		c, err := NewClient()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.getReconstructionV1(context.Background(), f.upstream, f.Hash, nil); err != nil {
			t.Fatalf("reconstruction: %v", err)
		}
		if casCalls.Load() != 1 {
			t.Fatalf("CAS requests = %d, want one carrying the current token", casCalls.Load())
		}
	}
}

// The hub token authenticates the resolve HEAD and every xet-auth fetch; a token issued inside the expiry margin is renewed before the CAS sees it.
func TestResolveHubToken(t *testing.T) {
	for _, tc := range []struct {
		name, token, want string
		tokens            []string
	}{
		{"hub token", "hub-token", "Bearer hub-token", []string{"A", "B"}},
		{"anonymous", "", "", []string{"A", "B"}},
		{"long-lived", "hub-token", "Bearer hub-token", []string{"A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			casSrv, check := newCAS(t, tc.tokens[len(tc.tokens)-1])
			hub, rec := newHub(t, casSrv.URL, xet.FileHash{}.String(), tc.tokens...)

			f := resolveThrough(t, hub.URL+resolvePath, tc.token)
			if f.Hash != (xet.FileHash{}) {
				t.Fatalf("hash = %s, want the link's", f.Hash)
			}
			check(f)
			if got := rec.get(resolvePath); len(got) != 1 || got[0] != tc.want {
				t.Fatalf("resolve HEAD Authorization = %q, want [%q]", got, tc.want)
			}
			if got, want := rec.get(authPath), slices.Repeat([]string{tc.want}, len(tc.tokens)); !slices.Equal(got, want) {
				t.Fatalf("xet-auth Authorization = %q, want %q", got, want)
			}
		})
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

// The hub token reaches the xet-auth endpoint only on the resolve URL's origin: another host and an http downgrade of the same host stay anonymous.
func TestResolveTokenOrigin(t *testing.T) {
	const hubOrigin = "https://hub.example"
	for _, tc := range []struct{ name, authURL, want string }{
		{"same origin", hubOrigin + authPath, "Bearer hub-token"},
		{"other host", "https://other.example" + authPath, ""},
		{"http downgrade", "http://hub.example" + authPath, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			casSrv, check := newCAS(t, "B")
			rec := &authRecorder{}
			httpClient := &http.Client{Transport: originTransport{hubHandler(t, rec, tc.authURL, casSrv.URL, xet.FileHash{}.String(), "A", "B")}}

			check(resolveThrough(t, hubOrigin+resolvePath, "hub-token", WithHTTPClient(httpClient)))
			if got := rec.get(resolvePath); len(got) == 0 || got[0] != "Bearer hub-token" {
				t.Fatalf("resolve HEAD Authorization = %q, want the hub token first", got)
			}
			if got := rec.get(authPath); len(got) != 2 || got[0] != tc.want || got[1] != tc.want {
				t.Fatalf("xet-auth Authorization = %q, want two of %q", got, tc.want)
			}
		})
	}
}

// A redirect off the xet-auth endpoint is reported, never followed with the hub token.
func TestAuthTokenRedirectNotFollowed(t *testing.T) {
	const hubOrigin = "https://hub.example"
	scenarios := map[string]struct {
		redirectAt int // the token fetch that answers with the redirect
		run        func(t *testing.T, httpClient *http.Client) error
	}{
		"initial": {1, func(t *testing.T, httpClient *http.Client) error {
			return resolveErr(t, hubOrigin+resolvePath, "hub-token", WithHTTPClient(httpClient))
		}},
		"renewal": {2, func(t *testing.T, httpClient *http.Client) error {
			f := resolveThrough(t, hubOrigin+resolvePath, "hub-token", WithHTTPClient(httpClient))
			_, _, err := f.upstream.Resolve(context.Background(), auth.Read)
			return err
		}},
		"provider": {1, func(_ *testing.T, httpClient *http.Client) error {
			_, _, err := NewTokenProvider(httpClient, "hub-token", map[auth.Permission]string{auth.Read: hubOrigin + readTokenPath}).Resolve(context.Background(), auth.Read)
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
					case r.URL.Path == resolvePath:
						w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="xet-auth", <https://cas.example/v1/reconstructions/%s>; rel="xet-reconstruction-info"`, hubOrigin+authPath, xet.FileHash{}.String()))
					case r.URL.Path == authPath || r.URL.Path == readTokenPath:
						if int(fetches.Add(1)) < sc.redirectAt {
							_, _ = fmt.Fprintf(w, `{"casUrl":"https://cas.example","accessToken":"A","exp":%d}`, time.Now().Add(tokenExpiryMargin/2).Unix())
							return
						}
						http.Redirect(w, r, target+authPath, http.StatusFound)
					default:
						http.NotFound(w, r)
					}
				})}}

				err := sc.run(t, httpClient)
				if got := leaked.get(authPath); len(got) != 0 {
					t.Fatalf("redirect target Authorization = %q, want no request", got)
				}
				if err == nil || !strings.Contains(err.Error(), "302") {
					t.Fatalf("err = %v, want the 302 reported", err)
				}
			})
		}
	}
}

// A redirect off the resolve URL is reported, never followed with the hub token.
func TestResolveRedirectNotFollowed(t *testing.T) {
	const hubOrigin = "https://hub.example"
	check := func(t *testing.T, resolveURL string, leaked *authRecorder, opts ...Options) {
		t.Helper()
		err := resolveErr(t, resolveURL, "hub-token", opts...)
		if got := leaked.get(resolvePath); len(got) != 0 {
			t.Fatalf("redirect target Authorization = %q, want no request", got)
		}
		if err == nil || !strings.Contains(err.Error(), "302") {
			t.Fatalf("err = %v, want the 302 reported", err)
		}
	}
	for _, target := range []string{"https://hub.example:8443", "https://evil.hub.example", "https://other.example"} {
		t.Run(target, func(t *testing.T) {
			leaked := &authRecorder{} // requests the redirect target received
			httpClient := &http.Client{Transport: originTransport{http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Scheme+"://"+r.URL.Host == target {
					leaked.record(r)
					return
				}
				http.Redirect(w, r, target+resolvePath, http.StatusFound)
			})}}
			check(t, hubOrigin+resolvePath, leaked, WithHTTPClient(httpClient))
		})
	}
	t.Run("default client", func(t *testing.T) {
		leaked := &authRecorder{}
		target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { leaked.record(r) }))
		defer target.Close()
		hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL+r.URL.Path, http.StatusFound)
		}))
		defer hub.Close()
		check(t, hub.URL+resolvePath, leaked)
	})
}

// A reconstruction link without a hash segment falls back to the X-Xet-Hash header.
func TestResolveHashFallback(t *testing.T) {
	const sampleHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"
	for _, tc := range []struct{ name, header, wantErr string }{{"header", sampleHash, ""}, {"missing", "", "X-Xet-Hash"}} {
		t.Run(tc.name, func(t *testing.T) {
			var hub *httptest.Server
			hub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == authPath {
					_, _ = fmt.Fprintf(w, `{"casUrl":"https://cas.example","accessToken":"cas","exp":%d}`, time.Now().Add(time.Hour).Unix())
					return
				}
				if tc.header != "" {
					w.Header().Set("X-Xet-Hash", tc.header)
				}
				w.Header().Set("Link", fmt.Sprintf(`<%s%s>; rel="xet-auth", <https://cas.example/v1/reconstructions>; rel="xet-reconstruction-info"`, hub.URL, authPath))
				w.WriteHeader(http.StatusOK)
			}))
			defer hub.Close()

			if tc.wantErr != "" {
				if err := resolveErr(t, hub.URL+resolvePath, ""); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if f := resolveThrough(t, hub.URL+resolvePath, ""); f.Hash.String() != sampleHash {
				t.Fatalf("hash = %s, want the header's", f.Hash)
			}
		})
	}
}
