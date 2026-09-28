package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// resolvePath is f.bin of org/repo at main, the file every hub fixture here serves.
const resolvePath = "/org/repo/resolve/main/f.bin"

// resolveErr resolves f.bin of hubURL's org/repo with token through httpClient and returns the error.
func resolveErr(t *testing.T, httpClient *http.Client, hubURL, token string) (*ResolvedFile, error) {
	t.Helper()
	c, err := NewClient(Repo{Endpoint: hubURL, RepoID: "org/repo"}, WithHTTPClient(httpClient), WithToken(token))
	if err != nil {
		t.Fatal(err)
	}
	return c.Resolve(context.Background(), "f.bin")
}

// resolveThrough is resolveErr failing the test on error.
func resolveThrough(t *testing.T, httpClient *http.Client, hubURL, token string) *ResolvedFile {
	t.Helper()
	f, err := resolveErr(t, httpClient, hubURL, token)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// The hash comes from the reconstruction link of the HEAD response; the HEAD carries the hub token and nothing when anonymous.
func TestResolveHuggingFace(t *testing.T) {
	const sampleHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"
	hub, rec := recordingServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead || r.URL.Path != resolvePath {
			t.Errorf("hub saw %s %s, want HEAD %s", r.Method, r.URL.Path, resolvePath)
		}
		w.Header().Set("Link", fmt.Sprintf(`<https://hub.example/api/models/org/repo/xet-read-token/main>; rel="xet-auth", <https://cas-server.example.com/v1/reconstructions/%s>; rel="xet-reconstruction-info"`, sampleHash))
		w.WriteHeader(http.StatusFound)
	})
	for _, tc := range []struct{ name, token, want string }{{"hub token", "hub-token", "Bearer hub-token"}, {"anonymous", "", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			before := len(rec.get(resolvePath))
			if f := resolveThrough(t, nil, hub.URL, tc.token); f.Hash.String() != sampleHash {
				t.Fatalf("hash = %s, want the link's", f.Hash)
			}
			if got := rec.get(resolvePath)[before:]; len(got) != 1 || got[0] != tc.want {
				t.Fatalf("resolve HEAD Authorization = %q, want [%q]", got, tc.want)
			}
		})
	}
}

// A response without a usable reconstruction link, or outside 2xx/3xx, is an error naming what is missing.
func TestResolveHuggingFaceMissingHeaders(t *testing.T) {
	for _, tc := range []struct {
		name, link string
		status     int
		wantErr    string
	}{
		{"no headers", "", http.StatusFound, "missing xet-reconstruction-info link"},
		{"relative link", `</v1/reconstructions/x>; rel="xet-reconstruction-info"`, http.StatusOK, "invalid reconstruction link"},
		{"not found", "", http.StatusNotFound, "hub API error (status 404"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.link != "" {
					w.Header().Set("Link", tc.link)
				}
				w.WriteHeader(tc.status)
			}))
			defer hub.Close()
			if _, err := resolveErr(t, nil, hub.URL, ""); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want one naming %q", err, tc.wantErr)
			}
		})
	}
}

// A redirect off the resolve URL is reported, never followed with the hub token.
func TestResolveRedirectNotFollowed(t *testing.T) {
	const hubOrigin = "https://hub.example"
	check := func(t *testing.T, httpClient *http.Client, hubURL string, leaked *authRecorder) {
		t.Helper()
		_, err := resolveErr(t, httpClient, hubURL, "hub-token")
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
			check(t, httpClient, hubOrigin, leaked)
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
		check(t, nil, hub.URL, leaked)
	})
}

// A reconstruction link without a hash segment falls back to the X-Xet-Hash header.
func TestResolveHashFallback(t *testing.T) {
	const sampleHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"
	for _, tc := range []struct{ name, header, wantErr string }{{"header", sampleHash, ""}, {"missing", "", "X-Xet-Hash"}} {
		t.Run(tc.name, func(t *testing.T) {
			hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("X-Xet-Hash", tc.header)
				}
				w.Header().Set("Link", `<https://cas.example/v1/reconstructions>; rel="xet-reconstruction-info"`)
				w.WriteHeader(http.StatusOK)
			}))
			defer hub.Close()

			if tc.wantErr != "" {
				if _, err := resolveErr(t, nil, hub.URL, ""); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one naming %s", err, tc.wantErr)
				}
				return
			}
			if f := resolveThrough(t, nil, hub.URL, ""); f.Hash.String() != sampleHash {
				t.Fatalf("hash = %s, want the header's", f.Hash)
			}
		})
	}
}
