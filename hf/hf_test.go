package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestResolveHuggingFace(t *testing.T) {
	const sampleHash = "aeb713fdee2a083353a999d46771858f952744509d8af12868a1e95e9c45c7e3"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"casUrl":"https://override.cas","accessToken":"token-123","exp":1234}`)
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

	hash, provider, err := ResolveDownload(context.Background(), nil, resolveSrv.URL)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	if got := hash.String(); got != sampleHash {
		t.Fatalf("unexpected hash: %s", got)
	}
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("BaseURL returned error: %v", err)
	}
	if baseURL != "https://override.cas" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "token-123" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestResolveHuggingFaceMissingHeaders(t *testing.T) {
	resolveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusFound)
	}))
	defer resolveSrv.Close()

	_, _, err := ResolveDownload(context.Background(), nil, resolveSrv.URL)
	if err == nil {
		t.Fatalf("expected error due to missing headers")
	}
}

func TestResolveUploadWithExplicitTarget(t *testing.T) {
	const wantPath = "/api/datasets/org/repo/xet-write-token/main"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request, got %s", r.Method)
		}
		if r.URL.Path != wantPath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Fatalf("unexpected auth header: %s", auth)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	target := Target{Endpoint: tokenSrv.URL, RepoType: "dataset", RepoID: "org/repo", Revision: "main"}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestResolveUploadWithExplicitOverrides(t *testing.T) {
	const wantPath = "/api/spaces/org/repo/xet-write-token/custom-rev"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != wantPath {
			t.Fatalf("unexpected escaped path: %s", r.URL.EscapedPath())
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	target := Target{
		Endpoint: tokenSrv.URL,
		RepoType: "space",
		RepoID:   "org/repo",
		Revision: "custom-rev",
	}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("BaseURL returned error: %v", err)
	}
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
}

func TestResolveReadWithExplicitTarget(t *testing.T) {
	const wantPath = "/api/models/org/repo/xet-read-token/main"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request, got %s", r.Method)
		}
		if r.URL.Path != wantPath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Fatalf("unexpected auth header: %s", auth)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-download.example.com","accessToken":"cas-read-token","exp":9876}`)
	}))
	defer tokenSrv.Close()

	target := Target{Endpoint: tokenSrv.URL, RepoID: "org/repo"}

	provider := NewReadTokenProvider(nil, target, "hf-token")
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("ResolveRead returned error: %v", err)
	}

	if baseURL != "https://cas-download.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-read-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// TestWriteTokenProviderFollowsSameHostRedirect verifies the write-token route
// may be redirected within the hub host (as huggingface.co does for moved
// repos) and that the hub credential is kept on same-host hops, matching
// xet-core's reqwest redirect policy.
func TestWriteTokenProviderFollowsSameHostRedirect(t *testing.T) {
	const oldPath = "/api/models/old-org/repo/xet-write-token/main"
	const newPath = "/api/models/new-org/repo/xet-write-token/main"

	mux := http.NewServeMux()
	mux.HandleFunc(oldPath, func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("unexpected auth header on redirecting route: %s", auth)
		}
		http.Redirect(w, r, newPath, http.StatusTemporaryRedirect)
	})
	mux.HandleFunc(newPath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET request, got %s", r.Method)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("same-host redirect must keep the hub token, got: %s", auth)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	})
	hubSrv := httptest.NewServer(mux)
	defer hubSrv.Close()

	target := Target{Endpoint: hubSrv.URL, RepoID: "old-org/repo"}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("BaseURL returned error: %v", err)
	}
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// TestWriteTokenProviderCrossHostRedirectDropsAuth verifies that when the
// write-token route redirects to a different host, the redirect is followed
// but the hub credential is dropped, matching xet-core's reqwest policy of
// removing sensitive headers once the chain leaves the original host.
func TestWriteTokenProviderCrossHostRedirectDropsAuth(t *testing.T) {
	const tokenPath = "/api/models/org/repo/xet-write-token/main"

	newHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("cross-host redirect must drop the hub token, got: %s", auth)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer newHub.Close()

	oldHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != tokenPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("unexpected auth header on redirecting route: %s", auth)
		}
		http.Redirect(w, r, newHub.URL+tokenPath, http.StatusTemporaryRedirect)
	}))
	defer oldHub.Close()

	target := Target{Endpoint: oldHub.URL, RepoID: "org/repo"}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// TestWriteTokenProviderMultiHopRedirect verifies a chain that leaves the hub
// host and later stays on the new host: once dropped, the credential must not
// reappear on subsequent hops, matching reqwest's header carry-forward.
func TestWriteTokenProviderMultiHopRedirect(t *testing.T) {
	otherMux := http.NewServeMux()
	otherHub := httptest.NewServer(otherMux)
	defer otherHub.Close()

	otherMux.HandleFunc("/hop", func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("cross-host hop must drop the hub token, got: %s", auth)
		}
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	otherMux.HandleFunc("/final", func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("credential must stay dropped after leaving the hub host, got: %s", auth)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	})

	hubMux := http.NewServeMux()
	hubSrv := httptest.NewServer(hubMux)
	defer hubSrv.Close()

	hubMux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("unexpected auth header on hub route: %s", auth)
		}
		http.Redirect(w, r, "/relocated", http.StatusMovedPermanently)
	})
	hubMux.HandleFunc("/relocated", func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("same-host hop must keep the hub token, got: %s", auth)
		}
		http.Redirect(w, r, otherHub.URL+"/hop", http.StatusTemporaryRedirect)
	})

	target := Target{Endpoint: hubSrv.URL, RepoID: "org/repo"}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// TestWriteTokenProviderRedirectLimit verifies the token fetch gives up after
// 10 hops, the same cap reqwest's default redirect policy enforces.
func TestWriteTokenProviderRedirectLimit(t *testing.T) {
	var hits atomic.Int64
	hubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Redirect(w, r, r.URL.Path, http.StatusTemporaryRedirect)
	}))
	defer hubSrv.Close()

	target := Target{Endpoint: hubSrv.URL, RepoID: "org/repo"}

	provider := NewWriteTokenProvider(nil, target, "hf-token")
	_, err := provider.Token(context.Background())
	if err == nil {
		t.Fatal("expected error from unbounded redirect chain")
	}
	if !strings.Contains(err.Error(), "stopped after 10 redirects") {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := hits.Load(); got != 11 {
		t.Fatalf("redirecting route hit %d times, want 11 (initial request + 10 followed hops)", got)
	}
}
