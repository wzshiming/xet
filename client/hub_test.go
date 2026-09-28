package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
)

const readTokenPath = "/api/models/org/repo/xet-read-token/main"

// resolve returns perm's base URL and token, failing the test on error.
func resolve(t *testing.T, provider client.UpstreamProvider, perm auth.Permission) (string, string) {
	t.Helper()
	baseURL, token, err := provider.Resolve(context.Background(), perm)
	if err != nil {
		t.Fatalf("Resolve(%s): %v", perm, err)
	}
	return baseURL, token
}

func TestHubTokenProviderWrite(t *testing.T) {
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
		_, _ = fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	repo := client.HubRepo{Endpoint: tokenSrv.URL, RepoType: "dataset", RepoID: "org/repo", Revision: "main"}

	baseURL, token := resolve(t, client.NewHubTokenProvider(nil, repo, "hf-token"), auth.Write)
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestHubTokenProviderWriteOverrides(t *testing.T) {
	const wantPath = "/api/spaces/org/repo/xet-write-token/custom-rev"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != wantPath {
			t.Fatalf("unexpected escaped path: %s", r.URL.EscapedPath())
		}
		_, _ = fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	repo := client.HubRepo{
		Endpoint: tokenSrv.URL,
		RepoType: "space",
		RepoID:   "org/repo",
		Revision: "custom-rev",
	}

	baseURL, token := resolve(t, client.NewHubTokenProvider(nil, repo, "hf-token"), auth.Write)
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
}

func TestHubTokenProviderRead(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("expected GET request, got %s", r.Method)
		}
		if r.URL.Path != readTokenPath {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Fatalf("unexpected auth header: %s", auth)
		}
		_, _ = fmt.Fprint(w, `{"casUrl":"https://cas-download.example.com","accessToken":"cas-read-token","exp":9876}`)
	}))
	defer tokenSrv.Close()

	repo := client.HubRepo{Endpoint: tokenSrv.URL, RepoID: "org/repo"}

	baseURL, token := resolve(t, client.NewHubTokenProvider(nil, repo, "hf-token"), auth.Read)
	if baseURL != "https://cas-download.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if token != "cas-read-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestHubTokenProviderKernel(t *testing.T) {
	const wantPath = "/api/kernels/org/repo/xet-read-token/main"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != wantPath {
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = fmt.Fprint(w, `{"casUrl":"https://cas-download.example.com","accessToken":"cas-kernel-token","exp":9876}`)
	}))
	defer tokenSrv.Close()

	repo := client.HubRepo{Endpoint: tokenSrv.URL, RepoType: "kernels", RepoID: "org/repo", Revision: "main"}

	_, token := resolve(t, client.NewHubTokenProvider(&http.Client{Timeout: 5 * time.Second}, repo, "hf-token"), auth.Read)
	if token != "cas-kernel-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// CommitURL names the revision's commit endpoint with the normalization NewHubTokenProvider applies to its token endpoints.
func TestHubRepoCommitURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo client.HubRepo
		want string
	}{
		{"model default", client.HubRepo{Endpoint: "https://hub.example", RepoID: "org/repo"}, "https://hub.example/api/models/org/repo/commit/main"},
		{"default endpoint", client.HubRepo{RepoID: "org/repo"}, "https://huggingface.co/api/models/org/repo/commit/main"},
		{"dataset", client.HubRepo{Endpoint: "https://hub.example", RepoType: "datasets", RepoID: "org/repo", Revision: "dev"}, "https://hub.example/api/datasets/org/repo/commit/dev"},
		{"escaped revision", client.HubRepo{Endpoint: "https://hub.example", RepoID: "org/repo", Revision: "refs/pr/1"}, "https://hub.example/api/models/org/repo/commit/refs%2Fpr%2F1"},
		{"trailing slashes", client.HubRepo{Endpoint: "https://hub.example/", RepoID: "/org/repo/"}, "https://hub.example/api/models/org/repo/commit/main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.repo.CommitURL(); got != tc.want {
				t.Fatalf("%+v.CommitURL() = %q, want %q", tc.repo, got, tc.want)
			}
		})
	}
}
