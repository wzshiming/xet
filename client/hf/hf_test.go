package hf_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf"
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

func TestTokenProviderWrite(t *testing.T) {
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

	repo := hf.Repo{Endpoint: tokenSrv.URL, RepoType: "dataset", RepoID: "org/repo", Revision: "main"}

	baseURL, token := resolve(t, hf.NewTokenProvider(nil, repo, "hf-token"), auth.Write)
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestTokenProviderWriteOverrides(t *testing.T) {
	const wantPath = "/api/spaces/org/repo/xet-write-token/custom-rev"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != wantPath {
			t.Fatalf("unexpected escaped path: %s", r.URL.EscapedPath())
		}
		_, _ = fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	repo := hf.Repo{
		Endpoint: tokenSrv.URL,
		RepoType: "space",
		RepoID:   "org/repo",
		Revision: "custom-rev",
	}

	baseURL, token := resolve(t, hf.NewTokenProvider(nil, repo, "hf-token"), auth.Write)
	if token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", token)
	}
	if baseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
}

func TestTokenProviderRead(t *testing.T) {
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

	repo := hf.Repo{Endpoint: tokenSrv.URL, RepoID: "org/repo"}

	baseURL, token := resolve(t, hf.NewTokenProvider(nil, repo, "hf-token"), auth.Read)
	if baseURL != "https://cas-download.example.com" {
		t.Fatalf("unexpected baseURL: %s", baseURL)
	}
	if token != "cas-read-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

func TestTokenProviderKernel(t *testing.T) {
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

	repo := hf.Repo{Endpoint: tokenSrv.URL, RepoType: "kernels", RepoID: "org/repo", Revision: "main"}

	_, token := resolve(t, hf.NewTokenProvider(&http.Client{Timeout: 5 * time.Second}, repo, "hf-token"), auth.Read)
	if token != "cas-kernel-token" {
		t.Fatalf("unexpected token: %s", token)
	}
}

// CommitURL names the revision's commit endpoint with the normalization NewTokenProvider applies to its token endpoints.
func TestRepoCommitURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo hf.Repo
		want string
	}{
		{"model default", hf.Repo{Endpoint: "https://hub.example", RepoID: "org/repo"}, "https://hub.example/api/models/org/repo/commit/main"},
		{"default endpoint", hf.Repo{RepoID: "org/repo"}, "https://huggingface.co/api/models/org/repo/commit/main"},
		{"dataset", hf.Repo{Endpoint: "https://hub.example", RepoType: "datasets", RepoID: "org/repo", Revision: "dev"}, "https://hub.example/api/datasets/org/repo/commit/dev"},
		{"escaped revision", hf.Repo{Endpoint: "https://hub.example", RepoID: "org/repo", Revision: "refs/pr/1"}, "https://hub.example/api/models/org/repo/commit/refs%2Fpr%2F1"},
		{"trailing slashes", hf.Repo{Endpoint: "https://hub.example/", RepoID: "/org/repo/"}, "https://hub.example/api/models/org/repo/commit/main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.repo.CommitURL(); got != tc.want {
				t.Fatalf("%+v.CommitURL() = %q, want %q", tc.repo, got, tc.want)
			}
		})
	}
}

// ResolveURL prefixes non-model types, escapes the revision whole and the path per segment.
func TestRepoResolveURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo hf.Repo
		path string
		want string
	}{
		{"model default", hf.Repo{Endpoint: "https://hub.example", RepoID: "org/repo"}, "config.json", "https://hub.example/org/repo/resolve/main/config.json"},
		{"default endpoint", hf.Repo{RepoID: "org/repo"}, "config.json", "https://huggingface.co/org/repo/resolve/main/config.json"},
		{"dataset", hf.Repo{Endpoint: "https://hub.example", RepoType: "datasets", RepoID: "org/repo", Revision: "dev"}, "data/train.parquet", "https://hub.example/datasets/org/repo/resolve/dev/data/train.parquet"},
		{"kernel", hf.Repo{Endpoint: "https://hub.example", RepoType: "kernel", RepoID: "org/repo"}, "build.toml", "https://hub.example/kernels/org/repo/resolve/main/build.toml"},
		{"escaped", hf.Repo{Endpoint: "https://hub.example/", RepoID: "/org/repo/", Revision: "refs/pr/1"}, "dir/a b.txt", "https://hub.example/org/repo/resolve/refs%2Fpr%2F1/dir/a%20b.txt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.repo.ResolveURL(tc.path); got != tc.want {
				t.Fatalf("%+v.ResolveURL(%q) = %q, want %q", tc.repo, tc.path, got, tc.want)
			}
		})
	}
}

// ParseResolveURL inverts ResolveURL: the parsed repository renders the same download and commit URLs and the path comes back unescaped.
func TestParseResolveURL(t *testing.T) {
	for _, tc := range []struct {
		name string
		repo hf.Repo
		path string
	}{
		{"model default", hf.Repo{Endpoint: "https://hub.example", RepoID: "org/repo"}, "config.json"},
		{"dataset", hf.Repo{Endpoint: "https://hub.example", RepoType: "datasets", RepoID: "org/repo", Revision: "dev"}, "data/train.parquet"},
		{"kernel", hf.Repo{Endpoint: "https://hub.example", RepoType: "kernel", RepoID: "org/repo"}, "build.toml"},
		{"escaped", hf.Repo{Endpoint: "https://hub.example/", RepoID: "/org/repo/", Revision: "refs/pr/1"}, "dir/a b.txt"},
		{"single segment", hf.Repo{Endpoint: "https://hub.example", RepoID: "gpt2"}, "config.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rawURL := tc.repo.ResolveURL(tc.path)
			repo, path, err := hf.ParseResolveURL(rawURL)
			if err != nil {
				t.Fatal(err)
			}
			if path != tc.path || repo.ResolveURL(path) != rawURL || repo.CommitURL() != tc.repo.CommitURL() {
				t.Fatalf("ParseResolveURL(%q) = %+v, %q; want %+v, %q", rawURL, repo, path, tc.repo, tc.path)
			}
		})
	}
	for _, rawURL := range []string{"ftp://x/a/resolve/b/c", "https://hub.example/org/repo/tree/main/x", "https://hub.example/org/repo/resolve/main"} {
		if repo, path, err := hf.ParseResolveURL(rawURL); err == nil {
			t.Errorf("ParseResolveURL(%q) = %+v, %q; want an error", rawURL, repo, path)
		}
	}
}

// NewClient binds the embedded client to the repository: its first CAS operation fetches the read token from the hub with the hub token and carries it to the CAS.
func TestNewClientBindsRepo(t *testing.T) {
	cas, casRec := recordingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{}`)
	})
	hub, hubRec := recordingServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"casUrl":%q,"accessToken":"cas-read-token","exp":%d}`, cas.URL, time.Now().Add(time.Hour).Unix())
	})

	c, err := hf.NewClient(nil, hf.Repo{Endpoint: hub.URL, RepoID: "org/repo"}, "hf-token")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetReconstructionV1(t.Context(), xet.FileHash{}, nil); err != nil {
		t.Fatalf("reconstruction through the bound client: %v", err)
	}
	if got := hubRec.get(readTokenPath); !slices.Equal(got, []string{"Bearer hf-token"}) {
		t.Fatalf("read-token Authorization = %q, want the hub token once", got)
	}
	if got := casRec.get("/v1/reconstructions/" + xet.FileHash{}.String()); !slices.Equal(got, []string{"Bearer cas-read-token"}) {
		t.Fatalf("CAS Authorization = %q, want the minted token once", got)
	}
}
