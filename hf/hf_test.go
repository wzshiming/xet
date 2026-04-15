package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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

	hash, token, err := ResolveDownload(context.Background(), nil, resolveSrv.URL)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	if got := hash.String(); got != sampleHash {
		t.Fatalf("unexpected hash: %s", got)
	}
	if token.BaseURL != "https://override.cas" {
		t.Fatalf("unexpected baseURL: %s", token.BaseURL)
	}
	if token.Token != "token-123" {
		t.Fatalf("unexpected token: %s", token.Token)
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

	info, err := ResolveXETWriteToken(context.Background(), nil, target, "hf-token")
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if info.BaseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
	}
	if info.Token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", info.Token)
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

	info, err := ResolveXETWriteToken(context.Background(), nil, target, "hf-token")
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if info.Token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", info.Token)
	}
	if info.BaseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
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

	info, err := ResolveXETReadToken(context.Background(), nil, target, "hf-token")
	if err != nil {
		t.Fatalf("ResolveRead returned error: %v", err)
	}

	if info.BaseURL != "https://cas-download.example.com" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
	}
	if info.Token != "cas-read-token" {
		t.Fatalf("unexpected token: %s", info.Token)
	}
}
