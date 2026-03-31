package hf

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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

	info, err := ResolveDownload(context.Background(), resolveSrv.URL)
	if err != nil {
		t.Fatalf("Resolve returned error: %v", err)
	}

	if got := info.Hash.String(); got != sampleHash {
		t.Fatalf("unexpected hash: %s", got)
	}
	if info.BaseURL != "https://override.cas" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
	}
	if info.Token != "token-123" {
		t.Fatalf("unexpected token: %s", info.Token)
	}
}

func TestResolveHuggingFaceMissingHeaders(t *testing.T) {
	resolveSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusFound)
	}))
	defer resolveSrv.Close()

	_, err := ResolveDownload(context.Background(), resolveSrv.URL)
	if err == nil {
		t.Fatalf("expected error due to missing headers")
	}
}

func TestResolveUploadFromRepoURL(t *testing.T) {
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

	info, err := ResolveXETWriteToken(context.Background(), tokenSrv.URL+"/datasets/org/repo", "hf-token", UploadOptions{})
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if info.BaseURL != "https://cas-upload.example.com" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
	}
	if info.Token != "cas-write-token" {
		t.Fatalf("unexpected token: %s", info.Token)
	}
	if info.RepoType != "dataset" || info.RepoID != "org/repo" || info.Revision != "main" {
		t.Fatalf("unexpected target info: %#v", info)
	}
}

func TestResolveUploadFromRepoIDWithEncodedRevision(t *testing.T) {
	const wantPath = "/api/models/org/repo/xet-write-token/refs%2Fpr%2F1"

	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.EscapedPath() != wantPath {
			t.Fatalf("unexpected escaped path: %s", r.URL.EscapedPath())
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-upload.example.com","accessToken":"cas-write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	info, err := ResolveXETWriteToken(context.Background(), "org/repo", "hf-token", UploadOptions{
		Endpoint: tokenSrv.URL,
		Revision: "refs/pr/1",
	})
	if err != nil {
		t.Fatalf("ResolveUpload returned error: %v", err)
	}

	if info.RepoType != "model" {
		t.Fatalf("unexpected repo type: %s", info.RepoType)
	}
	if info.Revision != "refs/pr/1" {
		t.Fatalf("unexpected revision: %s", info.Revision)
	}
}

func TestResolveUploadMissingToken(t *testing.T) {
	_, err := ResolveXETWriteToken(context.Background(), "org/repo", "", UploadOptions{})
	if err == nil || !strings.Contains(err.Error(), "missing Hugging Face token") {
		t.Fatalf("expected missing token error, got %v", err)
	}
}

func TestResolveReadFromRepoURL(t *testing.T) {
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

	info, err := ResolveXETReadToken(context.Background(), tokenSrv.URL+"/org/repo", "hf-token", UploadOptions{})
	if err != nil {
		t.Fatalf("ResolveRead returned error: %v", err)
	}

	if info.BaseURL != "https://cas-download.example.com" {
		t.Fatalf("unexpected baseURL: %s", info.BaseURL)
	}
	if info.Token != "cas-read-token" {
		t.Fatalf("unexpected token: %s", info.Token)
	}
	if info.RepoType != "model" || info.RepoID != "org/repo" || info.Revision != "main" {
		t.Fatalf("unexpected target info: %#v", info)
	}
}
