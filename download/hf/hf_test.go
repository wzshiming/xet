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

	info, err := Resolve(context.Background(), resolveSrv.URL)
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

	_, err := Resolve(context.Background(), resolveSrv.URL)
	if err == nil {
		t.Fatalf("expected error due to missing headers")
	}
}
