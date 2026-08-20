package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// TestHFUploadFollowsWriteTokenRedirect verifies the hf upload flow when the
// hub redirects the xet-write-token route (as huggingface.co does for moved or
// renamed repos). Aligned with xet-core's reqwest client, the token GET must
// follow the redirect — dropping the hub credential on the cross-host hop —
// and the upload then proceeds against the CAS endpoint named in the token
// response using the token it carried.
func TestHFUploadFollowsWriteTokenRedirect(t *testing.T) {
	casStorage, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	casHandler := server.NewHandler(server.WithStorage(casStorage))
	var casAuthOK atomic.Bool
	casAuthOK.Store(true)
	casServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer cas-write-token" {
			casAuthOK.Store(false)
		}
		casHandler.ServeHTTP(w, r)
	}))
	defer casServer.Close()

	const tokenPath = "/api/models/org/repo/xet-write-token/main"
	exp := time.Now().Add(time.Hour).Unix()

	var newHubHits atomic.Int64
	newHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newHubHits.Add(1)
		if r.URL.Path != tokenPath {
			t.Errorf("new hub: unexpected path: %s", r.URL.Path)
		}
		// The hop left the original host, so the hub token must be dropped.
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("new hub: cross-host redirect must drop the hub token, got: %s", auth)
		}
		fmt.Fprintf(w, `{"casUrl":%q,"accessToken":"cas-write-token","exp":%d}`, casServer.URL, exp)
	}))
	defer newHub.Close()

	var oldHubHits atomic.Int64
	oldHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		oldHubHits.Add(1)
		if r.URL.Path != tokenPath {
			t.Errorf("old hub: unexpected path: %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
			t.Errorf("old hub: unexpected auth header: %s", auth)
		}
		http.Redirect(w, r, newHub.URL+tokenPath, http.StatusTemporaryRedirect)
	}))
	defer oldHub.Close()

	target := hf.Target{Endpoint: oldHub.URL, RepoType: "model", RepoID: "org/repo", Revision: "main"}
	provider := hf.NewWriteTokenProvider(nil, target, "hf-token")

	uploadClient, err := client.NewClient(client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	data := deterministicData(3*128*1024 + 7919)
	fileHash, err := uploadClient.UploadFileWithAuthProvider(context.Background(), provider, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("upload through redirected write-token route: %v", err)
	}

	if oldHubHits.Load() == 0 {
		t.Error("old hub token route was never called")
	}
	if newHubHits.Load() == 0 {
		t.Error("redirect target token route was never called")
	}
	if !casAuthOK.Load() {
		t.Error("CAS requests did not carry the redirected write token")
	}

	// Round-trip through the CAS server to prove the upload landed intact.
	downloadClient, err := client.NewClient(
		client.WithBaseURL(casServer.URL),
		client.WithToken("cas-write-token"),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := downloadClient.DownloadFile(context.Background(), fileHash, out); err != nil {
		t.Fatalf("download uploaded file: %v", err)
	}
	got, err := os.ReadFile(out.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}
