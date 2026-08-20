package hf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/lfs"
)

func TestUploadAlreadyExists(t *testing.T) {
	data := []byte("already uploaded content")
	digest := sha256.Sum256(data)
	wantOID := hex.EncodeToString(digest[:])

	verifyCalled := false
	hub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/datasets/org/repo.git/info/lfs/objects/batch":
			var req struct {
				Operation string   `json:"operation"`
				Transfers []string `json:"transfers"`
				HashAlgo  string   `json:"hash_algo"`
				Objects   []struct {
					OID  string `json:"oid"`
					Size int64  `json:"size"`
				} `json:"objects"`
				Ref *struct {
					Name string `json:"name"`
				} `json:"ref"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode batch request: %v", err)
			}
			if auth := r.Header.Get("Authorization"); auth != "Bearer hf-token" {
				t.Errorf("unexpected auth header: %s", auth)
			}
			if req.Operation != "upload" {
				t.Errorf("operation = %q, want upload", req.Operation)
			}
			if len(req.Transfers) != 1 || req.Transfers[0] != "xet" {
				t.Errorf("transfers = %v, want [xet]", req.Transfers)
			}
			if req.HashAlgo != "sha256" {
				t.Errorf("hash_algo = %q, want sha256", req.HashAlgo)
			}
			if len(req.Objects) != 1 || req.Objects[0].OID != wantOID || req.Objects[0].Size != int64(len(data)) {
				t.Errorf("objects = %+v, want oid %s size %d", req.Objects, wantOID, len(data))
			}
			if req.Ref == nil || req.Ref.Name != "main" {
				t.Errorf("ref = %+v, want main", req.Ref)
			}
			// No actions: the object is already present on the Hub.
			fmt.Fprintf(w, `{"transfer":"xet","objects":[{"oid":"%s"}]}`, wantOID)
		default:
			verifyCalled = true
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer hub.Close()

	target := Target{Endpoint: hub.URL, RepoType: "dataset", RepoID: "org/repo"}
	result, err := Upload(context.Background(), target, "hf-token", bytes.NewReader(data))
	if err != nil {
		t.Fatalf("Upload returned error: %v", err)
	}

	if !result.AlreadyExists {
		t.Fatal("AlreadyExists = false, want true")
	}
	if result.FileHash != (xet.FileHash{}) {
		t.Fatalf("FileHash = %s, want zero", result.FileHash)
	}
	if result.OID != wantOID {
		t.Fatalf("OID = %s, want %s", result.OID, wantOID)
	}
	if result.Size != int64(len(data)) {
		t.Fatalf("Size = %d, want %d", result.Size, len(data))
	}
	if verifyCalled {
		t.Fatal("no further request expected when object already exists")
	}
}

func TestUploadAuthProviderPrefersBatchHeaders(t *testing.T) {
	action := &lfs.Action{Header: map[string]string{
		"X-Xet-Cas-Url":      "https://cas.example.com",
		"X-Xet-Access-Token": "cas-token",
	}}

	provider := uploadAuthProvider(action, Target{RepoID: "org/repo"}, "hf-token")
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("BaseURL returned error: %v", err)
	}
	if baseURL != "https://cas.example.com" {
		t.Fatalf("baseURL = %s, want https://cas.example.com", baseURL)
	}
	token, err := provider.Token(context.Background())
	if err != nil {
		t.Fatalf("Token returned error: %v", err)
	}
	if token != "cas-token" {
		t.Fatalf("token = %s, want cas-token", token)
	}
}

func TestUploadAuthProviderFallsBackToWriteToken(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/models/org/repo/xet-write-token/main" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"casUrl":"https://cas-write.example.com","accessToken":"write-token","exp":5678}`)
	}))
	defer tokenSrv.Close()

	action := &lfs.Action{Header: map[string]string{}}
	target := normalizeTarget(Target{Endpoint: tokenSrv.URL, RepoID: "org/repo"})

	provider := uploadAuthProvider(action, target, "hf-token")
	baseURL, err := provider.BaseURL(context.Background())
	if err != nil {
		t.Fatalf("BaseURL returned error: %v", err)
	}
	if baseURL != "https://cas-write.example.com" {
		t.Fatalf("baseURL = %s, want https://cas-write.example.com", baseURL)
	}
}

func TestComputeOIDAndSizeRestoresOffset(t *testing.T) {
	data := []byte("prefix|payload")
	r := bytes.NewReader(data)
	const offset = 7
	if _, err := r.Seek(offset, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	oid, size, err := computeOIDAndSize(r)
	if err != nil {
		t.Fatalf("computeOIDAndSize returned error: %v", err)
	}

	digest := sha256.Sum256(data[offset:])
	if want := hex.EncodeToString(digest[:]); oid != want {
		t.Fatalf("oid = %s, want %s", oid, want)
	}
	if size != int64(len(data)-offset) {
		t.Fatalf("size = %d, want %d", size, len(data)-offset)
	}
	if pos, err := r.Seek(0, io.SeekCurrent); err != nil || pos != offset {
		t.Fatalf("offset after hash = %d (err %v), want %d", pos, err, offset)
	}
}
