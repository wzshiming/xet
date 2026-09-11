package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/auth"
	"github.com/wzshiming/xet/client"
	hfclient "github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/server/internalapi"
	"github.com/wzshiming/xet/storage"
)

// newAuthServer wires the cmd/xetd composition: /internal/ behind its own
// token, CAS routes behind the issuer.
func newAuthServer(t *testing.T, issuer *auth.Issuer, internalToken string) *httptest.Server {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(internalapi.NewHandler(
		internalapi.WithStorage(stor),
		internalapi.WithAuthorizer(auth.AuthorizerFunc(func(r *http.Request, _ auth.Grant) error {
			token, ok := auth.BearerToken(r)
			if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(internalToken)) != 1 {
				return auth.ErrUnauthenticated
			}
			return nil
		})),
		internalapi.WithNext(server.NewHandler(server.WithStorage(stor), server.WithAuthorizer(issuer))),
	))
	t.Cleanup(srv.Close)
	return srv
}

type downloadFunc func(context.Context, client.AuthProvider, xet.FileHash, io.WriteSeeker) error

func downloadAs(t *testing.T, download downloadFunc, provider client.AuthProvider, fileHash xet.FileHash) ([]byte, error) {
	t.Helper()
	out, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := download(context.Background(), provider, fileHash, out); err != nil {
		return nil, err
	}
	return os.ReadFile(out.Name())
}

func assertDownloadAs(t *testing.T, download downloadFunc, provider client.AuthProvider, fileHash xet.FileHash, want []byte) {
	t.Helper()
	got, err := downloadAs(t, download, provider, fileHash)
	if err != nil {
		t.Fatalf("download %s: %v", fileHash, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded %d bytes, want %d", len(got), len(want))
	}
}

func wantErr(t *testing.T, err error, substr string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), substr) {
		t.Fatalf("err = %v, want %q", err, substr)
	}
}

func bearer(token string) http.Header {
	if token == "" {
		return nil
	}
	return http.Header{"Authorization": []string{"Bearer " + token}}
}

func assertStatus(t *testing.T, method, url, token string, want int) []byte {
	t.Helper()
	resp := doRequest(t, method, url, bearer(token))
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != want {
		t.Fatalf("%s %s with %q: status = %d, want %d; body %q", method, url, token, resp.StatusCode, want, body)
	}
	if want == http.StatusUnauthorized && resp.Header.Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("%s %s: 401 without Bearer challenge: %v", method, url, resp.Header)
	}
	return body
}

// TestAuthCASClientFlows drives the real xet client through every CAS route
// with each kind of credential: none, garbage, expired, the raw signing key,
// unbound Read/Write tokens, and tokens bound to a file or a sha256.
func TestAuthCASClientFlows(t *testing.T) {
	ctx := context.Background()
	clock := time.Now()
	issuer, err := auth.NewIssuer([]byte("signing-secret"), time.Minute, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	srv := newAuthServer(t, issuer, "internal-secret")
	c, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	sign := func(g auth.Grant) string {
		t.Helper()
		token, _, err := issuer.Sign(g)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	static := func(token string) client.AuthProvider { return client.StaticAuthProvider("", token) }

	fileA := deterministicData(3*128*1024 + 7919)
	fileB := invertedData(2*128*1024 + 101)
	fileC := []byte("sha256-bound upload content")
	fileD := []byte("content the sha256-bound token does not cover")
	digestC := sha256.Sum256(fileC)

	// All tokens are signed before any request so the injected clock is never
	// written while the server reads it.
	readToken := sign(auth.Grant{Permission: auth.Read})
	writeToken := sign(auth.Grant{Permission: auth.Write})
	shaWriteToken := sign(auth.Grant{Permission: auth.Write, SHA256: digestC})
	clock = clock.Add(-2 * time.Minute)
	expiredToken := sign(auth.Grant{Permission: auth.Read})
	clock = clock.Add(2 * time.Minute)

	var hashA, hashB, hashC xet.FileHash
	t.Run("upload requires a write token", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			token string
			want  string
		}{
			{"none", "", "status 401"},
			{"garbage", "garbage", "status 401"},
			{"expired", expiredToken, "status 401"},
			{"signing key", "signing-secret", "status 401"},
			{"read", readToken, "status 403"},
		} {
			_, err := c.UploadFileWithAuthProvider(ctx, static(test.token), bytes.NewReader(fileA))
			wantErr(t, err, test.want)
		}
		if hashA, err = c.UploadFileV1WithAuthProvider(ctx, static(writeToken), bytes.NewReader(fileA)); err != nil {
			t.Fatal(err)
		}
		if hashB, err = c.UploadFileV2WithAuthProvider(ctx, static(writeToken), bytes.NewReader(fileB)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("download requires a read token", func(t *testing.T) {
		for _, test := range []struct {
			name  string
			token string
			want  string
		}{
			{"none", "", "status 401"},
			{"garbage", "garbage", "status 401"},
			{"expired", expiredToken, "status 401"},
			{"signing key", "signing-secret", "status 401"},
			{"internal token", "internal-secret", "status 401"},
			{"write", writeToken, "status 403"},
		} {
			for _, download := range []downloadFunc{c.DownloadFileV1WithAuthProvider, c.DownloadFileV2WithAuthProvider, c.DownloadFileWithAuthProvider} {
				_, err := downloadAs(t, download, static(test.token), hashA)
				wantErr(t, err, test.want)
			}
		}
		for _, download := range []downloadFunc{c.DownloadFileV1WithAuthProvider, c.DownloadFileV2WithAuthProvider, c.DownloadFileWithAuthProvider} {
			assertDownloadAs(t, download, static(readToken), hashA, fileA)
			assertDownloadAs(t, download, static(readToken), hashB, fileB)
		}
		if _, err := c.GetBatchReconstructionWithAuthProvider(ctx, static(readToken), []xet.FileHash{hashA, hashB}); err != nil {
			t.Fatal(err)
		}
		_, err := c.GetBatchReconstructionWithAuthProvider(ctx, static(writeToken), []xet.FileHash{hashA, hashB})
		wantErr(t, err, "status 403")
	})

	t.Run("file-bound read token", func(t *testing.T) {
		boundToken := sign(auth.Grant{Permission: auth.Read, File: hashA})
		bound := static(boundToken)
		assertDownloadAs(t, c.DownloadFileV1WithAuthProvider, bound, hashA, fileA)
		assertDownloadAs(t, c.DownloadFileV2WithAuthProvider, bound, hashA, fileA)
		_, err := downloadAs(t, c.DownloadFileV1WithAuthProvider, bound, hashB)
		wantErr(t, err, "status 403")
		_, err = downloadAs(t, c.DownloadFileV2WithAuthProvider, bound, hashB)
		wantErr(t, err, "status 403")
		if _, err := c.GetBatchReconstructionWithAuthProvider(ctx, bound, []xet.FileHash{hashA}); err != nil {
			t.Fatal(err)
		}
		_, err = c.GetBatchReconstructionWithAuthProvider(ctx, bound, []xet.FileHash{hashA, hashB})
		wantErr(t, err, "status 403")
		_, err = c.UploadFileWithAuthProvider(ctx, bound, bytes.NewReader(fileC))
		wantErr(t, err, "status 403")

		// The empty file's all-zero hash is a real target, not "any file".
		zero := (xet.FileHash{}).String()
		assertStatus(t, http.MethodGet, srv.URL+"/v1/reconstructions/"+zero, boundToken, http.StatusForbidden)
		assertStatus(t, http.MethodGet, srv.URL+"/reconstructions?file_id="+hashA.String()+"&file_id="+zero, boundToken, http.StatusForbidden)
		assertStatus(t, http.MethodGet, srv.URL+"/v1/reconstructions/"+zero, readToken, http.StatusNotFound)
	})

	t.Run("sha256-bound write token", func(t *testing.T) {
		if hashC, err = c.UploadFileV1WithAuthProvider(ctx, static(shaWriteToken), bytes.NewReader(fileC)); err != nil {
			t.Fatal(err)
		}
		assertDownloadAs(t, c.DownloadFileWithAuthProvider, static(readToken), hashC, fileC)

		_, err := c.UploadFileV1WithAuthProvider(ctx, static(shaWriteToken), bytes.NewReader(fileD))
		wantErr(t, err, "status 403")
		_, err = c.UploadFileV2WithAuthProvider(ctx, static(shaWriteToken), bytes.NewReader(fileD))
		wantErr(t, err, "v2 shard upload failed: forbidden")
		if strings.Contains(err.Error(), "attempts") {
			t.Fatalf("per-file denial was retried: %v", err)
		}
		hashD := xet.ComputeFileHash([]xet.ChunkHash{xet.ComputeChunkHash(fileD)}, []uint64{uint64(len(fileD))})
		_, err = downloadAs(t, c.DownloadFileWithAuthProvider, static(readToken), hashD)
		wantErr(t, err, "404 not found")
	})

	t.Run("xorb and bridge downloads stay anonymous", func(t *testing.T) {
		info, err := c.GetReconstructionV1WithAuthProvider(ctx, static(readToken), hashA, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, entries := range info.FetchInfo {
			assertStatus(t, http.MethodGet, entries[0].URL, "", http.StatusOK)
		}
		assertBridge(t, srv.URL, fileA, http.StatusOK)
		assertBridge(t, srv.URL, fileB, http.StatusOK)
	})
}

// TestAuthInternalToken pins that /internal/ accepts only its own token —
// never CAS tokens or the signing key — and that the internal token opens
// nothing on the CAS routes.
func TestAuthInternalToken(t *testing.T) {
	ctx := context.Background()
	issuer, err := auth.NewIssuer([]byte("signing-secret"), time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := newAuthServer(t, issuer, "internal-secret")
	c, err := client.NewClient(client.WithBaseURL(srv.URL), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	readToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Read})
	if err != nil {
		t.Fatal(err)
	}
	writeToken, _, err := issuer.Sign(auth.Grant{Permission: auth.Write})
	if err != nil {
		t.Fatal(err)
	}
	fileA := deterministicData(128*1024 + 33)
	fileB := invertedData(64*1024 + 17)
	hashes, err := c.UploadFilesWithAuthProvider(ctx, client.StaticAuthProvider("", writeToken), []io.ReadSeeker{bytes.NewReader(fileA), bytes.NewReader(fileB)})
	if err != nil {
		t.Fatal(err)
	}
	digestB := sha256.Sum256(fileB)

	routes := []struct{ method, url string }{
		{http.MethodGet, srv.URL + "/internal/files"},
		{http.MethodPost, srv.URL + "/internal/gc/sweep?dry_run=true"},
		{http.MethodDelete, srv.URL + "/internal/files/xet/" + hashes[1].String()},
		{http.MethodDelete, srv.URL + "/internal/files/sha256/" + hex.EncodeToString(digestB[:])},
	}
	for _, token := range []string{"", "garbage", "signing-secret", readToken, writeToken} {
		for _, route := range routes {
			assertStatus(t, route.method, route.url, token, http.StatusUnauthorized)
		}
	}
	assertStatus(t, http.MethodGet, srv.URL+"/v1/reconstructions/"+hashes[0].String(), "internal-secret", http.StatusUnauthorized)
	assertStatus(t, http.MethodPost, srv.URL+"/v1/shards", "internal-secret", http.StatusUnauthorized)

	var entries []storage.FileListEntry
	if err := json.Unmarshal(assertStatus(t, http.MethodGet, routes[0].url, "internal-secret", http.StatusOK), &entries); err != nil || len(entries) != 2 {
		t.Fatalf("listing = %+v, %v, want both files", entries, err)
	}
	for _, route := range routes[1:] {
		assertStatus(t, route.method, route.url, "internal-secret", http.StatusOK)
	}
	assertDownloadAs(t, c.DownloadFileWithAuthProvider, client.StaticAuthProvider("", readToken), hashes[0], fileA)
	_, err = downloadAs(t, c.DownloadFileWithAuthProvider, client.StaticAuthProvider("", readToken), hashes[1])
	wantErr(t, err, "404 not found")
}

// TestAuthHubTokenFlow runs the hub-side credential flow a xet client
// performs against the mirror: the resolve Link hands out a token bound to
// that file, the repository read-token route hands out an unbound one, and
// write-token requests fall through to the upstream hub.
func TestAuthHubTokenFlow(t *testing.T) {
	ctx := context.Background()
	hub := newFakeHub()
	const pathA, pathB = "/org/repo/resolve/main/a.bin", "/org/repo/resolve/main/b.bin"
	fileA, fileB := deterministicData(2*128*1024+11), invertedData(128*1024+5)
	hub.set(pathA, fileA)
	hub.set(pathB, fileB)
	hub.api["/api/models/org/repo/xet-write-token/main"] = []byte(`{"casUrl":"https://cas.upstream.example","accessToken":"upstream-write-token","exp":4102444800}`)
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()
	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	waitMirrorReady(t, srv.URL+pathA)
	waitMirrorReady(t, srv.URL+pathB)

	c, err := client.NewClient(client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	hashA, boundA, err := hfclient.ResolveDownload(ctx, nil, srv.URL+pathA)
	if err != nil {
		t.Fatal(err)
	}
	hashB, _, err := hfclient.ResolveDownload(ctx, nil, srv.URL+pathB)
	if err != nil {
		t.Fatal(err)
	}
	assertDownloadAs(t, c.DownloadFileWithAuthProvider, boundA, hashA, fileA)
	_, err = downloadAs(t, c.DownloadFileWithAuthProvider, boundA, hashB)
	wantErr(t, err, "status 403")
	_, err = c.UploadFileWithAuthProvider(ctx, boundA, bytes.NewReader(fileB))
	wantErr(t, err, "status 403")

	target := hfclient.Target{Endpoint: srv.URL, RepoType: "model", RepoID: "org/repo", Revision: "main"}
	repoRead := hfclient.NewReadTokenProvider(nil, target, "")
	assertDownloadAs(t, c.DownloadFileWithAuthProvider, repoRead, hashA, fileA)
	assertDownloadAs(t, c.DownloadFileWithAuthProvider, repoRead, hashB, fileB)

	repoWrite := hfclient.NewWriteTokenProvider(nil, target, "hf-user-token")
	if token, err := repoWrite.Token(ctx); err != nil || token != "upstream-write-token" {
		t.Fatalf("write token = %q, %v, want the upstream's", token, err)
	}
	if base, err := repoWrite.BaseURL(ctx); err != nil || base != "https://cas.upstream.example" {
		t.Fatalf("write CAS URL = %q, %v, want the upstream's", base, err)
	}

	assertStatus(t, http.MethodGet, srv.URL+"/xet-token/"+(xet.FileHash{}).String(), "", http.StatusBadRequest)
	assertStatus(t, http.MethodGet, srv.URL+"/xet-token/nothex", "", http.StatusBadRequest)
}
