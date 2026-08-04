package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// TestXetBridgeUploadRestartAndDownload exercises the public HTTP upload path,
// persists the resulting SHA-256 index, restarts the server, and then verifies
// the bridge's complete-file, HEAD, conditional, and byte-range responses.
func TestXetBridgeUploadRestartAndDownload(t *testing.T) {
	storagePath := t.TempDir()
	uploadStorage, err := storage.NewFileStorage(storage.WithBasePath(storagePath))
	if err != nil {
		t.Fatal(err)
	}
	uploadServer := httptest.NewServer(server.NewHandler(server.WithStorage(uploadStorage)))

	uploadClient, err := client.NewClient(
		client.WithBaseURL(uploadServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	files := []struct {
		name string
		data []byte
	}{
		// Note: empty files are not supported because they have no chunks,
		// so no SHA-256 hash is computed or stored.
		{name: "small", data: []byte("Hello, World!")},
		{name: "single-chunk", data: deterministicData(128*1024 - 1)},
		{name: "multi-chunk", data: deterministicData(3*128*1024 + 7919)},
		{name: "large", data: deterministicData(10 * 1024 * 1024)},
		{name: "repeating", data: bytes.Repeat([]byte{0xAB}, 256*1024)},
	}
	for i := range files {
		if _, err := uploadClient.UploadFile(context.Background(), bytes.NewReader(files[i].data)); err != nil {
			t.Fatalf("upload %s: %v", files[i].name, err)
		}
	}
	uploadServer.Close()

	// Reopen storage so bridge lookup cannot be satisfied by an in-memory cache.
	downloadStorage, err := storage.NewFileStorage(storage.WithBasePath(storagePath))
	if err != nil {
		t.Fatal(err)
	}
	downloadServer := httptest.NewServer(server.NewHandler(server.WithStorage(downloadStorage)))
	defer downloadServer.Close()

	for _, file := range files {
		t.Run(file.name, func(t *testing.T) {
			digest := sha256.Sum256(file.data)
			url := downloadServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
			etag := `"` + hex.EncodeToString(digest[:]) + `"`

			// Test GET full file
			resp := doRequest(t, http.MethodGet, url, nil)
			assertResponse(t, resp, http.StatusOK, file.data)
			if got := resp.Header.Get("Content-Type"); got != "application/octet-stream" {
				t.Fatalf("Content-Type = %q, want %q", got, "application/octet-stream")
			}
			if got := resp.Header.Get("ETag"); got != etag {
				t.Fatalf("ETag = %q, want %q", got, etag)
			}
			// Strict validation: Content-Length should match
			if resp.ContentLength != int64(len(file.data)) {
				t.Fatalf("GET Content-Length = %d, want %d", resp.ContentLength, len(file.data))
			}
			// Accept-Ranges should be present for non-empty files
			if len(file.data) > 0 {
				if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
					t.Fatalf("Accept-Ranges = %q, want %q", got, "bytes")
				}
			}

			// Test HEAD request
			resp = doRequest(t, http.MethodHead, url, nil)
			assertResponse(t, resp, http.StatusOK, nil)
			if resp.ContentLength != int64(len(file.data)) {
				t.Fatalf("HEAD Content-Length = %d, want %d", resp.ContentLength, len(file.data))
			}
			if got := resp.Header.Get("ETag"); got != etag {
				t.Fatalf("HEAD ETag = %q, want %q", got, etag)
			}
			if len(file.data) > 0 {
				if got := resp.Header.Get("Accept-Ranges"); got != "bytes" {
					t.Fatalf("HEAD Accept-Ranges = %q, want %q", got, "bytes")
				}
			}

			// Test If-None-Match with matching ETag (304 Not Modified)
			resp = doRequest(t, http.MethodGet, url, http.Header{"If-None-Match": []string{etag}})
			assertResponse(t, resp, http.StatusNotModified, nil)

			// Test If-None-Match with non-matching ETag (200 OK)
			resp = doRequest(t, http.MethodGet, url, http.Header{"If-None-Match": []string{`"nonexistent"`}})
			assertResponse(t, resp, http.StatusOK, file.data)

			// Test If-Modified-Since with old timestamp (200 OK)
			oldDate := "Mon, 01 Jan 2024 00:00:00 GMT"
			resp = doRequest(t, http.MethodGet, url, http.Header{"If-Modified-Since": []string{oldDate}})
			assertResponse(t, resp, http.StatusOK, file.data)

			// Test byte range requests for non-empty files
			if len(file.data) != 0 {
				// Range: start to end (inclusive)
				start, end := 128*1024-17, 128*1024+29
				if end >= len(file.data) {
					start, end = 0, len(file.data)/2-1
				}
				headers := http.Header{"Range": []string{fmt.Sprintf("bytes=%d-%d", start, end)}}
				resp = doRequest(t, http.MethodGet, url, headers)
				expectedBody := file.data[start : end+1]
				assertResponse(t, resp, http.StatusPartialContent, expectedBody)
				if got, want := resp.Header.Get("Content-Range"), fmt.Sprintf("bytes %d-%d/%d", start, end, len(file.data)); got != want {
					t.Fatalf("Content-Range = %q, want %q", got, want)
				}
				// Content-Length in partial response should match actual body length
				if resp.ContentLength != int64(len(expectedBody)) {
					t.Fatalf("Partial Content-Length = %d, want %d", resp.ContentLength, len(expectedBody))
				}

				// Range from start to end of file
				headers = http.Header{"Range": []string{fmt.Sprintf("bytes=%d-", start)}}
				resp = doRequest(t, http.MethodGet, url, headers)
				expectedBody = file.data[start:]
				assertResponse(t, resp, http.StatusPartialContent, expectedBody)

				// Range: suffix - last N bytes
				suffixLen := 100
				if suffixLen < len(file.data) {
					headers = http.Header{"Range": []string{fmt.Sprintf("bytes=-%d", suffixLen)}}
					resp = doRequest(t, http.MethodGet, url, headers)
					expectedBody = file.data[len(file.data)-suffixLen:]
					assertResponse(t, resp, http.StatusPartialContent, expectedBody)
				}
			}
		})
	}
}

// TestXetBridgeNotFound tests that the bridge returns 404 for non-existent files
func TestXetBridgeNotFound(t *testing.T) {
	storagePath := t.TempDir()
	downloadStorage, err := storage.NewFileStorage(storage.WithBasePath(storagePath))
	if err != nil {
		t.Fatal(err)
	}
	downloadServer := httptest.NewServer(server.NewHandler(server.WithStorage(downloadStorage)))
	defer downloadServer.Close()

	// Non-existent SHA-256
	nonexistentHash := "0000000000000000000000000000000000000000000000000000000000000000"
	url := downloadServer.URL + "/xet-bridge/" + nonexistentHash

	resp := doRequest(t, http.MethodGet, url, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}

	resp = doRequest(t, http.MethodHead, url, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HEAD status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}

// TestXetBridgeInvalidHash tests that the bridge returns 400 for invalid SHA-256 hashes
func TestXetBridgeInvalidHash(t *testing.T) {
	storagePath := t.TempDir()
	downloadStorage, err := storage.NewFileStorage(storage.WithBasePath(storagePath))
	if err != nil {
		t.Fatal(err)
	}
	downloadServer := httptest.NewServer(server.NewHandler(server.WithStorage(downloadStorage)))
	defer downloadServer.Close()

	testCases := []struct {
		name string
		hash string
	}{
		{name: "too-short", hash: "abc"},
		{name: "too-long", hash: "000000000000000000000000000000000000000000000000000000000000000000"},
		{name: "invalid-chars", hash: "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"},
		{name: "odd-length", hash: "000000000000000000000000000000000000000000000000000000000000000"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			url := downloadServer.URL + "/xet-bridge/" + tc.hash
			resp := doRequest(t, http.MethodGet, url, nil)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
		})
	}
}

func deterministicData(size int) []byte {
	data := make([]byte, size)
	var state uint32 = 1
	for i := range data {
		state = state*1664525 + 1013904223
		data[i] = byte(state >> 24)
	}
	return data
}

func doRequest(t *testing.T, method, url string, headers http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header = headers
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func assertResponse(t *testing.T, resp *http.Response, wantStatus int, wantBody []byte) {
	t.Helper()
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d; body = %q", resp.StatusCode, wantStatus, body)
	}
	if !bytes.Equal(body, wantBody) {
		t.Fatalf("body length = %d, want %d", len(body), len(wantBody))
	}
}
