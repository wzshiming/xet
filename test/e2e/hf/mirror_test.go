package e2e_test

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
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/hf/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

// mirrorEnv holds a complete mirror setup: upstream, mirror server and storage.
type mirrorEnv struct {
	upstreamRequests       atomic.Int32
	xetAuthRequests        atomic.Int32
	reconstructionRequests atomic.Int32

	upstreamServer *httptest.Server
	mirrorServer   *httptest.Server
	mirrorHandler  *mirror.Handler
	storage        storage.Storage
}

func (e *mirrorEnv) close() {
	e.mirrorServer.Close()
	e.mirrorHandler.Close()
	e.upstreamServer.Close()
}

// resolveURL builds the HF resolve URL for the mirror server.
func (e *mirrorEnv) resolveURL(path string) string {
	return e.mirrorServer.URL + path
}

// newHTTPMirrorEnv creates an upstream plain-HTTP server exposing a single
// file at every resolve URL, plus a mirror server backed by a fresh storage.
func newHTTPMirrorEnv(t *testing.T, data []byte, etag string) *mirrorEnv {
	t.Helper()
	env := &mirrorEnv{}

	env.upstreamServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		env.upstreamRequests.Add(1)
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Content-Type", "application/octet-stream")
			if etag != "" {
				w.Header().Set("ETag", etag)
			}
		case http.MethodGet:
			rangeHdr := r.Header.Get("Range")
			if rangeHdr != "" {
				start := parseRangeStart(rangeHdr)
				if start < 0 || start >= int64(len(data)) {
					http.Error(w, "bad range", http.StatusRequestedRangeNotSatisfiable)
					return
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(data)-1, len(data)))
				w.Header().Set("Content-Length", strconv.Itoa(int(int64(len(data))-start)))
				w.Header().Set("Content-Type", "application/octet-stream")
				if etag != "" {
					w.Header().Set("ETag", etag)
				}
				w.WriteHeader(http.StatusPartialContent)
				w.Write(data[start:])
				return
			}
			http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(data))
		}
	}))

	storagePath := t.TempDir()
	var err error
	env.storage, err = storage.NewFileStorage(storage.WithBasePath(storagePath))
	if err != nil {
		t.Fatal(err)
	}

	xetHandler := server.NewHandler(server.WithStorage(env.storage))
	env.mirrorHandler, err = mirror.NewHandler(
		mirror.WithStorage(env.storage),
		mirror.WithNext(xetHandler),
		mirror.WithUpstream(env.upstreamServer.URL),
		mirror.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}
	env.mirrorServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/.xet-mirror/auth":
			env.xetAuthRequests.Add(1)
		case r.URL.Path == "/reconstructions",
			strings.HasPrefix(r.URL.Path, "/v1/reconstructions/"),
			strings.HasPrefix(r.URL.Path, "/v2/reconstructions/"):
			env.reconstructionRequests.Add(1)
		}
		env.mirrorHandler.ServeHTTP(w, r)
	}))
	return env
}

// waitForCached polls the resolve URL until the mirror returns 302 (file fully
// cached and converted to XET storage).
func waitForCached(t *testing.T, resolveURL string) {
	t.Helper()
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := noRedirect.Head(resolveURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusFound {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("mirror did not cache file within 15 s")
}

// getBody performs a GET and returns the full response body, failing on
// non-200 status or any read error.
func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	return body
}

// parseRangeStart extracts the start byte from "bytes=N-" range header.
func parseRangeStart(rangeHdr string) int64 {
	var start int64
	fmt.Sscanf(rangeHdr, "bytes=%d-", &start)
	return start
}

// --- Tests ---

// TestHFMirror_HTTPUpstreamStreamsThenCaches verifies that:
//   - A plain HTTP upstream file is streamed to the first requester while
//     the mirror downloads it in the background.
//   - After caching the mirror returns the file without hitting the upstream.
//   - The xet-bridge endpoint serves the cached content correctly.
//   - Clients never need to know the upstream address.
func TestHFMirror_HTTPUpstreamStreamsThenCaches(t *testing.T) {
	data := bytes.Repeat([]byte("hf-mirror-http-e2e-"), 8192) // ~155 KB

	env := newHTTPMirrorEnv(t, data, `"v1"`)
	defer env.close()

	resolveURL := env.resolveURL("/org/repo/resolve/main/model.bin")

	// First request: mirror streams while downloading from upstream.
	body := getBody(t, resolveURL)
	if !bytes.Equal(body, data) {
		t.Fatalf("first response: got %d bytes, want %d", len(body), len(data))
	}
	firstUpstreamCount := env.upstreamRequests.Load()
	if firstUpstreamCount == 0 {
		t.Fatal("upstream was never contacted for the first request")
	}

	// Wait for the mirror to finish caching.
	waitForCached(t, resolveURL)
	cachedUpstreamCount := env.upstreamRequests.Load()

	// Second request: served from local cache, no new upstream contact.
	body2 := getBody(t, resolveURL)
	if !bytes.Equal(body2, data) {
		t.Fatalf("cached response: got %d bytes, want %d", len(body2), len(data))
	}
	if env.upstreamRequests.Load() != cachedUpstreamCount {
		t.Fatal("cached response contacted the upstream (should be served locally)")
	}

	// Third request with a range: still served locally.
	start, end := int64(1000), int64(2000)
	rangeReq, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	rangeReq.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatal(err)
	}
	defer rangeResp.Body.Close()
	rangeBody, _ := io.ReadAll(rangeResp.Body)
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206", rangeResp.StatusCode)
	}
	if !bytes.Equal(rangeBody, data[start:end+1]) {
		t.Fatalf("range body mismatch: got %d bytes, want %d", len(rangeBody), end-start+1)
	}
	if env.upstreamRequests.Load() != cachedUpstreamCount {
		t.Fatal("range request contacted the upstream (should be served locally)")
	}

	// The xet-bridge endpoint must also serve the file correctly.
	digest := sha256.Sum256(data)
	bridgeURL := env.mirrorServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
	bridgeBody := getBody(t, bridgeURL)
	if !bytes.Equal(bridgeBody, data) {
		t.Fatalf("bridge body mismatch: got %d bytes, want %d", len(bridgeBody), len(data))
	}
}

// TestHFMirror_XETUpstreamUsesXETProtocol verifies that:
//   - When the upstream exposes XET headers the mirror uses the XET client
//     for downloading, never issuing a plain HTTP GET for the file body.
//   - After caching the mirror serves the file via its local XET storage.
//   - The client never contacts the upstream CAS or hub directly.
func TestHFMirror_XETUpstreamUsesXETProtocol(t *testing.T) {
	upstreamData := bytes.Repeat([]byte("xet-upstream-e2e-data-"), 8192) // ~176 KB

	// Upstream CAS: real XET server holding the upstream file.
	upstreamStoragePath := t.TempDir()
	upstreamStorage, err := storage.NewFileStorage(storage.WithBasePath(upstreamStoragePath))
	if err != nil {
		t.Fatal(err)
	}
	upstreamCAS := httptest.NewServer(server.NewHandler(server.WithStorage(upstreamStorage)))
	defer upstreamCAS.Close()

	// Upload the file to the upstream CAS via the client API.
	uploadClient, err := client.NewClient(
		client.WithBaseURL(upstreamCAS.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}
	fileHash, err := uploadClient.UploadFile(context.Background(), bytes.NewReader(upstreamData))
	if err != nil {
		t.Fatalf("upload to upstream CAS: %v", err)
	}

	// Upstream hub: returns XET headers; any plain GET for file data is an error.
	var upstreamHubURL string
	var upstreamHubGETs atomic.Int32
	upstreamHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/xet-auth" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"casUrl":      upstreamCAS.URL,
				"accessToken": "",
				"exp":         time.Now().Add(time.Hour).Unix(),
			})
			return
		}
		if r.Method == http.MethodGet {
			upstreamHubGETs.Add(1)
			http.Error(w, "plain HTTP GET for file data is forbidden on an XET upstream", http.StatusForbidden)
			return
		}
		// HEAD: return XET resolve headers.
		authURL := upstreamHubURL + "/xet-auth"
		reconURL := upstreamCAS.URL + "/v1/reconstructions/" + fileHash.String()
		w.Header().Set("Content-Length", strconv.Itoa(len(upstreamData)))
		w.Header().Set("X-Linked-Size", strconv.Itoa(len(upstreamData)))
		w.Header().Set("X-Xet-Hash", fileHash.String())
		w.Header().Set("Link", fmt.Sprintf(`<%s>; rel="xet-auth", <%s>; rel="xet-reconstruction-info"`, authURL, reconURL))
		w.WriteHeader(http.StatusFound)
	}))
	defer upstreamHub.Close()
	upstreamHubURL = upstreamHub.URL

	// Mirror server backed by its own local storage.
	localStoragePath := t.TempDir()
	localStorage, err := storage.NewFileStorage(storage.WithBasePath(localStoragePath))
	if err != nil {
		t.Fatal(err)
	}
	xetHandler := server.NewHandler(server.WithStorage(localStorage))
	mirrorHandler, err := mirror.NewHandler(
		mirror.WithStorage(localStorage),
		mirror.WithNext(xetHandler),
		mirror.WithUpstream(upstreamHub.URL),
		mirror.WithCacheDir(t.TempDir()),
		mirror.WithConcurrency(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	mirrorServer := httptest.NewServer(mirrorHandler)
	defer mirrorServer.Close()
	defer mirrorHandler.Close()

	resolveURL := mirrorServer.URL + "/org/repo/resolve/main/model.bin"

	// First request: mirror downloads via XET, streams body to client.
	body := getBody(t, resolveURL)
	if !bytes.Equal(body, upstreamData) {
		t.Fatalf("first XET response: got %d bytes, want %d", len(body), len(upstreamData))
	}
	if upstreamHubGETs.Load() != 0 {
		t.Fatal("mirror issued a plain HTTP GET to the XET upstream hub (should use XET protocol)")
	}

	// Wait for the mirror to finish converting to local XET storage.
	waitForCached(t, resolveURL)

	// Second request: served entirely from local mirror, no new upstream contact.
	body2 := getBody(t, resolveURL)
	if !bytes.Equal(body2, upstreamData) {
		t.Fatalf("cached XET response: got %d bytes, want %d", len(body2), len(upstreamData))
	}

	// The bridge endpoint must serve the correct bytes.
	digest := sha256.Sum256(upstreamData)
	bridgeURL := mirrorServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
	bridgeBody := getBody(t, bridgeURL)
	if !bytes.Equal(bridgeBody, upstreamData) {
		t.Fatalf("bridge body mismatch after XET caching")
	}
}

// TestHFMirror_MirroredAndHostedFilesCoexist verifies that files uploaded
// directly to the mirror's XET storage are accessible alongside mirrored
// upstream files, all served through the same mirror endpoint.
func TestHFMirror_MirroredAndHostedFilesCoexist(t *testing.T) {
	mirroredData := bytes.Repeat([]byte("mirrored-from-upstream-"), 4096) // ~92 KB
	hostedData := bytes.Repeat([]byte("directly-hosted-local-"), 3072)    // ~64 KB

	env := newHTTPMirrorEnv(t, mirroredData, `"repo-v1"`)
	defer env.close()

	// Upload the hosted file directly to the mirror's XET server.
	uploadClient, err := client.NewClient(
		client.WithBaseURL(env.mirrorServer.URL),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = uploadClient.UploadFile(context.Background(), bytes.NewReader(hostedData))
	if err != nil {
		t.Fatalf("upload hosted file to mirror: %v", err)
	}

	// Trigger mirror download for the upstream file and wait for it to be
	// converted to local XET storage so the bridge endpoint is ready.
	mirroredBody := getBody(t, env.resolveURL("/org/repo/resolve/main/model.bin"))
	if !bytes.Equal(mirroredBody, mirroredData) {
		t.Fatalf("mirrored file: got %d bytes, want %d", len(mirroredBody), len(mirroredData))
	}
	waitForCached(t, env.resolveURL("/org/repo/resolve/main/model.bin"))

	// Both files must be reachable via the xet-bridge on the same server.
	for name, data := range map[string][]byte{
		"mirrored": mirroredData,
		"hosted":   hostedData,
	} {
		digest := sha256.Sum256(data)
		bridgeURL := env.mirrorServer.URL + "/xet-bridge/" + hex.EncodeToString(digest[:])
		body := getBody(t, bridgeURL)
		if !bytes.Equal(body, data) {
			t.Fatalf("%s: bridge body mismatch (got %d bytes, want %d)", name, len(body), len(data))
		}
	}
}

// TestHFMirror_XETClientSeesLocalURLsOnly verifies the full XET client
// download path through the mirror:
//   - hf.ResolveDownload returns a file hash and auth provider pointing only
//     at the mirror server.
//   - The XET client downloads all chunks from the mirror server.
//   - No request is ever made to the upstream after initial caching.
func TestHFMirror_XETClientSeesLocalURLsOnly(t *testing.T) {
	data := bytes.Repeat([]byte("xet-client-e2e-isolation-"), 8192) // ~200 KB

	env := newHTTPMirrorEnv(t, data, `"sha1-stable"`)
	defer env.close()

	resolveURL := env.resolveURL("/datasets/org/repo/resolve/main/dataset.bin")

	// Trigger caching.
	firstBody := getBody(t, resolveURL)
	if !bytes.Equal(firstBody, data) {
		t.Fatalf("initial response: got %d bytes, want %d", len(firstBody), len(data))
	}
	waitForCached(t, resolveURL)
	upstreamCountAfterCache := env.upstreamRequests.Load()

	// A transport that rejects any request not aimed at the mirror server.
	mirrorURL := env.mirrorServer.URL
	mirrorOnlyTransport := &restrictedTransport{allowedHost: mirrorURL}

	httpClient := &http.Client{
		Transport: mirrorOnlyTransport,
		// Do not auto-follow redirects; hf.ResolveDownload needs to inspect 302.
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	// Resolve the file via the mirror using the XET protocol.
	fileHash, provider, err := hf.ResolveDownload(context.Background(), httpClient, resolveURL)
	if err != nil {
		t.Fatalf("ResolveDownload: %v", err)
	}

	// Download the file via the XET client, restricted to mirror-only traffic.
	xetClient, err := client.NewClient(
		client.WithHTTPClient(&http.Client{Transport: mirrorOnlyTransport}),
		client.WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := xetClient.DownloadFileWithAuthProvider(context.Background(), provider, fileHash, &seekableBuffer{Buffer: &buf}); err != nil {
		t.Fatalf("XET download: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("XET download: got %d bytes, want %d", buf.Len(), len(data))
	}

	// The upstream must not have been contacted after the initial cache was built.
	if env.upstreamRequests.Load() != upstreamCountAfterCache {
		t.Fatal("XET client-phase requests reached the upstream (should be served from local mirror)")
	}
}

// TestHFMirror_HFCLICompatibility exercises the mirror through Hugging Face's
// official hf CLI and its bundled hf-xet client. The first download populates
// the mirror; the second uses a fresh client cache and must be served through
// the mirror's local XET endpoints without contacting the upstream.
func TestHFMirror_HFCLICompatibility(t *testing.T) {
	hfPath, err := exec.LookPath("hf")
	if err != nil {
		if os.Getenv("XET_HF_CLI_REQUIRED") == "1" {
			t.Fatal("hf CLI is required but was not found in PATH")
		}
		t.Skip("hf CLI is not installed")
	}

	data := bytes.Repeat([]byte("official-hf-cli-mirror-compatibility-"), 8192)
	env := newHTTPMirrorEnv(t, data, `"hf-cli-v1"`)
	defer env.close()

	const (
		repoID   = "org/repo"
		filename = "model.bin"
	)
	resolveURL := env.resolveURL("/" + repoID + "/resolve/main/" + filename)

	firstDir := t.TempDir()
	runHFDownload(t, hfPath, env.mirrorServer.URL, repoID, filename, firstDir)
	assertFileContents(t, filepath.Join(firstDir, filename), data)
	waitForCached(t, resolveURL)

	upstreamAfterCache := env.upstreamRequests.Load()
	authBefore := env.xetAuthRequests.Load()
	reconstructionBefore := env.reconstructionRequests.Load()

	// Use a separate HF_HOME and destination so the CLI cannot reuse its own
	// file or XET caches from the first download.
	secondDir := t.TempDir()
	runHFDownload(t, hfPath, env.mirrorServer.URL, repoID, filename, secondDir)
	assertFileContents(t, filepath.Join(secondDir, filename), data)

	if got := env.upstreamRequests.Load(); got != upstreamAfterCache {
		t.Fatalf("cached hf download contacted upstream: requests = %d, want %d", got, upstreamAfterCache)
	}
	if env.xetAuthRequests.Load() <= authBefore {
		t.Fatal("cached hf download did not use the mirror's xet-auth endpoint")
	}
	if env.reconstructionRequests.Load() <= reconstructionBefore {
		t.Fatal("cached hf download did not use the mirror's XET reconstruction endpoint")
	}
}

// --- Helpers ---

func runHFDownload(t *testing.T, hfPath, endpoint, repoID, filename, destination string) {
	t.Helper()
	hfHome := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, hfPath, "download", repoID, filename, "--local-dir", destination, "--quiet")
	cmd.Env = append(os.Environ(),
		"HF_ENDPOINT="+endpoint,
		"HF_HOME="+hfHome,
		"HF_HUB_CACHE="+filepath.Join(hfHome, "hub"),
		"HF_XET_CACHE="+filepath.Join(hfHome, "xet"),
		"HF_HUB_DISABLE_IMPLICIT_TOKEN=1",
		"HF_HUB_DISABLE_PROGRESS_BARS=1",
		"HF_HUB_DISABLE_TELEMETRY=1",
		"HF_HUB_DISABLE_UPDATE_CHECK=1",
		"HF_HUB_DISABLE_XET=0",
		"HF_HUB_OFFLINE=0",
		"HF_TOKEN=",
		"NO_COLOR=1",
		"ALL_PROXY=",
		"HTTPS_PROXY=",
		"HTTP_PROXY=",
		"NO_PROXY=127.0.0.1,localhost",
		"all_proxy=",
		"https_proxy=",
		"http_proxy=",
		"no_proxy=127.0.0.1,localhost",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("hf download failed: %v\n%s", err, output)
	}
}

func assertFileContents(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read downloaded file %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("downloaded file %s: got %d bytes, want %d", path, len(got), len(want))
	}
}

// restrictedTransport is an http.RoundTripper that only allows requests to
// a single host, failing on any other destination.
type restrictedTransport struct {
	allowedHost string
}

func (rt *restrictedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	want := req.URL.Scheme + "://" + req.URL.Host
	if want != rt.allowedHost {
		return nil, fmt.Errorf("client made request to non-mirror host %q (only %q is allowed)", want, rt.allowedHost)
	}
	return http.DefaultTransport.RoundTrip(req)
}

// seekableBuffer wraps bytes.Buffer to implement io.WriteSeeker for the XET
// client download API (which needs to seek to determine the resume offset).
type seekableBuffer struct {
	*bytes.Buffer
	pos int64
}

func (b *seekableBuffer) Write(p []byte) (int, error) {
	n, err := b.Buffer.Write(p)
	b.pos += int64(n)
	return n, err
}

func (b *seekableBuffer) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		b.pos = offset
	case io.SeekCurrent:
		b.pos += offset
	case io.SeekEnd:
		b.pos = int64(b.Buffer.Len()) + offset
	}
	return b.pos, nil
}
