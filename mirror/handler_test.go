package mirror

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

func TestCachedResolveUsesLocalLinks(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", `<https://cas.example/reconstructions/upstream>; rel="xet-reconstruction-info"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	s, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: t.TempDir(), Storage: s})
	if err != nil {
		t.Fatal(err)
	}
	h.index["/owner/repo/resolve/main/file"] = strings.Repeat("a", 64)
	server := httptest.NewServer(h)
	defer server.Close()

	resp, err := http.Head(server.URL + "/owner/repo/resolve/main/file")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("X-Cache-Status"); got != "HIT" {
		t.Fatalf("cache status = %q", got)
	}
	if link := resp.Header.Get("Link"); !strings.Contains(link, server.URL+"/v1/reconstructions/"+strings.Repeat("a", 64)) || !strings.Contains(link, server.URL+"/api/xet-auth") {
		t.Fatalf("unexpected Link: %s", link)
	}
}

func TestPromotedCacheServesHeadAndGetWithoutUpstream(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	root := t.TempDir()
	store, err := storage.NewFileStorage(storage.WithBasePath(root))
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(root, "mirror")
	h, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: cacheDir, Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	path := "/owner/repo/resolve/main/offline.bin"
	want := []byte(strings.Repeat("durable xet mirror cache\n", 1000))
	raw := filepath.Join(t.TempDir(), "offline.bin")
	if err := os.WriteFile(raw, want, 0644); err != nil {
		t.Fatal(err)
	}
	metadata := http.Header{}
	metadata.Set("Content-Type", "application/octet-stream")
	metadata.Set("ETag", `"origin-etag"`)
	metadata.Set("X-Repo-Commit", "0123456789abcdef0123456789abcdef01234567")
	h.rememberMetadata(path, metadata)
	if err := h.convertFile(context.Background(), path, raw); err != nil {
		t.Fatal(err)
	}
	upstream.Close()

	// Recreate the handler to prove that both the index and resolve metadata
	// survive a process restart and no request needs the origin.
	restarted, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: cacheDir, Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(restarted)
	defer server.Close()

	head, err := http.Head(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	head.Body.Close()
	if head.StatusCode != http.StatusOK || head.Header.Get("X-Cache-Status") != "HIT" {
		t.Fatalf("offline HEAD status=%d cache=%q", head.StatusCode, head.Header.Get("X-Cache-Status"))
	}
	if head.Header.Get("ETag") != `"origin-etag"` || head.Header.Get("X-Repo-Commit") == "" {
		t.Fatalf("offline HEAD lost metadata: %v", head.Header)
	}
	if !strings.Contains(head.Header.Get("Link"), server.URL+"/v1/reconstructions/") {
		t.Fatalf("offline HEAD did not advertise local Xet: %s", head.Header.Get("Link"))
	}

	get, err := http.Get(server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(get.Body)
	get.Body.Close()
	if readErr != nil || !bytes.Equal(got, want) {
		t.Fatalf("offline GET failed: bytes=%d err=%v", len(got), readErr)
	}
}

func TestColdResolveFollowsRedirectInsideMirror(t *testing.T) {
	want := []byte(strings.Repeat("redirected origin body\n", 1000))
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(want)))
		_, _ = w.Write(want)
	}))
	defer cdn.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, cdn.URL+"/objects/file.bin", http.StatusFound)
	}))
	defer origin.Close()
	root := t.TempDir()
	store, err := storage.NewFileStorage(storage.WithBasePath(root))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{Upstream: origin.URL, CacheDir: filepath.Join(root, "mirror"), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	mirrorServer := httptest.NewServer(h)
	defer mirrorServer.Close()
	path := "/owner/repo/resolve/main/redirect.bin"

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return fmt.Errorf("mirror leaked a redirect")
	}}
	resp, err := client.Get(mirrorServer.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("cold redirected GET failed: bytes=%d err=%v", len(got), err)
	}
	if resp.Request.URL.Host != strings.TrimPrefix(mirrorServer.URL, "http://") {
		t.Fatalf("client left mirror: %s", resp.Request.URL)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.RLock()
		cached := h.index[path] != ""
		h.mu.RUnlock()
		if cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("redirected body was not promoted under the resolve path")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCacheFileUsesStableSHA256NameAndSurvivesInterruption(t *testing.T) {
	upstream := httptest.NewServer(http.NotFoundHandler())
	defer upstream.Close()
	root := t.TempDir()
	store, err := storage.NewFileStorage(storage.WithBasePath(root))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: filepath.Join(root, "mirror"), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	key := "/owner/repo/resolve/main/resumable.bin"
	content := []byte(strings.Repeat("resumable mirror body\n", 10_000))

	first, ok := h.startHTTPBodyCache(key, io.NopCloser(bytes.NewReader(content)))
	if !ok {
		t.Fatal("first cache fill did not start")
	}
	prefix := make([]byte, 4096)
	if _, err := io.ReadFull(first, prefix); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	cachePath := h.cacheFilePath(key)
	if filepath.Ext(cachePath) != "" || len(filepath.Base(cachePath)) != 64 {
		t.Fatalf("cache path is not a bare SHA-256: %s", cachePath)
	}
	info, err := os.Stat(cachePath)
	if err != nil || info.Size() != int64(len(prefix)) {
		t.Fatalf("interrupted cache was not retained: info=%v err=%v", info, err)
	}

	second, ok := h.startHTTPBodyCache(key, io.NopCloser(bytes.NewReader(content)))
	if !ok {
		t.Fatal("resumed cache fill did not start")
	}
	if _, err := io.Copy(io.Discard, second); err != nil {
		t.Fatal(err)
	}
	second.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.RLock()
		cached := h.index[key] != ""
		h.mu.RUnlock()
		if cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resumed cache was not converted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("completed cache file was not removed: %v", err)
	}
}

func TestRemovingHalfDownloadedClientFileKeepsMirrorProgress(t *testing.T) {
	content := []byte(strings.Repeat("client cache removal must not remove mirror progress\n", 20_000))
	const firstPart = 64 * 1024
	release := make(chan struct{})
	var resumedAt atomic.Int64
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if value := r.Header.Get("Range"); value != "" {
			var start int64
			if _, err := fmt.Sscanf(value, "bytes=%d-", &start); err != nil {
				http.Error(w, "bad range", http.StatusBadRequest)
				return
			}
			resumedAt.Store(start)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, len(content)-1, len(content)))
			w.Header().Set("Content-Length", fmt.Sprint(int64(len(content))-start))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[start:])
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		_, _ = w.Write(content[:firstPart])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
		_, _ = w.Write(content[firstPart:])
	}))
	defer upstream.Close()
	root := t.TempDir()
	store, err := storage.NewFileStorage(storage.WithBasePath(root))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: filepath.Join(root, "mirror"), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	mirrorServer := httptest.NewServer(h)
	defer mirrorServer.Close()
	path := "/repo/resolve/main/large.bin"

	resp, err := http.Get(mirrorServer.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	clientFile, err := os.CreateTemp(t.TempDir(), "client-partial-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.CopyN(clientFile, resp.Body, firstPart/2); err != nil {
		t.Fatal(err)
	}
	clientFile.Close()
	resp.Body.Close()
	if err := os.Remove(clientFile.Name()); err != nil {
		t.Fatal(err)
	}

	cachePath := h.cacheFilePath(path)
	deadline := time.Now().Add(5 * time.Second)
	for {
		h.mu.RLock()
		_, running := h.inflight[path]
		h.mu.RUnlock()
		if !running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("mirror did not release interrupted fill")
		}
		time.Sleep(10 * time.Millisecond)
	}
	info, err := os.Stat(cachePath)
	if err != nil || info.Size() == 0 {
		t.Fatalf("mirror partial cache disappeared with client cache: info=%v err=%v", info, err)
	}
	retainedSize := info.Size()

	close(release)
	retry, err := http.Get(mirrorServer.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(retry.Body)
	retry.Body.Close()
	if err != nil || string(got) != string(content) {
		t.Fatalf("retry after clearing client cache failed: %v", err)
	}
	if resumedAt.Load() != retainedSize {
		t.Fatalf("origin resumed at %d, want retained mirror size %d", resumedAt.Load(), retainedSize)
	}
	if retry.Header.Get("X-Cache-Status") != "RESUME" {
		t.Fatalf("retry cache status = %q, want RESUME", retry.Header.Get("X-Cache-Status"))
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		h.mu.RLock()
		cached := h.index[path] != ""
		h.mu.RUnlock()
		if cached {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retried file was not promoted to Xet cache")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
		t.Fatalf("raw progress should be removed after Xet promotion: %v", err)
	}
}

// TestE2EMirrorCompatibilityMatrix covers every upstream/downstream protocol
// combination supported by the mirror.
func TestE2EMirrorCompatibilityMatrix(t *testing.T) {
	t.Run("xet_upstream_http_and_xet_downstreams", testXetUpstream)
	t.Run("http_upstream_http_and_xet_downstreams", testHTTPUpstream)
}

// testXetUpstream exercises an HF-style Xet origin. Only HTTP works during the
// cold fill; after promotion both protocols share the local shard/xorbs.
func testXetUpstream(t *testing.T) {
	content := []byte(strings.Repeat("shared xet and http cache\n", 20_000))
	originFS, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	originStore := &baseURLStorage{Storage: originFS}
	originHash, err := upload.UploadFile(context.Background(), localAdapter{originStore}, strings.NewReader(string(content)))
	if err != nil {
		t.Fatal(err)
	}

	var originURL string
	originCAS := server.NewHandler(server.WithStorage(originStore))
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repo/resolve/main/file.bin":
			w.Header().Set("Link", "<"+originURL+"/v1/reconstructions/"+originHash.String()+">; rel=\"xet-reconstruction-info\", <"+originURL+"/auth>; rel=\"xet-auth\"")
			// Deliberately return different HTTP bytes. A cold mirror with Xet
			// metadata must reconstruct from the origin CAS, while still exposing
			// those reconstructed bytes to its downstream as ordinary HTTP.
			_, _ = w.Write([]byte("HTTP fallback must not be selected"))
		case "/auth":
			_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": originURL, "accessToken": "", "exp": time.Now().Add(time.Hour).Unix()})
		default:
			originCAS.ServeHTTP(w, r)
		}
	}))
	defer origin.Close()
	originURL = origin.URL
	originStore.baseURL = originURL

	mirrorRoot := t.TempDir()
	mirrorFS, err := storage.NewFileStorage(storage.WithBasePath(mirrorRoot))
	if err != nil {
		t.Fatal(err)
	}
	mirrorStore := &baseURLStorage{Storage: mirrorFS}
	mirrorProxy, err := NewHandler(Options{Upstream: originURL, CacheDir: filepath.Join(mirrorRoot, "mirror"), Storage: mirrorStore})
	if err != nil {
		t.Fatal(err)
	}
	mirrorCAS := server.NewHandler(server.WithStorage(mirrorStore), server.WithNext(mirrorProxy))
	mirrorServer := httptest.NewServer(mirrorCAS)
	defer mirrorServer.Close()
	mirrorStore.baseURL = mirrorServer.URL
	resolveURL := mirrorServer.URL + "/repo/resolve/main/file.bin"

	// A cold Xet-capable origin is deliberately exposed as HTTP-only.
	incomplete, err := http.Head(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	incomplete.Body.Close()
	if incomplete.Header.Get("Link") != "" || incomplete.Header.Get("X-Xet-Hash") != "" {
		t.Fatal("cold mirror advertised Xet before conversion completed")
	}
	if _, _, err := hf.ResolveDownload(context.Background(), nil, resolveURL+"?xet=cold"); err == nil {
		t.Fatal("cold Xet resolve unexpectedly succeeded")
	}
	coldHTTP, err := http.Get(resolveURL + "?download=1")
	if err != nil {
		t.Fatal(err)
	}
	coldData, err := io.ReadAll(coldHTTP.Body)
	coldHTTP.Body.Close()
	if err != nil || string(coldData) != string(content) {
		t.Fatalf("cold HTTP download failed: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Head(resolveURL)
		if err == nil && resp.Header.Get("X-Cache-Status") == "HIT" {
			resp.Body.Close()
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("mirror did not finish caching")
		}
		time.Sleep(20 * time.Millisecond)
	}

	ordinary, err := http.Get(resolveURL + "?download=1")
	if err != nil {
		t.Fatal(err)
	}
	ordinaryData, err := io.ReadAll(ordinary.Body)
	ordinary.Body.Close()
	if err != nil || string(ordinaryData) != string(content) {
		t.Fatalf("ordinary HTTP download mismatch: %v", err)
	}
	rangeRequest, _ := http.NewRequest(http.MethodGet, resolveURL, nil)
	rangeRequest.Header.Set("Range", "bytes=17-1234")
	rangeResponse, err := http.DefaultClient.Do(rangeRequest)
	if err != nil {
		t.Fatal(err)
	}
	rangeData, err := io.ReadAll(rangeResponse.Body)
	rangeResponse.Body.Close()
	if err != nil || rangeResponse.StatusCode != http.StatusPartialContent || string(rangeData) != string(content[17:1235]) {
		diff := -1
		for i := range min(len(rangeData), len(content[17:1235])) {
			if rangeData[i] != content[17+i] {
				diff = i
				break
			}
		}
		t.Fatalf("ordinary HTTP range download mismatch: status=%d len=%d want=%d first-diff=%d err=%v", rangeResponse.StatusCode, len(rangeData), len(content[17:1235]), diff, err)
	}

	localHash, provider, err := hf.ResolveDownload(context.Background(), nil, resolveURL+"?xet=1")
	if err != nil {
		t.Fatal(err)
	}
	xetClient, _ := client.NewClient()
	out, err := os.CreateTemp(t.TempDir(), "xet-output")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := xetClient.DownloadFileWithAuthProvider(context.Background(), provider, localHash, out); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	xetData, _ := io.ReadAll(out)
	if string(xetData) != string(content) {
		t.Fatal("Xet download mismatch")
	}
	if localHash != originHash {
		t.Fatalf("local hash %s differs from content hash %s", localHash, originHash)
	}

	// Persistent data consists only of the shared Xet representation and its
	// mapping; there is no second raw-file cache.
	if entries, _ := os.ReadDir(filepath.Join(mirrorRoot, "mirror", "files")); len(entries) != 0 {
		t.Fatalf("completed raw cache files remain: %v", entries)
	}
	if _, err := os.Stat(filepath.Join(mirrorRoot, "mirror", "index.json")); err != nil {
		t.Fatal("missing shared cache index")
	}
}

// testHTTPUpstream exercises a ModelScope-style origin with no Xet metadata.
// HTTP streams while cold, Xet is withheld until conversion, and both work warm.
func testHTTPUpstream(t *testing.T) {
	content := []byte(strings.Repeat("modelscope-compatible-body\n", 10_000))
	const firstPart = 64 * 1024
	release := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", fmt.Sprint(len(content)))
			return // This upstream deliberately has no Xet Link headers.
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		_, _ = w.Write(content[:firstPart])
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-release
		_, _ = w.Write(content[firstPart:])
	}))
	defer upstream.Close()

	root := t.TempDir()
	fs, err := storage.NewFileStorage(storage.WithBasePath(root))
	if err != nil {
		t.Fatal(err)
	}
	store := &baseURLStorage{Storage: fs}
	proxy, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: filepath.Join(root, "mirror"), Storage: store})
	if err != nil {
		t.Fatal(err)
	}
	combined := server.NewHandler(server.WithStorage(store), server.WithNext(proxy))
	mirrorServer := httptest.NewServer(combined)
	defer mirrorServer.Close()
	store.baseURL = mirrorServer.URL
	fileURL := mirrorServer.URL + "/owner/repo/file.bin"

	resp, err := http.Get(fileURL)
	if err != nil {
		t.Fatal(err)
	}
	first := make([]byte, firstPart)
	if _, err := io.ReadFull(resp.Body, first); err != nil {
		t.Fatal(err)
	}
	if string(first) != string(content[:firstPart]) {
		t.Fatal("first streamed bytes mismatch")
	}
	if resp.Header.Get("Link") != "" {
		t.Fatal("Xet was advertised before the non-Xet body was cached")
	}

	before, err := http.Head(fileURL)
	if err != nil {
		t.Fatal(err)
	}
	before.Body.Close()
	if before.Header.Get("Link") != "" {
		t.Fatal("HEAD advertised Xet while conversion was incomplete")
	}
	if _, _, err := hf.ResolveDownload(context.Background(), nil, fileURL); err == nil {
		t.Fatal("Xet downstream unexpectedly resolved before HTTP upstream conversion completed")
	}

	close(release)
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(append(first, got...)) != string(content) {
		t.Fatalf("streamed upstream body mismatch: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		after, err := http.Head(fileURL)
		if err == nil && strings.Contains(after.Header.Get("Link"), "xet-reconstruction-info") {
			after.Body.Close()
			break
		}
		if after != nil {
			after.Body.Close()
		}
		if time.Now().After(deadline) {
			t.Fatal("non-Xet response was not converted")
		}
		time.Sleep(20 * time.Millisecond)
	}

	hash, provider, err := hf.ResolveDownload(context.Background(), nil, fileURL)
	if err != nil {
		t.Fatal(err)
	}
	xetClient, _ := client.NewClient()
	out, _ := os.CreateTemp(t.TempDir(), "non-xet-output")
	defer out.Close()
	if err := xetClient.DownloadFileWithAuthProvider(context.Background(), provider, hash, out); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	converted, _ := io.ReadAll(out)
	if string(converted) != string(content) {
		t.Fatal("converted Xet content mismatch")
	}

	warmHTTP, err := http.Get(fileURL)
	if err != nil {
		t.Fatal(err)
	}
	warmData, err := io.ReadAll(warmHTTP.Body)
	warmHTTP.Body.Close()
	if err != nil || string(warmData) != string(content) || warmHTTP.Header.Get("X-Cache-Status") != "HIT" {
		t.Fatalf("warm HTTP downstream mismatch: status=%s cache=%s err=%v", warmHTTP.Status, warmHTTP.Header.Get("X-Cache-Status"), err)
	}
}

type baseURLStorage struct {
	storage.Storage
	baseURL string
}

func (s *baseURLStorage) GetXorbURL(namespace string, hash xet.Hash) string {
	return s.baseURL + "/v1/xorbs/" + namespace + "/" + hash.String()
}

func TestMissHidesUpstreamXetUntilLocalCacheIsReady(t *testing.T) {
	upstreamLink := `<http://127.0.0.1:1/reconstructions/` + strings.Repeat("b", 64) + `>; rel="xet-reconstruction-info", <http://127.0.0.1:1/auth>; rel="xet-auth"`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", upstreamLink)
		_, _ = io.WriteString(w, "upstream")
	}))
	defer upstream.Close()
	s, _ := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	h, err := NewHandler(Options{Upstream: upstream.URL, CacheDir: t.TempDir(), Storage: s})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	defer server.Close()
	resp, err := http.Head(server.URL + "/owner/repo/resolve/main/file")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if link := resp.Header.Get("Link"); link != "" {
		t.Fatalf("cold response advertised Xet: %s", link)
	}
	if got := resp.Header.Get("X-Cache-Status"); got != "MISS" {
		t.Fatalf("cache status = %q", got)
	}
}
