package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

func TestHTTPMissStreamsThenUsesLocalXETAndBridge(t *testing.T) {
	data := bytes.Repeat([]byte("stream-before-cache-"), 8192)
	reader, writer := io.Pipe()
	getStarted := make(chan struct{})
	var headCount atomic.Int32
	var getCount atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Method {
		case http.MethodHead:
			headCount.Add(1)
			return response(req, http.StatusOK, http.Header{
				"Content-Length": []string{fmt.Sprint(len(data))},
				"Content-Type":   []string{"application/octet-stream"},
				"ETag":           []string{`"http-object-v1"`},
			}, http.NoBody), nil
		case http.MethodGet:
			getCount.Add(1)
			close(getStarted)
			return response(req, http.StatusOK, http.Header{
				"Content-Length": []string{fmt.Sprint(len(data))},
				"Content-Type":   []string{"application/octet-stream"},
			}, reader), nil
		default:
			return nil, fmt.Errorf("unexpected upstream method %s", req.Method)
		}
	})

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(t.TempDir()),
		storage.WithBaseURL("http://mirror.local"),
	)
	if err != nil {
		t.Fatal(err)
	}
	xetServer := server.NewHandler(server.WithStorage(stor))
	h, err := NewHandler(
		WithStorage(stor),
		WithNext(xetServer),
		WithUpstream("http://hub.local"),
		WithCacheDir(t.TempDir()),
		WithPublicBaseURL("http://mirror.local"),
		WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	req := httptest.NewRequest(http.MethodGet, "http://mirror.local/org/repo/resolve/main/model.bin", nil)
	stream := newObservedResponseWriter()
	done := make(chan struct{})
	go func() {
		h.ServeHTTP(stream, req)
		close(done)
	}()
	<-getStarted

	prefix := data[:4096]
	if _, err := writer.Write(prefix); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stream.firstWrite:
	case <-time.After(5 * time.Second):
		t.Fatal("client did not receive bytes while the upstream download was active")
	}
	select {
	case <-done:
		t.Fatal("HTTP response completed before the upstream file did")
	default:
	}
	if _, err := writer.Write(data[len(prefix):]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("streaming response did not complete")
	}
	if got := stream.bodyBytes(); !bytes.Equal(got, data) {
		t.Fatalf("streamed body length = %d, want %d", len(got), len(data))
	}

	target, _ := h.parseTarget(req)
	entry := waitForEntry(t, h, target.cacheKey)
	if headCount.Load() != 1 || getCount.Load() != 1 {
		t.Fatalf("upstream requests: HEAD=%d GET=%d, want one each", headCount.Load(), getCount.Load())
	}

	cached := httptest.NewRecorder()
	h.ServeHTTP(cached, httptest.NewRequest(http.MethodHead, req.URL.String(), nil))
	if cached.Code != http.StatusFound {
		t.Fatalf("cached status = %d, want 302", cached.Code)
	}
	for _, value := range []string{cached.Header().Get("Location"), cached.Header().Get("Link")} {
		if strings.Contains(value, "hub.local") {
			t.Fatalf("cached response leaked upstream URL: %q", value)
		}
	}
	if got, want := cached.Header().Get("X-Xet-Hash"), entry.FileHash; got != want {
		t.Fatalf("X-Xet-Hash = %q, want %q", got, want)
	}

	bridge := httptest.NewRecorder()
	bridgePath := "/xet-bridge/" + entry.SHA256
	h.ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "http://mirror.local"+bridgePath, nil))
	if bridge.Code != http.StatusOK || !bytes.Equal(bridge.Body.Bytes(), data) {
		t.Fatalf("bridge status=%d body=%d bytes", bridge.Code, bridge.Body.Len())
	}
	if headCount.Load() != 1 || getCount.Load() != 1 {
		t.Fatal("cached access contacted the upstream")
	}
}

func TestHTTPPartialFileResumesAfterRestart(t *testing.T) {
	data := bytes.Repeat([]byte("resume-me-"), 4096)
	prefixSize := len(data) / 3
	var gotRange string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method == http.MethodHead {
			return response(req, http.StatusOK, http.Header{
				"Content-Length": []string{fmt.Sprint(len(data))},
				"Content-Type":   []string{"application/octet-stream"},
				"ETag":           []string{`"stable-version"`},
			}, http.NoBody), nil
		}
		gotRange = req.Header.Get("Range")
		if gotRange != fmt.Sprintf("bytes=%d-", prefixSize) {
			return nil, fmt.Errorf("unexpected Range %q", gotRange)
		}
		header := http.Header{
			"Content-Length": []string{fmt.Sprint(len(data) - prefixSize)},
			"Content-Range":  []string{fmt.Sprintf("bytes %d-%d/%d", prefixSize, len(data)-1, len(data))},
			"Content-Type":   []string{"application/octet-stream"},
		}
		return response(req, http.StatusPartialContent, header, io.NopCloser(bytes.NewReader(data[prefixSize:]))), nil
	})

	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	h, err := NewHandler(
		WithStorage(stor),
		WithNext(server.NewHandler(server.WithStorage(stor))),
		WithUpstream("http://hub.local"),
		WithCacheDir(cacheDir),
		WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	req := httptest.NewRequest(http.MethodGet, "http://mirror.local/datasets/org/repo/resolve/main/data.bin", nil)
	target, ok := h.parseTarget(req)
	if !ok {
		t.Fatal("resolve target was not recognized")
	}
	info, err := h.resolveSource(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	partialName := keyDigest(target.cacheKey) + "-" + keyDigest(info.identity) + ".part"
	if err := os.WriteFile(filepath.Join(h.partialDir(), partialName), data[:prefixSize], 0o644); err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("resumed response status=%d body=%d bytes", rec.Code, rec.Body.Len())
	}
	waitForEntry(t, h, target.cacheKey)
	if gotRange == "" {
		t.Fatal("resumed HTTP download did not use a Range request")
	}
}

func TestXETUpstreamUsesXETClientAndCoexistsWithHostedFiles(t *testing.T) {
	upstreamData := bytes.Repeat([]byte("upstream-xet-"), 32768)
	upstreamStorage, err := storage.NewFileStorage(
		storage.WithBasePath(t.TempDir()),
		storage.WithBaseURL("http://cas.local"),
	)
	if err != nil {
		t.Fatal(err)
	}
	upstreamHash, err := upload.UploadFile(context.Background(), storageUploadAdapter{storage: upstreamStorage}, bytes.NewReader(upstreamData),
		upload.WithConcurrency(2), upload.WithCacheDir(t.TempDir()), upload.WithEnableSHA256(true))
	if err != nil {
		t.Fatal(err)
	}
	upstreamCAS := server.NewHandler(server.WithStorage(upstreamStorage))
	var resolveGETs atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Host {
		case "hub.local":
			if got := req.Header.Get("Authorization"); got != "Bearer hf-secret" {
				return nil, fmt.Errorf("upstream Authorization = %q", got)
			}
			if req.URL.Path == "/auth" {
				body := fmt.Sprintf(`{"casUrl":"http://cas.local","accessToken":"read-token","exp":%d}`, time.Now().Add(time.Hour).Unix())
				return response(req, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, io.NopCloser(strings.NewReader(body))), nil
			}
			if req.Method == http.MethodGet {
				resolveGETs.Add(1)
				return nil, fmt.Errorf("mirror attempted an HTTP download for an XET source")
			}
			header := http.Header{
				"Content-Length": []string{fmt.Sprint(len(upstreamData))},
				"X-Linked-Size":  []string{fmt.Sprint(len(upstreamData))},
				"X-Xet-Hash":     []string{upstreamHash.String()},
				"Link": []string{fmt.Sprintf(
					"<http://hub.local/auth>; rel=\"xet-auth\", <http://cas.local/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", upstreamHash.String())},
			}
			return response(req, http.StatusFound, header, http.NoBody), nil
		case "cas.local":
			return serveHandler(upstreamCAS, req), nil
		default:
			return nil, fmt.Errorf("unexpected host %q", req.URL.Host)
		}
	})

	localStorage, err := storage.NewFileStorage(
		storage.WithBasePath(t.TempDir()),
		storage.WithBaseURL("http://mirror.local"),
	)
	if err != nil {
		t.Fatal(err)
	}
	hostedData := []byte("directly hosted beside mirror data")
	if _, err := upload.UploadFile(context.Background(), storageUploadAdapter{storage: localStorage}, bytes.NewReader(hostedData),
		upload.WithConcurrency(1), upload.WithCacheDir(t.TempDir()), upload.WithEnableSHA256(true)); err != nil {
		t.Fatal(err)
	}
	localXET := server.NewHandler(server.WithStorage(localStorage))
	h, err := NewHandler(
		WithStorage(localStorage),
		WithNext(localXET),
		WithUpstream("http://hub.local"),
		WithUpstreamToken("hf-secret"),
		WithCacheDir(t.TempDir()),
		WithPublicBaseURL("http://mirror.local"),
		WithHTTPClient(&http.Client{Transport: transport}),
		WithConcurrency(2),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	resolveURL := "http://mirror.local/org/repo/resolve/main/model.bin"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, resolveURL, nil))
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), upstreamData) {
		t.Fatalf("XET miss status=%d body=%d bytes", rec.Code, rec.Body.Len())
	}
	if rec.Header().Get("Link") != "" || rec.Header().Get("Location") != "" {
		t.Fatalf("uncached response leaked upstream routing headers: Link=%q Location=%q", rec.Header().Get("Link"), rec.Header().Get("Location"))
	}
	target, _ := h.parseTarget(httptest.NewRequest(http.MethodGet, resolveURL, nil))
	entry := waitForEntry(t, h, target.cacheKey)
	if resolveGETs.Load() != 0 {
		t.Fatal("XET source was fetched through ordinary HTTP")
	}

	// A standard resolve-aware XET client sees only local URLs after conversion.
	localTransport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "mirror.local" {
			return nil, fmt.Errorf("local XET client attempted host %q", req.URL.Host)
		}
		return serveHandler(h, req), nil
	})
	localHTTP := &http.Client{
		Transport: localTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resolvedHash, provider, err := hf.ResolveDownload(context.Background(), localHTTP, resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedHash.String() != entry.FileHash {
		t.Fatalf("resolved local hash = %s, want %s", resolvedHash.String(), entry.FileHash)
	}
	localClient, err := client.NewClient(client.WithHTTPClient(localHTTP), client.WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	out, err := os.CreateTemp(t.TempDir(), "mirror-download-")
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if err := localClient.DownloadFileWithAuthProvider(context.Background(), provider, resolvedHash, out); err != nil {
		t.Fatal(err)
	}
	if _, err := out.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(out)
	if err != nil || !bytes.Equal(got, upstreamData) {
		t.Fatalf("local XET download: err=%v size=%d", err, len(got))
	}

	for name, data := range map[string][]byte{"mirrored": upstreamData, "hosted": hostedData} {
		digest := sha256.Sum256(data)
		bridge := httptest.NewRecorder()
		h.ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "http://mirror.local/xet-bridge/"+hex.EncodeToString(digest[:]), nil))
		if bridge.Code != http.StatusOK || !bytes.Equal(bridge.Body.Bytes(), data) {
			t.Fatalf("%s bridge status=%d size=%d", name, bridge.Code, bridge.Body.Len())
		}
	}
}

func TestMirrorCachesEmptyFile(t *testing.T) {
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return response(req, http.StatusOK, http.Header{
			"Content-Length": []string{"0"},
			"Content-Type":   []string{"application/octet-stream"},
			"ETag":           []string{`"empty"`},
		}, http.NoBody), nil
	})
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(
		WithStorage(stor),
		WithNext(server.NewHandler(server.WithStorage(stor))),
		WithUpstream("http://hub.local"),
		WithCacheDir(t.TempDir()),
		WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	req := httptest.NewRequest(http.MethodGet, "http://mirror.local/org/repo/resolve/main/empty", nil)
	target, _ := h.parseTarget(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("empty miss status=%d body=%d", rec.Code, rec.Body.Len())
	}
	entry := waitForEntry(t, h, target.cacheKey)
	if entry.SHA256 != hex.EncodeToString(sha256.New().Sum(nil)) {
		t.Fatalf("empty SHA-256 = %s", entry.SHA256)
	}
	bridge := httptest.NewRecorder()
	h.ServeHTTP(bridge, httptest.NewRequest(http.MethodGet, "http://mirror.local/xet-bridge/"+entry.SHA256, nil))
	if bridge.Code != http.StatusOK || bridge.Body.Len() != 0 {
		t.Fatalf("empty bridge status=%d body=%d", bridge.Code, bridge.Body.Len())
	}
}

func TestParseTargetPreservesEncodedRevision(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(
		WithStorage(stor),
		WithUpstream("https://hub.example/prefix"),
		WithCacheDir(t.TempDir()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	req := httptest.NewRequest(http.MethodGet, "http://mirror.local/datasets/org/repo/resolve/refs%2Fpr%2F1/file.bin?download=true", nil)
	target, ok := h.parseTarget(req)
	if !ok {
		t.Fatal("encoded revision was not recognized")
	}
	if !strings.Contains(target.upstreamURL, "/prefix/datasets/org/repo/resolve/refs%2Fpr%2F1/file.bin?download=true") {
		t.Fatalf("upstream URL did not preserve encoded revision: %s", target.upstreamURL)
	}
}

func TestParseTargetAllowsRepositoryNamedResolve(t *testing.T) {
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(WithStorage(stor), WithUpstream("https://hub.example"), WithCacheDir(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	req := httptest.NewRequest(http.MethodGet, "http://mirror.local/org/resolve/resolve/main/file.bin", nil)
	target, ok := h.parseTarget(req)
	if !ok || !strings.Contains(target.upstreamURL, "/org/resolve/resolve/main/file.bin") {
		t.Fatalf("repository named resolve was parsed incorrectly: ok=%v url=%s", ok, target.upstreamURL)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(req *http.Request, status int, header http.Header, body io.ReadCloser) *http.Response {
	contentLength := int64(-1)
	if value := header.Get("Content-Length"); value != "" {
		_, _ = fmt.Sscan(value, &contentLength)
	}
	return &http.Response{
		StatusCode:    status,
		Status:        fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:        header,
		Body:          body,
		ContentLength: contentLength,
		Request:       req,
	}
}

func serveHandler(handler http.Handler, req *http.Request) *http.Response {
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Result()
}

func waitForEntry(t *testing.T, h *Handler, cacheKey string) *cacheEntry {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		entry, err := h.loadEntry(context.Background(), cacheKey)
		if err == nil {
			return entry
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mirror entry was not converted to XET")
	return nil
}

type observedResponseWriter struct {
	mu         sync.Mutex
	header     http.Header
	status     int
	body       bytes.Buffer
	firstWrite chan struct{}
	once       sync.Once
}

func newObservedResponseWriter() *observedResponseWriter {
	return &observedResponseWriter{header: http.Header{}, firstWrite: make(chan struct{})}
}

func (w *observedResponseWriter) Header() http.Header { return w.header }

func (w *observedResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = status
	}
}

func (w *observedResponseWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.once.Do(func() { close(w.firstWrite) })
	return w.body.Write(p)
}

func (w *observedResponseWriter) bodyBytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.body.Bytes())
}
