package mirror

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
)

func TestStandardHFRepositoryAPIIsProxiedAndSanitized(t *testing.T) {
	commit := "0123456789abcdef0123456789abcdef01234567"
	var mu sync.Mutex
	var paths []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		paths = append(paths, req.URL.RequestURI())
		mu.Unlock()
		switch req.URL.Path {
		case "/api/models/org/repo/revision/main":
			body := fmt.Sprintf(`{"id":"org/repo","sha":"%s","resource":"http://hub.local/assets/card.json","canonical":"https://huggingface.co/org/repo/blob/main/LICENSE"}`, commit)
			return response(req, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, io.NopCloser(strings.NewReader(body))), nil
		case "/api/models/org/repo/tree/main":
			body := `[{"type":"file","oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","size":3,"path":"a.txt"}]`
			return response(req, http.StatusOK, http.Header{
				"Content-Type": []string{"application/json"},
				"Link":         []string{`<http://hub.local/api/models/org/repo/tree/main?cursor=next>; rel="next"`},
			}, io.NopCloser(strings.NewReader(body))), nil
		default:
			return nil, fmt.Errorf("unexpected upstream request %s", req.URL.String())
		}
	})
	h := newRepoAPIHandler(t, "http://hub.local", transport)
	defer h.Close()

	info := httptest.NewRecorder()
	h.ServeHTTP(info, httptest.NewRequest(http.MethodGet, "http://mirror.local/api/models/org/repo/revision/main", nil))
	if info.Code != http.StatusOK {
		t.Fatalf("repo info status = %d, body = %q", info.Code, info.Body.String())
	}
	if strings.Contains(info.Body.String(), "hub.local") || strings.Contains(info.Body.String(), "huggingface.co") ||
		!strings.Contains(info.Body.String(), "mirror.local/assets/card.json") || !strings.Contains(info.Body.String(), "mirror.local/org/repo/blob/main/LICENSE") {
		t.Fatalf("repo info leaked or failed to rewrite upstream URL: %s", info.Body.String())
	}

	tree := httptest.NewRecorder()
	h.ServeHTTP(tree, httptest.NewRequest(http.MethodGet, "http://mirror.local/api/models/org/repo/tree/main?recursive=true", nil))
	if tree.Code != http.StatusOK || !strings.Contains(tree.Body.String(), `"path":"a.txt"`) {
		t.Fatalf("repo tree status = %d, body = %q", tree.Code, tree.Body.String())
	}
	if got := tree.Header().Get("Link"); strings.Contains(got, "hub.local") || !strings.Contains(got, "mirror.local") {
		t.Fatalf("pagination Link was not rewritten: %q", got)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, path := range paths {
		if strings.Contains(path, "/api/v1/") {
			t.Fatalf("standards-compatible upstream used legacy fallback: %q", path)
		}
	}
}

func TestLegacyRepositoryCapabilityFallbackCompletesHFMetadata(t *testing.T) {
	data := []byte(`{"model_type":"fallback"}`)
	digest := sha256.Sum256(data)
	sha := hex.EncodeToString(digest[:])
	commit := "89abcdef0123456789abcdef0123456789abcdef"
	var mu sync.Mutex
	var paths []string
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		mu.Lock()
		paths = append(paths, req.Method+" "+req.URL.RequestURI())
		mu.Unlock()
		switch {
		case strings.HasPrefix(req.URL.Path, "/api/models/"):
			return response(req, http.StatusOK, http.Header{"Content-Type": []string{"text/html"}}, io.NopCloser(strings.NewReader("<html>not an HF API</html>"))), nil
		case req.URL.Path == "/api/v1/models/org/repo/repo/files":
			if got := req.URL.Query().Get("Revision"); got != "master" {
				return nil, fmt.Errorf("legacy revision = %q, want master", got)
			}
			body := fmt.Sprintf(`{"Code":200,"Success":true,"Data":{"Files":[{"Path":"config.json","Size":%d,"Sha256":"%s","Revision":"%s","Type":"blob","CommittedDate":1}],"LatestCommitter":{"Id":"%s"}}}`, len(data), sha, commit, commit)
			return response(req, http.StatusOK, http.Header{"Content-Type": []string{"application/json"}}, io.NopCloser(strings.NewReader(body))), nil
		case req.URL.Path == "/org/repo/resolve/main/config.json" && req.Method == http.MethodHead:
			return response(req, http.StatusOK, http.Header{"X-Linked-Etag": []string{sha}}, http.NoBody), nil
		case req.URL.Path == "/org/repo/resolve/main/config.json" && req.Method == http.MethodGet:
			return response(req, http.StatusOK, http.Header{"Content-Length": []string{fmt.Sprint(len(data))}}, io.NopCloser(bytes.NewReader(data))), nil
		default:
			return nil, fmt.Errorf("unexpected upstream request %s %s", req.Method, req.URL.String())
		}
	})
	h := newRepoAPIHandler(t, "http://legacy.local", transport)
	defer h.Close()

	head := httptest.NewRecorder()
	h.ServeHTTP(head, httptest.NewRequest(http.MethodHead, "http://mirror.local/org/repo/resolve/main/config.json", nil))
	if head.Code != http.StatusOK {
		t.Fatalf("resolve HEAD status = %d, body = %q", head.Code, head.Body.String())
	}
	if got := head.Header().Get("X-Repo-Commit"); got != commit {
		t.Fatalf("X-Repo-Commit = %q, want %q", got, commit)
	}
	if got := head.Header().Get("Content-Length"); got != fmt.Sprint(len(data)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(data))
	}
	if got := head.Header().Get("ETag"); got != `"`+sha+`"` {
		t.Fatalf("ETag = %q, want SHA-256", got)
	}

	info := httptest.NewRecorder()
	h.ServeHTTP(info, httptest.NewRequest(http.MethodGet, "http://mirror.local/api/models/org/repo/revision/main", nil))
	if info.Code != http.StatusOK || !strings.Contains(info.Body.String(), commit) {
		t.Fatalf("translated repo info status = %d, body = %q", info.Code, info.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()
	for _, path := range paths {
		if strings.Contains(path, "/models/org/repo/resolve/") {
			t.Fatalf("provider-specific resolve prefix was injected: %q", path)
		}
	}
}

func newRepoAPIHandler(t *testing.T, upstream string, transport http.RoundTripper) *Handler {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()), storage.WithBaseURL("http://mirror.local"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(
		WithStorage(stor),
		WithNext(server.NewHandler(server.WithStorage(stor))),
		WithUpstream(upstream),
		WithCacheDir(t.TempDir()),
		WithPublicBaseURL("http://mirror.local"),
		WithHTTPClient(&http.Client{Transport: transport}),
	)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
