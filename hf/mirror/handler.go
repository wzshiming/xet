// Package mirror adapts Hugging Face resolve URLs to a local XET server.
package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
)

const authPath = "/.xet-mirror/auth"

// Handler is an HTTP adapter in front of an existing XET server. It handles
// Hugging Face repository metadata, resolve URLs, and its own xet-auth
// endpoint; all other requests are delegated to next.
type Handler struct {
	storage       storage.Storage
	next          http.Handler
	upstream      *url.URL
	upstreamToken string
	casToken      string
	cacheDir      string
	publicBaseURL string
	httpClient    *http.Client
	concurrency   int

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	jobs      map[string]*job
	repos     map[string]*upstreamRepo
	repoCalls map[string]*upstreamRepoCall
}

// Option configures a mirror Handler.
type Option func(*Handler) error

// WithStorage uses the same storage as the XET server behind the mirror.
func WithStorage(stor storage.Storage) Option {
	return func(h *Handler) error {
		h.storage = stor
		return nil
	}
}

// WithNext sets the existing XET server handler.
func WithNext(next http.Handler) Option {
	return func(h *Handler) error {
		h.next = next
		return nil
	}
}

// WithUpstream sets the Hugging Face compatible upstream endpoint.
func WithUpstream(endpoint string) Option {
	return func(h *Handler) error {
		u, err := url.Parse(strings.TrimRight(endpoint, "/"))
		if err != nil {
			return fmt.Errorf("parse upstream endpoint: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" || u.Host == "" {
			return fmt.Errorf("upstream endpoint must be an absolute HTTP URL")
		}
		h.upstream = u
		return nil
	}
}

// WithUpstreamToken sets the server-side Hugging Face token used for gated or
// private upstream repositories. Client credentials are never forwarded.
func WithUpstreamToken(token string) Option {
	return func(h *Handler) error {
		h.upstreamToken = token
		return nil
	}
}

// WithCASToken sets the token returned by the local xet-auth endpoint.
func WithCASToken(token string) Option {
	return func(h *Handler) error {
		h.casToken = token
		return nil
	}
}

// WithCacheDir sets the persistent directory for partial downloads and mirror
// metadata. XET data itself is stored by storage.
func WithCacheDir(dir string) Option {
	return func(h *Handler) error {
		h.cacheDir = dir
		return nil
	}
}

// WithPublicBaseURL overrides the URL placed in local XET and bridge links.
// When empty, the URL is derived from the incoming request.
func WithPublicBaseURL(baseURL string) Option {
	return func(h *Handler) error {
		if baseURL == "" {
			return nil
		}
		u, err := url.Parse(strings.TrimRight(baseURL, "/"))
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("public base URL must be an absolute URL")
		}
		h.publicBaseURL = strings.TrimRight(u.String(), "/")
		return nil
	}
}

// WithHTTPClient sets the client used by the mirror for all upstream access.
func WithHTTPClient(client *http.Client) Option {
	return func(h *Handler) error {
		if client == nil {
			return fmt.Errorf("HTTP client is nil")
		}
		h.httpClient = client
		return nil
	}
}

// WithConcurrency sets the XET download and local conversion concurrency.
func WithConcurrency(concurrency int) Option {
	return func(h *Handler) error {
		if concurrency < 1 {
			return fmt.Errorf("concurrency must be positive")
		}
		h.concurrency = concurrency
		return nil
	}
}

// NewHandler creates a Hugging Face mirror adapter.
func NewHandler(opts ...Option) (*Handler, error) {
	h := &Handler{
		cacheDir:    "./xet-mirror",
		httpClient:  &http.Client{},
		concurrency: 4,
		jobs:        map[string]*job{},
		repos:       map[string]*upstreamRepo{},
		repoCalls:   map[string]*upstreamRepoCall{},
	}
	for _, opt := range opts {
		if err := opt(h); err != nil {
			return nil, err
		}
	}
	if h.storage == nil {
		return nil, fmt.Errorf("mirror storage is required")
	}
	if h.upstream == nil {
		return nil, fmt.Errorf("mirror upstream is required")
	}
	for _, dir := range []string{h.entriesDir(), h.partialDir(), h.workDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create mirror directory %s: %w", dir, err)
		}
	}
	h.ctx, h.cancel = context.WithCancel(context.Background())
	return h, nil
}

// Close cancels active downloads and waits until their partial files have
// been closed. A later Handler using the same cache directory resumes them.
func (h *Handler) Close() error {
	h.cancel()
	h.wg.Wait()
	return nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == authPath {
		h.serveAuth(w, r)
		return
	}
	if h.serveHFRepoAPI(w, r) {
		return
	}
	target, ok := h.parseTarget(r)
	if !ok {
		if h.next != nil {
			h.next.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	entry, err := h.loadEntry(r.Context(), target.cacheKey)
	if err == nil {
		h.serveCached(w, r, target, entry)
		return
	}
	if !errors.Is(err, os.ErrNotExist) {
		http.Error(w, "Invalid mirror cache entry", http.StatusInternalServerError)
		return
	}

	j, err := h.getOrStart(target)
	if err != nil {
		var upstreamErr *upstreamError
		if errors.As(err, &upstreamErr) {
			http.Error(w, http.StatusText(upstreamErr.status), upstreamErr.status)
			return
		}
		http.Error(w, "Failed to resolve upstream file", http.StatusBadGateway)
		return
	}
	h.serveGrowing(w, r, j)
}

func (h *Handler) serveAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      h.baseURL(r),
		"accessToken": h.casToken,
		"exp":         time.Now().Add(time.Hour).Unix(),
	})
}

func (h *Handler) serveCached(w http.ResponseWriter, r *http.Request, target target, entry *cacheEntry) {
	base := h.baseURL(r)
	bridgeURL := base + "/xet-bridge/" + entry.SHA256
	reconstructionURL := base + "/v1/reconstructions/" + entry.FileHash
	authURL := base + authPath

	w.Header().Set("ETag", `"`+entry.SHA256+`"`)
	w.Header().Set("X-Linked-Etag", `"`+entry.SHA256+`"`)
	w.Header().Set("X-Linked-Size", strconv.FormatInt(entry.Size, 10))
	w.Header().Set("X-Xet-Hash", entry.FileHash)
	w.Header().Set("Link", fmt.Sprintf("<%s>; rel=\"xet-auth\", <%s>; rel=\"xet-reconstruction-info\"", authURL, reconstructionURL))
	w.Header().Set("Location", bridgeURL)
	copyEntryHeaders(w.Header(), entry.Header)
	if w.Header().Get("X-Repo-Commit") == "" {
		w.Header().Set("X-Repo-Commit", syntheticRepoCommit(target))
	}
	w.WriteHeader(http.StatusFound)
	if r.Method == http.MethodGet {
		_, _ = io.WriteString(w, "Found.\n")
	}
}

func (h *Handler) serveGrowing(w http.ResponseWriter, r *http.Request, j *job) {
	defer j.releaseReader()
	copyEntryHeaders(w.Header(), j.info.header)
	if w.Header().Get("X-Repo-Commit") == "" {
		w.Header().Set("X-Repo-Commit", syntheticRepoCommit(j.target))
	}
	if j.info.etag != "" {
		w.Header().Set("ETag", j.info.etag)
	}
	rsc, err := newGrowingReadSeeker(r.Context(), j)
	if err != nil {
		http.Error(w, "Failed to open mirror cache", http.StatusInternalServerError)
		return
	}
	defer rsc.Close()

	if j.info.size >= 0 {
		name := filepath.Base(j.target.file)
		http.ServeContent(w, r, name, j.info.modTime, rsc)
		return
	}
	if r.Header.Get("Range") != "" {
		http.Error(w, "Range requests require a known upstream size", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.Header().Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rsc)
}

func (h *Handler) baseURL(r *http.Request) string {
	if h.publicBaseURL != "" {
		return h.publicBaseURL
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if forwarded := strings.TrimSpace(strings.Split(r.Header.Get("X-Forwarded-Proto"), ",")[0]); forwarded != "" {
		scheme = forwarded
	}
	return scheme + "://" + r.Host
}

func (h *Handler) entriesDir() string { return filepath.Join(h.cacheDir, "entries") }
func (h *Handler) partialDir() string { return filepath.Join(h.cacheDir, "partial") }
func (h *Handler) workDir() string    { return filepath.Join(h.cacheDir, "work") }

type cacheEntry struct {
	CacheKey string            `json:"cache_key"`
	FileHash string            `json:"file_hash"`
	SHA256   string            `json:"sha256"`
	Size     int64             `json:"size"`
	Header   map[string]string `json:"header,omitempty"`
}

func (h *Handler) entryPath(cacheKey string) string {
	return filepath.Join(h.entriesDir(), keyDigest(cacheKey)+".json")
}

func (h *Handler) loadEntry(ctx context.Context, cacheKey string) (*cacheEntry, error) {
	b, err := os.ReadFile(h.entryPath(cacheKey))
	if err != nil {
		return nil, err
	}
	var entry cacheEntry
	if err := json.Unmarshal(b, &entry); err != nil {
		return nil, fmt.Errorf("%w: invalid cache manifest: %v", os.ErrNotExist, err)
	}
	if entry.CacheKey != keyDigest(cacheKey) || entry.Size < 0 {
		return nil, fmt.Errorf("%w: cache entry does not match request", os.ErrNotExist)
	}
	digest, err := hex.DecodeString(entry.SHA256)
	if err != nil || len(digest) != sha256.Size {
		return nil, fmt.Errorf("%w: invalid cached SHA-256", os.ErrNotExist)
	}
	if _, err := xet.ParseFileHash(entry.FileHash); err != nil {
		return nil, fmt.Errorf("%w: invalid cached file hash: %v", os.ErrNotExist, err)
	}
	var sum [sha256.Size]byte
	copy(sum[:], digest)
	rsc, err := h.storage.GetReconstructedFile(ctx, "default", sum)
	if err != nil {
		return nil, fmt.Errorf("%w: cached XET data is missing", os.ErrNotExist)
	}
	_ = rsc.Close()
	return &entry, nil
}

func (h *Handler) storeEntry(entry *cacheEntry) error {
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	path := filepath.Join(h.entriesDir(), entry.CacheKey+".json")
	tmp, err := os.CreateTemp(h.entriesDir(), filepath.Base(path)+".tmp-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func keyDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func copyEntryHeaders(dst http.Header, src map[string]string) {
	for _, key := range []string{"Content-Type", "Content-Disposition", "Cache-Control", "X-Repo-Commit"} {
		if value := src[key]; value != "" {
			dst.Set(key, value)
		}
	}
}
