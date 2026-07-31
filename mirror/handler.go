// Package mirror implements a streaming Hugging Face Xet cache.
package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

// Handler proxies Hugging Face immediately and populates a local Xet CAS in
// the background. A resolve request is switched to the local CAS only after
// the complete file has been downloaded, converted, and committed.
type Handler struct {
	proxy        *httputil.ReverseProxy
	store        storage.Storage
	cacheDir     string
	indexPath    string
	metadataPath string
	concurrency  int
	hfToken      string
	localToken   string
	upstream     *url.URL
	mu           sync.RWMutex
	index        map[string]string
	metadata     map[string]http.Header
	inflight     map[string]struct{}
}

type cacheKeyContextKey struct{}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type Options struct {
	Upstream    string
	HFToken     string
	CacheDir    string
	Storage     storage.Storage
	Concurrency int
	LocalToken  string
}

// NewHandler creates a Hugging Face mirror handler.
func NewHandler(opts Options) (*Handler, error) {
	if opts.Storage == nil {
		return nil, fmt.Errorf("mirror storage is required")
	}
	if opts.CacheDir == "" {
		opts.CacheDir = "./xet-data/mirror"
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if err := os.MkdirAll(opts.CacheDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(opts.CacheDir, "files"), 0755); err != nil {
		return nil, err
	}
	u, err := url.Parse(opts.Upstream)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("invalid upstream %q", opts.Upstream)
	}
	h := &Handler{store: opts.Storage, cacheDir: opts.CacheDir, indexPath: filepath.Join(opts.CacheDir, "index.json"), metadataPath: filepath.Join(opts.CacheDir, "metadata.json"), concurrency: opts.Concurrency, hfToken: opts.HFToken, localToken: opts.LocalToken, upstream: u, index: map[string]string{}, metadata: map[string]http.Header{}, inflight: map[string]struct{}{}}
	_ = h.loadIndex()
	_ = h.loadMetadata()
	p := httputil.NewSingleHostReverseProxy(u)
	// Resolve endpoints commonly redirect to a CDN. Follow those redirects in
	// the mirror so a cold client never leaves the mirror and the final HTTP
	// body can populate the transient cache.
	redirectClient := &http.Client{Transport: http.DefaultTransport}
	p.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		out := r.Clone(r.Context())
		out.RequestURI = ""
		return redirectClient.Do(out)
	})
	originalDirector := p.Director
	p.Director = func(r *http.Request) {
		originalHost := r.Host
		originalScheme := "http"
		if r.TLS != nil {
			originalScheme = "https"
		}
		originalDirector(r)
		r.Host = u.Host
		if r.Header.Get("X-Forwarded-Host") == "" {
			r.Header.Set("X-Forwarded-Host", originalHost)
		}
		if r.Header.Get("X-Forwarded-Proto") == "" {
			r.Header.Set("X-Forwarded-Proto", originalScheme)
		}
		if opts.HFToken != "" {
			r.Header.Set("Authorization", "Bearer "+opts.HFToken)
		}
	}
	p.ModifyResponse = h.modifyResponse
	h.proxy = p
	return h, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/xet-auth" {
		base := forwardedBaseURL(r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"casUrl": base, "accessToken": h.localToken, "exp": int64(4102444800)})
		return
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		h.mu.RLock()
		localHash := h.index[cacheKey(r.URL)]
		h.mu.RUnlock()
		if localHash != "" {
			if r.Method == http.MethodHead {
				h.serveMetadata(w, r, localHash)
			} else {
				h.serveFile(w, r, localHash)
			}
			return
		}
		if r.Method == http.MethodGet && r.Header.Get("Range") == "" && h.tryServeResumedHTTP(w, r) {
			return
		}
	}
	ctx := context.WithValue(r.Context(), cacheKeyContextKey{}, cacheKey(r.URL))
	h.proxy.ServeHTTP(w, r.WithContext(ctx))
}

func (h *Handler) modifyResponse(resp *http.Response) error {
	key, _ := resp.Request.Context().Value(cacheKeyContextKey{}).(string)
	if key == "" {
		key = cacheKey(resp.Request.URL)
	}
	h.mu.RLock()
	localHash := h.index[key]
	h.mu.RUnlock()
	if localHash != "" {
		base := forwardedBaseURL(resp.Request)
		resp.Header.Set("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\", <%s/api/xet-auth>; rel=\"xet-auth\"", base, localHash, base))
		resp.Header.Set("X-Xet-Hash", localHash)
		resp.Header.Set("X-Cache-Status", "HIT")
		return nil
	}
	links := parseLinks(resp.Header.Values("Link"))
	if resp.Request.Method == http.MethodGet && resp.StatusCode == http.StatusOK && resp.Request.Header.Get("Range") == "" && links["xet-reconstruction-info"] != "" && links["xet-auth"] != "" {
		// Prefer the origin's Xet representation for the upstream half of a cold
		// fill. The reconstructed bytes still leave this handler as plain HTTP.
		if body, size, err := h.openUpstreamXet(resp); err == nil {
			_ = resp.Body.Close()
			resp.Body = body
			resp.ContentLength = size
			resp.Header.Set("Content-Length", strconv.FormatInt(size, 10))
			resp.Header.Set("X-Mirror-Upstream", "xet")
		}
	}
	// A cold mirror is an ordinary HTTP intermediary. Do not expose an
	// upstream Xet identity which the mirror cannot serve yet.
	if links["xet-reconstruction-info"] != "" || links["xet-auth"] != "" {
		resp.Header.Del("Link")
	}
	resp.Header.Del("X-Xet-Hash")
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusBadRequest {
		resp.Header.Set("X-Cache-Status", "MISS")
	}
	if resp.Request.Method == http.MethodGet && resp.StatusCode == http.StatusOK && resp.Request.Header.Get("Range") == "" {
		// Tee the ordinary HTTP bytes to the transient cache. Conversion begins
		// only after EOF, regardless of whether the origin itself supports Xet.
		h.rememberMetadata(key, resp.Header)
		if body, ok := h.startHTTPBodyCache(key, resp.Body); ok {
			resp.Body = body
			resp.Header.Set("X-Cache-Status", "MISS")
		}
	}
	return nil
}

func (h *Handler) openUpstreamXet(resp *http.Response) (io.ReadCloser, int64, error) {
	provider, fileHash, err := h.resolveUpstreamXet(resp)
	if err != nil {
		return nil, 0, err
	}
	baseURL, err := provider.BaseURL(resp.Request.Context())
	if err != nil {
		return nil, 0, err
	}
	token, err := provider.Token(resp.Request.Context())
	if err != nil {
		return nil, 0, err
	}
	cli, err := client.NewClient(client.WithBaseURL(baseURL), client.WithToken(token), client.WithConcurrency(h.concurrency), client.WithCacheDir(h.cacheDir))
	if err != nil {
		return nil, 0, err
	}
	if reconstruction, err := cli.GetReconstructionV2(resp.Request.Context(), fileHash, nil); err == nil {
		reader, err := download.NewReaderV2(resp.Request.Context(), cli, reconstruction, h.cacheDir, download.WithConcurrency(h.concurrency))
		if err == nil {
			return io.NopCloser(reader), download.ExpectedLengthV2(reconstruction), nil
		}
	}
	reconstruction, err := cli.GetReconstructionV1(resp.Request.Context(), fileHash, nil)
	if err != nil {
		return nil, 0, err
	}
	reader, err := download.NewReaderV1(resp.Request.Context(), cli, reconstruction, h.cacheDir, download.WithConcurrency(h.concurrency))
	if err != nil {
		return nil, 0, err
	}
	return io.NopCloser(reader), download.ExpectedLengthV1(reconstruction), nil
}

func (h *Handler) resolveUpstreamXet(resp *http.Response) (client.AuthProvider, xet.Hash, error) {
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		out := r.Clone(r.Context())
		if h.hfToken != "" && out.Header.Get("Authorization") == "" {
			out.Header.Set("Authorization", "Bearer "+h.hfToken)
		}
		return http.DefaultTransport.RoundTrip(out)
	})
	httpClient := &http.Client{Transport: transport}
	fileHash, provider, err := hf.ResolveResponse(resp.Request.Context(), httpClient, resp)
	return provider, fileHash, err
}

// serveMetadata makes a promoted resolve entry independent of the origin.
// Hugging Face metadata captured during the fill is replayed, while the Xet
// identity and content length are always derived from the committed local CAS.
func (h *Handler) serveMetadata(w http.ResponseWriter, r *http.Request, hashString string) {
	fileHash, err := xet.ParseHash(hashString)
	if err != nil {
		http.Error(w, "invalid cached hash", http.StatusInternalServerError)
		return
	}
	sh, err := h.store.GetShardByFileHash(r.Context(), fileHash)
	h.mu.RLock()
	metadata := h.metadata[cacheKey(r.URL)].Clone()
	h.mu.RUnlock()
	for name, values := range metadata {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	base := forwardedBaseURL(r)
	w.Header().Set("Link", fmt.Sprintf("<%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\", <%s/api/xet-auth>; rel=\"xet-auth\"", base, hashString, base))
	w.Header().Set("X-Xet-Hash", hashString)
	w.Header().Set("X-Cache-Status", "HIT")
	w.Header().Set("Accept-Ranges", "bytes")
	if err == nil {
		w.Header().Set("Content-Length", strconv.FormatInt(fileSize(sh, fileHash), 10))
	}
}

func (h *Handler) rememberMetadata(key string, header http.Header) {
	keep := http.Header{}
	for _, name := range []string{"Cache-Control", "Content-Disposition", "Content-Type", "ETag", "Last-Modified", "X-Linked-Etag", "X-Repo-Commit"} {
		if values := header.Values(name); len(values) != 0 {
			keep[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
		}
	}
	h.mu.Lock()
	h.metadata[key] = keep
	h.mu.Unlock()
}

// serveFile reconstructs an ordinary HTTP response directly from the local
// Xet objects. It never creates a second persistent raw-file cache.
func (h *Handler) serveFile(w http.ResponseWriter, r *http.Request, hashString string) {
	fileHash, err := xet.ParseHash(hashString)
	if err != nil {
		http.Error(w, "invalid cached hash", http.StatusInternalServerError)
		return
	}
	sh, err := h.store.GetShardByFileHash(r.Context(), fileHash)
	if err != nil {
		http.Error(w, "cached file not found", http.StatusNotFound)
		return
	}
	total := fileSize(sh, fileHash)
	start, end, partial, err := parseHTTPRange(r.Header.Get("Range"), total)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	rangeHeader := ""
	if partial {
		rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
	}
	reconstruction, err := download.BuildReconstructionResponseV1(r.Context(), h.store, "default", sh, fileHash, rangeHeader)
	if err != nil {
		http.Error(w, "build cached reconstruction", http.StatusInternalServerError)
		return
	}
	reader, err := download.NewReaderV1(r.Context(), localDownloadAdapter{h.store}, reconstruction, "", download.WithConcurrency(h.concurrency))
	if err != nil {
		http.Error(w, "open cached reconstruction", http.StatusInternalServerError)
		return
	}
	length := total
	if partial {
		length = end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprint(length))
	w.Header().Set("X-Cache-Status", "HIT")
	if partial {
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = io.CopyN(w, reader, length)
}

// tryServeResumedHTTP serves the retained prefix to a fresh client and asks
// the origin only for the missing suffix. Thus deleting the client-side cache
// does not force the mirror to download bytes it already has again.
func (h *Handler) tryServeResumedHTTP(w http.ResponseWriter, r *http.Request) bool {
	key := cacheKey(r.URL)
	name := h.cacheFilePath(key)
	info, err := os.Stat(name)
	if err != nil || info.Size() <= 0 {
		return false
	}
	h.mu.Lock()
	if _, running := h.inflight[key]; running {
		h.mu.Unlock()
		return false
	}
	h.inflight[key] = struct{}{}
	h.mu.Unlock()
	prefixSize := info.Size()
	target := *h.upstream
	target.Path = singleJoiningSlash(h.upstream.Path, r.URL.Path)
	target.RawPath = ""
	target.RawQuery = r.URL.RawQuery
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, target.String(), nil)
	if err != nil {
		h.finishInflight(key)
		return false
	}
	req.Header = r.Header.Clone()
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", prefixSize))
	if h.hfToken != "" {
		req.Header.Set("Authorization", "Bearer "+h.hfToken)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.finishInflight(key)
		return false
	}
	if resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		h.finishInflight(key)
		return false
	}
	tailLength := resp.ContentLength
	if tailLength < 0 {
		resp.Body.Close()
		h.finishInflight(key)
		return false
	}
	file, err := os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		resp.Body.Close()
		h.finishInflight(key)
		return false
	}
	for header, values := range resp.Header {
		if strings.EqualFold(header, "Content-Range") || strings.EqualFold(header, "Content-Length") {
			continue
		}
		for _, value := range values {
			w.Header().Add(header, value)
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(prefixSize+tailLength, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Cache-Status", "RESUME")
	// First replay the durable prefix, then tee only the missing upstream suffix.
	if _, err := io.CopyN(w, file, prefixSize); err != nil {
		file.Close()
		resp.Body.Close()
		h.finishInflight(key)
		return true
	}
	offset := prefixSize
	buf := make([]byte, 32*1024)
	complete := false
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			if _, err := file.WriteAt(buf[:n], offset); err != nil {
				break
			}
			offset += int64(n)
			if _, err := w.Write(buf[:n]); err != nil {
				break
			}
		}
		if readErr == io.EOF {
			complete = offset == prefixSize+tailLength
			break
		}
		if readErr != nil {
			break
		}
	}
	resp.Body.Close()
	if complete {
		_ = file.Truncate(offset)
	}
	file.Close()
	if !complete {
		h.finishInflight(key)
		return true
	}
	go func() {
		defer h.finishInflight(key)
		if err := h.convertFile(context.Background(), key, name); err != nil {
			log.Printf("xet mirror: convert resumed HTTP response %s: %v", key, err)
			return
		}
		_ = os.Remove(name)
	}()
	return true
}

func singleJoiningSlash(left, right string) string {
	return strings.TrimRight(left, "/") + "/" + strings.TrimLeft(right, "/")
}

func cacheKey(u *url.URL) string { return u.EscapedPath() }

func fileSize(sh *shard.Shard, hash xet.Hash) int64 {
	for _, file := range sh.Files {
		if file.FileHash == hash {
			var size int64
			for _, entry := range file.Entries {
				size += int64(entry.UnpackedSegBytes)
			}
			return size
		}
	}
	return 0
}

func parseHTTPRange(value string, size int64) (start, end int64, partial bool, err error) {
	if value == "" {
		return 0, size - 1, false, nil
	}
	if !strings.HasPrefix(value, "bytes=") || strings.Contains(value, ",") {
		return 0, 0, false, fmt.Errorf("unsupported range")
	}
	parts := strings.SplitN(strings.TrimPrefix(value, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, false, fmt.Errorf("invalid range")
	}
	if parts[0] == "" {
		var suffix int64
		if _, err = fmt.Sscan(parts[1], &suffix); err != nil || suffix <= 0 {
			return 0, 0, false, fmt.Errorf("invalid range")
		}
		if suffix > size {
			suffix = size
		}
		return size - suffix, size - 1, true, nil
	}
	if _, err = fmt.Sscan(parts[0], &start); err != nil || start < 0 || start >= size {
		return 0, 0, false, fmt.Errorf("range out of bounds")
	}
	end = size - 1
	if parts[1] != "" {
		if _, err = fmt.Sscan(parts[1], &end); err != nil || end < start {
			return 0, 0, false, fmt.Errorf("invalid range")
		}
		if end >= size {
			end = size - 1
		}
	}
	return start, end, true, nil
}

func (h *Handler) startHTTPBodyCache(key string, upstream io.ReadCloser) (io.ReadCloser, bool) {
	h.mu.Lock()
	if _, cached := h.index[key]; cached {
		h.mu.Unlock()
		return nil, false
	}
	if _, running := h.inflight[key]; running {
		h.mu.Unlock()
		return nil, false
	}
	cachePath := h.cacheFilePath(key)
	tmp, err := os.OpenFile(cachePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		h.mu.Unlock()
		log.Printf("xet mirror: create HTTP cache file for %s: %v", key, err)
		return nil, false
	}
	h.inflight[key] = struct{}{}
	h.mu.Unlock()
	return &cachingBody{
		upstream: upstream,
		file:     tmp,
		finish: func(complete bool) {
			if !complete {
				tmp.Close()
				h.finishInflight(key)
				return
			}
			if err := tmp.Close(); err != nil {
				h.finishInflight(key)
				return
			}
			go func() {
				defer h.finishInflight(key)
				if err := h.convertFile(context.Background(), key, tmp.Name()); err != nil {
					log.Printf("xet mirror: convert HTTP response %s: %v", key, err)
					return
				}
				_ = os.Remove(tmp.Name())
			}()
		},
	}, true
}

func (h *Handler) finishInflight(key string) {
	h.mu.Lock()
	delete(h.inflight, key)
	h.mu.Unlock()
}

// cachingBody tees bytes read from upstream. A premature Close keeps the
// SHA-256-named partial object for the next fill; only EOF promotes it to Xet.
type cachingBody struct {
	upstream io.ReadCloser
	file     *os.File
	finish   func(bool)
	once     sync.Once
	offset   int64
}

func (b *cachingBody) Read(p []byte) (int, error) {
	n, err := b.upstream.Read(p)
	if n > 0 {
		if _, writeErr := b.file.WriteAt(p[:n], b.offset); writeErr != nil {
			b.once.Do(func() { b.finish(false) })
			return n, err
		}
		b.offset += int64(n)
	}
	if err == io.EOF {
		if truncateErr := b.file.Truncate(b.offset); truncateErr != nil {
			b.once.Do(func() { b.finish(false) })
			return n, truncateErr
		}
		b.once.Do(func() { b.finish(true) })
	}
	return n, err
}

func (b *cachingBody) Close() error {
	b.once.Do(func() { b.finish(false) })
	return b.upstream.Close()
}

func (h *Handler) cacheFilePath(key string) string {
	digest := sha256.Sum256([]byte(key))
	return filepath.Join(h.cacheDir, "files", fmt.Sprintf("%x", digest[:]))
}

func (h *Handler) convertFile(ctx context.Context, key, name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	localHash, err := upload.UploadFile(ctx, localAdapter{h.store}, f, upload.WithConcurrency(h.concurrency), upload.WithCacheDir(h.cacheDir))
	f.Close()
	if err != nil {
		return err
	}
	h.mu.Lock()
	h.index[key] = localHash.String()
	err = h.saveIndexLocked()
	h.mu.Unlock()
	return err
}

func (h *Handler) loadIndex() error {
	b, err := os.ReadFile(h.indexPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &h.index)
}

func (h *Handler) loadMetadata() error {
	b, err := os.ReadFile(h.metadataPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &h.metadata)
}

func (h *Handler) saveIndexLocked() error {
	b, err := json.MarshalIndent(h.index, "", "  ")
	if err != nil {
		return err
	}
	tmp := h.indexPath + ".tmp"
	if err = os.WriteFile(tmp, b, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, h.indexPath); err != nil {
		return err
	}
	metadata, err := json.MarshalIndent(h.metadata, "", "  ")
	if err != nil {
		return err
	}
	metadataTmp := h.metadataPath + ".tmp"
	if err := os.WriteFile(metadataTmp, metadata, 0644); err != nil {
		return err
	}
	return os.Rename(metadataTmp, h.metadataPath)
}

func forwardedBaseURL(r *http.Request) string {
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "http"
		if r.TLS != nil {
			scheme = "https"
		}
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host
}
func parseLinks(values []string) map[string]string {
	out := map[string]string{}
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			part = strings.TrimSpace(part)
			end := strings.Index(part, ">")
			if !strings.HasPrefix(part, "<") || end < 0 {
				continue
			}
			for _, p := range strings.Split(part[end+1:], ";") {
				p = strings.TrimSpace(p)
				if strings.HasPrefix(strings.ToLower(p), "rel=") {
					out[strings.Trim(p[4:], "\"")] = part[1:end]
				}
			}
		}
	}
	return out
}

type localAdapter struct{ storage.Storage }

type localDownloadAdapter struct{ storage.Storage }

func (a localDownloadAdapter) DownloadXorbWithURL(ctx context.Context, rawURL string, header http.Header) (io.ReadCloser, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	hash, err := xet.ParseHash(filepath.Base(u.Path))
	if err != nil {
		return nil, err
	}
	r, err := a.Storage.GetXorbReadSeekCloser(ctx, "default", hash)
	if err != nil {
		return nil, err
	}
	if value := header.Get("Range"); value != "" {
		var start, end int64
		if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
			r.Close()
			return nil, err
		}
		if _, err := r.Seek(start, io.SeekStart); err != nil {
			r.Close()
			return nil, err
		}
		return &sectionReadCloser{Reader: io.LimitReader(r, end-start+1), Closer: r}, nil
	}
	return r, nil
}

func (a localDownloadAdapter) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	return nil, nil, fmt.Errorf("multipart xorb download is not used for HTTP reconstruction")
}

type sectionReadCloser struct {
	io.Reader
	io.Closer
}

func (a localAdapter) HasXorb(ctx context.Context, hash xet.Hash) (bool, error) {
	return a.Storage.HasXorb(ctx, "default", hash)
}
func (a localAdapter) UploadXorb(ctx context.Context, hash xet.Hash, r io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	inserted, err := a.Storage.PutXorb(ctx, "default", hash, r)
	return &upload.XorbUploadResponse{WasInserted: inserted}, err
}
func (a localAdapter) UploadShard(ctx context.Context, s *shard.Shard) (*upload.ShardUploadResponse, error) {
	inserted, err := a.Storage.PutShard(ctx, s)
	result := 0
	if inserted {
		result = 1
	}
	return &upload.ShardUploadResponse{Result: result}, err
}
func (a localAdapter) QueryDedupShards(ctx context.Context, hashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	out := make(map[xet.Hash]*upload.DeduplicationResult)
	for _, hash := range hashes {
		s, err := a.Storage.GetShardByChunkHash(ctx, "default", hash)
		if err != nil {
			continue
		}
		for _, cas := range s.CASInfos {
			for i, chunk := range cas.Chunks {
				if chunk.ChunkHash == hash {
					out[hash] = &upload.DeduplicationResult{ChunkHash: hash, IsNew: false, XorbHash: cas.CASHash, ChunkIndex: uint32(i)}
				}
			}
		}
	}
	return out, nil
}
