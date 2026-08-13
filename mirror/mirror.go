// Package mirror implements a full-cache middle layer that sits between a hub
// upstream (huggingface.co, an HF mirror, modelscope.cn, ...) and downstream
// clients, bridging every combination of xet-capable and plain peers.
//
// Upstream capability is detected per response and only from headers: the
// presence of the xet-reconstruction-info / xet-auth Link headers on the
// upstream resolve response. Downstream needs no detection: once a file is
// ready, the resolve response carries both the xet Link headers (pointing at
// the mirror's own CAS and token endpoints) and the normal plain redirect,
// so xet clients follow the links while plain clients ignore them.
//
// All bytes flow through the mirror and land in local storage as xorbs and
// shards. The first request for a file starts the one background ingestion
// download; concurrent requests (including the first) are served plain HTTP
// from the growing spool as bytes arrive. A client disconnect never cancels
// ingestion, and partial spool bytes survive task failures and process
// restarts: the next task resumes from them when the upstream etag still
// matches.
package mirror

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/hf"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"golang.org/x/sync/singleflight"
)

// resolveRe matches hub-style download paths. The prefix before /resolve/ is
// treated as an opaque repo identity, so no platform-specific routing exists.
var resolveRe = regexp.MustCompile(`^/(.+?)/resolve/([^/]+)/(.+)$`)

// apiTreeRe matches hub tree listing API paths. Their entries carry per-file
// xet hashes that would steer downstream clients straight to the upstream CAS.
var apiTreeRe = regexp.MustCompile(`^/api/(models|datasets|spaces)/(.+?)/tree/([^/]+)(/.*)?$`)

// xetTokenRe matches the hub token refresh route hub clients construct
// themselves ({endpoint}/api/{type}s/{repo}/xet-read-token/{revision})
// when the resolve response carries no xet-auth Link header.
var xetTokenRe = regexp.MustCompile(`^/api/(models|datasets|spaces)/(.+?)/xet-read-token/([^/]+)$`)

// commitRevRe matches revision strings that pin an immutable commit.
var commitRevRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

const (
	tokenEndpointPath  = "/xet-token"
	maxFetchAttempts   = 5
	failureBackoffBase = 10 * time.Second
	failureBackoffCap  = 10 * time.Minute
	maxFailureShift    = 6
)

var (
	errUpstreamNotFound = errors.New("upstream file not found")
	// errSpoolCorrupt marks spooled bytes that failed verification; the spool
	// must be discarded rather than kept for resume.
	errSpoolCorrupt = errors.New("spool corrupt")
)

// Handler is the mirror HTTP front end: resolve requests are served from the
// local cache (ingesting on miss) and the token endpoint hands downstream
// clients their CAS credential. Everything else falls through to next, which
// defaults to a reverse proxy that forwards to the upstream with the mirror's
// credentials injected (control plane only, never file bytes). Mount the
// Handler as the CAS server's next; token minting and validation are wired by
// the caller through WithMintToken and the server's AuthFunc.
type Handler struct {
	storage            storage.Storage
	upstreamRaw        string
	upstream           *url.URL
	upstreamToken      string
	external           string
	cacheDir           string
	indexDir           string
	spoolDir           string
	revalidateInterval time.Duration
	mintToken          func(now time.Time) (token string, exp int64)

	probeClient  *http.Client // does not follow redirects; used for metadata probes
	fetchClient  *http.Client // follows redirects; body drops resume via httpseek
	xetClient    *client.Client
	next         http.Handler
	localAdapter *localCAS

	mu       sync.Mutex
	flight   singleflight.Group
	entries  map[string]*fileEntry
	branches map[string]*branchEntry
	tasks    map[string]*task
}

// Option configures the Handler.
type Option func(*Handler)

// WithStorage sets the storage backend shared with the embedded CAS server. Required.
func WithStorage(s storage.Storage) Option {
	return func(h *Handler) { h.storage = s }
}

// WithUpstream sets the upstream hub base URL, e.g. https://huggingface.co. Required.
func WithUpstream(upstream string) Option {
	return func(h *Handler) { h.upstreamRaw = upstream }
}

// WithUpstreamToken sets the credential the mirror uses against the upstream
// hub. Downstream Authorization headers are never forwarded upstream.
func WithUpstreamToken(token string) Option {
	return func(h *Handler) { h.upstreamToken = token }
}

// WithCacheDir sets the directory holding the persisted index, in-flight
// spool files, and the xet chunk cache. Defaults to ./xet-mirror.
func WithCacheDir(dir string) Option {
	return func(h *Handler) { h.cacheDir = dir }
}

// WithExternalURL sets the externally visible base URL used in Link headers
// and minted tokens. When empty it is derived from each request.
func WithExternalURL(external string) Option {
	return func(h *Handler) { h.external = strings.TrimRight(external, "/") }
}

// WithMintToken sets the function the token endpoint uses to mint downstream
// CAS tokens, typically the Mint of a token.Issuer shared with the CAS
// server's AuthFunc. When unset the endpoint returns an empty anonymous
// token, suitable for an unauthenticated CAS.
func WithMintToken(mint func(now time.Time) (token string, exp int64)) Option {
	return func(h *Handler) { h.mintToken = mint }
}

// WithNext sets the handler for requests that are neither resolve nor token
// requests. When unset it defaults to a reverse proxy that forwards to the
// upstream with the mirror's credentials injected.
func WithNext(next http.Handler) Option {
	return func(h *Handler) { h.next = next }
}

// WithRevalidateInterval sets how often ready entries for branch (non-commit)
// revisions are re-checked against the upstream. Zero revalidates on every
// request; negative disables revalidation. Defaults to 5 minutes.
func WithRevalidateInterval(d time.Duration) Option {
	return func(h *Handler) { h.revalidateInterval = d }
}

// NewHandler creates a mirror handler.
func NewHandler(opts ...Option) (*Handler, error) {
	h := &Handler{
		cacheDir:           "./xet-mirror",
		revalidateInterval: 5 * time.Minute,
		entries:            map[string]*fileEntry{},
		tasks:              map[string]*task{},
	}
	for _, opt := range opts {
		opt(h)
	}

	if h.storage == nil {
		return nil, fmt.Errorf("mirror: storage is required")
	}
	if h.upstreamRaw == "" {
		return nil, fmt.Errorf("mirror: upstream is required")
	}
	u, err := url.Parse(h.upstreamRaw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("mirror: invalid upstream URL %q", h.upstreamRaw)
	}
	h.upstream = u

	h.indexDir = filepath.Join(h.cacheDir, "index")
	h.spoolDir = filepath.Join(h.cacheDir, "spool")
	branchDir := h.branchDir()
	chunkCacheDir := filepath.Join(h.cacheDir, "chunks")
	for _, dir := range []string{h.indexDir, branchDir, h.spoolDir, chunkCacheDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mirror: create %s: %w", dir, err)
		}
	}

	h.entries, err = loadIndex(h.indexDir)
	if err != nil {
		return nil, fmt.Errorf("mirror: load index: %w", err)
	}
	h.branches, err = loadBranches(branchDir)
	if err != nil {
		return nil, fmt.Errorf("mirror: load branches: %w", err)
	}

	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	injecting := &authInjector{inner: baseTransport, host: u.Host, token: h.upstreamToken}
	h.probeClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: injecting,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	h.fetchClient = &http.Client{
		Transport: httpseek.NewMustReaderTransport(injecting, func(r *http.Request, retry int, err error) error {
			if retry >= maxFetchAttempts {
				return fmt.Errorf("max retries reached: %w", err)
			}
			return nil
		}),
	}

	// Without an explicit cache dir the client would fall back to a shared
	// location under os.TempDir; keep the chunk cache with the mirror data.
	h.xetClient, err = client.NewClient(client.WithCacheDir(chunkCacheDir))
	if err != nil {
		return nil, fmt.Errorf("mirror: create xet client: %w", err)
	}

	h.localAdapter = &localCAS{storage: h.storage, namespace: "default"}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = u.Host
			pr.Out.Header.Del("Authorization")
			if h.upstreamToken != "" {
				pr.Out.Header.Set("Authorization", "Bearer "+h.upstreamToken)
			}
		},
		// Upstream response headers are dropped except the entity headers
		// describing the relayed body and the redirect target without which
		// 3xx cannot work.
		ModifyResponse: func(resp *http.Response) error {
			kept := http.Header{}
			for _, k := range []string{"Content-Type", "Content-Length", "Etag", "Date", "Location"} {
				if v := resp.Header.Get(k); v != "" {
					kept.Set(k, v)
				}
			}
			resp.Header = kept
			return nil
		},
	}
	if h.next == nil {
		h.next = proxy
	}

	return h, nil
}

// ServeHTTP handles resolve and token requests; everything else falls through
// to next (the upstream proxy by default).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.EscapedPath()
	if p == tokenEndpointPath && r.Method == http.MethodGet {
		h.handleToken(w, r)
		return
	}
	if r.Method == http.MethodGet && xetTokenRe.MatchString(p) {
		// Clients that skipped the resolve HEAD (hub tree caches) refresh
		// their CAS credential here; answer locally so they stay on the
		// mirror instead of the upstream CAS.
		h.handleToken(w, r)
		return
	}
	if r.Method == http.MethodGet && apiTreeRe.MatchString(p) {
		h.handleTree(w, r)
		return
	}
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && resolveRe.MatchString(p) {
		h.handleResolve(w, r, p)
		return
	}
	h.next.ServeHTTP(w, r)
}

// handleToken hands out a short-lived CAS token in the same JSON shape as the
// hub token endpoint, pointing casUrl at the mirror itself.
func (h *Handler) handleToken(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	var tok string
	exp := now.Add(15 * time.Minute).Unix()
	if h.mintToken != nil {
		tok, exp = h.mintToken(now)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"casUrl":      h.externalBase(r),
		"accessToken": tok,
		"exp":         exp,
	})
}

// externalBase returns the mirror's externally visible base URL.
func (h *Handler) externalBase(r *http.Request) string {
	if h.external != "" {
		return h.external
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}
	return scheme + "://" + r.Host
}

// upstreamURL maps a local resolve path to the upstream equivalent.
func (h *Handler) upstreamURL(key string) string {
	return strings.TrimRight(h.upstream.String(), "/") + key
}

func (h *Handler) branchDir() string {
	return filepath.Join(h.indexDir, "branches")
}

// branchStale reports whether a branch mapping must be re-checked, following
// the revalidation cadence: zero interval re-checks every request, negative
// never does.
func (h *Handler) branchStale(b *branchEntry) bool {
	if h.revalidateInterval < 0 {
		return false
	}
	return time.Since(b.CheckedAt) >= h.revalidateInterval
}

// branchProbe is the singleflight result of a branch mapping refresh. The
// probe result is handed back to the caller whose key was probed so its
// ingest task can reuse it instead of probing again.
type branchProbe struct {
	b   *branchEntry
	key string
	pr  *probeResult
}

// branchCommit resolves a branch revision to the upstream commit it points
// at, probing the upstream when the cached mapping is stale. It reports
// ok=false when no real commit is available (upstreams that synthesize
// pseudo-commits keep the branch-keyed flow with per-entry revalidation);
// probe failures fall back to the last known commit so cached content stays
// served while the upstream is unreachable. The returned probe result is
// non-nil when this call probed the upstream for exactly this key.
func (h *Handler) branchCommit(key string, m []string) (string, *probeResult, bool) {
	name := m[1] + "\x00" + m[2]
	h.mu.Lock()
	b := h.branches[name]
	h.mu.Unlock()
	if b != nil && !h.branchStale(b) {
		return b.Commit, nil, b.Commit != ""
	}

	v, _, _ := h.flight.Do("branch\x00"+name, func() (any, error) {
		h.mu.Lock()
		b := h.branches[name]
		h.mu.Unlock()
		if b != nil && !h.branchStale(b) {
			return &branchProbe{b: b}, nil
		}
		// Background context: the probe result is shared by every request of
		// the repo branch, so one disconnecting client must not fail it.
		pr, err := h.probe(context.Background(), key)
		if err != nil {
			return &branchProbe{b: b}, nil // unreachable upstream: serve the last known commit
		}
		nb := &branchEntry{Repo: m[1], Rev: m[2], CheckedAt: time.Now()}
		if pr.realCommit && commitRevRe.MatchString(pr.commit) {
			nb.Commit = pr.commit
		}
		h.mu.Lock()
		h.branches[name] = nb
		h.mu.Unlock()
		_ = persistBranch(h.branchDir(), nb)
		return &branchProbe{b: nb, key: key, pr: pr}, nil
	})
	bp := v.(*branchProbe)
	var pr *probeResult
	if bp.key == key {
		pr = bp.pr
	}
	if bp.b != nil {
		return bp.b.Commit, pr, bp.b.Commit != ""
	}
	return "", pr, false
}

// treeLFS is the lfs pointer block of a hub tree entry; OID is the sha256
// naming the file bytes. The entry itself stays a raw map because expand
// requests add fields (lastCommit, securityFileStatus, ...) that must pass
// through untouched.
type treeLFS struct {
	OID         string `json:"oid"`
	PointerSize int64  `json:"pointerSize"`
	Size        int64  `json:"size"`
}

// handleTree proxies a tree listing API request, rewriting each entry's
// xetHash: entries whose lfs sha256 oid resolves in local storage advertise
// the mirror's own hash so xet clients reconstruct them from the mirror CAS;
// all other entries lose the hash so clients fall back to the resolve flow,
// which ingests through the mirror. Left untouched, upstream hashes would
// send clients directly to the upstream CAS, bypassing the mirror entirely.
// Matching by content makes revision and repo irrelevant. The array is
// rewritten in a streaming pass, so a listing is never buffered whole; no
// upstream header is forwarded, only the body is relayed.
func (h *Handler) handleTree(w http.ResponseWriter, r *http.Request) {
	u := h.upstreamURL(r.URL.EscapedPath())
	if r.URL.RawQuery != "" {
		u += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	resp, err := h.fetchClient.Do(req)
	if err != nil {
		http.Error(w, "upstream tree fetch failed", http.StatusBadGateway)
		return
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body := bufio.NewReader(resp.Body)
	if resp.StatusCode != http.StatusOK || !startsJSONArray(body) {
		// Error status or unexpected shape: relay the bytes untouched.
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, body)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	dec := json.NewDecoder(body)
	if _, err := dec.Token(); err != nil {
		return
	}
	_, _ = io.WriteString(w, "[")
	for first := true; dec.More(); first = false {
		// Raw values keep every untouched field byte-exact through the re-encode.
		var item map[string]json.RawMessage
		if err := dec.Decode(&item); err != nil {
			// Mid-stream failure: stop without the closing bracket so the
			// client sees invalid JSON rather than a truncated listing.
			return
		}
		if _, ok := item["xetHash"]; ok {
			var lfs treeLFS
			if raw, ok := item["lfs"]; ok {
				_ = json.Unmarshal(raw, &lfs)
			}
			if hash, ok := h.lookupXetHash(r.Context(), lfs.OID); ok {
				quoted, _ := json.Marshal(hash)
				item["xetHash"] = quoted
			} else {
				delete(item, "xetHash")
			}
		}
		out, err := json.Marshal(item)
		if err != nil {
			return
		}
		if !first {
			_, _ = io.WriteString(w, ",")
		}
		if _, err := w.Write(out); err != nil {
			return
		}
	}
	_, _ = io.WriteString(w, "]")
}

// startsJSONArray peeks past leading whitespace and reports whether the next
// byte opens a JSON array, consuming nothing.
func startsJSONArray(br *bufio.Reader) bool {
	for i := 1; ; i++ {
		buf, _ := br.Peek(i)
		if len(buf) < i {
			return false
		}
		switch buf[i-1] {
		case ' ', '\t', '\r', '\n':
		default:
			return buf[i-1] == '['
		}
	}
}

// lookupXetHash resolves an lfs sha256 oid to the local xet file hash through
// the storage sha256 index; ok is false when the content is not held locally.
func (h *Handler) lookupXetHash(ctx context.Context, oid string) (string, bool) {
	digest, err := hex.DecodeString(oid)
	if err != nil || len(digest) != sha256.Size {
		return "", false
	}
	fileHash, err := h.storage.GetFileHashBySHA256(ctx, "default", [32]byte(digest))
	if err != nil {
		return "", false
	}
	return fileHash.String(), true
}

// task tracks one in-flight ingestion. All concurrent requests for the same
// key attach to it, so the upstream is downloaded exactly once per file.
type task struct {
	spool    *spool        // set by runTask before probed closes (when the probe succeeded)
	probed   chan struct{} // closed once probe metadata (or probeErr) is set
	sized    chan struct{} // closed once size is known or no early source remains
	sizeOnce sync.Once
	probeErr error
	notFound bool
	probe    *probeResult
	size     atomic.Int64 // final content length, -1 until known
}

// setSize records the content length once known (first value wins) and
// unblocks resolve replies waiting on it; n < 0 only signals.
func (t *task) setSize(n int64) {
	if n >= 0 {
		t.size.CompareAndSwap(-1, n)
	}
	t.sizeOnce.Do(func() { close(t.sized) })
}

// handleResolve serves GET/HEAD for hub-style download paths.
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request, key string) {
	m := resolveRe.FindStringSubmatch(key)
	rev := m[2]

	// Branch revisions are pinned to the upstream commit they point at, so
	// entries, tasks, and spools are all keyed by immutable content. The
	// branch mapping is re-checked on the revalidation cadence; one probe
	// covers every file of the repo branch and is reused by the ingest task.
	var preProbe *probeResult
	if !commitRevRe.MatchString(rev) {
		commit, pr, ok := h.branchCommit(key, m)
		preProbe = pr
		if ok {
			rev = commit
			key = "/" + m[1] + "/resolve/" + rev + "/" + m[3]
		}
	}

	h.mu.Lock()
	t := h.tasks[key]
	e := h.entries[key]
	h.mu.Unlock()

	if t != nil {
		h.serveTaskOrEntry(w, r, key, t)
		return
	}
	if e != nil {
		switch e.State {
		case stateReady:
			if h.needsRevalidate(e, rev) {
				e = h.revalidate(r.Context(), key, e)
			}
			if e != nil {
				h.serveReady(w, r, e)
				return
			}
			// stale: fall through and re-ingest
		case stateFailed:
			if time.Now().Before(e.nextRetry) {
				h.serveFailed(w, e)
				return
			}
		}
	}

	t, e, err := h.startTask(key, preProbe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if e != nil {
		// Lost the race: another request finished (or failed) the ingest first.
		h.serveEntry(w, r, e)
		return
	}
	h.serveTaskOrEntry(w, r, key, t)
}

// serveEntry answers from a completed entry, ready or failed.
func (h *Handler) serveEntry(w http.ResponseWriter, r *http.Request, e *fileEntry) {
	if e.State == stateReady {
		h.serveReady(w, r, e)
	} else {
		h.serveFailed(w, e)
	}
}

// serveTaskOrEntry streams from the in-flight task, falling back to the entry
// published by a completed ingest when the spool was drained and removed
// before this request could attach.
func (h *Handler) serveTaskOrEntry(w http.ResponseWriter, r *http.Request, key string, t *task) {
	if h.serveFromTask(w, r, t) {
		return
	}
	h.mu.Lock()
	e := h.entries[key]
	h.mu.Unlock()
	if e == nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	h.serveEntry(w, r, e)
}

// startTask returns the in-flight ingestion task for key, or the entry when
// the ingest already completed (or failed and is backing off). Only the task
// startup itself runs inside the singleflight, so the task is registered
// exactly once per key. A pre-probe result from the branch mapping refresh is
// handed to the task so the upstream is not probed twice.
func (h *Handler) startTask(key string, pre *probeResult) (*task, *fileEntry, error) {
	v, err, _ := h.flight.Do(key, func() (any, error) {
		// A previous flight may have registered a task, or finished the whole
		// ingest, between the caller's check and this one.
		h.mu.Lock()
		t := h.tasks[key]
		e := h.entries[key]
		h.mu.Unlock()
		if t != nil {
			return t, nil
		}
		if e != nil {
			if e.State == stateReady {
				return e, nil
			}
			if e.State == stateFailed && time.Now().Before(e.nextRetry) {
				return e, nil
			}
		}
		nt := &task{probed: make(chan struct{}), sized: make(chan struct{})}
		nt.size.Store(-1)
		h.mu.Lock()
		h.tasks[key] = nt
		h.mu.Unlock()
		go h.runTask(key, nt, pre)
		return nt, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if t, ok := v.(*task); ok {
		return t, nil, nil
	}
	return nil, v.(*fileEntry), nil
}

// runTask executes one ingestion end to end on a background context; client
// disconnects never cancel it. The spool opens after the probe so partial
// bytes from a previous failed task (or a previous process) are resumed when
// the upstream etag still matches. A non-nil pre stands in for the probe.
func (h *Handler) runTask(key string, t *task, pre *probeResult) {
	ctx := context.Background()

	pr, err := pre, error(nil)
	if pr == nil {
		pr, err = h.probe(ctx, key)
	}
	if err == nil {
		switch {
		case pr.status == http.StatusNotFound:
			err = errUpstreamNotFound
		case pr.status < 200 || pr.status >= 300:
			err = fmt.Errorf("upstream status %d", pr.status)
		}
	}
	if err != nil {
		t.probeErr = err
		t.notFound = errors.Is(err, errUpstreamNotFound)
		close(t.probed)
		h.failTask(key, t, err)
		return
	}

	sp, err := openSpool(h.spoolDir, key, pr.etag, pr.size)
	if err != nil {
		t.probeErr = err
		close(t.probed)
		h.failTask(key, t, err)
		return
	}
	t.spool = sp
	sp.acquire() // held by runTask until ingest completes
	defer sp.release()

	t.probe = pr
	if pr.size >= 0 {
		t.setSize(pr.size)
	}
	close(t.probed)
	defer t.setSize(-1) // unblock size waiters at the latest when the task ends

	switch {
	case pr.size >= 0 && sp.size() == pr.size:
		// A previous task already spooled the whole file (e.g. it failed
		// between fetch and ingest); skip the refetch.
	case pr.xet:
		// The xet ingest reports no size before completion; when the probe
		// found none there is no early source, so stop replies waiting for one.
		t.setSize(-1)
		err = h.fetchXet(ctx, t, key)
	default:
		err = h.fetchPlain(ctx, t, key)
	}
	if err == nil {
		if want := t.size.Load(); want >= 0 && t.spool.size() != want {
			err = fmt.Errorf("upstream size mismatch: got %d bytes, want %d", t.spool.size(), want)
			if t.spool.size() > want {
				t.spool.markRemove()
			}
		}
	}
	if err != nil {
		t.spool.finish(err)
		h.failTask(key, t, err)
		return
	}
	t.size.Store(t.spool.size())
	t.setSize(-1) // definitive size stored above; signal any waiters
	t.spool.finish(nil)

	entry, err := h.ingestSpool(ctx, t, key)
	if err != nil {
		if errors.Is(err, errSpoolCorrupt) {
			t.spool.markRemove()
		}
		h.failTask(key, t, err)
		return
	}

	h.mu.Lock()
	h.entries[key] = entry
	delete(h.tasks, key)
	h.mu.Unlock()
	t.spool.markRemove() // bytes now live in storage; drop the spool when drained
}

// failTask records a failure with exponential backoff and clears the task.
func (h *Handler) failTask(key string, t *task, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	failures := 1
	if prev := h.entries[key]; prev != nil && prev.State == stateFailed {
		failures = prev.failures + 1
	}
	shift := min(failures-1, maxFailureShift)
	backoff := min(failureBackoffBase<<shift, failureBackoffCap)
	h.entries[key] = &fileEntry{
		Key:       key,
		State:     stateFailed,
		failures:  failures,
		nextRetry: time.Now().Add(backoff),
		lastErr:   err,
		notFound:  t.notFound,
	}
	delete(h.tasks, key)
}

// fetchXet downloads the file through the upstream xet CAS into the spool,
// resuming from the current spool offset on retries. Resolve and token
// handling reuse the hf package: the returned provider refreshes short-lived
// CAS tokens from the upstream's xet-auth endpoint, and term fetches resume
// dropped bodies via the client's built-in httpseek transport. The resolve
// itself runs inside the retry loop so a transient failure there does not
// fail the whole task.
func (h *Handler) fetchXet(ctx context.Context, t *task, key string) error {
	return fetchWithRetries(ctx, "xet download", func() error {
		fileHash, provider, err := hf.ResolveDownload(ctx, h.probeClient, h.upstreamURL(key))
		if err != nil {
			return fmt.Errorf("resolve upstream xet download: %w", err)
		}
		return h.xetClient.DownloadFileWithAuthProvider(ctx, provider, fileHash, t.spool)
	})
}

// fetchPlain downloads the file bytes over plain HTTP into the spool, resuming
// from the current spool offset with Range requests on retries.
func (h *Handler) fetchPlain(ctx context.Context, t *task, key string) error {
	return fetchWithRetries(ctx, "plain download", func() error {
		return h.fetchPlainOnce(ctx, t, key)
	})
}

func fetchWithRetries(ctx context.Context, operation string, fetch func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxFetchAttempts; attempt++ {
		if err := sleepBackoff(ctx, attempt); err != nil {
			return err
		}
		lastErr = fetch()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("%s failed after %d attempts: %w", operation, maxFetchAttempts, lastErr)
}

func (h *Handler) fetchPlainOnce(ctx context.Context, t *task, key string) error {
	offset := t.spool.size()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.upstreamURL(key), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept-Encoding", "identity")
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	resp, err := h.fetchClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body := io.Reader(resp.Body)
	switch {
	case offset == 0 && resp.StatusCode == http.StatusOK:
		if resp.ContentLength >= 0 {
			t.setSize(resp.ContentLength)
		}
	case offset > 0 && resp.StatusCode == http.StatusPartialContent:
		if total := parseContentRangeTotal(resp.Header.Get("Content-Range")); total >= 0 {
			t.setSize(total)
		}
	case offset > 0 && resp.StatusCode == http.StatusOK:
		// Upstream ignored the Range; skip what the spool already holds.
		if _, err := io.CopyN(io.Discard, body, offset); err != nil {
			return fmt.Errorf("skip resumed bytes: %w", err)
		}
	default:
		return fmt.Errorf("upstream fetch status %d", resp.StatusCode)
	}

	_, err = io.Copy(t.spool, body)
	return err
}

func parseContentRangeTotal(value string) int64 {
	// Format: bytes <start>-<end>/<total>
	idx := strings.LastIndexByte(value, '/')
	if idx < 0 {
		return -1
	}
	total, err := strconv.ParseInt(value[idx+1:], 10, 64)
	if err != nil {
		return -1
	}
	return total
}

func sleepBackoff(ctx context.Context, attempt int) error {
	if attempt == 0 {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
		return nil
	}
}

// ingestSpool verifies the spooled bytes and runs the standard upload pipeline
// against local storage, then returns the ready index entry.
func (h *Handler) ingestSpool(ctx context.Context, t *task, key string) (*fileEntry, error) {
	f, err := os.Open(t.spool.f.Name())
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return nil, fmt.Errorf("hash spool: %w", err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if t.probe.sha256 != "" && digest != t.probe.sha256 {
		return nil, fmt.Errorf("%w: sha256 mismatch: got %s, upstream advertised %s", errSpoolCorrupt, digest, t.probe.sha256)
	}

	entry := &fileEntry{
		Key:       key,
		State:     stateReady,
		SHA256:    digest,
		Size:      size,
		ETag:      t.probe.etag,
		Commit:    t.probe.commit,
		CheckedAt: time.Now(),
	}

	if size > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind spool: %w", err)
		}
		fileHash, err := upload.UploadFile(ctx, h.localAdapter, f,
			upload.WithEnableSHA256(true),
			upload.WithConcurrency(4),
			upload.WithCacheDir(h.spoolDir),
		)
		if err != nil {
			return nil, fmt.Errorf("ingest into storage: %w", err)
		}
		entry.FileHash = fileHash.String()
	}

	if err := persistEntry(h.indexDir, entry); err != nil {
		return nil, err
	}
	return entry, nil
}

// needsRevalidate reports whether a ready entry must be re-checked upstream.
func (h *Handler) needsRevalidate(e *fileEntry, rev string) bool {
	if h.revalidateInterval < 0 || commitRevRe.MatchString(rev) {
		return false
	}
	return time.Since(e.CheckedAt) >= h.revalidateInterval
}

// revalidate re-probes the upstream. It returns the entry to serve, or nil
// when the entry went stale and must be re-ingested. Upstream errors keep
// serving the cached copy.
func (h *Handler) revalidate(ctx context.Context, key string, e *fileEntry) *fileEntry {
	pr, err := h.probe(ctx, key)
	if err != nil || pr.status < 200 || pr.status >= 300 {
		return e
	}
	if pr.etag == e.ETag {
		e.CheckedAt = time.Now()
		_ = persistEntry(h.indexDir, e)
		return e
	}
	h.mu.Lock()
	if h.entries[key] == e {
		delete(h.entries, key)
	}
	h.mu.Unlock()
	_ = os.Remove(indexEntryPath(h.indexDir, e.Commit, key))
	return nil
}

// serveReady answers a resolve request for a fully cached file: metadata plus
// xet Link headers for capable clients, and a redirect to the sha256 bridge
// for everyone else.
func (h *Handler) serveReady(w http.ResponseWriter, r *http.Request, e *fileEntry) {
	base := h.externalBase(r)
	writeMetadataHeaders(w, e.ETag, e.Size, e.Commit)
	if e.FileHash != "" {
		w.Header().Add("Link", fmt.Sprintf("<%s%s>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, tokenEndpointPath, base, e.FileHash))
		w.Header().Set("X-Xet-Hash", e.FileHash)
	}
	if e.Size == 0 {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	// The Location must be absolute: hub clients follow relative redirects
	// before reading metadata, which would strip the xet headers off the
	// response they end up looking at.
	http.Redirect(w, r, base+"/xet-bridge/"+e.SHA256, http.StatusFound)
}

func (h *Handler) serveFailed(w http.ResponseWriter, e *fileEntry) {
	serveFetchError(w, e.notFound, e.lastErr)
}

// serveFetchError maps a failed ingest to its HTTP response: 404 when the
// upstream lacks the file, 502 with the underlying error otherwise.
func serveFetchError(w http.ResponseWriter, notFound bool, err error) {
	if notFound {
		http.Error(w, "File not found upstream", http.StatusNotFound)
		return
	}
	msg := "upstream fetch failed"
	if err != nil {
		msg = err.Error()
	}
	http.Error(w, msg, http.StatusBadGateway)
}

// serveFromTask streams a file that is still being ingested. Bytes are served
// from the growing spool; range requests for regions not yet spooled block
// until the data lands. It reports false when the ingest finished and the
// spool was already drained before this request could attach.
func (h *Handler) serveFromTask(w http.ResponseWriter, r *http.Request, t *task) bool {
	select {
	case <-t.probed:
	case <-r.Context().Done():
		return true
	}
	if t.probeErr != nil {
		serveFetchError(w, t.notFound, t.probeErr)
		return true
	}

	pr := t.probe
	// The size may only be learned from the ingest download's first response
	// headers (e.g. modelscope.cn sends none on HEAD); downstream hub clients
	// refuse metadata without it, so wait rather than answer size-less.
	select {
	case <-t.sized:
	case <-r.Context().Done():
		return true
	}
	size := t.size.Load()
	if r.Method == http.MethodHead {
		writeMetadataHeaders(w, pr.etag, size, pr.commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		if size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
		w.WriteHeader(http.StatusOK)
		return true
	}

	if size < 0 {
		// Total size unknown until the ingest completes (xet upstream whose
		// probe carried no size): ServeContent cannot handle that, so stream
		// the whole body as it lands, ignoring Range.
		rc := t.spool.newReader(r.Context(), 0)
		if rc == nil {
			return false
		}
		defer func() {
			_ = rc.Close()
		}()
		writeMetadataHeaders(w, pr.etag, size, pr.commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, rc)
		return true
	}

	rs := t.spool.newSeekReader(r.Context(), size)
	if rs == nil {
		return false
	}
	defer func() {
		_ = rs.Close()
	}()
	writeMetadataHeaders(w, pr.etag, size, pr.commit)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, rs)
	return true
}

// writeMetadataHeaders emits the header set downstream tooling relies on,
// mirroring what the upstream hub advertised.
func writeMetadataHeaders(w http.ResponseWriter, etag string, size int64, commit string) {
	if etag != "" {
		quoted := `"` + etag + `"`
		w.Header().Set("ETag", quoted)
		w.Header().Set("X-Linked-Etag", quoted)
	}
	if size >= 0 {
		w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
	}
	if commit != "" {
		w.Header().Set("X-Repo-Commit", commit)
	}
	w.Header().Set("Accept-Ranges", "bytes")
}
