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
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/storage"
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
	// ErrUpstreamNotFound reports that the upstream hub has no file at the
	// requested key. Errors returned by Ingest match it with errors.Is.
	ErrUpstreamNotFound = errors.New("upstream file not found")
	// errSpoolCorrupt marks spooled bytes that failed verification; the spool
	// must be discarded rather than kept for resume.
	errSpoolCorrupt = errors.New("spool corrupt")
)

// resolveKey identifies one (repo, rev, path) file and keys the in-memory
// entry and task maps. Fields hold escaped URL path segments exactly as they
// appear in the hub-style resolve path.
type resolveKey struct {
	repo string
	rev  string
	path string
}

// parseResolveKey splits a hub-style download path into its key.
func parseResolveKey(p string) (resolveKey, bool) {
	m := resolveRe.FindStringSubmatch(p)
	if m == nil {
		return resolveKey{}, false
	}
	return resolveKey{repo: m[1], rev: m[2], path: m[3]}, true
}

// String renders the hub-style resolve path, the form used for upstream
// URLs, spool names, and the persisted index.
func (k resolveKey) String() string {
	return "/" + k.repo + "/resolve/" + k.rev + "/" + k.path
}

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
	entries  map[resolveKey]*fileEntry
	branches map[string]*branchEntry
	tasks    map[resolveKey]*task
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

// WithCacheDir sets the directory holding the persisted index and in-flight
// spool files. Defaults to ./xet-mirror.
func WithCacheDir(dir string) Option {
	return func(h *Handler) { h.cacheDir = dir }
}

// WithClient sets the xet client used for upstream xet downloads, letting
// the caller configure it (chunk cache location, concurrency, ...). When
// unset a default client is created.
func WithClient(c *client.Client) Option {
	return func(h *Handler) { h.xetClient = c }
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
		entries:            map[resolveKey]*fileEntry{},
		tasks:              map[resolveKey]*task{},
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
	for _, dir := range []string{h.indexDir, branchDir, h.spoolDir} {
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

	if h.xetClient == nil {
		h.xetClient, err = client.NewClient()
		if err != nil {
			return nil, fmt.Errorf("mirror: create xet client: %w", err)
		}
	}

	h.localAdapter = &localCAS{storage: h.storage, namespace: "default"}

	if h.next == nil {
		h.next = h.newUpstreamProxy()
	}

	return h, nil
}

// newUpstreamProxy builds the default next handler: a reverse proxy that
// forwards control-plane requests to the upstream with the mirror's
// credentials injected.
func (h *Handler) newUpstreamProxy() http.Handler {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(h.upstream)
			pr.Out.Host = h.upstream.Host
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
			for _, k := range []string{"Content-Type", "Content-Length", "Content-Encoding", "Etag", "Date", "Location"} {
				if v := resp.Header.Get(k); v != "" {
					kept.Set(k, v)
				}
			}
			resp.Header = kept
			return nil
		},
	}
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
