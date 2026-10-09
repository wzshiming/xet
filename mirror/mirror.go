// Package mirror implements the ingestion engine of a full-cache middle
// layer that sits between a hub upstream (huggingface.co, an HF mirror,
// modelscope.cn, ...) and downstream clients, bridging every combination of
// xet-capable and plain peers.
//
// Upstream capability is detected per response and only from headers: the
// presence of the xet-reconstruction-info / xet-auth Link headers on the
// upstream resolve response. Capable upstreams are ingested over the xet
// protocol, plain upstreams over ranged HTTP; either way all bytes flow
// through the mirror and land in local storage as xorbs and shards.
//
// The first resolution of a file starts the one background ingestion
// download; concurrent resolutions (including the first) attach to it and
// read from the growing spool as bytes arrive. Abandoning a resolution never
// cancels ingestion, and partial spool bytes survive task failures and
// process restarts: the next task resumes from them when the upstream etag
// still matches. Downloads are shared by content within one upstream origin:
// hub etags are content hashes, so a file requested under another path,
// commit or repository of the same origin joins the download already running
// for its etag and resumes the spool that origin and etag name, while another
// origin advertising the same etag is never trusted with them. Content whose
// sha256 local storage already holds is published without downloading at all,
// whatever its origin: storage verified the digest when the file was stored.
//
// The package exposes no HTTP surface of its own. The downstream hub routes
// (resolve, token, tree) are implemented by the server/hf package on top of
// the exported boundary: Resolve for cache-or-ingest resolution,
// LookupXetHash for tree rewriting, and FetchUpstream for authenticated
// upstream reads.
package mirror

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"

	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/client/hf"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/internal/lru"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
	"golang.org/x/sync/singleflight"
)

// resolveRe matches hub-style download paths. The prefix before /resolve/ is
// treated as an opaque repo identity, so no platform-specific routing exists.
var resolveRe = regexp.MustCompile(`^/(.+?)/resolve/([^/]+)/(.+)$`)

// commitRevRe matches revision strings that pin an immutable commit.
var commitRevRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

const (
	maxFetchAttempts   = 5
	failureBackoffBase = 10 * time.Second
	failureBackoffCap  = 10 * time.Minute
	maxFailureShift    = 6
	defaultMaxIngests  = 16
	maxFailedEntries   = 16384 // bounds the process-local failure records a 404 scan can accumulate
)

var (
	// ErrUpstreamNotFound reports that the upstream hub has no file at the
	// requested key. Errors returned by Resolve and Ingest match it with
	// errors.Is.
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
	seg := resolveRe.FindStringSubmatch(p)
	if seg == nil {
		return resolveKey{}, false
	}
	return resolveKey{repo: seg[1], rev: seg[2], path: seg[3]}, true
}

// String renders the hub-style resolve path, the form used for upstream
// URLs, spool names, and the persisted index.
func (k resolveKey) String() string {
	return "/" + k.repo + "/resolve/" + k.rev + "/" + k.path
}

// revKey identifies one (repo, rev) pair: a branch pointer or a commit manifest.
type revKey struct {
	repo string
	rev  string
}

func (k resolveKey) revKey() revKey {
	return revKey{repo: k.repo, rev: k.rev}
}

// upstreamOrigin validates raw as an absolute http(s) URL and returns it with its scheme://host origin.
func upstreamOrigin(raw string) (*url.URL, string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, "", fmt.Errorf("mirror: invalid upstream URL %q", raw)
	}
	return u, u.Scheme + "://" + u.Host, nil
}

// parseUpstreamURL splits a hub download URL into its origin and path-only key.
func parseUpstreamURL(raw string) (string, resolveKey, error) {
	u, origin, err := upstreamOrigin(raw)
	if err != nil {
		return "", resolveKey{}, err
	}
	key, ok := parseResolveKey(u.EscapedPath())
	if !ok {
		return "", resolveKey{}, fmt.Errorf("mirror: %q is not a hub download URL", raw)
	}
	return origin, key, nil
}

// Mirror is the ingestion engine: resolutions are answered from the local
// cache (ingesting on miss) while every byte is published to storage as
// xorbs and shards. Files are named by their hub download path (repo,
// revision, path) alone; a call's origin and token only say where and how to
// fetch, and the first resolver of a file pins both on its ingest. A mirror
// must therefore route each repository to a single upstream, as the
// server/hf selector does. It serves the server/hf package's hub front end
// through Resolve, LookupXetHash, and FetchUpstream; upstream selection,
// token minting and the downstream HTTP surface are wired there.
type Mirror struct {
	storage            storage.Storage
	cacheDir           string
	indexDir           string
	spoolDir           string
	revalidateInterval time.Duration
	maxIngests         int
	ingestSlots        chan struct{}
	transport          http.RoundTripper

	probeClient  *http.Client  // does not follow redirects; used for metadata probes
	fetchClient  *http.Client  // follows redirects; body drops resume via httpseek
	hubClient    *http.Client  // hub and CAS requests of the per-download hf clients; hf.NewClient adds the no-redirect and idle guards
	cache        *client.Cache // chunk cache shared by the per-download xet clients
	clientOpts   []client.Options
	localAdapter *localCAS

	mu        sync.Mutex
	spoolMu   sync.Mutex // serializes spool opens against SweepSpools
	persistMu sync.Mutex // orders index snapshots and writes
	flight    singleflight.Group
	entries   map[resolveKey]*fileEntry
	branches  map[revKey]*branchEntry // branch rev -> pin, loaded lazily from disk
	commits   map[revKey]*commitState // commit -> manifest state, loaded lazily from disk
	tasks     map[resolveKey]*task
	inflight  map[string]*task                // origin and content hash -> the task downloading it; later tasks for that content on that origin follow it
	failed    lru.Cache[resolveKey, struct{}] // failed entries by recency; eviction drops them from entries
}

// Option configures the Mirror.
type Option func(*Mirror)

// WithStorage sets the storage backend shared with the embedded CAS server. Required.
func WithStorage(s storage.Storage) Option {
	return func(m *Mirror) { m.storage = s }
}

// WithCacheDir stores indexes, spools, the chunk cache and upload staging under dir (index, spool, download, upload); defaults to ./xet-mirror.
func WithCacheDir(dir string) Option {
	return func(m *Mirror) { m.cacheDir = dir }
}

// WithClientOptions configures the per-download xet clients; the mirror sets their transport, chunk cache and upstream provider itself (WithCache in opts replaces the mirror's cache).
func WithClientOptions(opts ...client.Options) Option {
	return func(m *Mirror) { m.clientOpts = opts }
}

// WithRevalidateInterval sets how often ready entries for branch (non-commit)
// revisions are re-checked against the upstream. Zero revalidates on every
// request; negative disables revalidation. Defaults to 5 minutes.
func WithRevalidateInterval(d time.Duration) Option {
	return func(m *Mirror) { m.revalidateInterval = d }
}

// WithMaxConcurrentIngests limits active ingests; nonpositive values use 16.
func WithMaxConcurrentIngests(n int) Option {
	return func(m *Mirror) { m.maxIngests = n }
}

// WithTransport sets the transport under the upstream auth, idle, and resume wrappers, and of the per-download xet clients; nil uses a clone of http.DefaultTransport.
func WithTransport(rt http.RoundTripper) Option {
	return func(m *Mirror) { m.transport = rt }
}

// NewMirror creates a mirror engine.
func NewMirror(opts ...Option) (*Mirror, error) {
	m := &Mirror{
		cacheDir:           "./xet-mirror",
		revalidateInterval: 5 * time.Minute,
		maxIngests:         defaultMaxIngests,
		entries:            map[resolveKey]*fileEntry{},
		branches:           map[revKey]*branchEntry{},
		commits:            map[revKey]*commitState{},
		tasks:              map[resolveKey]*task{},
		inflight:           map[string]*task{},
	}
	m.failed.MaxEntries = maxFailedEntries
	// Runs under m.mu: every touchFailed and forgetFailed caller holds it.
	m.failed.OnEvicted = func(k resolveKey, _ struct{}) {
		if e := m.entries[k]; e != nil && e.State == stateFailed {
			delete(m.entries, k)
		}
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.storage == nil {
		return nil, fmt.Errorf("mirror: storage is required")
	}

	if m.maxIngests <= 0 {
		m.maxIngests = defaultMaxIngests
	}
	m.ingestSlots = make(chan struct{}, m.maxIngests)

	m.indexDir = filepath.Join(m.cacheDir, "index")
	m.spoolDir = filepath.Join(m.cacheDir, "spool")
	for _, dir := range []string{m.indexDir, m.spoolDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mirror: create %s: %w", dir, err)
		}
	}

	if m.transport == nil {
		m.transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	injecting := &authInjector{inner: client.NewIdleTimeoutTransport(m.transport, client.DefaultIdleTimeout)}
	m.probeClient = &http.Client{
		Timeout:   30 * time.Second,
		Transport: injecting,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	m.fetchClient = &http.Client{
		Transport: httpseek.NewMustReaderTransport(injecting, func(r *http.Request, retry int, err error) error {
			if retry >= maxFetchAttempts {
				return fmt.Errorf("max retries reached: %w", err)
			}
			return nil
		}),
	}

	m.hubClient = &http.Client{Transport: m.transport}
	m.cache = client.NewCache(m.cacheDir, download.DefaultCacheSize, upload.DefaultCacheSize)

	m.localAdapter = &localCAS{storage: m.storage, namespace: "default"}

	return m, nil
}

// newClient binds a xet client to repo with token for one download: the mirror's transport and cache, the caller's options, and the repository's token endpoints bound last.
func (m *Mirror) newClient(repo hf.Repo, token string) (*hf.Client, error) {
	return hf.NewClient(repo, hf.WithHTTPClient(m.hubClient), hf.WithToken(token), hf.WithClientOptions(append([]client.Options{client.WithCache(m.cache)}, m.clientOpts...)...))
}
