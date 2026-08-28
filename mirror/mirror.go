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
// still matches.
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
	"github.com/wzshiming/xet/storage"
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

// Mirror is the ingestion engine: resolutions are answered from the local
// cache (ingesting on miss) while every byte is published to storage as
// xorbs and shards. It serves the server/hf package's hub front end through
// Resolve, LookupXetHash, and FetchUpstream; token minting and the
// downstream HTTP surface are wired there.
type Mirror struct {
	storage            storage.Storage
	upstreamRaw        string
	upstream           *url.URL
	upstreamToken      string
	cacheDir           string
	indexDir           string
	spoolDir           string
	revalidateInterval time.Duration

	probeClient  *http.Client // does not follow redirects; used for metadata probes
	fetchClient  *http.Client // follows redirects; body drops resume via httpseek
	xetClient    *client.Client
	localAdapter *localCAS

	mu       sync.Mutex
	flight   singleflight.Group
	entries  map[resolveKey]*fileEntry
	branches map[string]*branchEntry
	tasks    map[resolveKey]*task
}

// Option configures the Mirror.
type Option func(*Mirror)

// WithStorage sets the storage backend shared with the embedded CAS server. Required.
func WithStorage(s storage.Storage) Option {
	return func(m *Mirror) { m.storage = s }
}

// WithUpstream sets the upstream hub base URL, e.g. https://huggingface.co. Required.
func WithUpstream(upstream string) Option {
	return func(m *Mirror) { m.upstreamRaw = upstream }
}

// WithUpstreamToken sets the credential the mirror uses against the upstream
// hub. Downstream Authorization headers are never forwarded upstream.
func WithUpstreamToken(token string) Option {
	return func(m *Mirror) { m.upstreamToken = token }
}

// WithCacheDir sets the directory holding the persisted index and in-flight
// spool files. Defaults to ./xet-mirror.
func WithCacheDir(dir string) Option {
	return func(m *Mirror) { m.cacheDir = dir }
}

// WithClient sets the xet client used for upstream xet downloads, letting
// the caller configure it (chunk cache location, concurrency, ...). When
// unset a default client is created.
func WithClient(c *client.Client) Option {
	return func(m *Mirror) { m.xetClient = c }
}

// WithRevalidateInterval sets how often ready entries for branch (non-commit)
// revisions are re-checked against the upstream. Zero revalidates on every
// request; negative disables revalidation. Defaults to 5 minutes.
func WithRevalidateInterval(d time.Duration) Option {
	return func(m *Mirror) { m.revalidateInterval = d }
}

// NewMirror creates a mirror engine.
func NewMirror(opts ...Option) (*Mirror, error) {
	m := &Mirror{
		cacheDir:           "./xet-mirror",
		revalidateInterval: 5 * time.Minute,
		entries:            map[resolveKey]*fileEntry{},
		tasks:              map[resolveKey]*task{},
	}
	for _, opt := range opts {
		opt(m)
	}

	if m.storage == nil {
		return nil, fmt.Errorf("mirror: storage is required")
	}
	if m.upstreamRaw == "" {
		return nil, fmt.Errorf("mirror: upstream is required")
	}
	u, err := url.Parse(m.upstreamRaw)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("mirror: invalid upstream URL %q", m.upstreamRaw)
	}
	m.upstream = u

	m.indexDir = filepath.Join(m.cacheDir, "index")
	m.spoolDir = filepath.Join(m.cacheDir, "spool")
	branchDir := m.branchDir()
	for _, dir := range []string{m.indexDir, branchDir, m.spoolDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("mirror: create %s: %w", dir, err)
		}
	}

	m.entries, err = loadIndex(m.indexDir)
	if err != nil {
		return nil, fmt.Errorf("mirror: load index: %w", err)
	}
	m.branches, err = loadBranches(branchDir)
	if err != nil {
		return nil, fmt.Errorf("mirror: load branches: %w", err)
	}

	baseTransport := http.DefaultTransport.(*http.Transport).Clone()
	injecting := &authInjector{inner: baseTransport, host: u.Host, token: m.upstreamToken}
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

	if m.xetClient == nil {
		m.xetClient, err = client.NewClient()
		if err != nil {
			return nil, fmt.Errorf("mirror: create xet client: %w", err)
		}
	}

	m.localAdapter = &localCAS{storage: m.storage, namespace: "default"}

	return m, nil
}
