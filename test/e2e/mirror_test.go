package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/mirror"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/token"
)

// fakeHub is a configurable plain hub: resolve requests answer metadata
// headers and redirect to a /cdn path that serves the bytes.
type fakeHub struct {
	mu    sync.Mutex
	files map[string][]byte

	commit        string // "" omits X-Repo-Commit
	etagOverride  string // "" uses the real sha256 of the data
	sizeOverride  string // "" uses the real length
	omitSize      bool   // no X-Linked-Size on resolve, no Content-Length on cdn HEAD
	resolveStatus int    // when > 0, every resolve request answers this status

	gate         chan struct{} // when set, non-Range cdn GETs stall halfway until closed
	gateHit      chan struct{}
	gateOnce     sync.Once
	dropNextData atomic.Bool // abort the next cdn GET halfway through the body

	probes   atomic.Int64 // HEAD requests on resolve paths
	dataGETs atomic.Int64 // GET requests on cdn paths
}

func newFakeHub() *fakeHub {
	return &fakeHub{files: map[string][]byte{}, commit: "commit-e2e"}
}

func (u *fakeHub) set(path string, data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[path] = data
}

func (u *fakeHub) get(path string) ([]byte, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	data, ok := u.files[path]
	return data, ok
}

func (u *fakeHub) etagFor(data []byte) string {
	if u.etagOverride != "" {
		return u.etagOverride
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (u *fakeHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/cdn/") {
		u.serveData(w, r, strings.TrimPrefix(r.URL.Path, "/cdn"))
		return
	}
	u.serveResolve(w, r)
}

func (u *fakeHub) serveResolve(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		u.probes.Add(1)
	}
	if u.resolveStatus > 0 {
		w.WriteHeader(u.resolveStatus)
		return
	}
	data, ok := u.get(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	etag := u.etagFor(data)
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	if !u.omitSize {
		size := u.sizeOverride
		if size == "" {
			size = fmt.Sprint(len(data))
		}
		w.Header().Set("X-Linked-Size", size)
	}
	if u.commit != "" {
		w.Header().Set("X-Repo-Commit", u.commit)
	}
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

func (u *fakeHub) serveData(w http.ResponseWriter, r *http.Request, path string) {
	data, ok := u.get(path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodHead {
		if !u.omitSize {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	u.dataGETs.Add(1)
	if u.dropNextData.CompareAndSwap(true, false) {
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data[:len(data)/2])
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler) // simulate a mid-body connection drop
	}
	if u.gate != nil && r.Header.Get("Range") == "" {
		w.Header().Set("Content-Length", fmt.Sprint(len(data)))
		w.WriteHeader(http.StatusOK)
		half := len(data) / 2
		_, _ = w.Write(data[:half])
		w.(http.Flusher).Flush()
		u.gateOnce.Do(func() { close(u.gateHit) })
		<-u.gate
		_, _ = w.Write(data[half:])
		return
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
}

// newMirrorServer wires the same composition as cmd/xetd: the CAS server
// matches its routes first and falls through to the mirror handler.
func newMirrorServer(t *testing.T, upstreamURL, storageDir, cacheDir string, opts ...mirror.Option) *httptest.Server {
	t.Helper()
	var inner atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inner.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)

	stor, err := storage.NewFileStorage(
		storage.WithBasePath(storageDir),
		storage.WithBaseURL(srv.URL),
	)
	if err != nil {
		t.Fatal(err)
	}
	issuer, err := token.NewIssuer(nil, 15*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := mirror.NewUpstreamProxy(upstreamURL, "")
	if err != nil {
		t.Fatal(err)
	}
	h, err := mirror.NewHandler(
		append([]mirror.Option{
			mirror.WithStorage(stor),
			mirror.WithUpstream(upstreamURL),
			mirror.WithExternalURL(srv.URL),
			mirror.WithCacheDir(cacheDir),
			mirror.WithMintToken(issuer.Mint),
			mirror.WithNext(proxy),
		}, opts...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	inner.Store(http.Handler(server.NewHandler(
		server.WithStorage(stor),
		server.WithAuthFunc(func(tok string) bool { return issuer.Validate(tok, time.Now()) }),
		server.WithNext(h),
	)))
	return srv
}

func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// waitMirrorReady polls until the mirror answers with the ready-state
// redirect and returns that response (body already consumed).
func waitMirrorReady(t *testing.T, resolveURL string) *http.Response {
	t.Helper()
	c := noRedirectClient()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusFound {
			return resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("mirror never reached ready state")
	return nil
}

// pollResolveStatus polls until the resolve URL answers with the wanted
// status and returns the response body.
func pollResolveStatus(t *testing.T, resolveURL string, want int) string {
	t.Helper()
	c := noRedirectClient()
	deadline := time.Now().Add(15 * time.Second)
	lastStatus := -1
	lastBody := ""
	for time.Now().Before(deadline) {
		resp, err := c.Get(resolveURL)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body) // mid-ingest bodies may abort; only the status matters
		resp.Body.Close()
		lastStatus, lastBody = resp.StatusCode, string(b)
		if lastStatus == want {
			return lastBody
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("resolve never returned %d, last status %d body %q", want, lastStatus, lastBody)
	return ""
}

// waitIndexPersisted waits for the on-disk index entry, the mirror's
// persistence contract for ready files. Entries live under per-commit
// directories; branch mappings under index/branches do not count.
func waitIndexPersisted(t *testing.T, cacheDir string) {
	t.Helper()
	dir := filepath.Join(cacheDir, "index")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		_ = filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			if de.IsDir() {
				if de.Name() == "branches" {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasSuffix(de.Name(), ".json") {
				found = true
				return filepath.SkipAll
			}
			return nil
		})
		if found {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("index entry never persisted")
}

func getBody(t *testing.T, url string) []byte {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200; body %q", resp.StatusCode, body)
	}
	return body
}

// TestMirrorEmptyFile: zero-byte files are cached and served as a direct 200
// with no redirect and no xet Link headers, and survive a restart.
func TestMirrorEmptyFile(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/empty.bin"
	hub.set(resolvePath, []byte{})

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	srv := newMirrorServer(t, hubSrv.URL, storageDir, cacheDir)
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); len(body) != 0 {
		t.Fatalf("first request body = %d bytes, want 0", len(body))
	}
	waitIndexPersisted(t, cacheDir)

	emptySum := sha256.Sum256(nil)
	wantETag := `"` + hex.EncodeToString(emptySum[:]) + `"`

	resp, err := noRedirectClient().Get(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ready status = %d, want 200 (not a redirect)", resp.StatusCode)
	}
	if resp.ContentLength != 0 {
		t.Fatalf("Content-Length = %d, want 0", resp.ContentLength)
	}
	if got := resp.Header.Get("ETag"); got != wantETag {
		t.Fatalf("ETag = %q, want %q", got, wantETag)
	}
	if got := resp.Header.Get("X-Linked-Size"); got != "0" {
		t.Fatalf("X-Linked-Size = %q, want 0", got)
	}
	if links := resp.Header.Values("Link"); len(links) != 0 {
		t.Fatalf("empty file must carry no xet Link headers, got %q", links)
	}

	// Restart with the same storage and cache: still served, no re-probe.
	probesBefore := hub.probes.Load()
	srv2 := newMirrorServer(t, hubSrv.URL, storageDir, cacheDir)
	if body := getBody(t, srv2.URL+resolvePath); len(body) != 0 {
		t.Fatalf("post-restart body = %d bytes, want 0", len(body))
	}
	if got := hub.probes.Load(); got != probesBefore {
		t.Fatalf("restart re-probed upstream: %d -> %d", probesBefore, got)
	}
}

// TestMirrorUpstreamFailureCached: probe failures are answered immediately and
// cached with backoff, so repeated requests do not hammer the upstream.
func TestMirrorUpstreamFailureCached(t *testing.T) {
	t.Run("server error maps to 502", func(t *testing.T) {
		hub := newFakeHub()
		hub.resolveStatus = http.StatusInternalServerError
		hubSrv := httptest.NewServer(hub)
		defer hubSrv.Close()

		srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
		resolveURL := srv.URL + "/org/repo/resolve/main/broken.bin"

		for i := range 2 {
			resp, err := http.Get(resolveURL)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadGateway {
				t.Fatalf("request %d: status = %d, want 502", i, resp.StatusCode)
			}
		}
		if got := hub.probes.Load(); got != 1 {
			t.Fatalf("upstream probes = %d, want 1 (failure not cached)", got)
		}
	})

	t.Run("not found maps to 404", func(t *testing.T) {
		hub := newFakeHub()
		hubSrv := httptest.NewServer(hub)
		defer hubSrv.Close()

		srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
		resolveURL := srv.URL + "/org/repo/resolve/main/missing.bin"

		for i := range 2 {
			resp, err := http.Get(resolveURL)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("request %d: status = %d, want 404", i, resp.StatusCode)
			}
		}
		if got := hub.probes.Load(); got != 1 {
			t.Fatalf("upstream probes = %d, want 1 (404 not cached)", got)
		}
	})
}

// TestMirrorSHA256Mismatch: when the upstream-advertised sha256 etag does not
// match the bytes, in-flight readers still get the bytes but the ingest fails
// and the file ends in a failed state instead of being cached corrupt.
func TestMirrorSHA256Mismatch(t *testing.T) {
	hub := newFakeHub()
	hub.etagOverride = strings.Repeat("ab", 32) // valid hex shape, wrong digest

	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	data := deterministicData(64 * 1024)
	const resolvePath = "/org/repo/resolve/main/lying-etag.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := srv.URL + resolvePath

	// Serve-while-caching still delivers the actual bytes.
	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatalf("in-flight body mismatch: %d bytes, want %d", len(body), len(data))
	}

	body := pollResolveStatus(t, resolveURL, http.StatusBadGateway)
	if !strings.Contains(body, "sha256 mismatch") {
		t.Fatalf("error body = %q, want it to mention the sha256 mismatch", body)
	}
}

// TestMirrorSizeMismatch: an upstream that advertises the wrong size must not
// be cached as ready.
func TestMirrorSizeMismatch(t *testing.T) {
	hub := newFakeHub()
	data := deterministicData(32 * 1024)
	hub.sizeOverride = fmt.Sprint(len(data) + 16)

	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	const resolvePath = "/org/repo/resolve/main/lying-size.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := srv.URL + resolvePath

	// Trigger ingestion; the in-flight response is truncated mid-body, so
	// tolerate the read error and only care about the final state.
	if resp, err := http.Get(resolveURL); err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	body := pollResolveStatus(t, resolveURL, http.StatusBadGateway)
	if !strings.Contains(body, "size mismatch") {
		t.Fatalf("error body = %q, want it to mention the size mismatch", body)
	}
}

// TestMirrorClientDisconnectKeepsIngest: dropping the only downstream client
// mid-transfer must not cancel the background ingestion.
func TestMirrorClientDisconnectKeepsIngest(t *testing.T) {
	hub := newFakeHub()
	hub.gate = make(chan struct{})
	hub.gateHit = make(chan struct{})
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	data := deterministicData(128 * 1024)
	const resolvePath = "/org/repo/resolve/main/abandoned.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := srv.URL + resolvePath

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resolveURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	<-hub.gateHit // upstream has streamed half into the spool
	cancel()      // the only client goes away
	_ = resp.Body.Close()
	close(hub.gate)

	waitMirrorReady(t, resolveURL)
	if got := hub.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream data GETs = %d, want 1 (disconnect restarted the ingest)", got)
	}
	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("cached body mismatch after client disconnect")
	}
	if got := hub.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream data GETs after ready = %d, want 1", got)
	}
}

// TestMirrorCommitRevisionPinned: a 40-hex commit revision is immutable and
// must never be revalidated, even when the revalidate interval is zero.
func TestMirrorCommitRevisionPinned(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	rev := strings.Repeat("c0ffee", 6) + "abcd" // 40 hex chars
	resolvePath := "/org/repo/resolve/" + rev + "/pinned.bin"
	v1 := deterministicData(48 * 1024)
	hub.set(resolvePath, v1)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir(), mirror.WithRevalidateInterval(0))
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); !bytes.Equal(body, v1) {
		t.Fatal("initial body mismatch")
	}
	waitMirrorReady(t, resolveURL)
	probesAfterIngest := hub.probes.Load()

	// Upstream content changes (which real hubs never do for a commit).
	hub.set(resolvePath, deterministicData(10*1024))

	if body := getBody(t, resolveURL); !bytes.Equal(body, v1) {
		t.Fatal("commit-pinned content changed; it must keep serving the cached bytes")
	}
	if got := hub.probes.Load(); got != probesAfterIngest {
		t.Fatalf("commit revision was revalidated: probes %d -> %d", probesAfterIngest, got)
	}
}

// TestMirrorServesCacheDuringUpstreamOutage: once a file is ready, an upstream
// outage must not break downloads even when revalidation is due.
func TestMirrorServesCacheDuringUpstreamOutage(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	data := deterministicData(48 * 1024)
	const resolvePath = "/org/repo/resolve/main/outage.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir(), mirror.WithRevalidateInterval(0))
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("initial body mismatch")
	}
	waitMirrorReady(t, resolveURL)

	// Upstream starts failing; revalidation is attempted on every request.
	hub.resolveStatus = http.StatusInternalServerError

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("cached body not served during upstream outage")
	}
	resp := waitMirrorReady(t, resolveURL)
	if got := resp.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("X-Linked-Size = %q, want %d", got, len(data))
	}
}

// TestMirrorSizeOnlyKnownFromFetch: upstreams like modelscope.cn answer HEAD
// without any size; the mirror must learn it from the ingest download's
// response headers and only then answer metadata requests.
func TestMirrorSizeOnlyKnownFromFetch(t *testing.T) {
	hub := newFakeHub()
	hub.omitSize = true
	hub.gate = make(chan struct{})
	hub.gateHit = make(chan struct{})
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	data := deterministicData(96 * 1024)
	const resolvePath = "/org/repo/resolve/main/nosize.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := srv.URL + resolvePath

	type result struct {
		body []byte
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get(resolveURL)
		if err != nil {
			got <- result{nil, err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- result{b, err}
	}()

	<-hub.gateHit // ingest is mid-flight: size learned from GET headers only

	resp, err := noRedirectClient().Head(resolveURL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mid-ingest HEAD status = %d, want 200", resp.StatusCode)
	}
	if resp.ContentLength != int64(len(data)) {
		t.Fatalf("mid-ingest HEAD Content-Length = %d, want %d", resp.ContentLength, len(data))
	}
	if got := resp.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("mid-ingest X-Linked-Size = %q, want %d", got, len(data))
	}
	if got := resp.Header.Get("X-Repo-Commit"); got == "" {
		t.Fatal("mid-ingest HEAD missing X-Repo-Commit")
	}

	close(hub.gate)
	r := <-got
	if r.err != nil {
		t.Fatalf("streaming GET: %v", r.err)
	}
	if !bytes.Equal(r.body, data) {
		t.Fatalf("streaming GET body mismatch: %d bytes, want %d", len(r.body), len(data))
	}

	ready := waitMirrorReady(t, resolveURL)
	if got := ready.Header.Get("X-Linked-Size"); got != fmt.Sprint(len(data)) {
		t.Fatalf("ready X-Linked-Size = %q, want %d", got, len(data))
	}
}

// TestMirrorResumeAfterUpstreamDrop: a connection drop halfway through the
// upstream download must be resumed, and the cached file must still be intact.
func TestMirrorResumeAfterUpstreamDrop(t *testing.T) {
	hub := newFakeHub()
	hub.dropNextData.Store(true)
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	data := deterministicData(256 * 1024)
	const resolvePath = "/org/repo/resolve/main/flaky.bin"
	hub.set(resolvePath, data)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
	resolveURL := srv.URL + resolvePath

	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatalf("body after upstream drop mismatch: %d bytes, want %d", len(body), len(data))
	}
	waitMirrorReady(t, resolveURL)

	if got := hub.dataGETs.Load(); got < 2 {
		t.Fatalf("upstream data GETs = %d, want >= 2 (drop should force a resume)", got)
	}
	if body := getBody(t, resolveURL); !bytes.Equal(body, data) {
		t.Fatal("cached body mismatch after resumed ingest")
	}
}

// TestMirrorPseudoCommit: upstreams that send no X-Repo-Commit still yield a
// commit header downstream: synthesized and stable for branches, the revision
// itself for commit revisions.
func TestMirrorPseudoCommit(t *testing.T) {
	commitRe := regexp.MustCompile(`^[0-9a-f]{40}$`)

	t.Run("branch revision synthesizes a stable commit", func(t *testing.T) {
		hub := newFakeHub()
		hub.commit = "" // upstream omits X-Repo-Commit
		hubSrv := httptest.NewServer(hub)
		defer hubSrv.Close()

		const resolvePath = "/org/repo/resolve/main/nocommit.bin"
		hub.set(resolvePath, deterministicData(8*1024))

		srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
		resolveURL := srv.URL + resolvePath

		getBody(t, resolveURL)
		first := waitMirrorReady(t, resolveURL).Header.Get("X-Repo-Commit")
		if !commitRe.MatchString(first) {
			t.Fatalf("X-Repo-Commit = %q, want a synthesized 40-hex commit", first)
		}
		if second := waitMirrorReady(t, resolveURL).Header.Get("X-Repo-Commit"); second != first {
			t.Fatalf("pseudo commit not stable: %q then %q", first, second)
		}
	})

	t.Run("commit revision is used verbatim", func(t *testing.T) {
		hub := newFakeHub()
		hub.commit = ""
		hubSrv := httptest.NewServer(hub)
		defer hubSrv.Close()

		rev := strings.Repeat("ab", 20)
		resolvePath := "/org/repo/resolve/" + rev + "/nocommit.bin"
		hub.set(resolvePath, deterministicData(8*1024))

		srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())
		resolveURL := srv.URL + resolvePath

		getBody(t, resolveURL)
		if got := waitMirrorReady(t, resolveURL).Header.Get("X-Repo-Commit"); got != rev {
			t.Fatalf("X-Repo-Commit = %q, want the pinned revision %q", got, rev)
		}
	})
}

// TestMirrorDistinctRevisions: the same path under different revisions must be
// cached as independent entries.
func TestMirrorDistinctRevisions(t *testing.T) {
	hub := newFakeHub()
	hubSrv := httptest.NewServer(hub)
	defer hubSrv.Close()

	v1 := deterministicData(16 * 1024)
	v2 := deterministicData(24 * 1024)
	const pathMain = "/org/repo/resolve/main/model.bin"
	const pathDev = "/org/repo/resolve/dev/model.bin"
	hub.set(pathMain, v1)
	hub.set(pathDev, v2)

	srv := newMirrorServer(t, hubSrv.URL, t.TempDir(), t.TempDir())

	if body := getBody(t, srv.URL+pathMain); !bytes.Equal(body, v1) {
		t.Fatal("main revision body mismatch")
	}
	if body := getBody(t, srv.URL+pathDev); !bytes.Equal(body, v2) {
		t.Fatal("dev revision body mismatch")
	}

	locMain := waitMirrorReady(t, srv.URL+pathMain).Header.Get("Location")
	locDev := waitMirrorReady(t, srv.URL+pathDev).Header.Get("Location")
	if locMain == locDev {
		t.Fatalf("revisions share a bridge location: %q", locMain)
	}
	if got := hub.dataGETs.Load(); got != 2 {
		t.Fatalf("upstream data GETs = %d, want 2", got)
	}
}
