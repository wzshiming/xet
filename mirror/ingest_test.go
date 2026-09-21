package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIngest(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	const resolvePath = "/org/repo/resolve/main/model.bin"
	upstream.set(resolvePath, data)

	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())

	t.Run("rejects invalid components", func(t *testing.T) {
		if _, err := m.Ingest("org/repo", "main/extra", "model.bin"); err == nil {
			t.Fatal("expected an error for a rev containing a slash")
		}
		if _, err := m.Ingest("org/repo", "main", ""); err == nil {
			t.Fatal("expected an error for an empty path")
		}
	})

	t.Run("resolves once done", func(t *testing.T) {
		in, err := m.Ingest("org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		entry, err := in.Entry()
		if err != nil {
			t.Fatalf("Entry: %v", err)
		}
		sum := sha256.Sum256(data)
		if entry.SHA256 != hex.EncodeToString(sum[:]) {
			t.Fatalf("SHA256 = %q, want %q", entry.SHA256, hex.EncodeToString(sum[:]))
		}
		if entry.Size != int64(len(data)) {
			t.Fatalf("Size = %d, want %d", entry.Size, len(data))
		}
		if entry.FileHash == "" {
			t.Fatal("FileHash is empty")
		}
		const wantCommit = "4dddf896b78f84b58402ab0da2c89514b0e17d1f"
		if entry.Commit != wantCommit {
			t.Fatalf("Commit = %q, want %q", entry.Commit, wantCommit)
		}
		if entry.ETag == "" {
			t.Fatal("ETag is empty")
		}

		// Readiness must hold the moment Done closes: Resolve answers with
		// the ready entry and storage serves the bytes.
		res, err := m.Resolve(context.Background(), "org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Resolve: %v", err)
		}
		if res.Entry == nil {
			t.Fatal("Resolve did not return the ready entry immediately after Done")
		}
		if res.Entry.FileHash != entry.FileHash {
			t.Fatalf("Resolve FileHash = %q, want %q", res.Entry.FileHash, entry.FileHash)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
			t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
		}
	})

	t.Run("ready entry resolves without new downloads", func(t *testing.T) {
		before := upstream.dataGETs.Load()
		in, err := m.Ingest("org/repo", "main", "model.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		entry, err := in.Entry()
		if err != nil {
			t.Fatalf("Entry: %v", err)
		}
		if entry.Size != int64(len(data)) {
			t.Fatalf("Size = %d, want %d", entry.Size, len(data))
		}
		if got := upstream.dataGETs.Load(); got != before {
			t.Fatalf("upstream GETs went %d -> %d, want no new downloads", before, got)
		}
	})

	t.Run("not found matches ErrUpstreamNotFound", func(t *testing.T) {
		in, err := m.Ingest("org/repo", "main", "missing.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		if _, err := in.Entry(); !errors.Is(err, ErrUpstreamNotFound) {
			t.Fatalf("err = %v, want ErrUpstreamNotFound", err)
		}
		// The failure is cached with backoff and resolves without re-probing.
		in, err = m.Ingest("org/repo", "main", "missing.bin")
		if err != nil {
			t.Fatalf("Ingest: %v", err)
		}
		<-in.Done()
		if _, err := in.Entry(); !errors.Is(err, ErrUpstreamNotFound) {
			t.Fatalf("cached err = %v, want ErrUpstreamNotFound", err)
		}
	})
}

func awaitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(10 * time.Second):
		t.Fatalf("%s: timed out", what)
	}
}

func TestIngestInFlight(t *testing.T) {
	commit := strings.Repeat("ab", 20)
	for _, tc := range []struct{ name, rev string }{
		{"same alias", "main"},
		{"branch alias", "dev"},
		{"direct commit", commit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.gate = make(chan struct{})
			upstream.gateHit = make(chan struct{})
			upstream.commit = commit
			data := make([]byte, 128*1024)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			upstream.set("/org/repo/resolve/main/model.bin", data)
			upstream.set("/org/repo/resolve/dev/model.bin", data)
			upstreamSrv := httptest.NewServer(upstream)
			defer upstreamSrv.Close()
			var release sync.Once
			defer release.Do(func() { close(upstream.gate) }) // Close blocks on the gated handler

			m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			in, err := m.Ingest("org/repo", "main", "model.bin")
			if err != nil {
				t.Fatalf("Ingest: %v", err)
			}
			awaitClosed(t, upstream.gateHit, "upstream transfer")
			select {
			case <-in.Done():
				t.Fatal("Done closed while the upstream transfer is still gated")
			default:
			}
			if e, err := in.Entry(); e != nil || err != nil {
				t.Fatalf("Entry before Done = %v, %v; want nil, nil", e, err)
			}

			res, err := m.Resolve(ctx, "org/repo", tc.rev, "model.bin")
			if err != nil || res.Stream == nil {
				t.Fatalf("Resolve %s = %+v, %v; want the in-flight stream", tc.rev, res, err)
			}
			m.mu.Lock()
			pinned, tasks := m.tasks[resolveKey{repo: "org/repo", rev: commit, path: "model.bin"}], len(m.tasks)
			m.mu.Unlock()
			if pinned == nil || res.Stream.t != pinned || tasks != 1 {
				t.Fatalf("%s attached to task %p, pinned task %p, %d tasks; want one shared task", tc.rev, res.Stream.t, pinned, tasks)
			}
			if _, c, err := res.Stream.WaitMeta(ctx); err != nil || c != commit {
				t.Fatalf("WaitMeta = %q, %v; want commit %s", c, err, commit)
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs while gated = %d, want 1", got)
			}

			joined, err := m.Ingest("org/repo", tc.rev, "model.bin")
			if err != nil {
				t.Fatalf("joining Ingest: %v", err)
			}
			select {
			case <-joined.Done():
				t.Fatal("joined Done closed while the upstream transfer is still gated")
			default:
			}

			release.Do(func() { close(upstream.gate) })
			sum := sha256.Sum256(data)
			for name, h := range map[string]*Ingestion{"first": in, "joined": joined} {
				awaitClosed(t, h.Done(), name+" Done")
				entry, err := h.Entry()
				if err != nil {
					t.Fatalf("%s Entry: %v", name, err)
				}
				if entry.Commit != commit || entry.SHA256 != hex.EncodeToString(sum[:]) || entry.Size != int64(len(data)) {
					t.Fatalf("%s entry = %+v, want commit %s, sha256 %x, size %d", name, entry, commit, sum, len(data))
				}
			}
			if got := readStored(t, stor, hex.EncodeToString(sum[:])); !bytes.Equal(got, data) {
				t.Fatalf("stored bytes mismatch: got %d bytes, want %d", len(got), len(data))
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs = %d, want 1 (joins must share one download)", got)
			}
		})
	}
}

type gatedUpstream struct {
	mu      sync.Mutex
	files   map[string][]byte
	gates   map[string]chan struct{}
	held    int
	peak    int
	started chan string
	commit  string
	corrupt string // path whose advertised etag does not match its bytes
}

func newGatedUpstream(files map[string][]byte) *gatedUpstream {
	u := &gatedUpstream{files: files, gates: map[string]chan struct{}{}, started: make(chan string, len(files)), commit: strings.Repeat("ab", 20)}
	for p := range files {
		u.gates[p] = make(chan struct{})
	}
	return u
}

func (u *gatedUpstream) release(path string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if gate, ok := u.gates[path]; ok {
		close(gate)
		delete(u.gates, path)
	}
}

func (u *gatedUpstream) releaseAll() {
	u.mu.Lock()
	paths := make([]string, 0, len(u.gates))
	for p := range u.gates {
		paths = append(paths, p)
	}
	u.mu.Unlock()
	for _, p := range paths {
		u.release(p)
	}
}

func (u *gatedUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path, cdn := strings.CutPrefix(r.URL.Path, "/cdn")
	// Like a hub, the branch head is also served at its commit.
	path = strings.Replace(path, "/resolve/"+u.commit+"/", "/resolve/main/", 1)
	data, ok := u.files[path]
	if !ok {
		http.NotFound(w, r)
		return
	}
	if !cdn {
		sum := sha256.Sum256(data)
		if path == u.corrupt {
			sum[0] ^= 0xff
		}
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		w.Header().Set("X-Repo-Commit", u.commit)
		http.Redirect(w, r, "/cdn"+path, http.StatusFound)
		return
	}
	if r.Method == http.MethodGet {
		u.mu.Lock()
		gate := u.gates[path]
		u.held++
		u.peak = max(u.peak, u.held)
		u.mu.Unlock()
		u.started <- path
		if gate != nil {
			<-gate
		}
		u.mu.Lock()
		u.held--
		u.mu.Unlock()
	}
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
}

func awaitStarted(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(10 * time.Second):
		t.Fatal("upstream data GET: timed out")
	}
	return ""
}

func TestMirrorIngestConcurrencyBound(t *testing.T) {
	files := map[string][]byte{}
	for _, name := range []string{"a.bin", "b.bin", "c.bin"} {
		data := make([]byte, 64*1024)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		files["/org/repo/resolve/main/"+name] = data
	}
	up := newGatedUpstream(files)
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithMaxConcurrentIngests(2))
	t.Cleanup(up.releaseAll)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ingestions := map[string]*Ingestion{}
	for path := range files {
		in, err := m.Ingest("org/repo", "main", strings.TrimPrefix(path, "/org/repo/resolve/main/"))
		if err != nil {
			t.Fatal(err)
		}
		ingestions[path] = in
	}

	running := map[string]bool{awaitStarted(t, up.started): true}
	running[awaitStarted(t, up.started)] = true
	var queued string
	for path := range files {
		if !running[path] {
			queued = path
		}
	}
	if len(running) != 2 || queued == "" {
		t.Fatalf("running = %v, want two distinct transfers", running)
	}
	if held := len(m.ingestSlots); held != 2 {
		t.Fatalf("ingest slots held = %d, want 2", held)
	}
	select {
	case p := <-up.started:
		t.Fatalf("%s started beyond the cap", p)
	default:
	}

	res, err := m.Resolve(ctx, "org/repo", "main", strings.TrimPrefix(queued, "/org/repo/resolve/main/"))
	if err != nil || res.Stream == nil {
		t.Fatalf("Resolve queued = %+v, %v; want the in-flight stream", res, err)
	}
	if _, commit, err := res.Stream.WaitMeta(ctx); err != nil || commit != up.commit {
		t.Fatalf("queued WaitMeta = %q, %v; want commit %s", commit, err, up.commit)
	}
	if size, ok := res.Stream.WaitSize(ctx); !ok || size != int64(len(files[queued])) {
		t.Fatalf("queued WaitSize = %d, %v; want %d, true", size, ok, len(files[queued]))
	}
	var first string
	for path := range running {
		first = path
	}
	joined, err := m.Ingest("org/repo", "main", strings.TrimPrefix(first, "/org/repo/resolve/main/"))
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	tasks := len(m.tasks)
	m.mu.Unlock()
	if tasks != 3 {
		t.Fatalf("tasks = %d, want 3 (one per file, joins attach)", tasks)
	}

	up.release(first)
	if got := awaitStarted(t, up.started); got != queued {
		t.Fatalf("next transfer = %s, want the queued %s", got, queued)
	}
	up.releaseAll()

	for path, in := range ingestions {
		awaitClosed(t, in.Done(), path)
		entry, err := in.Entry()
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, files[path]) {
			t.Fatalf("%s: stored bytes mismatch", path)
		}
	}
	awaitClosed(t, joined.Done(), "joined")
	if _, err := joined.Entry(); err != nil {
		t.Fatal(err)
	}
	up.mu.Lock()
	peak := up.peak
	up.mu.Unlock()
	if peak != 2 {
		t.Fatalf("peak concurrent upstream transfers = %d, want 2", peak)
	}
}

func TestMirrorIngestFailureReleasesSlot(t *testing.T) {
	good := make([]byte, 32*1024)
	if _, err := rand.Read(good); err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"/org/repo/resolve/main/bad.bin": make([]byte, 32*1024), "/org/repo/resolve/main/good.bin": good}
	up := newGatedUpstream(files)
	up.corrupt = "/org/repo/resolve/main/bad.bin"
	srv := httptest.NewServer(up)
	t.Cleanup(srv.Close)

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithMaxConcurrentIngests(1))
	t.Cleanup(up.releaseAll)

	bad, err := m.Ingest("org/repo", "main", "bad.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := awaitStarted(t, up.started); got != up.corrupt {
		t.Fatalf("first transfer = %s, want %s", got, up.corrupt)
	}
	in, err := m.Ingest("org/repo", "main", "good.bin")
	if err != nil {
		t.Fatal(err)
	}
	// good.bin waits for the single slot; its size resolves meanwhile.
	res, err := m.Resolve(context.Background(), "org/repo", "main", "good.bin")
	if err != nil || res.Stream == nil {
		t.Fatalf("Resolve queued = %+v, %v; want the in-flight stream", res, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if size, ok := res.Stream.WaitSize(ctx); !ok || size != int64(len(good)) {
		t.Fatalf("queued WaitSize = %d, %v; want %d, true", size, ok, len(good))
	}
	select {
	case p := <-up.started:
		t.Fatalf("%s started while the slot is held", p)
	default:
	}
	up.release(up.corrupt)
	awaitClosed(t, bad.Done(), "bad ingest")
	if _, err := bad.Entry(); !errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("bad ingest err = %v, want spool corrupt", err)
	}

	if got := awaitStarted(t, up.started); got != "/org/repo/resolve/main/good.bin" {
		t.Fatalf("next transfer = %s, want good.bin", got)
	}
	up.releaseAll()
	awaitClosed(t, in.Done(), "good ingest")
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, good) {
		t.Fatal("stored bytes mismatch")
	}
}

func TestWithMaxConcurrentIngestsDefault(t *testing.T) {
	for _, n := range []int{0, -1} {
		m, _ := newTestMirror(t, "http://upstream.invalid", t.TempDir(), t.TempDir(), WithMaxConcurrentIngests(n))
		if got := cap(m.ingestSlots); got != 16 {
			t.Fatalf("WithMaxConcurrentIngests(%d): slots = %d, want 16", n, got)
		}
	}
	m, _ := newTestMirror(t, "http://upstream.invalid", t.TempDir(), t.TempDir())
	if got := cap(m.ingestSlots); got != 16 {
		t.Fatalf("default slots = %d, want 16", got)
	}
}
