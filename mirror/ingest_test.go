package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
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

	cacheDir := t.TempDir()
	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), cacheDir)

	t.Run("rejects invalid URLs", func(t *testing.T) {
		if _, err := m.Mirror.Ingest(upstreamSrv.URL+"/org/repo/tree/main/model.bin", ""); err == nil {
			t.Fatal("expected an error for a URL that is not a hub download URL")
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
		if _, err := os.Stat(filepath.Join(cacheDir, "upload")); err != nil {
			t.Fatalf("ingest did not stage under the mirror cache root: %v", err)
		}
		if _, err := os.Stat(filepath.Join(cacheDir, "upload", "chunks")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("ingest touched the chunk location cache: %v", err)
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

// Two keys whose upstream etag is the same content hash share one download:
// the second task follows the first, reading its spool while the upstream
// is still transferring and publishing the same stored file under its own key.
func TestIngestSharesDownloadByETag(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstream.commit = strings.Repeat("ab", 20)
	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	upstream.set("/org/repo/resolve/main/a.bin", data)
	upstream.set("/org/repo/resolve/main/b.bin", data)
	srv := httptest.NewServer(upstream)
	t.Cleanup(srv.Close)
	release := sync.OnceFunc(func() { close(upstream.gate) })
	t.Cleanup(release) // Close blocks on the gated handler

	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	a, err := m.Resolve(ctx, "org/repo", "main", "a.bin")
	if err != nil || a.Stream == nil {
		t.Fatalf("Resolve a.bin = %+v, %v; want an in-flight stream", a, err)
	}
	awaitClosed(t, upstream.gateHit, "upstream transfer")
	b, err := m.Resolve(ctx, "org/repo", "main", "b.bin")
	if err != nil || b.Stream == nil {
		t.Fatalf("Resolve b.bin = %+v, %v; want an in-flight stream", b, err)
	}
	for name, st := range map[string]*Stream{"a.bin": a.Stream, "b.bin": b.Stream} {
		if etag, commit, err := st.WaitMeta(ctx); err != nil || etag != hashHex(string(data)) || commit != upstream.commit {
			t.Fatalf("%s WaitMeta = %q, %q, %v; want the shared etag at %s", name, etag, commit, err, upstream.commit)
		}
		if size, ok := st.WaitSize(ctx); !ok || size != int64(len(data)) {
			t.Fatalf("%s WaitSize = %d, %v; want %d, true", name, size, ok, len(data))
		}
	}

	// The follower's first half arrives from the leader's spool while the upstream is still gated.
	rc := b.Stream.NewReader(ctx, 0)
	if rc == nil {
		t.Fatal("NewReader on the follower returned nil")
	}
	defer rc.Close()
	half := make([]byte, len(data)/2)
	if _, err := io.ReadFull(rc, half); err != nil || !bytes.Equal(half, data[:len(half)]) {
		t.Fatalf("follower's first half while gated: %v", err)
	}
	m.mu.Lock()
	inflight, tasks := len(m.inflight), len(m.tasks)
	m.mu.Unlock()
	if inflight != 1 || tasks != 2 || a.Stream.t == b.Stream.t || a.Stream.t.spool != b.Stream.t.spool {
		t.Fatalf("inflight %d, tasks %d, same task %v, shared spool %v; want 1, 2, false, true", inflight, tasks, a.Stream.t == b.Stream.t, a.Stream.t.spool == b.Stream.t.spool)
	}
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream GETs while gated = %d, want 1", got)
	}

	release()
	tail, err := io.ReadAll(rc)
	if err != nil || !bytes.Equal(tail, data[len(half):]) {
		t.Fatalf("follower's second half after the gate opened: %v", err)
	}
	_ = rc.Close()
	awaitClosed(t, a.Stream.t.done, "leader")
	awaitClosed(t, b.Stream.t.done, "follower")
	var entries [2]*Entry
	for i, p := range []string{"a.bin", "b.bin"} {
		res, err := m.Resolve(ctx, "org/repo", "main", p)
		if err != nil || res.Entry == nil {
			t.Fatalf("Resolve %s after the ingest = %+v, %v; want the ready entry", p, res, err)
		}
		entries[i] = res.Entry
	}
	ea, eb := entries[0], entries[1]
	if ea.FileHash == "" || eb.FileHash != ea.FileHash || eb.SHA256 != ea.SHA256 || eb.Size != ea.Size || ea.Commit != upstream.commit || eb.Commit != upstream.commit {
		t.Fatalf("entries differ: a.bin %+v, b.bin %+v", ea, eb)
	}
	if got := upstream.dataGETs.Load(); got != 1 {
		t.Fatalf("upstream GETs = %d, want 1 (the follower must share the download)", got)
	}
	// The leader leaves inflight after done closes, so give it a moment.
	deadline := time.Now().Add(5 * time.Second)
	for {
		m.mu.Lock()
		inflight, tasks = len(m.inflight), len(m.tasks)
		m.mu.Unlock()
		if inflight == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if inflight != 0 || tasks != 0 {
		t.Fatalf("inflight %d, tasks %d after both tasks finished; want 0, 0", inflight, tasks)
	}
	if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
		t.Fatalf("spool files after the shared ingest = %v, want none", files)
	}
	if got := readStored(t, stor, ea.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes differ from upstream data")
	}
}

// Content whose sha256 local storage already holds is published without a
// download: a branch moving to a commit with the same bytes costs no GET and
// no spool, and the first resolve under the new key already answers the
// published entry, with no task in between.
func TestIngestSkipsDownloadWhenStored(t *testing.T) {
	for _, noSize := range []bool{false, true} {
		t.Run(fmt.Sprintf("noSize=%v", noSize), func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.noSize = noSize
			c1, c2, c3 := strings.Repeat("ab", 20), strings.Repeat("cd", 20), strings.Repeat("ef", 20)
			data := make([]byte, 64*1024)
			if _, err := rand.Read(data); err != nil {
				t.Fatal(err)
			}
			upstream.commit = c1
			upstream.set("/org/repo/resolve/main/f.bin", data)
			srv := httptest.NewServer(upstream)
			t.Cleanup(srv.Close)
			m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
			ctx := context.Background()

			e1, err := ingestWait(t, m, "org/repo", "main", "f.bin")
			if err != nil || e1.Commit != c1 || e1.Size != int64(len(data)) {
				t.Fatalf("first ingest = %+v, %v; want %d bytes at %s", e1, err, len(data), c1)
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs after the first ingest = %d, want 1", got)
			}

			// The branch moves to a commit holding the same bytes.
			upstream.commit = c2
			upstream.set("/org/repo/resolve/main/f.bin", data)
			e2, err := ingestWait(t, m, "org/repo", "main", "f.bin")
			if err != nil || e2.Commit != c2 || e2.FileHash != e1.FileHash || e2.SHA256 != e1.SHA256 || e2.Size != e1.Size || e2.ETag != e1.ETag {
				t.Fatalf("ingest at the moved branch = %+v, %v; want %s with the stored file of %+v", e2, err, c2, e1)
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs after the move = %d, want 1 (stored content must not be downloaded)", got)
			}
			if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
				t.Fatalf("spool files after publishing stored content = %v, want none", files)
			}

			// Resolve under a cold key: the stored content is the answer at once.
			upstream.commit = c3
			upstream.set("/org/repo/resolve/main/f.bin", data)
			res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
			if err != nil || res.Entry == nil || res.Entry.Commit != c3 || res.Entry.FileHash != e1.FileHash || res.Entry.Size != e1.Size || res.Entry.ETag != e1.ETag {
				t.Fatalf("first resolve at %s = %+v, %v; want the stored file of %+v published at once", c3, res, err, e1)
			}
			m.mu.Lock()
			tasks := len(m.tasks)
			m.mu.Unlock()
			if tasks != 0 {
				t.Fatalf("tasks after publishing stored content = %d, want none", tasks)
			}
			res, err = m.Resolve(ctx, "org/repo", "main", "f.bin")
			if err != nil || res.Entry == nil || res.Entry.Commit != c3 || res.Entry.FileHash != e1.FileHash || res.Entry.Size != e1.Size {
				t.Fatalf("second resolve at %s = %+v, %v; want the published entry of %+v", c3, res, err, e1)
			}
			if got := upstream.dataGETs.Load(); got != 1 {
				t.Fatalf("upstream GETs after three commits = %d, want 1", got)
			}
			if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
				t.Fatalf("spool files after the cold resolve = %v, want none", files)
			}
			if got := readStored(t, stor, e1.SHA256); !bytes.Equal(got, data) {
				t.Fatal("stored bytes differ from upstream data")
			}
		})
	}
}

// An uppercase sha256 etag is the same digest: the bytes are verified
// against it, a key whose content storage already holds is published without
// a download, and the entry keeps the etag as the hub sent it.
func TestIngestVerifiesUppercaseSHA256(t *testing.T) {
	data, unstored, served := make([]byte, 32*1024), make([]byte, 32*1024), make([]byte, 32*1024)
	for _, b := range [][]byte{data, unstored, served} {
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
	}
	upper := strings.ToUpper(hashHex(string(data)))
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/main/a.bin", data)
	upstream.set("/org/repo/resolve/main/b.bin", data)
	upstream.set("/org/repo/resolve/main/c.bin", served) // advertised as unstored's digest
	advertised := map[string]string{"a.bin": upper, "b.bin": upper, "c.bin": strings.ToUpper(hashHex(string(unstored)))}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/cdn") {
			upstream.ServeHTTP(w, r)
			return
		}
		if _, ok := upstream.get(r.URL.Path); !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("ETag", `"`+advertised[path.Base(r.URL.Path)]+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		w.Header().Set("X-Repo-Commit", upstream.commit)
		http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())

	entry, err := ingestWait(t, m, "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if entry.SHA256 != hashHex(string(data)) || entry.ETag != upper {
		t.Fatalf("entry = %+v, want sha256 %s under etag %s", entry, hashHex(string(data)), upper)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes differ from upstream data")
	}
	held, err := ingestWait(t, m, "org/repo", "main", "b.bin")
	if err != nil {
		t.Fatal(err)
	}
	if held.FileHash != entry.FileHash || held.ETag != upper || upstream.dataGETs.Load() != 1 {
		t.Fatalf("b.bin = %+v after %d data GETs, want %+v published from storage", held, upstream.dataGETs.Load(), entry)
	}

	if _, err := ingestWait(t, m, "org/repo", "main", "c.bin"); !errors.Is(err, errSpoolCorrupt) {
		t.Fatalf("c.bin err = %v, want the bytes rejected against the uppercase digest", err)
	}
	if files := spoolFiles(t, m.spoolDir); len(files) != 0 {
		t.Fatalf("spool files after the rejected download = %v, want none", files)
	}
}

// Stored content is published without a download only when the upstream's
// size agrees with it: a key claiming a held digest at another length is
// downloaded, and that download fails on the size it advertised.
func TestHeldEntryRejectsSizeDisagreement(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	data := make([]byte, 32*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	upstream.set("/org/repo/resolve/main/a.bin", data)
	upstream.set("/org/repo/resolve/main/b.bin", data)
	// b.bin advertises a.bin's digest at one byte more than its length.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/b.bin") && !strings.HasPrefix(r.URL.Path, "/cdn") {
			w.Header().Set("ETag", `"`+hashHex(string(data))+`"`)
			w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)+1))
			w.Header().Set("X-Repo-Commit", upstream.commit)
			http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
			return
		}
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())

	stored, err := ingestWait(t, m, "org/repo", "main", "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	before := upstream.dataGETs.Load()
	if _, err := ingestWait(t, m, "org/repo", "main", "b.bin"); err == nil || !strings.Contains(err.Error(), "upstream size mismatch") {
		t.Fatalf("b.bin err = %v, want the download to fail on the advertised size", err)
	}
	if got := upstream.dataGETs.Load(); got != before+1 {
		t.Fatalf("data GETs went %d -> %d, want the disagreeing key downloaded rather than published from storage", before, got)
	}
	key := resolveKey{repo: "org/repo", rev: upstream.commit, path: "b.bin"}
	m.mu.Lock()
	e, published := m.entries[key], m.commits[key.revKey()].files["b.bin"]
	m.mu.Unlock()
	if e == nil || e.State != stateFailed || e.FileHash != "" || published != nil {
		t.Fatalf("b.bin holds %+v (manifest %+v); want a failure without %s's file", e, published, stored.FileHash)
	}
}
