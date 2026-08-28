package mirror

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/storage"
)

func TestSpoolTailRead(t *testing.T) {
	sp, err := openSpool(t.TempDir(), "k", "", -1)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}

	rc := sp.newReader(context.Background(), 0)
	got := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		got <- b
	}()

	// Write in pieces so the reader has to wait repeatedly.
	for i := 0; i < len(data); i += 8192 {
		if _, err := sp.Write(data[i : i+8192]); err != nil {
			t.Fatal(err)
		}
	}
	sp.finish(nil)

	if b := <-got; !bytes.Equal(b, data) {
		t.Fatalf("tail read mismatch: got %d bytes, want %d", len(b), len(data))
	}

	t.Run("canceled context unblocks reader", func(t *testing.T) {
		sp, err := openSpool(t.TempDir(), "k", "", -1)
		if err != nil {
			t.Fatal(err)
		}
		defer sp.finish(nil)
		ctx, cancel := context.WithCancel(context.Background())
		rc := sp.newReader(ctx, 0)
		defer rc.Close()
		errCh := make(chan error, 1)
		go func() {
			_, err := rc.Read(make([]byte, 1))
			errCh <- err
		}()
		cancel()
		if err := <-errCh; err != context.Canceled {
			t.Fatalf("read err = %v, want context.Canceled", err)
		}
	})

	t.Run("no readers after removal", func(t *testing.T) {
		sp, err := openSpool(t.TempDir(), "k", "", -1)
		if err != nil {
			t.Fatal(err)
		}
		sp.finish(nil) // no refs: the file is removed immediately
		if rc := sp.newReader(context.Background(), 0); rc != nil {
			t.Fatal("expected nil reader after removal")
		}
		if rs := sp.newSeekReader(context.Background(), 0); rs != nil {
			t.Fatal("expected nil seek reader after removal")
		}
	})
}

// newTestMirror builds an engine over a file storage rooted at storageDir,
// pointed at the given upstream. No HTTP surface is involved: tests drive the
// engine through Ingest and Resolve.
func newTestMirror(t *testing.T, upstream string, storageDir, cacheDir string, opts ...Option) (*Mirror, storage.Storage) {
	t.Helper()

	stor, err := storage.NewFileStorage(storage.WithBasePath(storageDir))
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewMirror(append([]Option{
		WithStorage(stor),
		WithUpstream(upstream),
		WithCacheDir(cacheDir),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	// Ingest tasks keep writing to storage and cache after resolvers let go;
	// wait for them so the t.TempDir removals running after this cleanup do
	// not race those writes.
	t.Cleanup(func() {
		timeout := time.After(30 * time.Second)
		for {
			m.mu.Lock()
			var done chan struct{}
			for _, tk := range m.tasks {
				done = tk.done
				break
			}
			m.mu.Unlock()
			if done == nil {
				return
			}
			select {
			case <-done:
			case <-timeout:
				t.Error("test mirror: in-flight ingest tasks did not finish")
				return
			}
		}
	})
	return m, stor
}

// readStored fetches the reconstructed bytes for a sha256 hex digest straight
// from storage, proving an ingest landed in the CAS.
func readStored(t *testing.T, stor storage.Storage, shaHex string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(shaHex)
	if err != nil || len(raw) != sha256.Size {
		t.Fatalf("bad sha256 digest %q", shaHex)
	}
	content, err := stor.GetReconstructedFile(context.Background(), "default", [sha256.Size]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer content.Close()
	data, err := io.ReadAll(content)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// plainUpstream is a hub without xet support: resolve requests redirect to a
// CDN path that serves the raw bytes with Range support.
type plainUpstream struct {
	mu       sync.Mutex
	files    map[string][]byte
	api      map[string][]byte // raw JSON served under /api/ paths
	commit   string
	dataGETs atomic.Int64
	seenAuth sync.Map // Authorization values observed on any request
	gate     chan struct{}
	gateHit  chan struct{}
}

func newPlainUpstream() *plainUpstream {
	return &plainUpstream{files: map[string][]byte{}, api: map[string][]byte{}, commit: "commit-1"}
}

func (u *plainUpstream) set(path string, data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[path] = data
}

func (u *plainUpstream) get(path string) ([]byte, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	data, ok := u.files[path]
	return data, ok
}

func (u *plainUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if auth := r.Header.Get("Authorization"); auth != "" {
		u.seenAuth.Store(auth, true)
	}

	if strings.HasPrefix(r.URL.Path, "/api/") {
		u.mu.Lock()
		data, ok := u.api[r.URL.Path]
		u.mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
		return
	}

	if after, ok := strings.CutPrefix(r.URL.Path, "/cdn"); ok {
		data, ok := u.get(after)
		if !ok {
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodGet {
			u.dataGETs.Add(1)
		}
		if u.gate != nil && r.Method == http.MethodGet && r.Header.Get("Range") == "" {
			// Stream in two halves so tests can observe serve-while-caching.
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			w.WriteHeader(http.StatusOK)
			half := len(data) / 2
			w.(http.Flusher).Flush()
			if _, err := w.Write(data[:half]); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			close(u.gateHit)
			<-u.gate
			_, _ = w.Write(data[half:])
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(data))
		return
	}

	data, ok := u.get(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(data)
	etag := hex.EncodeToString(sum[:])
	w.Header().Set("ETag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Etag", `"`+etag+`"`)
	w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
	w.Header().Set("X-Repo-Commit", u.commit)
	http.Redirect(w, r, "/cdn"+r.URL.Path, http.StatusFound)
}

// TestResolveStream drives the exported resolution boundary directly: the
// first Resolve of a cold key hands back an in-flight Stream whose metadata,
// size, and bytes are observable while the ingest still runs; once the task
// finishes, Resolve reports the terminal entry (or the terminal error).
func TestResolveStream(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.gate = make(chan struct{})
	upstream.gateHit = make(chan struct{})
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	data := make([]byte, 128*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	upstream.set("/org/repo/resolve/main/model.bin", data)

	m, stor := newTestMirror(t, upstreamSrv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()

	if _, err := m.Resolve(ctx, "org/repo", "main/extra", "model.bin"); err == nil {
		t.Fatal("expected an error for a rev containing a slash")
	}

	res, err := m.Resolve(ctx, "org/repo", "main", "model.bin")
	if err != nil {
		t.Fatal(err)
	}
	if res.Stream == nil {
		t.Fatal("first resolve did not return an in-flight stream")
	}

	etag, commit, err := res.Stream.WaitMeta(ctx)
	if err != nil {
		t.Fatalf("WaitMeta: %v", err)
	}
	if commit != "commit-1" {
		t.Fatalf("commit = %q, want commit-1", commit)
	}
	if etag == "" {
		t.Fatal("empty etag from WaitMeta")
	}
	size, ok := res.Stream.WaitSize(ctx)
	if !ok || size != int64(len(data)) {
		t.Fatalf("WaitSize = %d, %v, want %d, true", size, ok, len(data))
	}

	// The upstream stalls halfway; the tailing reader must still deliver the
	// full body once the gate opens.
	go func() {
		<-upstream.gateHit
		close(upstream.gate)
	}()
	rc := res.Stream.NewReader(ctx, 0)
	if rc == nil {
		t.Fatal("NewReader returned nil while the ingest is in flight")
	}
	body, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, data) {
		t.Fatal("streamed body differs from upstream data")
	}

	// Once the ingest completes, Resolve reports the terminal entry and the
	// bytes are in storage.
	var entry *Entry
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		res, err := m.Resolve(ctx, "org/repo", "main", "model.bin")
		if err != nil {
			t.Fatal(err)
		}
		if res.Entry != nil {
			entry = res.Entry
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if entry == nil {
		t.Fatal("resolve never returned a ready entry")
	}
	if entry.Size != int64(len(data)) || entry.Commit != "commit-1" {
		t.Fatalf("entry = %+v", entry)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes differ from upstream data")
	}

	// Missing files surface ErrUpstreamNotFound: first through the stream's
	// WaitMeta, then directly from Resolve once the failure is recorded.
	missing, err := m.Resolve(ctx, "org/repo", "main", "missing.bin")
	if err != nil {
		t.Fatal(err)
	}
	if missing.Stream == nil {
		t.Fatal("resolve of a missing file did not return a stream")
	}
	if _, _, err := missing.Stream.WaitMeta(ctx); !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("WaitMeta err = %v, want ErrUpstreamNotFound", err)
	}
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err = m.Resolve(ctx, "org/repo", "main", "missing.bin"); err != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("Resolve err = %v, want ErrUpstreamNotFound", err)
	}
}

func TestBranchEntryPath(t *testing.T) {
	dir := filepath.Join("idx", "branches")

	cases := []struct {
		repo, rev string
		want      string // relative to dir
	}{
		{"Qwen/Qwen3-0.6B", "main", "Qwen/Qwen3-0.6B/main.json"},
		{"gpt2", "main", "gpt2/main.json"},
		{"datasets/org/repo", "refs%2Fpr%2F1", "datasets/org/repo/refs%252Fpr%252F1.json"},
		// Traversal and hostile names stay escaped.
		{"..", "main", "%2E./main.json"},
		{"org/..", "..", "org/%2E./%2E..json"},
		{".hidden/repo", ".rev", "%2Ehidden/repo/%2Erev.json"},
		{"a%b", "r", "a%25b/r.json"},
		{"org/", "r", "org/%00/r.json"},
		// A repo segment ending in .json cannot collide with mapping files.
		{"org/main.json", "x", "org/main%2Ejson/x.json"},
	}
	for _, c := range cases {
		want := filepath.Join(dir, filepath.FromSlash(c.want))
		if got := branchEntryPath(dir, c.repo, c.rev); got != want {
			t.Errorf("branchEntryPath(%q, %q) = %q, want %q", c.repo, c.rev, got, want)
		}
	}

	t.Run("never escapes the branch dir", func(t *testing.T) {
		hostile := []struct{ repo, rev string }{
			{"../../etc", "passwd"},
			{"..", ".."},
			{"a/../../b", "r"},
			{"", ""},
			{"./.", "."},
		}
		for _, c := range hostile {
			p := branchEntryPath(dir, c.repo, c.rev)
			rel, err := filepath.Rel(dir, p)
			if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				t.Errorf("branchEntryPath(%q, %q) = %q escapes %q", c.repo, c.rev, p, dir)
			}
		}
	})

	t.Run("overlong segment falls back to hashed name", func(t *testing.T) {
		repo := strings.Repeat("x", 300)
		sum := sha256.Sum256([]byte(repo + "@main"))
		want := filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
		if got := branchEntryPath(dir, repo, "main"); got != want {
			t.Errorf("branchEntryPath overlong = %q, want hashed fallback %q", got, want)
		}
	})
}

// TestMirrorIndexLayout: branch mappings land at human-readable nested paths
// and file entries group under their commit directory.
func TestMirrorIndexLayout(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	commit := strings.Repeat("ab", 20)
	data := []byte("layout test content")
	upstream.commit = commit
	upstream.set("/Qwen/Qwen3-0.6B/resolve/main/f.bin", data)
	upstream.set("/Qwen/Qwen3-0.6B/resolve/"+commit+"/f.bin", data)

	cacheDir := t.TempDir()
	m, _ := newTestMirror(t, upstreamSrv.URL, t.TempDir(), cacheDir)

	in, err := m.Ingest("Qwen/Qwen3-0.6B", "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	if _, err := in.Entry(); err != nil {
		t.Fatal(err)
	}

	mappingPath := filepath.Join(cacheDir, "index", "branches", "Qwen", "Qwen3-0.6B", "main.json")
	raw, err := os.ReadFile(mappingPath)
	if err != nil {
		t.Fatalf("read branch mapping: %v", err)
	}
	var b branchEntry
	if err := json.Unmarshal(raw, &b); err != nil {
		t.Fatal(err)
	}
	if b.Repo != "Qwen/Qwen3-0.6B" || b.Rev != "main" || b.Commit != commit {
		t.Fatalf("branch mapping = %+v, want repo Qwen/Qwen3-0.6B rev main commit %s", b, commit)
	}

	key := "/Qwen/Qwen3-0.6B/resolve/" + commit + "/f.bin"
	sum := sha256.Sum256([]byte(key))
	entryPath := filepath.Join(cacheDir, "index", commit, hex.EncodeToString(sum[:])+".json")
	if _, err := os.Stat(entryPath); err != nil {
		t.Fatalf("file entry not grouped under commit dir: %v", err)
	}
}

// TestMirrorIndexMigration: entries persisted by older layouts (hashed branch
// mapping names, flat file entries) move to their canonical locations on
// startup and stay loaded.
func TestMirrorIndexMigration(t *testing.T) {
	upstream := newPlainUpstream()
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	cacheDir := t.TempDir()
	commit := strings.Repeat("cd", 20)

	// Legacy hashed branch mapping.
	branchDir := filepath.Join(cacheDir, "index", "branches")
	if err := os.MkdirAll(branchDir, 0755); err != nil {
		t.Fatal(err)
	}
	b := &branchEntry{Repo: "org/repo", Rev: "main", Commit: commit, CheckedAt: time.Now()}
	rawB, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	bSum := sha256.Sum256([]byte("org/repo@main"))
	legacyBranch := filepath.Join(branchDir, hex.EncodeToString(bSum[:])+".json")
	if err := os.WriteFile(legacyBranch, rawB, 0644); err != nil {
		t.Fatal(err)
	}

	// Legacy flat file entry.
	key := "/org/repo/resolve/" + commit + "/f.bin"
	e := &fileEntry{Key: key, State: stateReady, Size: 1, ETag: "etag-1", Commit: commit, CheckedAt: time.Now()}
	rawE, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	eSum := sha256.Sum256([]byte(key))
	flatEntry := filepath.Join(cacheDir, "index", hex.EncodeToString(eSum[:])+".json")
	if err := os.WriteFile(flatEntry, rawE, 0644); err != nil {
		t.Fatal(err)
	}

	m, _ := newTestMirror(t, upstreamSrv.URL, t.TempDir(), cacheDir)

	if _, err := os.Stat(legacyBranch); !os.IsNotExist(err) {
		t.Fatalf("legacy hashed branch mapping still present: %v", err)
	}
	readable := filepath.Join(branchDir, "org", "repo", "main.json")
	if _, err := os.Stat(readable); err != nil {
		t.Fatalf("migrated branch mapping missing: %v", err)
	}

	if _, err := os.Stat(flatEntry); !os.IsNotExist(err) {
		t.Fatalf("legacy flat entry still present: %v", err)
	}
	moved := filepath.Join(cacheDir, "index", commit, hex.EncodeToString(eSum[:])+".json")
	if _, err := os.Stat(moved); err != nil {
		t.Fatalf("migrated file entry missing: %v", err)
	}

	m.mu.Lock()
	k, _ := parseResolveKey(key)
	loadedEntry := m.entries[k]
	loadedBranch := m.branches["org/repo\x00main"]
	m.mu.Unlock()
	if loadedEntry == nil {
		t.Fatal("migrated file entry not loaded")
	}
	if loadedBranch == nil || loadedBranch.Commit != commit {
		t.Fatalf("migrated branch mapping not loaded: %+v", loadedBranch)
	}
}
