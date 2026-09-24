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
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
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

	stor, err := local.NewStorage(local.WithBasePath(storageDir))
	if err != nil {
		t.Fatal(err)
	}
	selector, err := StaticUpstream(upstream, "")
	if err != nil {
		t.Fatal(err)
	}

	m, err := NewMirror(append([]Option{
		WithStorage(stor),
		WithUpstream(selector),
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
	gateOnce sync.Once
}

func newPlainUpstream() *plainUpstream {
	return &plainUpstream{files: map[string][]byte{}, api: map[string][]byte{}, commit: "commit-1"}
}

// set publishes data at path; like a hub, the branch head is also served at
// the current commit when that is a real 40-hex commit.
func (u *plainUpstream) set(path string, data []byte) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.files[path] = data
	if seg := resolveRe.FindStringSubmatch(path); seg != nil && commitRevRe.MatchString(u.commit) {
		u.files["/"+seg[1]+"/resolve/"+u.commit+"/"+seg[3]] = data
	}
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
			u.gateOnce.Do(func() { close(u.gateHit) })
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
	upstream.commit = strings.Repeat("ab", 20)
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
	if commit != upstream.commit {
		t.Fatalf("commit = %q, want %s", commit, upstream.commit)
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
	if entry.Size != int64(len(data)) || entry.Commit != upstream.commit {
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

func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestBranchEntryPath(t *testing.T) {
	dir := "idx"

	cases := []struct{ repo, rev string }{
		{"Qwen/Qwen3-0.6B", "main"},
		{"datasets/org/repo", "refs%2Fpr%2F1"},
		{"..", ".."},
		{"org/main.json", "x.json.tmp"},
		{".hidden/repo", ".rev"},
		{"a%b\\c", "r:s\x01"},
		{"org/", ""},
		{strings.Repeat("x", 300), strings.Repeat("r", 300)},
	}
	wantLen := len(filepath.Join(hashHex(""), "branches", hashHex("")+".json"))
	for _, c := range cases {
		want := filepath.Join(dir, hashHex(c.repo), "branches", hashHex(c.rev)+".json")
		got := branchEntryPath(dir, c.repo, c.rev)
		if got != want {
			t.Errorf("branchEntryPath(%q, %q) = %q, want %q", c.repo, c.rev, got, want)
		}
		if rel, _ := filepath.Rel(dir, got); len(rel) != wantLen {
			t.Errorf("branchEntryPath(%q, %q) suffix length %d, want %d", c.repo, c.rev, len(rel), wantLen)
		}
	}

	t.Run("distinct identities never share a path", func(t *testing.T) {
		pairs := [][2][2]string{
			{{"org/repo", "main"}, {"datasets/org/repo", "main"}},
			{{"Org/Repo", "Main"}, {"org/repo", "main"}},
			{{"org/repo", "main"}, {"org/repo", "main.json"}},
		}
		for _, p := range pairs {
			a := branchEntryPath(dir, p[0][0], p[0][1])
			b := branchEntryPath(dir, p[1][0], p[1][1])
			if a == b || strings.EqualFold(a, b) {
				t.Errorf("%v and %v map to %q and %q", p[0], p[1], a, b)
			}
		}
	})
}

func TestCommitPath(t *testing.T) {
	dir := "idx"
	commit := strings.Repeat("ab", 20)

	for _, repo := range []string{
		"Qwen/Qwen3-0.6B",
		"gpt2",
		"datasets/org/repo",
		"..",
		".hidden/repo",
		"a%b\\c",
		"org/main.json",
		strings.Repeat("x", 300),
	} {
		want := filepath.Join(dir, hashHex(repo), "commits", commit+".json")
		if got := commitPath(dir, repo, commit); got != want {
			t.Errorf("commitPath(%q) = %q, want %q", repo, got, want)
		}
	}

	t.Run("one repo dir holds branches and commits", func(t *testing.T) {
		prefix := filepath.Join(dir, hashHex("org/repo")) + string(filepath.Separator)
		for _, p := range []string{
			branchEntryPath(dir, "org/repo", "main"),
			commitPath(dir, "org/repo", commit),
		} {
			if !strings.HasPrefix(p, prefix) {
				t.Errorf("%q not under %q", p, prefix)
			}
		}
	})

	t.Run("distinct repos never share a manifest", func(t *testing.T) {
		for _, p := range [][2]string{
			{"org/repo", "datasets/org/repo"},
			{"Org/Repo", "org/repo"},
			{"org/repo", "org/repo.json"},
		} {
			a, b := commitPath(dir, p[0], commit), commitPath(dir, p[1], commit)
			if a == b || strings.EqualFold(a, b) {
				t.Errorf("%q and %q map to %q and %q", p[0], p[1], a, b)
			}
		}
	})
}

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
	entry, err := in.Entry()
	if err != nil {
		t.Fatal(err)
	}
	if entry.Commit != commit {
		t.Fatalf("exported commit = %q, want %s", entry.Commit, commit)
	}

	repoDir := filepath.Join(cacheDir, "index", hashHex("Qwen/Qwen3-0.6B"))
	pointerPath := filepath.Join(repoDir, "branches", hashHex("main")+".json")
	manifestPath := filepath.Join(repoDir, "commits", commit+".json")

	if got, want := indexFiles(t, filepath.Join(cacheDir, "index")), []string{pointerPath, manifestPath}; strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("index files = %q, want %q", got, want)
	}

	pointer := jsonKeys(t, pointerPath)
	if got := sortedKeys(pointer); strings.Join(got, ",") != "checked_at,commit" {
		t.Fatalf("branch pointer keys = %v, want only checked_at and commit", got)
	}
	if got := string(pointer["commit"]); got != `"`+commit+`"` {
		t.Fatalf("branch pointer commit = %s, want %s", got, commit)
	}

	manifest := jsonKeys(t, manifestPath)
	if got := sortedKeys(manifest); strings.Join(got, ",") != "commit,files,repo" {
		t.Fatalf("manifest keys = %v, want commit,files,repo", got)
	}
	if string(manifest["repo"]) != `"Qwen/Qwen3-0.6B"` || string(manifest["commit"]) != `"`+commit+`"` {
		t.Fatalf("manifest identity = %s %s, want repo Qwen/Qwen3-0.6B commit %s", manifest["repo"], manifest["commit"], commit)
	}
	var files map[string]json.RawMessage
	if err := json.Unmarshal(manifest["files"], &files); err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files["f.bin"] == nil {
		t.Fatalf("manifest files = %s, want exactly f.bin", manifest["files"])
	}
	var record map[string]json.RawMessage
	if err := json.Unmarshal(files["f.bin"], &record); err != nil {
		t.Fatal(err)
	}
	if got := sortedKeys(record); strings.Join(got, ",") != "checked_at,etag,file_hash,sha256,size" {
		t.Fatalf("file record keys = %v, want checked_at,etag,file_hash,sha256,size", got)
	}
	if got := string(record["size"]); got != fmt.Sprint(len(data)) {
		t.Fatalf("file record size = %s, want %d", got, len(data))
	}
}

func TestMirrorUsage(t *testing.T) {
	ctx := context.Background()
	cacheDir := t.TempDir()
	m, _ := newTestMirror(t, "http://upstream.invalid", t.TempDir(), cacheDir)
	if got, err := m.Usage(ctx); err != nil || got != (Usage{}) {
		t.Fatalf("usage of fresh mirror = %+v, %v; want zero", got, err)
	}

	commit := strings.Repeat("ab", 20)
	manifest := commitPath(m.indexDir, "org/repo", commit)
	pointer := branchEntryPath(m.indexDir, "org/repo", "main")
	manifestJSON := []byte(`{"repo":"org/repo","commit":"` + commit + `","files":{}}`)
	pointerJSON := []byte(`{"commit":"` + commit + `"}`)
	writeRaw(t, manifest, manifestJSON)
	writeRaw(t, pointer, pointerJSON)
	writeRaw(t, manifest+".tmp", []byte("partial"))
	wantIndex := storage.ObjectUsage{Count: 3, Bytes: int64(len(manifestJSON) + len(pointerJSON) + len("partial"))}
	writeRaw(t, filepath.Join(cacheDir, "chunks", "aa", "bb", "cc", "x.json"), []byte(`{}`))
	writeRaw(t, filepath.Join(cacheDir, "chunks", "y.spool"), []byte("not ours"))
	if runtime.GOOS != "windows" {
		if err := os.Symlink(manifest, filepath.Join(filepath.Dir(pointer), "link.json")); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := m.Usage(ctx); err != nil || got != (Usage{Index: wantIndex}) {
		t.Fatalf("usage with index files = %+v, %v; want %+v", got, err, Usage{Index: wantIndex})
	}

	sp, err := openSpool(m.spoolDir, "/org/repo/resolve/main/f.bin", "etag1", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sp.Write(make([]byte, 40)); err != nil {
		t.Fatal(err)
	}
	if got, err := m.Usage(ctx); err != nil || got.Spool != (storage.ObjectUsage{Count: 1, Bytes: 40}) {
		t.Fatalf("usage with in-flight spool = %+v, %v; want spool count 1 bytes 40", got, err)
	}
	sp.finish(errors.New("interrupted"))
	consumed, err := openSpool(m.spoolDir, "/org/repo/resolve/main/g.bin", "etag2", 100)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumed.Write(make([]byte, 60)); err != nil {
		t.Fatal(err)
	}
	if got, err := m.Usage(ctx); err != nil || got.Spool != (storage.ObjectUsage{Count: 2, Bytes: 100}) {
		t.Fatalf("usage with retained and in-flight spools = %+v, %v; want spool count 2 bytes 100", got, err)
	}
	consumed.markRemove()
	consumed.finish(nil)
	want := Usage{Index: wantIndex, Spool: storage.ObjectUsage{Count: 1, Bytes: 40}}
	if got, err := m.Usage(ctx); err != nil || got != want {
		t.Fatalf("usage after ingest consumed a spool = %+v, %v; want %+v", got, err, want)
	}

	bare := &Mirror{indexDir: m.indexDir, spoolDir: m.spoolDir}
	if got, err := bare.Usage(ctx); err != nil || got != want {
		t.Fatalf("usage from directories only = %+v, %v; want %+v", got, err, want)
	}
	m.mu.Lock()
	loaded := len(m.entries) + len(m.commits) + len(m.branches)
	m.mu.Unlock()
	if loaded != 0 {
		t.Fatalf("usage loaded %d in-memory entries", loaded)
	}

	t.Run("missing dirs", func(t *testing.T) {
		root := filepath.Join(t.TempDir(), "never-created")
		absent := &Mirror{indexDir: filepath.Join(root, "index"), spoolDir: filepath.Join(root, "spool")}
		if got, err := absent.Usage(ctx); err != nil || got != (Usage{}) {
			t.Fatalf("usage of missing dirs = %+v, %v; want zero", got, err)
		}
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			t.Fatalf("usage must not create directories: %v", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		for _, mir := range []*Mirror{absent, m} {
			if got, err := mir.Usage(canceled); !errors.Is(err, context.Canceled) || got != (Usage{}) {
				t.Fatalf("usage with canceled ctx = %+v, %v; want zero, context.Canceled", got, err)
			}
		}
	})

	t.Run("scan failure", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("ENOTDIR is not reported on windows")
		}
		file := filepath.Join(t.TempDir(), "file")
		writeRaw(t, file, []byte("not a directory"))
		broken := &Mirror{indexDir: filepath.Join(file, "index"), spoolDir: m.spoolDir}
		if got, err := broken.Usage(ctx); !errors.Is(err, syscall.ENOTDIR) || got != (Usage{}) {
			t.Fatalf("usage through a file = %+v, %v; want zero, ENOTDIR", got, err)
		}
	})
}

// indexFiles lists every regular file under dir, sorted.
func indexFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	return files
}

func jsonKeys(t *testing.T, path string) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return obj
}

func sortedKeys(obj map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func writeRaw(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
}

func readManifest(t *testing.T, path string) commitManifest {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var man commitManifest
	if err := json.Unmarshal(raw, &man); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return man
}

// rawSource reads the manifest's source field from disk, "" when absent.
func rawSource(t *testing.T, path string) string {
	t.Helper()
	var src string
	if raw := jsonKeys(t, path)["source"]; raw != nil {
		if err := json.Unmarshal(raw, &src); err != nil {
			t.Fatalf("%s: source %s: %v", path, raw, err)
		}
	}
	return src
}

func ingestWait(t *testing.T, m *Mirror, repo, rev, path string) (*Entry, error) {
	t.Helper()
	in, err := m.Ingest(repo, rev, path)
	if err != nil {
		t.Fatal(err)
	}
	<-in.Done()
	return in.Entry()
}

// wantPseudo spells out the pseudo-commit scheme the index must keep using.
func wantPseudo(repo, rev string) string {
	sum := sha256.Sum256([]byte("xet-mirror-pseudo-commit\x00" + repo + "\x00" + rev))
	return hex.EncodeToString(sum[:20])
}

// countingServer serves upstream, counting requests and recording their paths.
func countingServer(t *testing.T, upstream http.Handler) (*httptest.Server, *atomic.Int64, *sync.Map) {
	t.Helper()
	var requests atomic.Int64
	var paths sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		paths.Store(r.URL.Path, true)
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, &requests, &paths
}

func deadServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	return srv, requests
}

// HTTP handlers cannot use t.Fatal to release a blocked gate.
func gateWait(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	case <-time.After(10 * time.Second):
		return false
	}
}

func TestMirrorSyntheticPin(t *testing.T) {
	pseudo := wantPseudo("org/repo", "main")
	for _, tc := range []struct{ name, header string }{{"absent", ""}, {"invalid", "not-a-commit"}, {"own pseudo", pseudo}} {
		t.Run("X-Repo-Commit "+tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.commit = tc.header
			dataA, dataB := []byte("synthetic a v1"), []byte("synthetic b")
			upstream.set("/org/repo/resolve/main/a.bin", dataA)
			upstream.set("/org/repo/resolve/main/b.bin", dataB)
			srv, _, paths := countingServer(t, upstream)

			storageDir, cacheDir := t.TempDir(), t.TempDir()
			indexDir := filepath.Join(cacheDir, "index")
			m, stor := newTestMirror(t, srv.URL, storageDir, cacheDir, WithRevalidateInterval(0))
			manifestPath := commitPath(indexDir, "org/repo", pseudo)

			for _, p := range []string{"a.bin", "b.bin"} {
				entry, err := ingestWait(t, m, "org/repo", "main", p)
				if err != nil {
					t.Fatal(err)
				}
				if entry.Commit != pseudo {
					t.Fatalf("%s: Commit = %q, want pseudo %s", p, entry.Commit, pseudo)
				}
			}
			if got := sortedKeys(jsonKeys(t, manifestPath)); strings.Join(got, ",") != "commit,files,repo,source" {
				t.Errorf("manifest keys = %v, want commit,files,repo,source", got)
			}
			if src := rawSource(t, manifestPath); src != "main" {
				t.Errorf("manifest source = %q, want main", src)
			}
			if man := readManifest(t, manifestPath); len(man.Files) != 2 {
				t.Fatalf("manifest = %+v, want a.bin and b.bin", man)
			}
			pointer := jsonKeys(t, branchEntryPath(indexDir, "org/repo", "main"))
			if got := sortedKeys(pointer); strings.Join(got, ",") != "checked_at,commit" || string(pointer["commit"]) != `"`+pseudo+`"` {
				t.Fatalf("branch pointer = %s, want commit %s and checked_at only", pointer, pseudo)
			}

			// a.bin changes upstream: its etag revalidation re-downloads it,
			// while the untouched sibling keeps serving from cache.
			dataA2 := []byte("synthetic a v2")
			upstream.set("/org/repo/resolve/main/a.bin", dataA2)
			before := upstream.dataGETs.Load()
			entry, err := ingestWait(t, m, "org/repo", "main", "a.bin")
			if err != nil {
				t.Fatal(err)
			}
			if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, dataA2) {
				t.Fatal("changed file not re-downloaded")
			}
			if got := upstream.dataGETs.Load(); got != before+1 {
				t.Fatalf("data GETs after change = %d, want %d", got, before+1)
			}
			if entry, err = ingestWait(t, m, "org/repo", "main", "b.bin"); err != nil || entry.Commit != pseudo {
				t.Fatalf("sibling = %+v, %v", entry, err)
			}
			if got := upstream.dataGETs.Load(); got != before+1 {
				t.Fatalf("sibling revalidation downloaded: data GETs = %d, want %d", got, before+1)
			}
			if man := readManifest(t, manifestPath); len(man.Files) != 2 || rawSource(t, manifestPath) != "main" {
				t.Errorf("manifest after refresh = %+v", man)
			}

			// Restart: the saved source serves direct pseudo-commit misses and revalidations from the branch.
			m2, stor2 := newTestMirror(t, srv.URL, storageDir, cacheDir, WithRevalidateInterval(0))
			dataC := []byte("synthetic c")
			upstream.set("/org/repo/resolve/main/c.bin", dataC)
			if entry, err = ingestWait(t, m2, "org/repo", pseudo, "c.bin"); err != nil || entry.Commit != pseudo {
				t.Fatalf("direct pseudo c.bin after restart = %+v, %v", entry, err)
			}
			if _, ok := paths.Load("/org/repo/resolve/main/c.bin"); !ok {
				t.Fatal("c.bin was not fetched through the source branch")
			}
			dataA3 := []byte("synthetic a v3")
			upstream.set("/org/repo/resolve/main/a.bin", dataA3)
			if entry, err = ingestWait(t, m2, "org/repo", pseudo, "a.bin"); err != nil {
				t.Fatal(err)
			}
			if got := readStored(t, stor2, entry.SHA256); !bytes.Equal(got, dataA3) {
				t.Fatal("direct pseudo request served the stale a.bin")
			}

			paths.Range(func(p, _ any) bool {
				if strings.Contains(p.(string), pseudo) {
					t.Errorf("upstream requested by pseudo commit: %s", p)
				}
				return true
			})
		})
	}

	t.Run("source pinned before first publish", func(t *testing.T) {
		upstream := newPlainUpstream()
		upstream.commit = ""
		upstream.set("/org/repo/resolve/main/held.bin", []byte("held"))
		upstream.set("/org/repo/resolve/main/other.bin", []byte("other"))
		hold := make(chan struct{})
		var release sync.Once
		defer release.Do(func() { close(hold) })
		srv, _, paths := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/held.bin") {
				<-hold
			}
			upstream.ServeHTTP(w, r)
		}))

		storageDir, cacheDir := t.TempDir(), t.TempDir()
		indexDir := filepath.Join(cacheDir, "index")
		manifestPath := commitPath(indexDir, "org/repo", pseudo)
		m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
		res, err := m.Resolve(context.Background(), "org/repo", "main", "held.bin")
		if err != nil || res.Stream == nil {
			t.Fatalf("Resolve = %+v, %v; want an in-flight stream", res, err)
		}
		if man := readManifest(t, manifestPath); len(man.Files) != 0 || rawSource(t, manifestPath) != "main" {
			t.Fatalf("manifest at pin time = %+v, source %q; want source main and no files", man, rawSource(t, manifestPath))
		}
		if _, err := os.Stat(branchEntryPath(indexDir, "org/repo", "main")); err != nil {
			t.Fatalf("branch pointer missing at pin time: %v", err)
		}

		m2, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
		entry, err := ingestWait(t, m2, "org/repo", pseudo, "other.bin")
		if err != nil || entry.Commit != pseudo {
			t.Fatalf("direct pseudo other.bin before any publish = %+v, %v", entry, err)
		}
		if _, ok := paths.Load("/org/repo/resolve/main/other.bin"); !ok {
			t.Fatal("other.bin was not fetched through the source branch")
		}
		release.Do(func() { close(hold) })
		if _, _, err := res.Stream.WaitMeta(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent source pin waits for persistence", func(t *testing.T) {
		engine := &Mirror{
			indexDir: t.TempDir(),
			commits: map[string]*commitState{
				"org/repo\x00" + pseudo: {source: "main"},
			},
		}
		engine.persistMu.Lock()
		var release sync.Once
		unlock := func() { release.Do(engine.persistMu.Unlock) }
		defer unlock()
		started, done := make(chan struct{}), make(chan struct{})
		go func() {
			close(started)
			engine.ensureSource("org/repo", pseudo, "main")
			close(done)
		}()
		<-started
		select {
		case <-done:
			t.Error("source pin returned before the pending manifest write")
		case <-time.After(50 * time.Millisecond):
		}
		manifestPath := commitPath(engine.indexDir, "org/repo", pseudo)
		if err := writeJSON(manifestPath, commitManifest{
			Repo: "org/repo", Commit: pseudo, Source: "main", Files: map[string]*fileEntry{},
		}); err != nil {
			t.Fatal(err)
		}
		unlock()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("source pin did not finish after persistence")
		}
		if source := rawSource(t, manifestPath); source != "main" {
			t.Fatalf("source = %q, want main", source)
		}
	})

	t.Run("real commit never revalidates", func(t *testing.T) {
		upstream := newPlainUpstream()
		commit := strings.Repeat("ab", 20)
		upstream.commit = commit
		data := []byte("real v1")
		upstream.set("/org/repo/resolve/main/f.bin", data)
		srv, requests, _ := countingServer(t, upstream)

		cacheDir := t.TempDir()
		m, _ := newTestMirror(t, srv.URL, t.TempDir(), cacheDir, WithRevalidateInterval(0))
		entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
		if err != nil || entry.Commit != commit {
			t.Fatalf("entry = %+v, %v", entry, err)
		}
		if got := sortedKeys(jsonKeys(t, commitPath(filepath.Join(cacheDir, "index"), "org/repo", commit))); strings.Join(got, ",") != "commit,files,repo" {
			t.Fatalf("real commit manifest keys = %v, want commit,files,repo", got)
		}

		// Same commit, new etag: the pinned content is immutable, so the
		// branch probe is the only upstream traffic and nothing re-downloads.
		upstream.set("/org/repo/resolve/main/f.bin", []byte("real v2"))
		before, gets := requests.Load(), upstream.dataGETs.Load()
		res, err := m.Resolve(context.Background(), "org/repo", "main", "f.bin")
		if err != nil || res.Entry == nil || res.Entry.SHA256 != entry.SHA256 {
			t.Fatalf("branch resolve = %+v, %v; want the cached entry", res, err)
		}
		if got := requests.Load() - before; got != 2 { // hub HEAD + redirect hop
			t.Fatalf("branch resolve made %d upstream requests, want the branch probe only", got)
		}
		if upstream.dataGETs.Load() != gets {
			t.Fatal("real commit entry was re-downloaded")
		}
		before = requests.Load()
		if res, err = m.Resolve(context.Background(), "org/repo", commit, "f.bin"); err != nil || res.Entry == nil {
			t.Fatalf("commit resolve = %+v, %v", res, err)
		}
		if got := requests.Load() - before; got != 0 {
			t.Fatalf("commit resolve made %d upstream requests, want 0", got)
		}
	})
}

func TestMirrorUnpinnableBranch(t *testing.T) {
	upstream := newPlainUpstream()
	commit := strings.Repeat("11", 20)
	upstream.commit = commit
	upstream.set("/org/repo/resolve/main/ok.bin", []byte("ok"))
	upstream.set("/org/repo/resolve/"+commit+"/other.bin", []byte("other"))
	var down atomic.Bool
	srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/down.bin") || (down.Load() && strings.HasPrefix(r.URL.Path, "/org/repo/resolve/main/")) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		upstream.ServeHTTP(w, r)
	}))

	cacheDir := t.TempDir()
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), cacheDir, WithRevalidateInterval(0))
	ctx := context.Background()

	for _, tc := range []struct {
		path     string
		notFound bool
	}{{"down.bin", false}, {"missing.bin", true}} {
		before := requests.Load()
		_, err := m.Resolve(ctx, "org/repo", "main", tc.path)
		if err == nil || errors.Is(err, ErrUpstreamNotFound) != tc.notFound {
			t.Fatalf("%s: err = %v, want not-found %v", tc.path, err, tc.notFound)
		}
		if _, err := ingestWait(t, m, "org/repo", "main", tc.path); err == nil {
			t.Fatalf("%s: Ingest succeeded against an unpinnable branch", tc.path)
		}
		if _, err := m.Resolve(ctx, "org/repo", "main", tc.path); err == nil {
			t.Fatalf("%s: repeated Resolve succeeded", tc.path)
		}
		if got := requests.Load() - before; got != 1 {
			t.Fatalf("%s: %d upstream requests for three failing calls, want 1", tc.path, got)
		}
	}
	m.mu.Lock()
	tasks := len(m.tasks)
	fe := m.entries[resolveKey{repo: "org/repo", rev: "main", path: "down.bin"}]
	m.mu.Unlock()
	if tasks != 0 || fe == nil || fe.State != stateFailed || fe.failures != 1 {
		t.Fatalf("tasks = %d, failure record = %+v; want no task and one recorded failure", tasks, fe)
	}
	if files := indexFiles(t, filepath.Join(cacheDir, "index")); len(files) != 0 {
		t.Fatalf("unpinnable probes persisted %v", files)
	}
	if spools, _ := os.ReadDir(filepath.Join(cacheDir, "spool")); len(spools) != 0 {
		t.Fatalf("unpinnable probes spooled %d files", len(spools))
	}

	// Once the backoff expires the probe is retried and the backoff grows.
	m.mu.Lock()
	fe.nextRetry = time.Time{}
	m.mu.Unlock()
	if _, err := m.Resolve(ctx, "org/repo", "main", "down.bin"); err == nil {
		t.Fatal("retry succeeded")
	}
	m.mu.Lock()
	fe = m.entries[resolveKey{repo: "org/repo", rev: "main", path: "down.bin"}]
	m.mu.Unlock()
	if fe.failures != 2 || time.Until(fe.nextRetry) <= failureBackoffBase {
		t.Fatalf("second failure = %+v, want failures 2 with a longer backoff", fe)
	}

	// A pinned branch keeps serving its cached commit while the upstream is
	// down, and new files ingest from the pinned commit, not from the failed
	// branch probe.
	if entry, err := ingestWait(t, m, "org/repo", "main", "ok.bin"); err != nil || entry.Commit != commit {
		t.Fatalf("ok.bin = %+v, %v", entry, err)
	}
	down.Store(true)
	res, err := m.Resolve(ctx, "org/repo", "main", "ok.bin")
	if err != nil || res.Entry == nil || res.Entry.Commit != commit {
		t.Fatalf("stale branch resolve = %+v, %v; want the cached entry", res, err)
	}
	entry, err := ingestWait(t, m, "org/repo", "main", "other.bin")
	if err != nil || entry.Commit != commit || entry.Size != int64(len("other")) {
		t.Fatalf("other.bin through stale pin = %+v, %v", entry, err)
	}

	// The pinned branch never bypasses a file's active failure: no probe, same record.
	before := requests.Load()
	if _, err := m.Resolve(ctx, "org/repo", "main", "down.bin"); err == nil {
		t.Fatal("down.bin resolved inside its backoff")
	}
	m.mu.Lock()
	again := m.entries[resolveKey{repo: "org/repo", rev: "main", path: "down.bin"}]
	m.mu.Unlock()
	if got := requests.Load() - before; got != 0 || again != fe {
		t.Fatalf("backoff bypassed: %d upstream requests, record = %+v, want 0 and %+v", got, again, fe)
	}
	m.mu.Lock()
	fe.nextRetry = time.Time{}
	m.mu.Unlock()
	if _, err := ingestWait(t, m, "org/repo", "main", "down.bin"); err == nil {
		t.Fatal("down.bin ingested from a failing upstream")
	}
	if requests.Load() == before {
		t.Fatal("expired backoff did not probe the upstream")
	}
}

func TestMirrorConcurrentPublish(t *testing.T) {
	const n = 16
	for _, tc := range []struct{ name, header string }{{"real", strings.Repeat("ab", 20)}, {"synthetic", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.commit = tc.header
			commit, wantSource := tc.header, ""
			if commit == "" {
				commit, wantSource = wantPseudo("org/repo", "main"), "main"
			}
			hold := make(chan struct{})
			srv, _, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/slow.bin") {
					<-hold
				}
				upstream.ServeHTTP(w, r)
			}))
			upstream.set("/org/repo/resolve/main/slow.bin", []byte("slow"))

			storageDir, cacheDir := t.TempDir(), t.TempDir()
			manifestPath := commitPath(filepath.Join(cacheDir, "index"), "org/repo", commit)
			ingestAll := func(m *Mirror, prefix string) {
				t.Helper()
				var wg sync.WaitGroup
				for i := range n {
					name := fmt.Sprintf("%s%02d.bin", prefix, i)
					upstream.set("/org/repo/resolve/main/"+name, []byte(name))
					wg.Go(func() {
						if _, err := ingestWait(t, m, "org/repo", "main", name); err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
			}
			check := func(want int) {
				t.Helper()
				man := readManifest(t, manifestPath)
				if src := rawSource(t, manifestPath); len(man.Files) != want || src != wantSource || man.Commit != commit {
					t.Fatalf("manifest has %d files, source %q, commit %s; want %d, %q, %s", len(man.Files), src, man.Commit, want, wantSource, commit)
				}
				for p := range man.Files {
					if p == "slow.bin" || p == "missing.bin" {
						t.Fatalf("manifest lists %s, which never became ready", p)
					}
				}
			}

			m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
			var release sync.Once
			t.Cleanup(func() { release.Do(func() { close(hold) }) })
			slow, err := m.Ingest("org/repo", "main", "slow.bin")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ingestWait(t, m, "org/repo", "main", "missing.bin"); !errors.Is(err, ErrUpstreamNotFound) {
				t.Fatalf("missing.bin err = %v", err)
			}
			ingestAll(m, "f")
			check(n)
			release.Do(func() { close(hold) })
			<-slow.Done()
			if _, err := slow.Entry(); err != nil {
				t.Fatal(err)
			}
			if man := readManifest(t, manifestPath); man.Files["slow.bin"] == nil || len(man.Files) != n+1 {
				t.Fatalf("manifest after the slow ingest has %d files, want %d with slow.bin", len(man.Files), n+1)
			}

			m2, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
			ingestAll(m2, "g")
			m2.mu.Lock()
			loaded := len(m2.entries)
			m2.mu.Unlock()
			if loaded != 2*n+1 {
				t.Fatalf("restarted engine holds %d entries, want %d loaded + %d published", loaded, n+1, n)
			}
			if man, src := readManifest(t, manifestPath), rawSource(t, manifestPath); len(man.Files) != 2*n+1 || src != wantSource {
				t.Fatalf("manifest after restart has %d files, source %q; want %d, %q", len(man.Files), src, 2*n+1, wantSource)
			}
		})
	}
}

func TestMirrorIndexRestart(t *testing.T) {
	upstream := newPlainUpstream()
	commit := strings.Repeat("ef", 20)
	upstream.commit = commit
	upstream.set("/org/repo/resolve/main/a.bin", []byte("restart a"))
	upstream.set("/org/repo/resolve/main/b.bin", []byte("restart b"))
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	storageDir, cacheDir := t.TempDir(), t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	m, _ := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)
	for _, p := range []string{"a.bin", "b.bin"} {
		if _, err := ingestWait(t, m, "org/repo", "main", p); err != nil {
			t.Fatal(err)
		}
	}

	dead, requests := deadServer(t)
	m2, stor2 := newTestMirror(t, dead.URL, storageDir, cacheDir, WithRevalidateInterval(-1))
	ctx := context.Background()
	m2.mu.Lock()
	preloaded := len(m2.entries) + len(m2.branches) + len(m2.commits)
	m2.mu.Unlock()
	if preloaded != 0 {
		t.Fatalf("restart preloaded %d records, want lazy maps", preloaded)
	}
	res, err := m2.Resolve(ctx, "org/repo", "main", "a.bin")
	if err != nil || res.Entry == nil || res.Entry.Commit != commit {
		t.Fatalf("Resolve after restart = %+v, %v; want the pinned entry", res, err)
	}
	entryA := res.Entry
	m2.mu.Lock()
	sibling := m2.entries[resolveKey{repo: "org/repo", rev: commit, path: "b.bin"}]
	m2.mu.Unlock()
	if sibling == nil {
		t.Fatal("sibling not loaded with the manifest")
	}
	for _, rev := range []string{"main", commit} {
		if res, err = m2.Resolve(ctx, "org/repo", rev, "b.bin"); err != nil || res.Entry == nil || res.Entry.Commit != commit {
			t.Fatalf("Resolve %s/b.bin = %+v, %v", rev, res, err)
		}
	}
	if count := requests.Load(); count != 0 {
		t.Fatalf("restart made %d upstream requests", count)
	}

	// Unlinked storage drops one file from the manifest and leaves the
	// sibling; dropping the last file removes the manifest.
	gc := storage.NewGC(stor2)
	manifestPath := commitPath(indexDir, "org/repo", commit)
	for i, p := range []string{"a.bin", "b.bin"} {
		hash := entryA.FileHash
		if p == "b.bin" {
			hash = sibling.FileHash
		}
		fileHash, err := xet.ParseFileHash(hash)
		if err != nil {
			t.Fatal(err)
		}
		if removed, err := gc.Unlink(ctx, fileHash); err != nil || !removed {
			t.Fatalf("unlink %s = %v, %v", p, removed, err)
		}
		if _, err := ingestWait(t, m2, "org/repo", "main", p); err == nil {
			t.Fatalf("%s re-ingested from a dead upstream", p)
		}
		if i == 0 {
			if man := readManifest(t, manifestPath); len(man.Files) != 1 || man.Files["b.bin"] == nil {
				t.Fatalf("manifest after dropping a.bin = %+v, want only b.bin", man)
			}
		}
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("empty manifest not removed: %v", err)
	}
	if _, err := os.Stat(branchEntryPath(indexDir, "org/repo", "main")); err != nil {
		t.Fatalf("branch pointer lost: %v", err)
	}

	t.Run("synthetic last drop keeps source", func(t *testing.T) {
		upstream := newPlainUpstream()
		upstream.commit = ""
		data := []byte("synthetic restart a")
		upstream.set("/org/repo/resolve/main/a.bin", data)
		srv, _, paths := countingServer(t, upstream)

		storageDir, cacheDir := t.TempDir(), t.TempDir()
		pseudo := wantPseudo("org/repo", "main")
		manifestPath := commitPath(filepath.Join(cacheDir, "index"), "org/repo", pseudo)
		m, stor := newTestMirror(t, srv.URL, storageDir, cacheDir)
		entry, err := ingestWait(t, m, "org/repo", "main", "a.bin")
		if err != nil {
			t.Fatal(err)
		}
		fileHash, err := xet.ParseFileHash(entry.FileHash)
		if err != nil {
			t.Fatal(err)
		}
		if removed, err := storage.NewGC(stor).Unlink(ctx, fileHash); err != nil || !removed {
			t.Fatalf("unlink = %v, %v", removed, err)
		}

		dead, _ := deadServer(t)
		m2, _ := newTestMirror(t, dead.URL, storageDir, cacheDir, WithRevalidateInterval(-1))
		if _, err := ingestWait(t, m2, "org/repo", "main", "a.bin"); err == nil {
			t.Fatal("a.bin re-ingested from a dead upstream")
		}
		if man, src := readManifest(t, manifestPath), rawSource(t, manifestPath); len(man.Files) != 0 || src != "main" {
			t.Fatalf("manifest after dropping the last file = %+v, source %q; want no files and source main", man, src)
		}

		m3, stor3 := newTestMirror(t, srv.URL, storageDir, cacheDir)
		if entry, err = ingestWait(t, m3, "org/repo", pseudo, "a.bin"); err != nil || entry.Commit != pseudo {
			t.Fatalf("direct pseudo a.bin after restart = %+v, %v", entry, err)
		}
		if got := readStored(t, stor3, entry.SHA256); !bytes.Equal(got, data) {
			t.Fatal("re-ingested bytes mismatch")
		}
		if _, ok := paths.Load("/org/repo/resolve/main/a.bin"); !ok {
			t.Fatal("a.bin was not fetched through the source branch")
		}
		paths.Range(func(p, _ any) bool {
			if strings.Contains(p.(string), pseudo) {
				t.Errorf("upstream requested by pseudo commit: %s", p)
			}
			return true
		})
	})
}

func TestMirrorIndexRejectsMismatchedManifest(t *testing.T) {
	commit, other := strings.Repeat("ab", 20), strings.Repeat("cd", 20)
	pseudo := wantPseudo("org/repo", "main")
	record := `{"size":1,"etag":"e","checked_at":"2026-01-01T00:00:00Z"}`
	for _, live := range []bool{false, true} {
		t.Run(fmt.Sprintf("live upstream %v", live), func(t *testing.T) {
			cacheDir := t.TempDir()
			indexDir := filepath.Join(cacheDir, "index")
			for rev, c := range map[string]string{"main": commit, "dev": other} {
				writeRaw(t, branchEntryPath(indexDir, "org/repo", rev), []byte(`{"commit":"`+c+`","checked_at":"2026-01-01T00:00:00Z"}`))
			}
			manifests := map[string][]byte{
				commitPath(indexDir, "org/repo", commit): []byte(`{"repo":"org/other","commit":"` + commit + `","files":{"f.bin":` + record + `}}`),
				commitPath(indexDir, "org/repo", other):  []byte(`{"repo":"org/repo","commit":"` + commit + `","files":{"f.bin":` + record + `}}`),
				commitPath(indexDir, "org/repo", pseudo): []byte(`{"repo":"org/repo","commit":"` + pseudo + `","source":"dev","files":{"f.bin":` + record + `}}`),
			}
			for p, raw := range manifests {
				writeRaw(t, p, raw)
			}
			before := indexFiles(t, indexDir)

			srv, _ := deadServer(t)
			if live {
				upstream := newPlainUpstream()
				upstream.commit = commit
				upstream.set("/org/repo/resolve/main/f.bin", []byte("live f"))
				upstream.set("/org/repo/resolve/"+other+"/f.bin", []byte("live other f"))
				srv, _, _ = countingServer(t, upstream)
			}
			m, _ := newTestMirror(t, srv.URL, t.TempDir(), cacheDir, WithRevalidateInterval(-1))
			for _, rev := range []string{"main", "dev", commit, other, pseudo} {
				entry, err := ingestWait(t, m, "org/repo", rev, "f.bin")
				if served := err == nil; served != (live && rev != pseudo) { // the pseudo commit's source is unknown
					t.Fatalf("%s/f.bin = %+v, %v; want served %v", rev, entry, err, !served)
				}
			}
			for p, raw := range manifests {
				if got, err := os.ReadFile(p); err != nil || !bytes.Equal(got, raw) {
					t.Fatalf("mismatched manifest %s rewritten: %q, %v", p, got, err)
				}
			}
			if after := indexFiles(t, indexDir); strings.Join(after, "\n") != strings.Join(before, "\n") {
				t.Fatalf("index files changed: %q -> %q", before, after)
			}
		})
	}
}

func TestMirrorIndexUnreadableManifest(t *testing.T) {
	for _, tc := range []struct{ name, header string }{{"real", strings.Repeat("ab", 20)}, {"synthetic", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.commit = tc.header
			commit, wantSource := tc.header, ""
			if commit == "" {
				commit, wantSource = wantPseudo("org/repo", "main"), "main"
			}
			files := []string{"a.bin", "b.bin", "c.bin", "d.bin"}
			for _, p := range files {
				upstream.set("/org/repo/resolve/main/"+p, []byte("unreadable "+p))
			}
			srv, _, _ := countingServer(t, upstream)
			storageDir, cacheDir := t.TempDir(), t.TempDir()
			indexDir := filepath.Join(cacheDir, "index")
			manifestPath := commitPath(indexDir, "org/repo", commit)
			m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir, WithRevalidateInterval(0))
			for _, p := range files[:2] {
				if _, err := ingestWait(t, m, "org/repo", "main", p); err != nil {
					t.Fatal(err)
				}
			}
			orig, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			// A directory in the manifest's place fails every read without ENOENT.
			aside := manifestPath + ".aside"
			if err := os.Rename(manifestPath, aside); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(manifestPath, 0755); err != nil {
				t.Fatal(err)
			}

			m2, stor2 := newTestMirror(t, srv.URL, storageDir, cacheDir, WithRevalidateInterval(0))
			entry, err := ingestWait(t, m2, "org/repo", "main", "c.bin")
			if err != nil || entry.Commit != commit {
				t.Fatalf("c.bin under an unreadable manifest = %+v, %v", entry, err)
			}
			// Dropping the only file in memory must not take the empty-manifest removal path.
			fileHash, err := xet.ParseFileHash(entry.FileHash)
			if err != nil {
				t.Fatal(err)
			}
			if removed, err := storage.NewGC(stor2).Unlink(context.Background(), fileHash); err != nil || !removed {
				t.Fatalf("unlink = %v, %v", removed, err)
			}
			if _, err := ingestWait(t, m2, "org/repo", "main", "c.bin"); err != nil {
				t.Fatal(err)
			}
			if fi, err := os.Lstat(manifestPath); err != nil || !fi.IsDir() {
				t.Fatalf("unreadable manifest path replaced: %v, %v", fi, err)
			}
			if got, err := os.ReadFile(aside); err != nil || !bytes.Equal(got, orig) {
				t.Fatalf("original manifest changed: %q, %v", got, err)
			}

			if err := os.Remove(manifestPath); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(aside, manifestPath); err != nil {
				t.Fatal(err)
			}
			gets := upstream.dataGETs.Load()
			res, err := m2.Resolve(context.Background(), "org/repo", "main", "b.bin")
			if err != nil || res.Entry == nil || res.Entry.Commit != commit {
				t.Fatalf("b.bin after recovery = %+v, %v", res, err)
			}
			if got := upstream.dataGETs.Load(); got != gets {
				t.Fatal("b.bin re-downloaded instead of loaded from the recovered manifest")
			}
			if _, err := ingestWait(t, m2, "org/repo", "main", "d.bin"); err != nil {
				t.Fatal(err)
			}
			man := readManifest(t, manifestPath)
			for _, p := range files {
				if man.Files[p] == nil {
					t.Errorf("merged manifest lacks %s", p)
				}
			}
			if src := rawSource(t, manifestPath); len(man.Files) != len(files) || src != wantSource {
				t.Fatalf("merged manifest has %d files, source %q; want %d, %q", len(man.Files), src, len(files), wantSource)
			}
			if got := indexFiles(t, indexDir); len(got) != 2 {
				t.Fatalf("index files = %q, want the pointer and the manifest only", got)
			}
		})
	}

	t.Run("overwrite blocked", func(t *testing.T) {
		if runtime.GOOS == "windows" || os.Geteuid() == 0 {
			t.Skip("needs file permissions the process cannot bypass")
		}
		upstream := newPlainUpstream()
		commit := strings.Repeat("ab", 20)
		upstream.commit = commit
		upstream.set("/org/repo/resolve/main/a.bin", []byte("perm a"))
		upstream.set("/org/repo/resolve/main/b.bin", []byte("perm b"))
		srv, _, _ := countingServer(t, upstream)
		storageDir, cacheDir := t.TempDir(), t.TempDir()
		manifestPath := commitPath(filepath.Join(cacheDir, "index"), "org/repo", commit)
		m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
		if _, err := ingestWait(t, m, "org/repo", "main", "a.bin"); err != nil {
			t.Fatal(err)
		}
		orig, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(manifestPath, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(manifestPath, 0644) })

		m2, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
		if _, err := ingestWait(t, m2, "org/repo", "main", "b.bin"); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(manifestPath, 0644); err != nil {
			t.Fatal(err)
		}
		if got, err := os.ReadFile(manifestPath); err != nil || !bytes.Equal(got, orig) {
			t.Fatalf("unreadable manifest overwritten: %q, %v", got, err)
		}
	})
}

func TestMirrorIndexRecoveryDuringRevalidation(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = ""
	upstream.set("/org/repo/resolve/main/a.bin", []byte("old disk a"))
	upstream.set("/org/repo/resolve/main/b.bin", []byte("disk sibling b"))
	storageDir, cacheDir := t.TempDir(), t.TempDir()
	commit := wantPseudo("org/repo", "main")
	manifestPath := commitPath(filepath.Join(cacheDir, "index"), "org/repo", commit)
	aside := manifestPath + ".aside"
	var recoverOnHead atomic.Bool
	srv, _, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead && recoverOnHead.Swap(false) {
			if err := os.Remove(manifestPath); err != nil {
				t.Error(err)
			}
			if err := os.Rename(aside, manifestPath); err != nil {
				t.Error(err)
			}
		}
		upstream.ServeHTTP(w, r)
	}))
	seed, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	for _, file := range []string{"a.bin", "b.bin"} {
		if _, err := ingestWait(t, seed, "org/repo", "main", file); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Rename(manifestPath, aside); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(manifestPath, 0755); err != nil {
		t.Fatal(err)
	}
	upstream.set("/org/repo/resolve/main/a.bin", []byte("new memory a"))
	m, stor := newTestMirror(t, srv.URL, storageDir, cacheDir, WithRevalidateInterval(0))
	if _, err := ingestWait(t, m, "org/repo", "main", "a.bin"); err != nil {
		t.Fatal(err)
	}
	latest := []byte("latest upstream a")
	upstream.set("/org/repo/resolve/main/a.bin", latest)
	recoverOnHead.Store(true)
	entry, err := ingestWait(t, m, "org/repo", commit, "a.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, latest) {
		t.Errorf("revalidation returned %q, want %q", got, latest)
	}
	m.mu.Lock()
	ready := m.entries[resolveKey{repo: "org/repo", rev: commit, path: "a.bin"}]
	indexed := m.commits["org/repo\x00"+commit].files["a.bin"]
	m.mu.Unlock()
	if ready == nil || ready != indexed {
		t.Errorf("ready entry %+v differs from commit entry %+v", ready, indexed)
	}
	manifest := readManifest(t, manifestPath)
	if manifest.Files["a.bin"] == nil || manifest.Files["b.bin"] == nil {
		t.Fatalf("recovered manifest lost a file: %+v", manifest)
	}
}

func TestMirrorIndexCommitIsolation(t *testing.T) {
	upstream := newPlainUpstream()
	commit, other := strings.Repeat("ab", 20), strings.Repeat("cd", 20)
	upstream.commit = commit
	upstream.set("/org/a/resolve/main/x.bin", []byte("a x"))
	upstream.set("/org/a/resolve/main/y.bin", []byte("a y"))
	upstream.set("/org/a/resolve/"+other+"/z.bin", []byte("a z"))
	upstream.set("/org/b/resolve/main/x.bin", []byte("b x"))
	upstream.set("/org/a/resolve/main/w.bin", []byte("a w"))
	srv, _, _ := countingServer(t, upstream)
	storageDir, cacheDir := t.TempDir(), t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	want := func(repo, commit string, files ...string) {
		t.Helper()
		man := readManifest(t, commitPath(indexDir, repo, commit))
		got := make([]string, 0, len(man.Files))
		for p := range man.Files {
			got = append(got, p)
		}
		sort.Strings(got)
		if man.Repo != repo || man.Commit != commit || strings.Join(got, ",") != strings.Join(files, ",") {
			t.Fatalf("%s@%s manifest = %+v, want files %v", repo, commit, man, files)
		}
	}

	m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	for _, k := range []resolveKey{{"org/a", "main", "x.bin"}, {"org/a", "main", "y.bin"}, {"org/a", other, "z.bin"}, {"org/b", "main", "x.bin"}} {
		if _, err := ingestWait(t, m, k.repo, k.rev, k.path); err != nil {
			t.Fatal(err)
		}
	}
	want("org/a", commit, "x.bin", "y.bin")
	want("org/a", other, "z.bin")
	want("org/b", commit, "x.bin")
	sum := sha256.Sum256([]byte("a x"))
	if got := readManifest(t, commitPath(indexDir, "org/a", commit)).Files["x.bin"].SHA256; got != hex.EncodeToString(sum[:]) {
		t.Fatalf("org/a x.bin sha256 = %s, want its own content", got)
	}

	key := resolveKey{repo: "org/a", rev: commit, path: "x.bin"}
	m.mu.Lock()
	e := m.entries[key]
	m.mu.Unlock()
	m.dropEntry(key, e)
	want("org/a", commit, "y.bin")
	want("org/a", other, "z.bin")
	want("org/b", commit, "x.bin")

	m2, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	if _, err := ingestWait(t, m2, "org/a", "main", "w.bin"); err != nil {
		t.Fatal(err)
	}
	want("org/a", commit, "w.bin", "y.bin")
	want("org/a", other, "z.bin")
	want("org/b", commit, "x.bin")
}

func TestMirrorPersistWaitsForLock(t *testing.T) {
	cacheDir := t.TempDir()
	m, _ := newTestMirror(t, "http://example.invalid", t.TempDir(), cacheDir)
	key := resolveKey{repo: "org/repo", rev: strings.Repeat("ab", 20), path: "f.bin"}
	manifestPath := commitPath(filepath.Join(cacheDir, "index"), key.repo, key.rev)
	e := &fileEntry{State: stateReady, Size: 1, ETag: "e", CheckedAt: time.Unix(1, 0).UTC()}
	m.mu.Lock()
	m.entries[key] = e
	m.loadCommit(key.repo, key.rev).publish(key.path, e)

	m.persistMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(m.persistMu.Unlock) }
	defer unlock()
	done := make(chan error, 1)
	go func() { done <- m.persistCommit(key.repo, key.rev) }()
	m.mu.Unlock() // a writer that skipped persistMu is now free to snapshot and write
	select {
	case err := <-done:
		t.Fatalf("persist finished (%v) while persistMu was held", err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := os.Stat(manifestPath); !os.IsNotExist(err) {
		t.Fatalf("manifest written while persistMu was held: %v", err)
	}

	unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("persist did not finish after persistMu was released")
	}
	if man := readManifest(t, manifestPath); len(man.Files) != 1 || man.Files["f.bin"] == nil || man.Files["f.bin"].ETag != "e" {
		t.Fatalf("manifest after release = %+v, want f.bin only", man)
	}
}

func TestWriteJSONCleansTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.json")
	tmp := filepath.Join(dir, ".x.json.tmp")
	value := func(want string) {
		t.Helper()
		if got := jsonKeys(t, path)["v"]; string(got) != want {
			t.Fatalf("destination v = %s, want %s", got, want)
		}
		if _, err := os.Lstat(tmp); !os.IsNotExist(err) {
			t.Fatalf("temp file left behind: %v", err)
		}
	}
	if err := writeJSON(path, map[string]int{"v": 1}); err != nil {
		t.Fatal(err)
	}
	value("1")
	if err := os.Mkdir(tmp, 0755); err != nil { // a directory in the temp's place fails the write
		t.Fatal(err)
	}
	if err := writeJSON(path, map[string]int{"v": 2}); err == nil {
		t.Fatal("write with an unwritable temp succeeded")
	}
	value("1")
	if err := writeJSON(path, map[string]int{"v": 3}); err != nil {
		t.Fatal(err)
	}
	value("3")
}

func TestMirrorIndexHostileNames(t *testing.T) {
	type pin struct{ repo, rev, commit, path, etag string }
	cA, cB := strings.Repeat("ab", 20), strings.Repeat("cd", 20)
	cases := []struct{ name, repoA, revA, repoB, revB string }{
		{"json rev", "org/repo", "main", "org/repo", "main.json"},
		{"json.tmp rev", "org/repo", "main", "org/repo", "main.json.tmp"},
		{"JSON repo", "org/repo", "main", "org/repo.JSON", "main"},
		{"dot control percent backslash", "..", ".\x01", "a%2E\\b", "%00"},
		{"case variants", "Org/Repo", "Main", "org/repo", "main"},
		{"long names", strings.Repeat("x", 300), "main", strings.Repeat("x", 300), strings.Repeat("r", 300)},
	}
	for _, c := range cases {
		a := pin{c.repoA, c.revA, cA, "a.bin", "ea"}
		b := pin{c.repoB, c.revB, cB, "b.bin", "eb"}
		for _, order := range []string{"A-then-B", "B-then-A"} {
			t.Run(c.name+"/"+order, func(t *testing.T) {
				cacheDir := t.TempDir()
				m, _ := newTestMirror(t, "http://example.invalid", t.TempDir(), cacheDir)
				publish := func(p pin) {
					e := &fileEntry{State: stateReady, Size: 1, ETag: p.etag, CheckedAt: time.Unix(1, 0).UTC()}
					m.mu.Lock()
					m.branches[p.repo+"\x00"+p.rev] = &branchEntry{Commit: p.commit, CheckedAt: time.Unix(1, 0).UTC()}
					m.entries[resolveKey{repo: p.repo, rev: p.commit, path: p.path}] = e
					m.loadCommit(p.repo, p.commit).publish(p.path, e)
					m.mu.Unlock()
					if err := m.persistBranch(p.repo, p.rev); err != nil {
						t.Fatal(err)
					}
					if err := m.persistCommit(p.repo, p.commit); err != nil {
						t.Fatal(err)
					}
				}
				first, second := a, b
				if order == "B-then-A" {
					first, second = b, a
				}
				for _, p := range []pin{first, second, first} {
					publish(p)
				}
				if files := indexFiles(t, filepath.Join(cacheDir, "index")); len(files) != 4 {
					t.Fatalf("index holds %d files, want 2 pointers + 2 manifests: %q", len(files), files)
				}

				m2, _ := newTestMirror(t, "http://example.invalid", t.TempDir(), cacheDir)
				for _, p := range []pin{a, b} {
					m2.mu.Lock()
					bp := m2.loadBranch(p.repo, p.rev)
					m2.loadCommit(p.repo, p.commit)
					e := m2.entries[resolveKey{repo: p.repo, rev: p.commit, path: p.path}]
					m2.mu.Unlock()
					if bp == nil || bp.Commit != p.commit {
						t.Fatalf("branch %q@%q loaded as %+v, want commit %s", p.repo, p.rev, bp, p.commit)
					}
					if e == nil || e.ETag != p.etag {
						t.Fatalf("entry %q@%s/%s loaded as %+v, want etag %s", p.repo, p.commit, p.path, e, p.etag)
					}
				}
			})
		}
	}
}

func TestMirrorIndexSymlinkRoot(t *testing.T) {
	cacheDir := t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	target := t.TempDir()
	if err := os.Symlink(target, indexDir); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	upstream := newPlainUpstream()
	commit := strings.Repeat("ab", 20)
	upstream.commit = commit
	upstream.set("/org/repo/resolve/main/f.bin", []byte("through symlink"))
	upstreamSrv := httptest.NewServer(upstream)
	defer upstreamSrv.Close()

	storageDir := t.TempDir()
	m, _ := newTestMirror(t, upstreamSrv.URL, storageDir, cacheDir)
	if _, err := ingestWait(t, m, "org/repo", "main", "f.bin"); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{branchEntryPath(target, "org/repo", "main"), commitPath(target, "org/repo", commit)} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("record not written behind the symlink: %v", err)
		}
	}

	dead, requests := deadServer(t)
	m2, _ := newTestMirror(t, dead.URL, storageDir, cacheDir, WithRevalidateInterval(-1))
	res, err := m2.Resolve(context.Background(), "org/repo", "main", "f.bin")
	if err != nil || res.Entry == nil || res.Entry.Commit != commit {
		t.Fatalf("Resolve through symlink root = %+v, %v", res, err)
	}
	if count := requests.Load(); count != 0 {
		t.Fatalf("symlink restart made %d upstream requests", count)
	}
}

// Earlier layouts are neither loaded nor touched.
func TestMirrorIndexIgnoresLegacy(t *testing.T) {
	cacheDir := t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	repoDir := filepath.Join(indexDir, hashHex("org/repo"))
	commit := strings.Repeat("cd", 20)
	key := "/org/repo/resolve/" + commit + "/f.bin"
	rawB := []byte(`{"repo":"org/repo","rev":"main","commit":"` + commit + `","checked_at":"2026-01-01T00:00:00Z"}`)
	rawE := []byte(`{"key":"` + key + `","state":"ready","size":1,"etag":"etag-1","commit":"` + commit + `","checked_at":"2026-01-01T00:00:00Z"}`)
	h := hashHex(key)
	leaf := filepath.Join(h[:2], h[2:4], h[4:]+".json")
	legacy := map[string][]byte{
		filepath.Join(indexDir, "branches", hashHex("org/repo@main")+".json"): rawB,
		filepath.Join(indexDir, "branches", "org", "repo", "main.json"):       rawB,
		filepath.Join(indexDir, h+".json"):                                    rawE,
		filepath.Join(indexDir, commit, leaf):                                 rawE,
		filepath.Join(indexDir, "files", "org", "repo", commit, leaf):         rawE,
		filepath.Join(repoDir, "commits", commit, leaf):                       rawE,
		filepath.Join(repoDir, "files", leaf):                                 rawE,
	}
	for p, raw := range legacy {
		writeRaw(t, p, raw)
	}
	before := indexFiles(t, indexDir)

	dead, _ := deadServer(t)
	m, _ := newTestMirror(t, dead.URL, t.TempDir(), cacheDir, WithRevalidateInterval(-1))
	for _, rev := range []string{"main", commit} {
		if entry, err := ingestWait(t, m, "org/repo", rev, "f.bin"); err == nil {
			t.Fatalf("%s/f.bin served from a legacy record: %+v", rev, entry)
		}
	}
	for p, raw := range legacy {
		if got, err := os.ReadFile(p); err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("legacy record %s changed: %q, %v", p, got, err)
		}
	}
	if after := indexFiles(t, indexDir); strings.Join(after, "\n") != strings.Join(before, "\n") {
		t.Fatalf("index files changed: %q -> %q", before, after)
	}

	// A same-path pointer from the previous layout still carries repo and rev keys.
	t.Run("prior pointer reused", func(t *testing.T) {
		cacheDir := t.TempDir()
		indexDir := filepath.Join(cacheDir, "index")
		now := time.Now().UTC().Format(time.RFC3339Nano)
		writeRaw(t, branchEntryPath(indexDir, "org/repo", "main"), []byte(`{"repo":"org/repo","rev":"main","commit":"`+commit+`","checked_at":"`+now+`"}`))
		writeRaw(t, commitPath(indexDir, "org/repo", commit), []byte(`{"repo":"org/repo","commit":"`+commit+`","files":{"f.bin":{"size":1,"etag":"e","checked_at":"`+now+`"}}}`))

		dead, requests := deadServer(t)
		m, _ := newTestMirror(t, dead.URL, t.TempDir(), cacheDir)
		res, err := m.Resolve(context.Background(), "org/repo", "main", "f.bin")
		if err != nil || res.Entry == nil || res.Entry.Commit != commit {
			t.Fatalf("Resolve through the prior pointer = %+v, %v", res, err)
		}
		if count := requests.Load(); count != 0 {
			t.Fatalf("fresh prior pointer made %d upstream requests", count)
		}
	})
}

func TestMirrorBranchProbeFailureAfterPin(t *testing.T) {
	commitB := strings.Repeat("bb", 20)
	for _, synthetic := range []bool{false, true} {
		for _, tc := range []struct {
			status   int
			notFound bool // the failure matches ErrUpstreamNotFound
			keepPin  bool // the old pin keeps serving
		}{
			{http.StatusNotFound, true, false},
			{http.StatusUnauthorized, false, false},
			{http.StatusForbidden, false, false},
			{http.StatusGone, false, false},
			{http.StatusServiceUnavailable, false, true},
			{http.StatusTooManyRequests, false, true},
		} {
			name, commitA, header := fmt.Sprintf("real/%d", tc.status), strings.Repeat("aa", 20), strings.Repeat("aa", 20)
			if synthetic {
				name, commitA, header = fmt.Sprintf("synthetic/%d", tc.status), wantPseudo("org/repo", "main"), ""
			}
			t.Run(name, func(t *testing.T) {
				upstream := newPlainUpstream()
				upstream.commit = header
				upstream.set("/org/repo/resolve/main/f.bin", []byte("pinned content"))
				upstream.set("/org/repo/resolve/main/g.bin", []byte("sibling"))
				var status atomic.Int64
				srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if s := status.Load(); s != 0 && r.URL.Path == "/org/repo/resolve/main/f.bin" {
						w.Header().Set("X-Repo-Commit", commitB)
						w.WriteHeader(int(s))
						return
					}
					upstream.ServeHTTP(w, r)
				}))
				m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
				ctx := context.Background()
				branchKey := resolveKey{repo: "org/repo", rev: "main", path: "f.bin"}
				pinned := func() string {
					m.mu.Lock()
					defer m.mu.Unlock()
					if b := m.branches["org/repo\x00main"]; b != nil {
						return b.Commit
					}
					return ""
				}
				// expire ends the branch key's current backoff and reports its recorded failures.
				expire := func() int {
					m.mu.Lock()
					defer m.mu.Unlock()
					n := 0
					if fe := m.entries[branchKey]; fe != nil && fe.State == stateFailed {
						fe.nextRetry = time.Time{}
						n += fe.failures
					}
					if b := m.branches["org/repo\x00main"]; b != nil {
						b.nextRetry = time.Time{}
						n += b.failures
					}
					return n
				}
				resolveMain := func(step string) {
					t.Helper()
					res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
					if tc.keepPin {
						if err != nil || res.Entry == nil || res.Entry.Commit != commitA {
							t.Fatalf("%s: main = %+v, %v; want the retained pin %s", step, res, err, commitA)
						}
						return
					}
					if err == nil || errors.Is(err, ErrUpstreamNotFound) != tc.notFound {
						t.Fatalf("%s: main = %+v, %v; want a failure with not-found %v", step, res, err, tc.notFound)
					}
				}

				entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
				if err != nil || entry.Commit != commitA {
					t.Fatalf("initial ingest = %+v, %v", entry, err)
				}

				status.Store(int64(tc.status))
				before, gets := requests.Load(), upstream.dataGETs.Load()
				for i := range 3 {
					resolveMain(fmt.Sprintf("resolve %d", i))
				}
				if got := requests.Load() - before; got != 1 {
					t.Fatalf("three resolves made %d upstream requests, want the one probe", got)
				}
				if got := pinned(); got != commitA {
					t.Fatalf("branch pinned to %q after a %d probe, want %s kept", got, tc.status, commitA)
				}
				res, err := m.Resolve(ctx, "org/repo", commitA, "f.bin")
				if err != nil || res.Entry == nil || res.Entry.Commit != commitA {
					t.Fatalf("direct commit = %+v, %v; want the cached entry", res, err)
				}
				if got := requests.Load() - before; got != 1 {
					t.Fatalf("direct commit resolve made %d upstream requests, want 0", got-1)
				}

				// The next backoff window probes once more and grows the failure count.
				if n := expire(); n != 1 {
					t.Fatalf("failures after one probe = %d, want 1", n)
				}
				before = requests.Load()
				resolveMain("after expiry")
				resolveMain("inside the second backoff")
				if got := requests.Load() - before; got != 1 {
					t.Fatalf("second window made %d upstream requests, want 1", got)
				}
				if upstream.dataGETs.Load() != gets {
					t.Fatal("f.bin was re-downloaded during the failure")
				}
				if entry, err := ingestWait(t, m, "org/repo", "main", "g.bin"); err != nil || entry.Commit != commitA {
					t.Fatalf("sibling g.bin = %+v, %v; want commit %s", entry, err, commitA)
				}

				// Recovery: the successful probe serves the pin again and clears the failure state.
				status.Store(0)
				if n := expire(); n != 2 {
					t.Fatalf("failures after two probes = %d, want 2", n)
				}
				if res, err = m.Resolve(ctx, "org/repo", "main", "f.bin"); err != nil || res.Entry == nil || res.Entry.Commit != commitA {
					t.Fatalf("recovered main = %+v, %v", res, err)
				}
				m.mu.Lock()
				fe, b := m.entries[branchKey], m.branches["org/repo\x00main"]
				m.mu.Unlock()
				if fe != nil || b == nil || b.failures != 0 {
					t.Fatalf("failure state after recovery: entry %+v, pin %+v; want none", fe, b)
				}
			})
		}
	}
}

func TestMirrorSourcelessFailureRetiredOnPin(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = ""
	data := []byte("sourceless then sourced")
	upstream.set("/org/repo/resolve/main/f.bin", data)
	upstream.set("/org/repo/resolve/main/bad.bin", data)
	srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/cdn/org/repo/resolve/main/bad.bin" {
			corrupt := bytes.Repeat([]byte("x"), len(data))
			w.Header().Set("Content-Length", fmt.Sprint(len(corrupt)))
			_, _ = w.Write(corrupt)
			return
		}
		upstream.ServeHTTP(w, r)
	}))
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	ctx := context.Background()
	pseudo := wantPseudo("org/repo", "main")

	if _, err := ingestWait(t, m, "org/repo", pseudo, "f.bin"); !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("direct pseudo commit in a fresh process: err = %v, want ErrUpstreamNotFound", err)
	}
	entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
	if err != nil || entry.Commit != pseudo {
		t.Fatalf("main after the sourceless miss = %+v, %v; want commit %s", entry, err, pseudo)
	}
	res, err := m.Resolve(ctx, "org/repo", pseudo, "f.bin")
	if err != nil || res.Entry == nil || res.Entry.SHA256 != entry.SHA256 {
		t.Fatalf("direct pseudo commit after the branch pin = %+v, %v; want the ingested entry", res, err)
	}

	if _, err := ingestWait(t, m, "org/repo", "main", "bad.bin"); err == nil || !strings.Contains(err.Error(), "sha256 mismatch") {
		t.Fatalf("corrupt download err = %v, want a sha256 mismatch", err)
	}
	before := requests.Load()
	if _, err := ingestWait(t, m, "org/repo", "main", "bad.bin"); err == nil {
		t.Fatal("bad.bin ingested again inside its backoff")
	}
	if _, err := m.Resolve(ctx, "org/repo", pseudo, "bad.bin"); err == nil {
		t.Fatal("direct pseudo commit bypassed the source failure backoff")
	}
	if got := requests.Load() - before; got != 0 {
		t.Fatalf("backoff made %d upstream requests, want 0", got)
	}
}

func TestMirrorObsoleteSourceFailureIgnored(t *testing.T) {
	data := []byte("moving target")
	commitB := strings.Repeat("bb", 20)
	synthetic, real := newPlainUpstream(), newPlainUpstream()
	synthetic.commit, real.commit = "", commitB
	synthetic.set("/org/repo/resolve/main/f.bin", data)
	real.set("/org/repo/resolve/main/f.bin", data)
	var moved atomic.Bool
	hold, started := make(chan struct{}), make(chan struct{})
	var startOnce, release sync.Once
	srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/cdn/org/repo/resolve/main/f.bin" {
			startOnce.Do(func() { close(started) })
			if !gateWait(hold) {
				return
			}
			corrupt := bytes.Repeat([]byte("x"), len(data))
			w.Header().Set("Content-Length", fmt.Sprint(len(corrupt)))
			_, _ = w.Write(corrupt)
			return
		}
		if moved.Load() {
			real.ServeHTTP(w, r)
			return
		}
		synthetic.ServeHTTP(w, r)
	}))
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
	t.Cleanup(func() { release.Do(func() { close(hold) }) })
	ctx := context.Background()

	// Ingest would join the held flight by its original request key.
	first, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || first.Stream == nil {
		t.Fatalf("first resolve = %+v, %v; want an in-flight stream", first, err)
	}
	awaitClosed(t, started, "synthetic download")
	moved.Store(true)
	entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
	if err != nil || entry.Commit != commitB {
		t.Fatalf("main after the upstream move = %+v, %v; want commit %s", entry, err, commitB)
	}
	release.Do(func() { close(hold) })
	awaitClosed(t, first.Stream.t.done, "obsolete ingest")
	m.mu.Lock()
	obsolete := m.entries[resolveKey{repo: "org/repo", rev: wantPseudo("org/repo", "main"), path: "f.bin"}]
	m.mu.Unlock()
	if obsolete == nil || obsolete.State != stateFailed {
		t.Fatalf("obsolete synthetic ingest ended as %+v, want a failure on the corrupt bytes", obsolete)
	}
	res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || res.Entry == nil || res.Entry.Commit != commitB {
		t.Fatalf("main after the obsolete failure = %+v, %v; want commit %s", res, err, commitB)
	}
	before := requests.Load()
	for range 3 {
		if _, err := ingestWait(t, m, "org/repo", wantPseudo("org/repo", "main"), "f.bin"); !errors.Is(err, errSpoolCorrupt) {
			t.Fatalf("obsolete commit retry = %v, want the retained ingest failure", err)
		}
	}
	if got := requests.Load() - before; got != 0 {
		t.Fatalf("obsolete commit retries made %d upstream requests, want 0", got)
	}
}

func TestMirrorRejectedFileSurvivesSiblingBackoff(t *testing.T) {
	for _, tc := range []struct{ name, header string }{{"real", strings.Repeat("aa", 20)}, {"synthetic", ""}} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := newPlainUpstream()
			upstream.commit = tc.header
			upstream.set("/org/repo/resolve/main/f.bin", []byte("cached f"))
			upstream.set("/org/repo/resolve/main/g.bin", []byte("cached g"))
			var fStatus, gStatus atomic.Int64 // non-zero replaces the upstream answer for that file
			srv, requests, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if s := fStatus.Load(); s != 0 && r.URL.Path == "/org/repo/resolve/main/f.bin" {
					w.WriteHeader(int(s))
					return
				}
				if s := gStatus.Load(); s != 0 && r.URL.Path == "/org/repo/resolve/main/g.bin" {
					w.WriteHeader(int(s))
					return
				}
				upstream.ServeHTTP(w, r)
			}))
			m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
			ctx := context.Background()
			fKey := resolveKey{repo: "org/repo", rev: "main", path: "f.bin"}
			expire := func() {
				m.mu.Lock()
				defer m.mu.Unlock()
				m.entries[fKey].nextRetry = time.Time{}
			}
			for _, file := range []string{"f.bin", "g.bin"} {
				if _, err := ingestWait(t, m, "org/repo", "main", file); err != nil {
					t.Fatal(err)
				}
			}

			fStatus.Store(http.StatusNotFound)
			gStatus.Store(http.StatusServiceUnavailable)
			if _, err := m.Resolve(ctx, "org/repo", "main", "f.bin"); !errors.Is(err, ErrUpstreamNotFound) {
				t.Fatalf("f.bin after the 404 probe: err = %v, want ErrUpstreamNotFound", err)
			}
			expire()
			if res, err := m.Resolve(ctx, "org/repo", "main", "g.bin"); err != nil || res.Entry == nil {
				t.Fatalf("g.bin during the 503 outage = %+v, %v; want the cached entry", res, err)
			}
			before := requests.Load()
			res, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
			if !errors.Is(err, ErrUpstreamNotFound) {
				t.Fatalf("f.bin during the sibling backoff = %+v, %v after %d requests; want ErrUpstreamNotFound", res, err, requests.Load()-before)
			}

			expire()
			fStatus.Store(http.StatusServiceUnavailable)
			if res, err := m.Resolve(ctx, "org/repo", "main", "f.bin"); err == nil {
				t.Fatalf("f.bin after its own 503 probe = %+v; want the rejection kept", res)
			}
			expire()
			fStatus.Store(0)
			if res, err := m.Resolve(ctx, "org/repo", "main", "f.bin"); err != nil || res.Entry == nil {
				t.Fatalf("f.bin after its successful probe = %+v, %v; want the cached entry", res, err)
			}
			m.mu.Lock()
			fe := m.entries[fKey]
			m.mu.Unlock()
			if fe != nil {
				t.Fatalf("rejection record kept after the successful probe: %+v", fe)
			}
		})
	}
}

func TestMirrorDirectPseudoRecoveryRetiresSourceFailure(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = ""
	data := []byte("checksum content")
	upstream.set("/org/repo/resolve/main/f.bin", data)
	var corrupt atomic.Bool
	corrupt.Store(true)
	srv, _, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if corrupt.Load() && r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cdn/") {
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			_, _ = w.Write(bytes.Repeat([]byte("x"), len(data)))
			return
		}
		upstream.ServeHTTP(w, r)
	}))
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	src := resolveKey{repo: "org/repo", rev: "main", path: "f.bin"}
	key := resolveKey{repo: "org/repo", rev: wantPseudo("org/repo", "main"), path: "f.bin"}
	if _, err := ingestWait(t, m, "org/repo", "main", "f.bin"); err == nil {
		t.Fatal("corrupt first ingest succeeded")
	}
	m.mu.Lock()
	m.entries[src].nextRetry = time.Time{}
	m.mu.Unlock()

	corrupt.Store(false)
	if _, err := ingestWait(t, m, "org/repo", key.rev, "f.bin"); err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	retained, ready := m.entries[src], m.entries[key]
	m.mu.Unlock()
	if retained != nil {
		t.Errorf("source failure retained after the recovery: %+v", retained)
	}
	m.dropEntry(key, ready)
	corrupt.Store(true)
	if _, err := ingestWait(t, m, "org/repo", key.rev, "f.bin"); err == nil {
		t.Fatal("corrupt ingest after the recovery succeeded")
	}
	m.mu.Lock()
	fe := m.entries[src]
	m.mu.Unlock()
	if fe == nil || fe.failures != 1 {
		t.Errorf("source failure after the recovery = %+v, want one failure", fe)
	}
}

func TestMirrorOverlappingProbeKeepsIngestFailure(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.commit = ""
	data := []byte("checksum content")
	upstream.set("/org/repo/resolve/main/f.bin", data)
	getStarted, headStarted := make(chan struct{}), make(chan struct{})
	releaseGet, releaseHead := make(chan struct{}), make(chan struct{})
	var heads atomic.Int64
	var getOnce, freeGet, freeHead sync.Once
	srv, _, _ := countingServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/org/repo/resolve/main/f.bin" && heads.Add(1) == 2:
			close(headStarted)
			if !gateWait(releaseHead) {
				return
			}
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cdn/"):
			getOnce.Do(func() { close(getStarted) })
			if !gateWait(releaseGet) {
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			_, _ = w.Write(bytes.Repeat([]byte("x"), len(data)))
			return
		}
		upstream.ServeHTTP(w, r)
	}))
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithRevalidateInterval(0))
	t.Cleanup(func() {
		freeGet.Do(func() { close(releaseGet) })
		freeHead.Do(func() { close(releaseHead) })
	})
	ctx := context.Background()

	first, err := m.Resolve(ctx, "org/repo", "main", "f.bin")
	if err != nil || first.Stream == nil {
		t.Fatalf("first resolve = %+v, %v; want an in-flight stream", first, err)
	}
	awaitClosed(t, getStarted, "first download")
	done := make(chan struct{})
	var second *Resolution
	var secondErr error
	go func() {
		defer close(done)
		second, secondErr = m.Resolve(ctx, "org/repo", "main", "f.bin")
	}()
	awaitClosed(t, headStarted, "second probe")
	freeGet.Do(func() { close(releaseGet) })
	awaitClosed(t, first.Stream.t.done, "first ingest")
	m.mu.Lock()
	failure := m.entries[resolveKey{repo: "org/repo", rev: "main", path: "f.bin"}]
	m.mu.Unlock()
	if failure == nil || failure.State != stateFailed {
		t.Fatalf("first ingest ended as %+v, want the checksum failure", failure)
	}
	freeHead.Do(func() { close(releaseHead) })
	awaitClosed(t, done, "second resolve")
	if secondErr == nil {
		t.Fatalf("second resolve after the overlapping probe = %+v, want the ingest failure", second)
	}
}

// loadBranch remembers absent and rejected pointers, not filesystem faults.
func TestMirrorLoadBranchRemembersMisses(t *testing.T) {
	m, _ := newTestMirror(t, "http://example.invalid", t.TempDir(), t.TempDir())
	commit := strings.Repeat("ab", 20)
	valid := []byte(`{"commit":"` + commit + `","checked_at":"2026-01-01T00:00:00Z"}`)
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tc := range []struct {
		rev, name string
		raw       []byte
	}{
		{"main", "absent", nil},
		{"dev", "corrupt", []byte("{")},
		{"tag", "invalid commit", []byte(`{"commit":"nope"}`)},
	} {
		p := branchEntryPath(m.indexDir, "org/repo", tc.rev)
		if tc.raw != nil {
			writeRaw(t, p, tc.raw)
		}
		if b := m.loadBranch("org/repo", tc.rev); b != nil {
			t.Fatalf("%s pointer loaded as %+v", tc.name, b)
		}
		writeRaw(t, p, valid)
		if b := m.loadBranch("org/repo", tc.rev); b != nil {
			t.Fatalf("%s pointer reread after the miss: %+v", tc.name, b)
		}
	}

	p := branchEntryPath(m.indexDir, "org/repo", "faulty")
	if err := os.MkdirAll(p, 0755); err != nil {
		t.Fatal(err)
	}
	if b := m.loadBranch("org/repo", "faulty"); b != nil {
		t.Fatalf("directory in place of the pointer loaded as %+v", b)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	writeRaw(t, p, valid)
	if b := m.loadBranch("org/repo", "faulty"); b == nil || b.Commit != commit {
		t.Fatalf("pointer not loaded after the fault cleared: %+v", b)
	}
}

func TestMirrorRepoContainingResolve(t *testing.T) {
	const repo = "org/resolve/x"
	upstream := newPlainUpstream()
	upstream.commit = ""
	data := []byte("resolve inside the repo name")
	upstream.set("/"+repo+"/resolve/main/f.bin", data)
	upstream.set("/"+repo+"/resolve/main/g.bin", []byte("second"))
	srv, _, paths := countingServer(t, upstream)
	storageDir, cacheDir := t.TempDir(), t.TempDir()
	pseudo := wantPseudo(repo, "main")

	m, stor := newTestMirror(t, srv.URL, storageDir, cacheDir)
	entry, err := ingestWait(t, m, repo, "main", "f.bin")
	if err != nil || entry.Commit != pseudo {
		t.Fatalf("main = %+v, %v; want pseudo commit %s", entry, err, pseudo)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes differ from upstream data")
	}
	if src := rawSource(t, commitPath(filepath.Join(cacheDir, "index"), repo, pseudo)); src != "main" {
		t.Fatalf("manifest source = %q, want main", src)
	}

	m2, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	res, err := m2.Resolve(context.Background(), repo, pseudo, "f.bin")
	if err != nil || res.Entry == nil || res.Entry.SHA256 != entry.SHA256 {
		t.Fatalf("direct pseudo f.bin after restart = %+v, %v; want the cached entry", res, err)
	}
	if entry, err = ingestWait(t, m2, repo, pseudo, "g.bin"); err != nil || entry.Commit != pseudo {
		t.Fatalf("direct pseudo g.bin after restart = %+v, %v", entry, err)
	}
	if _, ok := paths.Load("/" + repo + "/resolve/main/g.bin"); !ok {
		t.Fatal("g.bin was not fetched through the source branch")
	}
}

func TestStaticUpstream(t *testing.T) {
	selector, err := StaticUpstream("https://hub.example/", "tok")
	if err != nil {
		t.Fatal(err)
	}
	u, token, err := selector(context.Background(), "org/repo")
	if err != nil || u.String() != "https://hub.example/" || token != "tok" {
		t.Fatalf("selector = %v, %q, %v", u, token, err)
	}
	for _, raw := range []string{"hub.example", "http://", "://x"} {
		if _, err := StaticUpstream(raw, ""); err == nil || err.Error() != fmt.Sprintf("mirror: invalid upstream URL %q", raw) {
			t.Fatalf("StaticUpstream(%q) err = %v", raw, err)
		}
	}

	stor, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewMirror(WithStorage(stor), WithCacheDir(t.TempDir())); err == nil || err.Error() != "mirror: upstream is required" {
		t.Fatalf("NewMirror without upstream err = %v", err)
	}

	t.Run("nil URL from selector", func(t *testing.T) {
		none := func(context.Context, string) (*url.URL, string, error) { return nil, "", nil }
		m, _ := newTestMirror(t, "http://unused.invalid", t.TempDir(), t.TempDir(), WithUpstream(none))
		if _, err := ingestWait(t, m, "org/repo", "main", "f.bin"); err == nil || errors.Is(err, ErrUpstreamNotFound) {
			t.Fatalf("ingest with nil upstream URL err = %v, want a plain error", err)
		}
		if _, err := m.FetchUpstream(context.Background(), "org/repo", "/api/models/org/repo"); err == nil {
			t.Fatal("FetchUpstream with nil upstream URL succeeded")
		}
	})
}

func TestMirrorPerRepoUpstream(t *testing.T) {
	const repoA, repoB = "org/a", "datasets/org/b%20c"
	upA, upB := newPlainUpstream(), newPlainUpstream()
	dataA, dataB := []byte("bytes from hub A"), []byte("bytes from hub B")
	upA.set("/org/a/resolve/main/f.bin", dataA)
	upB.set("/datasets/org/b c/resolve/main/f.bin", dataB) // the hub sees the decoded path
	srvA, srvB := httptest.NewServer(upA), httptest.NewServer(upB)
	t.Cleanup(srvA.Close)
	t.Cleanup(srvB.Close)
	urlA, _ := url.Parse(srvA.URL)
	urlB, _ := url.Parse(srvB.URL)

	errBoom := errors.New("selector boom")
	var seen sync.Map
	selector := func(_ context.Context, repo string) (*url.URL, string, error) {
		seen.Store(repo, true)
		switch repo {
		case repoA:
			return urlA, "tok-a", nil
		case repoB:
			return urlB, "tok-b", nil
		case "org/boom":
			return nil, "", errBoom
		}
		return nil, "", fmt.Errorf("no upstream for %q: %w", repo, ErrUpstreamNotFound)
	}
	m, stor := newTestMirror(t, "http://unused.invalid", t.TempDir(), t.TempDir(), WithUpstream(selector))

	inA, err := m.Ingest(repoA, "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	inB, err := m.Ingest(repoB, "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	awaitClosed(t, inA.Done(), "repo A ingest")
	awaitClosed(t, inB.Done(), "repo B ingest")
	for _, tc := range []struct {
		name         string
		in           *Ingestion
		up           *plainUpstream
		data         []byte
		token, other string
	}{
		{"A", inA, upA, dataA, "Bearer tok-a", "Bearer tok-b"},
		{"B", inB, upB, dataB, "Bearer tok-b", "Bearer tok-a"},
	} {
		entry, err := tc.in.Entry()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, tc.data) {
			t.Fatalf("%s: stored %q, want %q", tc.name, got, tc.data)
		}
		if _, ok := tc.up.seenAuth.Load(tc.token); !ok {
			t.Fatalf("%s: hub did not receive its own token", tc.name)
		}
		if _, ok := tc.up.seenAuth.Load(tc.other); ok {
			t.Fatalf("%s: hub received the other repo's token", tc.name)
		}
	}
	for _, repo := range []string{repoA, repoB} {
		if _, ok := seen.Load(repo); !ok {
			t.Fatalf("selector was never asked for %q", repo)
		}
	}
	if _, ok := seen.Load("datasets/org/b c"); ok {
		t.Fatal("selector was asked with the decoded repo identity")
	}

	if _, err := ingestWait(t, m, "org/unmapped", "main", "f.bin"); !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("unmapped repo ingest err = %v, want ErrUpstreamNotFound", err)
	}
	if _, err := m.Resolve(context.Background(), "org/unmapped", "main", "f.bin"); !errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("unmapped repo Resolve err = %v, want ErrUpstreamNotFound", err)
	}
	if _, err := ingestWait(t, m, "org/boom", "main", "f.bin"); !errors.Is(err, errBoom) || errors.Is(err, ErrUpstreamNotFound) {
		t.Fatalf("selector failure ingest err = %v, want errBoom only", err)
	}
}

func TestMirrorFetchUpstream(t *testing.T) {
	upstream := newPlainUpstream()
	upstream.api["/api/models/org/a"] = []byte(`{"id":"org/a"}`)
	var queries sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Store(r.URL.RawQuery, true)
		upstream.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	hub, _ := url.Parse(srv.URL)

	type ctxKey struct{}
	errSelectorCanceled := errors.New("selector saw the cancellation")
	var seen sync.Map // repo -> caller ctx value observed by the selector
	selector := func(ctx context.Context, repo string) (*url.URL, string, error) {
		if err := ctx.Err(); err != nil {
			return nil, "", errors.Join(errSelectorCanceled, err)
		}
		seen.Store(repo, ctx.Value(ctxKey{}))
		return hub, "tok-a", nil
	}
	m, _ := newTestMirror(t, "http://unused.invalid", t.TempDir(), t.TempDir(), WithUpstream(selector))

	ctx := context.WithValue(context.Background(), ctxKey{}, "caller value")
	resp, err := m.FetchUpstream(ctx, "org/a", "/api/models/org/a?expand=1")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil || resp.StatusCode != http.StatusOK || string(body) != `{"id":"org/a"}` {
		t.Fatalf("FetchUpstream = %d %q, %v", resp.StatusCode, body, err)
	}
	if v, _ := seen.Load("org/a"); v != "caller value" {
		t.Fatalf("selector saw ctx value %v, want the caller's", v)
	}
	if _, ok := queries.Load("expand=1"); !ok {
		t.Fatal("query not preserved on the upstream request")
	}
	if _, ok := upstream.seenAuth.Load("Bearer tok-a"); !ok {
		t.Fatal("hub did not receive the selected token")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.FetchUpstream(canceled, "org/a", "/api/models/org/a"); !errors.Is(err, errSelectorCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled FetchUpstream err = %v, want the selector's cancellation error", err)
	}
}

func TestMirrorUpstreamAuthHostGuard(t *testing.T) {
	cdn := newPlainUpstream() // serves the /cdn paths and records Authorization
	data := []byte("cross-host redirect payload")
	cdn.set("/org/repo/resolve/main/f.bin", data)
	cdnSrv := httptest.NewServer(cdn)
	t.Cleanup(cdnSrv.Close)

	var hubAuth sync.Map // ?probe= marker -> Authorization seen by the hub
	hubSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hubAuth.Store(r.URL.Query().Get("probe"), r.Header.Get("Authorization"))
		data, ok := cdn.get(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}
		sum := sha256.Sum256(data)
		w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:])+`"`)
		w.Header().Set("X-Linked-Size", fmt.Sprint(len(data)))
		w.Header().Set("X-Repo-Commit", "commit-1")
		http.Redirect(w, r, cdnSrv.URL+"/cdn"+r.URL.Path, http.StatusFound)
	}))
	t.Cleanup(hubSrv.Close)
	hub, _ := url.Parse(hubSrv.URL)
	fixed := func(context.Context, string) (*url.URL, string, error) { return hub, "hub-secret", nil }
	m, stor := newTestMirror(t, "http://unused.invalid", t.TempDir(), t.TempDir(), WithUpstream(fixed))

	entry, err := ingestWait(t, m, "org/repo", "main", "f.bin")
	if err != nil {
		t.Fatal(err)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data) {
		t.Fatal("stored bytes mismatch")
	}
	if v, _ := hubAuth.Load(""); v != "Bearer hub-secret" {
		t.Fatalf("hub saw Authorization %q, want the selected token", v)
	}
	if _, ok := cdn.seenAuth.Load("Bearer hub-secret"); ok {
		t.Fatal("hub token leaked to the redirect target on another host")
	}

	ctx, _, err := m.upstreamTarget(context.Background(), "org/repo", "/")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		marker, auth string
		ctx          context.Context
	}{
		{"keep", "Bearer keep", ctx},       // an Authorization already set wins
		{"none", "", context.Background()}, // no selected upstream: nothing injected
	} {
		req, err := http.NewRequestWithContext(tc.ctx, http.MethodHead, hubSrv.URL+"/org/repo/resolve/main/f.bin?probe="+tc.marker, nil)
		if err != nil {
			t.Fatal(err)
		}
		if tc.auth != "" {
			req.Header.Set("Authorization", tc.auth)
		}
		resp, err := m.probeClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if v, _ := hubAuth.Load(tc.marker); v != tc.auth {
			t.Fatalf("%s: hub saw Authorization %q, want %q", tc.marker, v, tc.auth)
		}
	}
}
