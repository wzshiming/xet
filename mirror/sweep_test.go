package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"maps"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
)

// SweepIndex drops the entries whose bytes left storage, removes the
// manifests that leaves empty and sourceless, and clears interrupted index
// writes past the grace; dry runs only report, and unparsable manifests are
// left alone.
func TestSweepIndex(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20) // a real upstream commit: the manifest carries no source, so emptying it removes it
	data1, data2 := []byte("first file"), []byte("second file")
	upstream.set("/org/repo/resolve/main/f1.bin", data1)
	upstream.set("/org/repo/resolve/main/f2.bin", data2)
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())

	var entries [2]*Entry
	for i, p := range []string{"f1.bin", "f2.bin"} {
		e, err := ingestWait(t, m, "org/repo", "main", p)
		if err != nil || e.Commit != upstream.commit {
			t.Fatalf("ingest %s = %+v, %v; want a ready entry at %s", p, e, err, upstream.commit)
		}
		entries[i] = e
	}
	manifest := commitPath(m.indexDir, "org/repo", upstream.commit)
	if man := readManifest(t, manifest); len(man.Files) != 2 || man.Source != "" {
		t.Fatalf("manifest after the ingests = %+v, want 2 files and no source", man)
	}
	files := func() []string { return slices.Sorted(maps.Keys(readManifest(t, manifest).Files)) }
	key1 := resolveKey{repo: "org/repo", rev: upstream.commit, path: "f1.bin"}
	has := func(k resolveKey) bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.entries[k] != nil
	}

	killStored(t, stor, entries[0])
	got, err := m.Sweep(ctx, storage.SweepOptions{DryRun: true})
	if err != nil || got != (SweepResult{DryRun: true, DroppedEntries: 1}) {
		t.Fatalf("dry-run SweepIndex = %+v, %v; want a dry run of 1 dropped entry", got, err)
	}
	if got := files(); !slices.Equal(got, []string{"f1.bin", "f2.bin"}) || !has(key1) {
		t.Fatalf("dry run changed the index: manifest files = %v, f1 in memory = %v", got, has(key1))
	}

	got, err = m.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{DroppedEntries: 1}) {
		t.Fatalf("SweepIndex = %+v, %v; want 1 dropped entry", got, err)
	}
	if got := files(); !slices.Equal(got, []string{"f2.bin"}) || has(key1) {
		t.Fatalf("index after the sweep: manifest files = %v, f1 in memory = %v; want f2 alone", got, has(key1))
	}

	oldTmp := filepath.Join(filepath.Dir(manifest), ".old.json.tmp")
	writeRaw(t, oldTmp, []byte("{"))
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(oldTmp, old, old); err != nil {
		t.Fatal(err)
	}
	freshTmp := filepath.Join(repoDir(m.indexDir, "org/repo"), "branches", ".fresh.json.tmp")
	writeRaw(t, freshTmp, []byte("{"))
	garbage := filepath.Join(filepath.Dir(manifest), "garbage.json")
	writeRaw(t, garbage, []byte("{"))
	got, err = m.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{RemovedTempFiles: 1}) {
		t.Fatalf("default-grace SweepIndex = %+v, %v; want 1 temp file", got, err)
	}
	if _, err := os.Stat(oldTmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("old temp file after the sweep: %v, want removed", err)
	}
	if _, err := os.Stat(freshTmp); err != nil {
		t.Fatalf("fresh temp file removed inside the grace: %v", err)
	}
	got, err = m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{RemovedTempFiles: 1}) {
		t.Fatalf("no-grace SweepIndex = %+v, %v; want 1 temp file", got, err)
	}
	if _, err := os.Stat(freshTmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("fresh temp file after the no-grace sweep: %v, want removed", err)
	}
	if _, err := os.Stat(garbage); err != nil {
		t.Fatalf("unparsable manifest swept: %v", err)
	}

	killStored(t, stor, entries[1])
	got, err = m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{DroppedEntries: 1, RemovedManifests: 1}) {
		t.Fatalf("SweepIndex with every file dead = %+v, %v; want 1 dropped entry and 1 removed manifest", got, err)
	}
	if _, err := os.Stat(manifest); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("manifest after the sweep: %v, want removed", err)
	}
	m.mu.Lock()
	_, cached := m.commits[revKey{repo: "org/repo", rev: upstream.commit}]
	m.mu.Unlock()
	if cached {
		t.Fatal("removed manifest still has a commit state in memory")
	}

	// The branch pointer survives, so the dropped file is simply ingested again.
	entry, err := ingestWait(t, m, "org/repo", "main", "f1.bin")
	if err != nil || entry.Commit != upstream.commit {
		t.Fatalf("re-ingest of the dropped file = %+v, %v; want a ready entry at %s", entry, err, upstream.commit)
	}
	if got := readStored(t, stor, entry.SHA256); !bytes.Equal(got, data1) {
		t.Fatal("re-ingested bytes differ from upstream data")
	}
	if got := files(); !slices.Equal(got, []string{"f1.bin"}) {
		t.Fatalf("manifest after the re-ingest = %v, want f1 alone", got)
	}

	t.Run("missing dir", func(t *testing.T) {
		absent := &Mirror{indexDir: filepath.Join(t.TempDir(), "absent")}
		if got, err := absent.Sweep(ctx, storage.SweepOptions{Grace: -1}); err != nil || got != (SweepResult{}) {
			t.Fatalf("SweepIndex of a missing dir = %+v, %v; want zero", got, err)
		}
	})
}

// killStored takes a file out of storage the way the GC endpoints do: both unlinks, then a sweep.
func killStored(t *testing.T, stor storage.Storage, e *Entry) {
	t.Helper()
	ctx := context.Background()
	fileHash, err := xet.ParseFileHash(e.FileHash)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hex.DecodeString(e.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	gc := storage.NewGC(stor)
	if _, err := gc.Unlink(ctx, fileHash); err != nil {
		t.Fatal(err)
	}
	if _, err := gc.UnlinkSHA256(ctx, [sha256.Size]byte(digest)); err != nil {
		t.Fatal(err)
	}
	if _, err := storage.Sweep(ctx, stor, storage.SweepOptions{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	if _, err := stor.GetShard(ctx, fileHash); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("GetShard after the unlinks and sweep err = %v, want ErrNotExist", err)
	}
}

// A temp file is only a leftover while no index write is in flight: the sweep waits for persistMu.
func TestSweepIndexWaitsForWriter(t *testing.T) {
	ctx := context.Background()
	m, _ := newTestMirror(t, "http://example.invalid", t.TempDir(), t.TempDir())
	tmp := filepath.Join(repoDir(m.indexDir, "org/repo"), "commits", ".x.json.tmp")
	writeRaw(t, tmp, []byte("{"))

	m.persistMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(m.persistMu.Unlock) }
	defer unlock()
	type outcome struct {
		res SweepResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
		done <- outcome{res, err}
	}()
	select {
	case o := <-done:
		t.Fatalf("SweepIndex finished (%+v, %v) while persistMu was held", o.res, o.err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := os.Stat(tmp); err != nil {
		t.Fatalf("temp file removed while persistMu was held: %v", err)
	}

	unlock()
	select {
	case o := <-done:
		if o.err != nil || o.res != (SweepResult{RemovedTempFiles: 1}) {
			t.Fatalf("SweepIndex after the writer released = %+v, %v; want 1 temp file", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SweepIndex did not finish after persistMu was released")
	}
	if _, err := os.Stat(tmp); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("temp file after the sweep: %v, want removed", err)
	}
}

// A manifest rewrite that fails is reported as an error with nothing counted; the entry memory already
// dropped stays on disk and the next sweep removes it from there.
func TestSweepIndexReportsPersistFailure(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/main/f1.bin", []byte("first file"))
	upstream.set("/org/repo/resolve/main/f2.bin", []byte("second file"))
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	m, stor := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir())
	var entries [2]*Entry
	for i, p := range []string{"f1.bin", "f2.bin"} {
		e, err := ingestWait(t, m, "org/repo", "main", p)
		if err != nil {
			t.Fatal(err)
		}
		entries[i] = e
	}
	manifest := commitPath(m.indexDir, "org/repo", upstream.commit)
	files := func() []string { return slices.Sorted(maps.Keys(readManifest(t, manifest).Files)) }
	key1 := resolveKey{repo: "org/repo", rev: upstream.commit, path: "f1.bin"}
	killStored(t, stor, entries[0])

	commits := filepath.Dir(manifest)
	if err := os.Chmod(commits, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(commits, 0o755) })
	got, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err == nil || got != (SweepResult{}) {
		t.Fatalf("SweepIndex with an unwritable commits dir = %+v, %v; want zero counts and an error", got, err)
	}
	m.mu.Lock()
	_, inMemory := m.entries[key1]
	m.mu.Unlock()
	if inMemory || !slices.Equal(files(), []string{"f1.bin", "f2.bin"}) {
		t.Fatalf("after the failed sweep: f1 in memory = %v, manifest files = %v; want dropped from memory and untouched on disk", inMemory, files())
	}

	if err := os.Chmod(commits, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err = m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{DroppedEntries: 1}) {
		t.Fatalf("SweepIndex once the dir is writable = %+v, %v; want the stale disk entry dropped", got, err)
	}
	if got := files(); !slices.Equal(got, []string{"f2.bin"}) {
		t.Fatalf("manifest after the retry = %v, want f2 alone", got)
	}
}

// The counts describe the manifest the sweep wrote: a file published while the sweep waited for
// persistMu is in that manifest, so the sweep drops the dead entry but removes nothing.
func TestSweepIndexCountsWhatItWrites(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/main/f1.bin", []byte("first file"))
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	base, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	gated := &gatedStorage{Storage: base, arrived: make(chan struct{}), release: make(chan struct{})}
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithStorage(gated))
	entry, err := ingestWait(t, m, "org/repo", "main", "f1.bin")
	if err != nil {
		t.Fatal(err)
	}
	manifest := commitPath(m.indexDir, "org/repo", upstream.commit)
	killStored(t, base, entry)

	// The sweep is pinned after its first snapshot, inside the liveness check, and then at persistMu.
	gated.armed.Store(true)
	m.persistMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(m.persistMu.Unlock) }
	defer unlock()
	type outcome struct {
		res SweepResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
		done <- outcome{res, err}
	}()
	select {
	case <-gated.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep never checked the dead entry's liveness")
	}
	close(gated.release)
	select {
	case o := <-done:
		t.Fatalf("SweepIndex finished (%+v, %v) while persistMu was held", o.res, o.err)
	case <-time.After(200 * time.Millisecond):
	}
	late := resolveKey{repo: "org/repo", rev: upstream.commit, path: "g.bin"}
	e := &fileEntry{State: stateReady, Size: 1, ETag: "g", CheckedAt: time.Unix(1, 0).UTC()}
	m.mu.Lock()
	m.entries[late] = e
	m.openCommit(late.repo, late.rev).publish(late.path, e)
	m.mu.Unlock()

	unlock()
	select {
	case o := <-done:
		if o.err != nil || o.res != (SweepResult{DroppedEntries: 1}) {
			t.Fatalf("SweepIndex with a file published meanwhile = %+v, %v; want 1 dropped entry and no removed manifest", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SweepIndex did not finish after persistMu was released")
	}
	if got := slices.Sorted(maps.Keys(readManifest(t, manifest).Files)); !slices.Equal(got, []string{"g.bin"}) {
		t.Fatalf("manifest after the sweep = %v, want g alone", got)
	}
	m.mu.Lock()
	_, cached := m.commits[revKey{repo: "org/repo", rev: upstream.commit}]
	m.mu.Unlock()
	if !cached {
		t.Fatal("commit state dropped from memory although its manifest still has a file")
	}
}

// gatedStorage blocks one GetShard, signaling arrived, until release is closed.
type gatedStorage struct {
	storage.Storage
	armed   atomic.Bool
	arrived chan struct{}
	release chan struct{}
}

func (g *gatedStorage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	if g.armed.CompareAndSwap(true, false) {
		close(g.arrived)
		<-g.release
	}
	return g.Storage.GetShard(ctx, fileHash)
}

// Two sweeps over one dead manifest: the second finds it gone and counts nothing.
func TestSweepIndexConcurrentPassesCountOnce(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/main/f1.bin", []byte("first file"))
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	base, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	gated := &gatedStorage{Storage: base, arrived: make(chan struct{}), release: make(chan struct{})}
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), t.TempDir(), WithStorage(gated))
	entry, err := ingestWait(t, m, "org/repo", "main", "f1.bin")
	if err != nil {
		t.Fatal(err)
	}
	manifest := commitPath(m.indexDir, "org/repo", upstream.commit)
	killStored(t, base, entry)

	// The first pass is held inside its liveness check while the second pass removes the manifest.
	gated.armed.Store(true)
	type outcome struct {
		res SweepResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
		done <- outcome{res, err}
	}()
	select {
	case <-gated.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("first sweep never checked the dead entry's liveness")
	}
	got, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{DroppedEntries: 1, RemovedManifests: 1}) {
		t.Fatalf("second sweep = %+v, %v; want 1 dropped entry and 1 removed manifest", got, err)
	}
	if _, err := os.Stat(manifest); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("manifest after the second sweep: %v, want removed", err)
	}
	close(gated.release)
	select {
	case o := <-done:
		if o.err != nil || o.res != (SweepResult{}) {
			t.Fatalf("first sweep after the manifest was removed under it = %+v, %v; want nothing counted", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first sweep did not finish")
	}
}

// A restarted mirror holds no index state: the sweep judges its manifests on
// disk against storage and loads none of them, so the index keeps loading
// lazily, one commit per request.
func TestSweepIndexLeavesNonResidentCommits(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	commits := []string{strings.Repeat("aa", 20), strings.Repeat("bb", 20), strings.Repeat("cc", 20)}
	// Files published at their commits directly: three sourceless manifests.
	commitOf := map[string]string{"a1.bin": commits[0], "a2.bin": commits[0], "b1.bin": commits[1], "c1.bin": commits[2]}
	for p, c := range commitOf {
		upstream.set("/org/repo/resolve/"+c+"/"+p, []byte("content of "+p))
	}
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	storageDir, cacheDir := t.TempDir(), t.TempDir()
	first, stor := newTestMirror(t, srv.URL, storageDir, cacheDir)
	entries := map[string]*Entry{}
	for _, p := range []string{"a1.bin", "a2.bin", "b1.bin", "c1.bin"} {
		e, err := ingestWait(t, first, "org/repo", commitOf[p], p)
		if err != nil || e.Commit != commitOf[p] {
			t.Fatalf("ingest %s = %+v, %v; want a ready entry at %s", p, e, err, commitOf[p])
		}
		entries[p] = e
	}
	killStored(t, stor, entries["a1.bin"])
	killStored(t, stor, entries["b1.bin"])

	m, _ := newTestMirror(t, srv.URL, storageDir, cacheDir)
	resident := func() (int, int) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return len(m.entries), len(m.commits)
	}
	files := func(commit string) []string {
		return slices.Sorted(maps.Keys(readManifest(t, commitPath(m.indexDir, "org/repo", commit)).Files))
	}
	if e, c := resident(); e != 0 || c != 0 {
		t.Fatalf("restarted mirror holds %d entries and %d commits before any request", e, c)
	}
	got, err := m.Sweep(ctx, storage.SweepOptions{DryRun: true})
	if err != nil || got != (SweepResult{DryRun: true, DroppedEntries: 2, RemovedManifests: 1}) {
		t.Fatalf("dry-run SweepIndex = %+v, %v; want a dry run of 2 dropped entries and 1 removed manifest", got, err)
	}
	if e, c := resident(); e != 0 || c != 0 {
		t.Fatalf("dry run loaded %d entries and %d commits into memory", e, c)
	}
	if got := files(commits[0]); !slices.Equal(got, []string{"a1.bin", "a2.bin"}) {
		t.Fatalf("dry run changed the manifest: files = %v", got)
	}

	got, err = m.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{DroppedEntries: 2, RemovedManifests: 1}) {
		t.Fatalf("SweepIndex = %+v, %v; want 2 dropped entries and 1 removed manifest", got, err)
	}
	if e, c := resident(); e != 0 || c != 0 {
		t.Fatalf("sweep loaded %d entries and %d commits into memory", e, c)
	}
	if got := files(commits[0]); !slices.Equal(got, []string{"a2.bin"}) {
		t.Fatalf("manifest of the half-dead commit after the sweep = %v, want a2 alone", got)
	}
	if kept := readManifest(t, commitPath(m.indexDir, "org/repo", commits[0])).Files["a2.bin"]; kept.FileHash != entries["a2.bin"].FileHash || kept.SHA256 != entries["a2.bin"].SHA256 {
		t.Fatalf("kept entry after the sweep = %+v, want the ingested a2", kept)
	}
	if _, err := os.Stat(commitPath(m.indexDir, "org/repo", commits[1])); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("manifest of the all-dead commit after the sweep: %v, want removed", err)
	}
	if got := files(commits[2]); !slices.Equal(got, []string{"c1.bin"}) {
		t.Fatalf("manifest of the live commit after the sweep = %v, want c1 alone", got)
	}
	got, err = m.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{}) {
		t.Fatalf("second SweepIndex = %+v, %v; want nothing counted", got, err)
	}

	gets := upstream.dataGETs.Load()
	e, err := ingestWait(t, m, "org/repo", commits[0], "a2.bin")
	if err != nil || e.FileHash != entries["a2.bin"].FileHash {
		t.Fatalf("ingest of a live file after the sweep = %+v, %v; want the indexed entry", e, err)
	}
	if got := upstream.dataGETs.Load(); got != gets {
		t.Fatalf("a live indexed file was downloaded again after the sweep: data GETs %d -> %d", gets, got)
	}
	if e, c := resident(); e != 1 || c != 1 {
		t.Fatalf("after the lazy load: %d entries and %d commits in memory, want the one commit requested", e, c)
	}
}

// A commit a writer opens while the sweep judges its manifest from disk is
// resident from then on: the sweep yields and the next pass judges the dead
// entry through memory.
func TestSweepIndexNonResidentYieldsToWriter(t *testing.T) {
	ctx := context.Background()
	upstream := newPlainUpstream()
	upstream.commit = strings.Repeat("ab", 20)
	upstream.set("/org/repo/resolve/main/f1.bin", []byte("first file"))
	srv := httptest.NewServer(upstream)
	defer srv.Close()
	base, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()
	first, _ := newTestMirror(t, srv.URL, t.TempDir(), cacheDir, WithStorage(base))
	entry, err := ingestWait(t, first, "org/repo", "main", "f1.bin")
	if err != nil {
		t.Fatal(err)
	}
	killStored(t, base, entry)

	gated := &gatedStorage{Storage: base, arrived: make(chan struct{}), release: make(chan struct{})}
	m, _ := newTestMirror(t, srv.URL, t.TempDir(), cacheDir, WithStorage(gated))
	manifest := commitPath(m.indexDir, "org/repo", upstream.commit)
	files := func() []string { return slices.Sorted(maps.Keys(readManifest(t, manifest).Files)) }
	gated.armed.Store(true)
	type outcome struct {
		res SweepResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
		done <- outcome{res, err}
	}()
	select {
	case <-gated.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep never checked the dead entry's liveness")
	}
	late := resolveKey{repo: "org/repo", rev: upstream.commit, path: "g.bin"}
	e := &fileEntry{State: stateReady, Size: 1, ETag: "g", CheckedAt: time.Unix(1, 0).UTC()}
	m.mu.Lock()
	m.entries[late] = e
	m.openCommit(late.repo, late.rev).publish(late.path, e)
	m.mu.Unlock()
	if err := m.persistCommit(late.repo, late.rev); err != nil {
		t.Fatal(err)
	}
	close(gated.release)
	select {
	case o := <-done:
		if o.err != nil || o.res != (SweepResult{}) {
			t.Fatalf("SweepIndex over a commit opened meanwhile = %+v, %v; want nothing counted", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SweepIndex did not finish")
	}
	if got := files(); !slices.Equal(got, []string{"f1.bin", "g.bin"}) {
		t.Fatalf("manifest after the yielded sweep = %v, want f1 and g", got)
	}

	got, err := m.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{DroppedEntries: 1}) {
		t.Fatalf("second SweepIndex = %+v, %v; want 1 dropped entry", got, err)
	}
	if got := files(); !slices.Equal(got, []string{"g.bin"}) {
		t.Fatalf("manifest after the second sweep = %v, want g alone", got)
	}
}
