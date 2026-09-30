package upload

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// fakeClient knows no xorb upstream and drains every upload; onUpload runs
// inside UploadXorb.
type fakeClient struct {
	hasXorbErr error
	uploadErr  error
	onUpload   func()
}

func (c *fakeClient) HasXorb(context.Context, xet.XorbHash) (bool, error) {
	return false, c.hasXorbErr
}

func (c *fakeClient) UploadXorb(_ context.Context, _ xet.XorbHash, r io.ReadSeeker) (*XorbUploadResponse, error) {
	if c.onUpload != nil {
		c.onUpload()
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		return nil, err
	}
	return &XorbUploadResponse{WasInserted: true}, c.uploadErr
}

func (c *fakeClient) UploadShard(context.Context, *shard.Shard) (*ShardUploadResponse, error) {
	return nil, errors.New("unexpected UploadShard")
}

func (c *fakeClient) QueryDedupShards(context.Context, []xet.ChunkHash, ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	return nil, errors.New("unexpected QueryDedupShards")
}

// singleChunkGroups returns n xorb groups of one distinct 1-byte chunk each.
func singleChunkGroups(n int) []*xorbGroup {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i)
	}
	reader := newSyncReadSeeker(bytes.NewReader(data))
	groups := make([]*xorbGroup, n)
	for i := range groups {
		groups[i] = &xorbGroup{
			Chunks:      []Chunk{{Reader: reader, Offset: int64(i), Size: 1}},
			ChunkHashes: []xet.ChunkHash{xet.ComputeChunkHash(data[i : i+1])},
			StartIndex:  i,
		}
	}
	return groups
}

func openFDs() (int, error) {
	entries, err := os.ReadDir("/dev/fd")
	return len(entries), err
}

// Staged xorbs are closed until their upload, so open files grow with the
// concurrency, not with the upload.
func TestUploadXorbsClosesStagedFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("counts /dev/fd")
	}
	const concurrency = 2
	var once sync.Once
	inside, insideErr := -1, error(nil)
	client := &fakeClient{onUpload: func() {
		once.Do(func() { inside, insideErr = openFDs() })
	}}
	groups := singleChunkGroups(64)
	if _, err := openFDs(); err != nil {
		t.Fatal(err)
	}
	warm, err := os.Create(filepath.Join(t.TempDir(), "warm"))
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Close()
	baseline, err := openFDs()
	if err != nil {
		t.Fatal(err)
	}
	if err := uploadXorbs(t.Context(), client, map[xet.ChunkHash]shard.ChunkLocation{}, groups, concurrency, NewCacheManager(t.TempDir(), 0), nil); err != nil {
		t.Fatal(err)
	}
	if insideErr != nil || inside < 0 {
		t.Fatalf("open files inside the first upload = %d, %v", inside, insideErr)
	}
	if extra := inside - baseline; extra > concurrency+3 {
		t.Fatalf("%d extra open files during the first upload, want at most %d for concurrency %d", extra, concurrency+3, concurrency)
	}
}

// During the first upload every staged xorb sits flat under <dir>/staging;
// nothing is left once uploadXorbs returns, and every group's chunk is
// recorded at index 0 of its own xorb.
func TestUploadXorbsStaging(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(dir, "staging")
	var once sync.Once
	var files []os.DirEntry
	var seenErr error
	client := &fakeClient{onUpload: func() {
		once.Do(func() { files, seenErr = os.ReadDir(staging) })
	}}
	groups := singleChunkGroups(4)
	uploaded := map[xet.ChunkHash]shard.ChunkLocation{}
	if err := uploadXorbs(t.Context(), client, uploaded, groups, 1, NewCacheManager(dir, 0), nil); err != nil {
		t.Fatal(err)
	}
	if seenErr != nil {
		t.Fatalf("staging during the first upload: %v", seenErr)
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		names = append(names, f.Name())
	}
	if len(files) != 4 {
		t.Fatalf("staging during the first upload = %v; want 4 xorb-* files", names)
	}
	for _, f := range files {
		if !f.Type().IsRegular() || !strings.HasPrefix(f.Name(), "xorb-") {
			t.Fatalf("staging during the first upload = %v; want only regular xorb-* files", names)
		}
	}
	if left, err := os.ReadDir(staging); err != nil || len(left) != 0 {
		t.Fatalf("staging after the upload = %v, %v; want empty", left, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != "staging" || !entries[0].IsDir() {
		t.Fatalf("manager dir after the upload = %v, %v; want only the staging dir", entries, err)
	}
	if len(uploaded) != len(groups) {
		t.Fatalf("uploaded = %v; want one location per group", uploaded)
	}
	for _, group := range groups {
		want := shard.ChunkLocation{XorbHash: xet.ComputeXorbHash(group.ChunkHashes, []uint64{1})}
		if loc, ok := uploaded[group.ChunkHashes[0]]; !ok || loc != want {
			t.Fatalf("uploaded[%x] = %+v, %v; want %+v", group.ChunkHashes[0][:3], loc, ok, want)
		}
	}
}

func TestUploadXorbsErrorRemovesStaging(t *testing.T) {
	boom := errors.New("boom")
	for name, client := range map[string]*fakeClient{
		"has xorb":    {hasXorbErr: boom},
		"upload xorb": {uploadErr: boom},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			err := uploadXorbs(t.Context(), client, map[xet.ChunkHash]shard.ChunkLocation{}, singleChunkGroups(4), 2, NewCacheManager(dir, 0), nil)
			if !errors.Is(err, boom) {
				t.Fatalf("uploadXorbs error = %v, want %v", err, boom)
			}
			if left, err := os.ReadDir(filepath.Join(dir, "staging")); err != nil || len(left) != 0 {
				t.Fatalf("staging after the failed upload = %v, %v; want empty", left, err)
			}
		})
	}
}

func TestCacheManagerUsage(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		dir     string
		ctx     context.Context
		wantErr error
	}{
		{"empty", t.TempDir(), t.Context(), nil},
		{"missing", filepath.Join(t.TempDir(), "missing"), t.Context(), nil},
		{"canceled", t.TempDir(), canceled, context.Canceled},
		{"through a file", filepath.Join(file, "upload"), t.Context(), syscall.ENOTDIR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr == syscall.ENOTDIR && runtime.GOOS == "windows" {
				t.Skip("ENOTDIR is not reported on windows")
			}
			if got, err := NewCacheManager(tc.dir, 0).Usage(tc.ctx); !errors.Is(err, tc.wantErr) || got != (CacheUsage{}) {
				t.Fatalf("usage = %+v, %v; want zero, %v", got, err, tc.wantErr)
			}
		})
	}
}

// Another user's staging directory under a shared root, unreadable to this
// one, is skipped rather than failing the count.
func TestCacheManagerUsageSkipsUnreadableDir(t *testing.T) {
	if runtime.GOOS == "windows" || os.Getuid() == 0 {
		t.Skip("needs a directory the test user cannot read")
	}
	dir := t.TempDir()
	other := filepath.Join(dir, "staging", "other")
	if err := os.MkdirAll(other, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(other, "xorb-1"), []byte("theirs"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(other, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(other, 0o755) })
	if err := os.WriteFile(filepath.Join(dir, "staging", "stray"), []byte("ours"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, err := NewCacheManager(dir, 0).Usage(t.Context()); err != nil || got != (CacheUsage{Count: 1, Bytes: 4}) {
		t.Fatalf("usage = %+v, %v; want the one readable 4-byte file", got, err)
	}
}

func TestDefaultCacheDir(t *testing.T) {
	if got, want := defaultCacheDir(""), filepath.Join(os.TempDir(), "xet-cache", "upload"); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// stubClient accepts every upload and knows no existing chunk.
type stubClient struct{ *fakeClient }

func (stubClient) UploadShard(context.Context, *shard.Shard) (*ShardUploadResponse, error) {
	return &ShardUploadResponse{}, nil
}

func (stubClient) QueryDedupShards(context.Context, []xet.ChunkHash, ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	return map[xet.ChunkHash]shard.ChunkLocation{}, nil
}

// Byte 7 leads the hash string, so these land in distinct fanout directories.
var (
	hashA = xet.ChunkHash{7: 1}
	hashB = xet.ChunkHash{7: 2}
	hashC = xet.ChunkHash{7: 3}
	hashD = xet.ChunkHash{7: 4}
)

// trackedEntries reads the manager's in-memory entry count.
func trackedEntries(m *CacheManager) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lru.Len()
}

// storeEntry caches one location for h under testEndpoint through m.
func storeEntry(t *testing.T, m *CacheManager, h xet.ChunkHash) {
	t.Helper()
	if err := m.Store(testEndpoint, map[xet.ChunkHash]shard.ChunkLocation{h: {XorbHash: xet.XorbHash{h[7]}}}); err != nil {
		t.Fatal(err)
	}
}

// seedAged creates an empty file at path whose mtime is age in the past.
func seedAged(t *testing.T, path string, age time.Duration) {
	t.Helper()
	seed(t, path, nil)
	touch(t, path, time.Now().Add(-age))
}

// oldTemp is the path of a crashed Store's temp file next to h's entry.
func oldTemp(dir string, h xet.ChunkHash) string {
	return filepath.Join(filepath.Dir(entryFile(dir, h)), "old.tmp")
}

func pathExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	return err == nil
}

func TestCacheStoreUnderCapacityIsInMemoryOnly(t *testing.T) {
	dir := t.TempDir()
	// An entry from another manager and a crashed leftover, both unknown to
	// the manager under test.
	storeEntry(t, NewCacheManager(dir, 0), hashB)
	leftover := oldTemp(dir, hashC)
	seedAged(t, leftover, 2*leftoverAge)

	m := NewCacheManager(dir, 10*entryCost)
	storeEntry(t, m, hashA)
	if got := trackedEntries(m); got != 1 {
		t.Fatalf("under-capacity store tracks %d entries, want only its own", got)
	}
	if !pathExists(t, leftover) {
		t.Fatal("crashed leftover was removed on the store path")
	}

	// The first prepare is the slow path: it adopts the foreign entry and
	// cleans up the leftover.
	m.prepare()
	if got := trackedEntries(m); got != 2 {
		t.Fatalf("prepare tracks %d entries, want the foreign one adopted", got)
	}
	if pathExists(t, leftover) {
		t.Fatal("prepare kept the crashed leftover")
	}
}

func TestCacheEvictionThrottlesDirectoryWalk(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 2*entryCost)
	// Reaching capacity runs the first full walk.
	storeEntry(t, m, hashA)
	storeEntry(t, m, hashB)
	if got := trackedEntries(m); got != 2 {
		t.Fatalf("tracks %d entries at capacity, want 2", got)
	}

	// A foreign entry written after that walk is not re-discovered while the
	// throttle is active: the next overage evicts the oldest tracked entry only.
	storeEntry(t, NewCacheManager(dir, 0), hashC)
	storeEntry(t, m, hashD)
	if pathExists(t, entryFile(dir, hashA)) {
		t.Fatal("oldest tracked entry was not evicted")
	}
	for _, h := range []xet.ChunkHash{hashB, hashD} {
		if !pathExists(t, entryFile(dir, h)) {
			t.Fatalf("tracked entry %x within capacity was evicted", h[7])
		}
	}
	if !pathExists(t, entryFile(dir, hashC)) {
		t.Fatal("foreign entry should survive eviction within the reconcile interval")
	}
	if got := trackedEntries(m); got != 2 {
		t.Fatalf("throttled eviction tracks %d entries, want 2 without the foreign one", got)
	}
}

func TestCachePrepareScansOnce(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 2*entryCost)
	m.prepare()

	// Leftovers appearing after the initial scan are only cleaned up on the
	// slow path, not by later prepares.
	leftover := oldTemp(dir, hashA)
	seedAged(t, leftover, 2*leftoverAge)
	stale := filepath.Join(dir, "staging", "xorb-stale")
	seedAged(t, stale, 2*leftoverAge)
	m.prepare()
	for _, p := range []string{leftover, stale} {
		if !pathExists(t, p) {
			t.Fatalf("%s removed: prepare after the initial scan walked the directory", p)
		}
	}
}

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	dir := t.TempDir()
	// Room for exactly two entries.
	m := NewCacheManager(dir, 2*entryCost)
	storeEntry(t, m, hashA)
	storeEntry(t, m, hashB)

	// A hit refreshes A, so B becomes the least recently used.
	if got := m.Lookup(t.Context(), testEndpoint, []xet.ChunkHash{hashA}); len(got) != 1 {
		t.Fatalf("resolved %v, want A", got)
	}
	storeEntry(t, m, hashC)
	if pathExists(t, entryFile(dir, hashB)) {
		t.Fatal("least recently used entry B was not evicted")
	}
	for _, h := range []xet.ChunkHash{hashA, hashC} {
		if !pathExists(t, entryFile(dir, h)) {
			t.Fatalf("entry %x was evicted unexpectedly", h[7])
		}
	}
}

func TestCacheBoundsDirectoryAcrossManagers(t *testing.T) {
	dir := t.TempDir()
	// Two managers over the same directory, each with room for two entries.
	m1 := NewCacheManager(dir, 2*entryCost)
	m2 := NewCacheManager(dir, 2*entryCost)

	storeEntry(t, m1, hashA)
	storeEntry(t, m2, hashB)
	// m1 only ever wrote A, but must still count B when evicting.
	storeEntry(t, m1, hashC)

	if pathExists(t, entryFile(dir, hashA)) {
		t.Fatal("directory-level bound was not enforced across managers")
	}
	for _, h := range []xet.ChunkHash{hashB, hashC} {
		if !pathExists(t, entryFile(dir, h)) {
			t.Fatalf("entry %x was evicted unexpectedly", h[7])
		}
	}
}

func TestCacheUnboundedWhenCapacityNotPositive(t *testing.T) {
	dir := t.TempDir()
	leftover := oldTemp(dir, hashC)
	seedAged(t, leftover, 2*leftoverAge)
	stale := filepath.Join(dir, "staging", "xorb-stale")
	seedAged(t, stale, 2*leftoverAge)

	m := NewCacheManager(dir, 0)
	storeEntry(t, m, hashA)
	storeEntry(t, m, hashB)
	if got := m.Lookup(t.Context(), testEndpoint, []xet.ChunkHash{hashA, hashB}); len(got) != 2 {
		t.Fatalf("resolved %v, want both", got)
	}
	m.prepare()
	if got := trackedEntries(m); got != 0 {
		t.Fatalf("unbounded manager tracks %d entries, want none", got)
	}
	for _, h := range []xet.ChunkHash{hashA, hashB} {
		if !pathExists(t, entryFile(dir, h)) {
			t.Fatalf("entry %x was evicted despite unbounded cache", h[7])
		}
	}
	if !pathExists(t, leftover) {
		t.Fatal("unbounded manager walked chunks/ and removed the leftover")
	}
	if pathExists(t, stale) {
		t.Fatal("stale staging xorb was not swept")
	}
}

func TestCacheScanAdoptsExistingEntriesAndCleansLeftovers(t *testing.T) {
	dir := t.TempDir()
	// Write entries via a throwaway manager so a fresh manager has to discover
	// them from disk.
	writer := NewCacheManager(dir, 0)
	for _, h := range []xet.ChunkHash{hashA, hashB, hashC} {
		storeEntry(t, writer, h)
	}
	oldTmp, youngTmp := oldTemp(dir, hashA), filepath.Join(filepath.Dir(entryFile(dir, hashB)), "young.tmp")
	oldXorb, youngXorb := filepath.Join(dir, "staging", "xorb-old"), filepath.Join(dir, "staging", "xorb-young")
	seedAged(t, oldTmp, 2*leftoverAge)
	seedAged(t, youngTmp, leftoverAge/2)
	seedAged(t, oldXorb, 2*leftoverAge)
	seedAged(t, youngXorb, leftoverAge/2)

	m := NewCacheManager(dir, 10*entryCost)
	m.prepare()
	if got := trackedEntries(m); got != 3 {
		t.Fatalf("prepare tracks %d entries, want the 3 on disk", got)
	}
	for _, p := range []string{oldTmp, oldXorb} {
		if pathExists(t, p) {
			t.Fatalf("crashed leftover %s was not cleaned up", p)
		}
	}
	for _, p := range []string{youngTmp, youngXorb, entryFile(dir, hashA), entryFile(dir, hashB), entryFile(dir, hashC)} {
		if !pathExists(t, p) {
			t.Fatalf("%s was removed", p)
		}
	}
}

func TestCacheScanLeavesForeignEntriesAlone(t *testing.T) {
	dir := t.TempDir()
	chunks, tagDir, name := filepath.Join(dir, "chunks"), hex.EncodeToString(tag[:]), hashC.String()
	// Well-formed names outside the exact entry shape are not ours and must be
	// neither tracked nor cleaned up.
	foreign := []string{
		filepath.Join(chunks, "abcd", name[:2], name[2:4], name[4:]),
		filepath.Join(chunks, tagDir, "zz", name[2:4], name[4:]),
		filepath.Join(chunks, tagDir, name[:2], name[2:4], "cc", name[6:]),
		filepath.Join(chunks, "foreign.tmp"),
		filepath.Join(chunks, "abcd", name[:2], name[2:4], "foreign.tmp"),
		filepath.Join(chunks, tagDir, "zz", name[2:4], "foreign.tmp"),
		filepath.Join(dir, "staging", "lock"),
	}
	for _, p := range foreign {
		seedAged(t, p, 2*leftoverAge)
	}
	// A directory where an entry file should be, and one named like a staged xorb.
	dirs := []string{entryFile(dir, hashD), filepath.Join(dir, "staging", "xorb-dir")}
	for _, p := range dirs {
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		touch(t, p, time.Now().Add(-2*leftoverAge))
	}

	m := NewCacheManager(dir, entryCost)
	m.prepare()
	if got := trackedEntries(m); got != 0 {
		t.Fatalf("prepare tracks %d foreign files, want none", got)
	}
	// Eviction pressure must only ever touch tracked entries.
	storeEntry(t, m, hashA)
	storeEntry(t, m, hashB)
	if pathExists(t, entryFile(dir, hashA)) {
		t.Fatal("older entry was not evicted")
	}
	for _, p := range foreign {
		if !pathExists(t, p) {
			t.Fatalf("foreign file %s was removed", p)
		}
	}
	for _, p := range dirs {
		if info, err := os.Stat(p); err != nil || !info.IsDir() {
			t.Fatalf("foreign directory %s: %v, want kept", p, err)
		}
	}
	if got := trackedEntries(m); got != 1 {
		t.Fatalf("tracks %d entries after eviction, want the one real entry", got)
	}
}

func TestCacheEvictionRemovesEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	// Room for one entry: storing B evicts A.
	m := NewCacheManager(dir, entryCost)
	storeEntry(t, m, hashA)
	storeEntry(t, m, hashB)
	pathA := entryFile(dir, hashA)
	if pathExists(t, pathA) {
		t.Fatal("older entry was not evicted")
	}
	for _, p := range []string{filepath.Dir(pathA), filepath.Dir(filepath.Dir(pathA))} {
		if pathExists(t, p) {
			t.Fatalf("emptied fanout dir %s was not removed", p)
		}
	}
	for _, p := range []string{filepath.Dir(filepath.Dir(filepath.Dir(pathA))), filepath.Join(dir, "chunks"), entryFile(dir, hashB)} {
		if !pathExists(t, p) {
			t.Fatalf("%s was removed", p)
		}
	}
}

// Eviction leaves 10% headroom under the cap so a manager at the cap is not
// due for another walk after every store.
func TestCacheEvictionLeavesHeadroom(t *testing.T) {
	dir := t.TempDir()
	locations := make(map[xet.ChunkHash]shard.ChunkLocation)
	for i := range byte(11) {
		locations[xet.ChunkHash{7: i + 1}] = shard.ChunkLocation{XorbHash: xet.XorbHash{i}}
	}
	if err := NewCacheManager(dir, 0).Store(testEndpoint, locations); err != nil {
		t.Fatal(err)
	}
	m := NewCacheManager(dir, 10*entryCost)
	m.prepare()
	if left, tracked := entries(t, dir), trackedEntries(m); len(left) != 9 || tracked != 9 {
		t.Fatalf("evicting 11 entries to a 10-entry cap left %d files and %d tracked; want 9", len(left), tracked)
	}
}

// UploadFiles prepares its manager before any work, so crashed leftovers are
// gone even when nothing is deduplicated.
func TestUploadFilesPreparesCache(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "staging", "xorb-stale")
	seedAged(t, stale, 2*leftoverAge)
	leftover := oldTemp(dir, hashA)
	seedAged(t, leftover, 2*leftoverAge)

	m := NewCacheManager(dir, DefaultCacheSize)
	data := bytes.Repeat([]byte("xet"), 1024)
	if _, err := UploadFiles(t.Context(), stubClient{&fakeClient{}}, []io.ReadSeeker{bytes.NewReader(data)}, WithCacheManager(m)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{stale, leftover} {
		if pathExists(t, p) {
			t.Fatalf("%s survived UploadFiles; want prepare to remove it", p)
		}
	}
}
