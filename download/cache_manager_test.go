package download

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet/internal/flock"
)

// writeCacheEntry publishes one single-chunk cache entry and returns its
// final file path.
func writeCacheEntry(t *testing.T, m *CacheManager, hash string, payload string) string {
	t.Helper()
	cache, err := newChunkCache(bytes.NewReader([]byte(payload)), m, hash, 0, 1, 0, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	cache.Done()
	return findFinalFile(t, m.dir, hash)
}

// setCapacity changes the eviction bound of an existing manager.
func setCapacity(m *CacheManager, capacity int64) {
	m.mu.Lock()
	m.capacity = capacity
	m.mu.Unlock()
}

// cacheEntryFileSize is the on-disk size of a single-chunk entry: header
// (numOffsets + 2 offsets) plus the payload and the crc32 trailer.
func cacheEntryFileSize(payload string) int64 {
	return int64(4 + 4*2 + len(payload) + 4)
}

func findFinalFile(t *testing.T, dir, hash string) string {
	t.Helper()
	hashDir := filepath.Join(dir, hash[:2], hash[2:])
	entries, err := os.ReadDir(hashDir)
	if err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		t.Fatal(err)
	}
	for _, de := range entries {
		if _, _, _, _, ok := parseCacheFileName(de.Name()); ok {
			return filepath.Join(hashDir, de.Name())
		}
	}
	return ""
}

func entryExists(t *testing.T, dir, hash string) bool {
	t.Helper()
	return findFinalFile(t, dir, hash) != ""
}

const (
	testHashA = "aa11111111111111"
	testHashB = "bb22222222222222"
	testHashC = "cc33333333333333"
	testHashD = "dd44444444444444"
)

// trackedTotal reads the manager's in-memory byte total.
func trackedTotal(m *CacheManager) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.total
}

// writeOrphanEntry drops an incomplete cache entry with no lock holder, as a
// crashed download would leave behind.
func writeOrphanEntry(t *testing.T, dir, hash string) string {
	t.Helper()
	orphan := cacheFilePath(dir, hash, 0, 1, 0, 10)
	if err := os.MkdirAll(filepath.Dir(orphan), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	return orphan
}

func TestCachePublishUnderCapacityIsInMemoryOnly(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)

	// An entry from another manager and a crashed leftover, both unknown to
	// the manager under test.
	writeCacheEntry(t, NewCacheManager(dir, 0), testHashB, payload)
	orphan := writeOrphanEntry(t, dir, testHashC)

	// Publishing and releasing while under capacity must stay in memory: the
	// foreign entry is not adopted and the leftover is not cleaned up.
	m := NewCacheManager(dir, entrySize*10)
	writeCacheEntry(t, m, testHashA, payload)
	if got := trackedTotal(m); got != entrySize {
		t.Fatalf("under-capacity publish should only track its own entry: total %d, want %d", got, entrySize)
	}
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("crashed leftover should be untouched on the publish path: %v", err)
	}

	// The first prepare is the slow path: it adopts the foreign entry and
	// cleans up the leftover.
	m.prepare()
	if got := trackedTotal(m); got != entrySize*2 {
		t.Fatalf("prepare should adopt the foreign entry: total %d, want %d", got, entrySize*2)
	}
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("prepare should clean up crashed leftovers: %v", err)
	}
}

func TestCacheEvictionThrottlesDirectoryWalk(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)
	m := NewCacheManager(dir, entrySize*2)

	// Filling the cache to capacity runs the first full walk.
	writeCacheEntry(t, m, testHashA, payload)
	writeCacheEntry(t, m, testHashB, payload)

	// A foreign entry written after that walk is not re-discovered while the
	// throttle is active: the next overage is resolved from tracked state
	// alone, evicting the oldest tracked entry.
	writeCacheEntry(t, NewCacheManager(dir, 0), testHashC, payload)
	writeCacheEntry(t, m, testHashD, payload)

	if entryExists(t, dir, testHashA) {
		t.Fatal("oldest tracked entry was not evicted")
	}
	if !entryExists(t, dir, testHashB) || !entryExists(t, dir, testHashD) {
		t.Fatal("tracked entries within capacity were evicted")
	}
	if !entryExists(t, dir, testHashC) {
		t.Fatal("foreign entry should survive eviction within the reconcile interval")
	}
	if got := trackedTotal(m); got != entrySize*2 {
		t.Fatalf("throttled eviction should not adopt foreign entries: total %d, want %d", got, entrySize*2)
	}
}

func TestCachePrepareScansOnce(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.prepare()

	// A leftover appearing after the initial scan is only cleaned up on the
	// slow path, not by later prepares.
	orphan := writeOrphanEntry(t, dir, testHashA)
	m.prepare()
	if _, err := os.Stat(orphan); err != nil {
		t.Fatalf("prepare after the initial scan should not walk the directory: %v", err)
	}
}

func TestCacheEvictsLeastRecentlyUsed(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)
	// Room for exactly two entries.
	m := NewCacheManager(dir, entrySize*2)

	writeCacheEntry(t, m, testHashA, payload)
	writeCacheEntry(t, m, testHashB, payload)
	writeCacheEntry(t, m, testHashC, payload)

	if entryExists(t, dir, testHashA) {
		t.Fatal("oldest entry was not evicted")
	}
	for _, h := range []string{testHashB, testHashC} {
		if !entryExists(t, dir, h) {
			t.Fatalf("entry %s was evicted unexpectedly", h)
		}
	}

	// Touch B so C becomes the least recently used.
	cc, err := openCachedRange(m, testHashB, 0, 1)
	if err != nil || cc == nil {
		t.Fatalf("open cached range: %v", err)
	}
	cc.Done()

	writeCacheEntry(t, m, testHashA, payload)
	if entryExists(t, dir, testHashC) {
		t.Fatal("least recently used entry C was not evicted")
	}
	if !entryExists(t, dir, testHashB) {
		t.Fatal("recently used entry B was evicted")
	}
	if !entryExists(t, dir, testHashA) {
		t.Fatal("just written entry A was evicted")
	}
}

func TestCacheDoesNotEvictInUseEntry(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	m := NewCacheManager(dir, 1) // everything is over capacity

	// While publishing, the writer itself still references the entry, so it
	// must survive the eviction that runs at publish time.
	cache, err := newChunkCache(bytes.NewReader([]byte(payload)), m, testHashA, 0, 1, 0, int64(len(payload)))
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	if !entryExists(t, dir, testHashA) {
		t.Fatal("entry was evicted while its writer was still using it")
	}
	// Done drops the writer's reference and must re-evaluate on its own.
	cache.Done()
	if entryExists(t, dir, testHashA) {
		t.Fatal("released entry was not evicted after Done")
	}

	// Hold a reader on a fresh entry while eviction runs.
	setCapacity(m, 0) // disable eviction while publishing B
	writeCacheEntry(t, m, testHashB, payload)
	reader, err := openCachedRange(m, testHashB, 0, 1)
	if err != nil || reader == nil {
		t.Fatalf("open cached range: %v", err)
	}

	setCapacity(m, 1)
	m.evaluate()
	if !entryExists(t, dir, testHashB) {
		t.Fatal("in-use entry was evicted")
	}

	buf := make([]byte, 32)
	n, err := reader.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != payload {
		t.Fatalf("got %q, want %q", got, payload)
	}
	// Done drops the reader's reference and must re-evaluate on its own.
	reader.Done()
	if entryExists(t, dir, testHashB) {
		t.Fatal("released entry was not evicted")
	}
}

func TestCacheBoundsDirectoryAcrossManagers(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)
	// Two managers over the same directory, each with room for two entries.
	m1 := NewCacheManager(dir, entrySize*2)
	m2 := NewCacheManager(dir, entrySize*2)

	writeCacheEntry(t, m1, testHashA, payload)
	writeCacheEntry(t, m2, testHashB, payload)
	// m1 only ever wrote A, but must still count B when evicting.
	writeCacheEntry(t, m1, testHashC, payload)

	if entryExists(t, dir, testHashA) {
		t.Fatal("directory-level bound was not enforced across managers")
	}
	for _, h := range []string{testHashB, testHashC} {
		if !entryExists(t, dir, h) {
			t.Fatalf("entry %s was evicted unexpectedly", h)
		}
	}
}

func TestCacheEvictionSkipsFlockedEntry(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	m := NewCacheManager(dir, 0)

	finalPath := writeCacheEntry(t, m, testHashA, payload)

	// Simulate another process holding the entry's flock.
	lockFile, err := os.OpenFile(finalPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := flock.TryLock(lockFile); err != nil {
		t.Fatal(err)
	}

	setCapacity(m, 1)
	m.evaluate()
	if !entryExists(t, dir, testHashA) {
		t.Fatal("flocked entry was evicted")
	}

	if err := flock.Unlock(lockFile); err != nil {
		t.Fatal(err)
	}
	m.evaluate()
	if entryExists(t, dir, testHashA) {
		t.Fatal("unlocked entry was not evicted")
	}
}

func TestCacheUnboundedWhenCapacityNotPositive(t *testing.T) {
	dir := t.TempDir()
	payload := strings.Repeat("x", 64)
	m := NewCacheManager(dir, 0)

	for _, h := range []string{testHashA, testHashB, testHashC} {
		writeCacheEntry(t, m, h, payload)
	}
	for _, h := range []string{testHashA, testHashB, testHashC} {
		if !entryExists(t, dir, h) {
			t.Fatalf("entry %s was evicted despite unbounded cache", h)
		}
	}
}

func TestCacheScanAdoptsExistingEntriesAndCleansOrphans(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	// Write entries via a throwaway manager so a fresh manager has to
	// discover them from disk.
	writer := NewCacheManager(dir, 0)
	pathA := writeCacheEntry(t, writer, testHashA, payload)
	pathB := writeCacheEntry(t, writer, testHashB, payload)

	// Make A clearly older than B.
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(pathA, old, old); err != nil {
		t.Fatal(err)
	}

	// A crashed download leaves an incomplete entry with no lock holder.
	orphanDir := filepath.Join(dir, testHashC[:2], testHashC[2:])
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(orphanDir, cacheFileName(0, 1, 0, 10))
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := NewCacheManager(dir, cacheEntryFileSize(payload)) // room for one entry
	fresh.prepare()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("crashed leftover was not cleaned up: %v", err)
	}
	if entryExists(t, dir, testHashA) {
		t.Fatal("older adopted entry was not evicted")
	}
	if !entryExists(t, dir, testHashB) {
		t.Fatal("newer adopted entry was evicted")
	}
	if got := findFinalFile(t, dir, testHashB); got != pathB {
		t.Fatalf("got %q, want %q", got, pathB)
	}
}

func TestCacheScanKeepsLockedIncompleteEntry(t *testing.T) {
	dir := t.TempDir()
	path := cacheFilePath(dir, testHashA, 0, 1, 0, 10)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("in-progress"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Simulate an active writer holding the entry's flock.
	lockFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer lockFile.Close()
	if err := flock.TryLock(lockFile); err != nil {
		t.Fatal(err)
	}
	defer flock.Unlock(lockFile) //nolint:errcheck

	fresh := NewCacheManager(dir, 0)
	fresh.prepare()

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("locked in-progress entry was removed: %v", err)
	}
}

func TestCacheEvictionRemovesEmptyDirs(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	m := NewCacheManager(dir, 0)
	writeCacheEntry(t, m, testHashA, payload)

	setCapacity(m, 1)
	m.evaluate()
	if _, err := os.Stat(filepath.Join(dir, testHashA[:2])); !os.IsNotExist(err) {
		t.Fatalf("empty hash prefix dir was not removed: %v", err)
	}
}

func TestCacheEvictionOrderStress(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)
	m := NewCacheManager(dir, entrySize*3)

	hashes := make([]string, 6)
	for i := range hashes {
		hashes[i] = fmt.Sprintf("%02d11111111111111", i)
		writeCacheEntry(t, m, hashes[i], payload)
	}
	// Only the three most recent entries survive.
	for _, h := range hashes[:3] {
		if entryExists(t, dir, h) {
			t.Fatalf("entry %s should have been evicted", h)
		}
	}
	for _, h := range hashes[3:] {
		if !entryExists(t, dir, h) {
			t.Fatalf("entry %s should have survived", h)
		}
	}
}

func TestOpenCachedRangeTreatsUnreadableEntryAsMiss(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod does not restrict reads on windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses permission checks")
	}
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	path := writeCacheEntry(t, m, testHashA, "0123456789")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}

	cached, err := openCachedRange(m, testHashA, 0, 1)
	if err != nil {
		t.Fatalf("unreadable entry should be a cache miss, got error: %v", err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("unreadable entry should be a cache miss, got a cache hit")
	}
}
