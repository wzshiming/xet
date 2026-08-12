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
// (crc32 + numOffsets + 2 offsets) plus the payload.
func cacheEntryFileSize(payload string) int64 {
	return int64(4 + 4 + 4*2 + len(payload))
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
		if _, _, _, _, _, ok := parseCacheFileName(de.Name()); ok {
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
)

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
	partialPath, ok := cachePartialPathFor(finalPath)
	if !ok {
		t.Fatal("no partial path for final file")
	}

	// Simulate another process holding the entry's flock.
	lockFile, err := os.OpenFile(partialPath, os.O_RDWR, 0)
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
	if _, err := os.Stat(partialPath); !os.IsNotExist(err) {
		t.Fatalf("partial file was not removed with the entry: %v", err)
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

	// A crashed download leaves an orphaned partial with no published file.
	orphanDir := filepath.Join(dir, testHashC[:2], testHashC[2:])
	if err := os.MkdirAll(orphanDir, 0o755); err != nil {
		t.Fatal(err)
	}
	orphan := filepath.Join(orphanDir, cacheFileBaseName(0, 1, 0, 10)+".partial")
	if err := os.WriteFile(orphan, []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := NewCacheManager(dir, cacheEntryFileSize(payload)) // room for one entry
	fresh.prepare()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphaned partial was not cleaned up: %v", err)
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

func TestCacheScanKeepsPartialOfPublishedEntry(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	finalPath := writeCacheEntry(t, NewCacheManager(dir, 0), testHashA, payload)
	partialPath, _ := cachePartialPathFor(finalPath)

	fresh := NewCacheManager(dir, 0)
	fresh.prepare()

	if _, err := os.Stat(partialPath); err != nil {
		t.Fatalf("partial flock target of published entry was removed: %v", err)
	}
	if !entryExists(t, dir, testHashA) {
		t.Fatal("published entry disappeared")
	}
}

func TestCachePartialPathFor(t *testing.T) {
	final := filepath.Join("cache", "ab", "cdef", "0-4_10-20_1234")
	got, ok := cachePartialPathFor(final)
	if !ok {
		t.Fatal("expected ok")
	}
	want := filepath.Join("cache", "ab", "cdef", "0-4_10-20.partial")
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
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
