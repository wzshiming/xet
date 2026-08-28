package download

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// chunksReader returns one chunk per Read call, matching the decoder
// contract expected by chunkCache.load.
type chunksReader struct {
	chunks []string
	idx    int
}

func (r *chunksReader) Read(p []byte) (int, error) {
	if r.idx >= len(r.chunks) {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[r.idx])
	r.idx++
	return n, nil
}

// writeRangeEntry publishes one sealed multi-chunk entry and returns its path.
func writeRangeEntry(t *testing.T, m *CacheManager, hash string, cs, ce uint32, bs, be int64, chunks []string) string {
	t.Helper()
	if int(ce-cs) != len(chunks) {
		t.Fatalf("chunk count %d does not match range [%d,%d)", len(chunks), cs, ce)
	}
	cache, err := newChunkCache(&chunksReader{chunks: chunks}, m, hash, cs, ce, bs, be)
	if err != nil {
		t.Fatal(err)
	}
	if cache.readonly {
		cache.Done()
		t.Fatalf("range [%d,%d) was already cached", cs, ce)
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	cache.Done()
	return cacheFilePath(m.dir, hash, cs, ce, bs, be)
}

// listEntryNames returns the names of all files in hash's cache directory.
func listEntryNames(t *testing.T, dir, hash string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, hash[:2], hash[2:]))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, de := range entries {
		names = append(names, de.Name())
	}
	return names
}

// verifyChunks opens [0, len(want)) with a fresh manager and checks contents.
func verifyChunks(t *testing.T, dir, hash string, want []string) {
	t.Helper()
	cached, err := openCachedRange(NewCacheManager(dir, 0), hash, 0, uint32(len(want)))
	if err != nil {
		t.Fatal(err)
	}
	if cached == nil {
		t.Fatal("merged range was not found in cache")
	}
	defer cached.Done()
	if len(cached.files) != 0 {
		t.Fatalf("range served from %d files, want a single file", len(cached.files)+1)
	}
	buf := make([]byte, 64)
	for i, w := range want {
		n, err := cached.Chunk(uint32(i), buf)
		if err != nil {
			t.Fatal(err)
		}
		if got := string(buf[:n]); got != w {
			t.Fatalf("chunk %d: got %q, want %q", i, got, w)
		}
	}
}

func TestMergeAdjacentEntries(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	// Park the background merger so manual passes stay deterministic.
	m.mergeQuiet = time.Hour

	writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"ccc", "ddd"})

	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}

	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != cacheFileName(0, 4, 0, 200) {
		t.Fatalf("got entries %v, want single %q", names, cacheFileName(0, 4, 0, 200))
	}
	verifyChunks(t, dir, testCacheHash, []string{"aaa", "bbb", "ccc", "ddd"})

	info, err := os.Stat(cacheFilePath(dir, testCacheHash, 0, 4, 0, 200))
	if err != nil {
		t.Fatal(err)
	}
	if got := trackedTotal(m); got != info.Size() {
		t.Fatalf("tracked total %d, want merged file size %d", got, info.Size())
	}
}

func TestMergeOverlappingEntries(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	writeRangeEntry(t, m, testCacheHash, 0, 3, 0, 150, []string{"c0", "c1", "c2"})
	writeRangeEntry(t, m, testCacheHash, 2, 5, 100, 250, []string{"c2", "c3", "c4"})

	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}

	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != cacheFileName(0, 5, 0, 250) {
		t.Fatalf("got entries %v, want single %q", names, cacheFileName(0, 5, 0, 250))
	}
	verifyChunks(t, dir, testCacheHash, []string{"c0", "c1", "c2", "c3", "c4"})
}

func TestMergeRemovesRedundantSubset(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	// The subset must be written first; a covering entry would satisfy it.
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"c2", "c3"})
	covering := writeRangeEntry(t, m, testCacheHash, 0, 6, 0, 300, []string{"c0", "c1", "c2", "c3", "c4", "c5"})

	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}

	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != filepath.Base(covering) {
		t.Fatalf("got entries %v, want only the covering entry %q", names, filepath.Base(covering))
	}
	verifyChunks(t, dir, testCacheHash, []string{"c0", "c1", "c2", "c3", "c4", "c5"})

	info, err := os.Stat(covering)
	if err != nil {
		t.Fatal(err)
	}
	if got := trackedTotal(m); got != info.Size() {
		t.Fatalf("tracked total %d, want covering file size %d", got, info.Size())
	}
}

func TestMergeKeepsInUseSource(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	first := writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"ccc", "ddd"})

	pinned, err := openCachedRange(m, testCacheHash, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pinned == nil {
		t.Fatal("first entry was not found in cache")
	}

	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(first); err != nil {
		t.Fatalf("in-use source was removed: %v", err)
	}
	if _, err := os.Stat(cacheFilePath(dir, testCacheHash, 0, 4, 0, 200)); err != nil {
		t.Fatalf("merged entry was not written: %v", err)
	}
	buf := make([]byte, 64)
	n, err := pinned.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "aaa" {
		t.Fatalf("pinned reader got %q, want %q", got, "aaa")
	}
	pinned.Done()

	// With the reference gone, the next pass removes the now-redundant source.
	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}
	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != cacheFileName(0, 4, 0, 200) {
		t.Fatalf("got entries %v, want single %q", names, cacheFileName(0, 4, 0, 200))
	}
	verifyChunks(t, dir, testCacheHash, []string{"aaa", "bbb", "ccc", "ddd"})
}

func TestMergeWorkerRetriesPinnedSource(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = 10 * time.Millisecond

	// Pin the first entry so the worker's pass cannot remove it, then
	// publish the neighbor to arm the merger.
	first := writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	pinned, err := openCachedRange(m, testCacheHash, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if pinned == nil {
		t.Fatal("first entry was not found in cache")
	}
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"ccc", "ddd"})

	// The worker writes the covering entry but must keep the pinned source.
	merged := cacheFilePath(dir, testCacheHash, 0, 4, 0, 200)
	mergedName := cacheFileName(0, 4, 0, 200)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if info, err := os.Stat(merged); err == nil && isCompleteCacheFile(merged, 0, 4, info.Size()) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not write the merged entry")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Let the publish-hinted retry pass fire and be refused while the pin
	// is still held, so releasing it is the only remaining trigger.
	time.Sleep(20 * m.mergeQuiet)
	if _, err := os.Stat(first); err != nil {
		t.Fatalf("pinned source was removed: %v", err)
	}

	// Releasing the pin must be enough: the worker retries on its own,
	// with no further publish or read hinting this hash.
	pinned.Done()
	for {
		names := listEntryNames(t, dir, testCacheHash)
		if len(names) == 1 && names[0] == mergedName {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not retry after pin release, still have %v", names)
		}
		time.Sleep(10 * time.Millisecond)
	}
	verifyChunks(t, dir, testCacheHash, []string{"aaa", "bbb", "ccc", "ddd"})
}

func TestMergeIgnoresIncompleteNeighbor(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	sealed := writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	incomplete := cacheFilePath(dir, testCacheHash, 2, 4, 100, 200)
	if err := os.WriteFile(incomplete, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(sealed); err != nil {
		t.Fatalf("sealed entry was disturbed: %v", err)
	}
	if _, err := os.Stat(incomplete); err != nil {
		t.Fatalf("incomplete entry was disturbed: %v", err)
	}
	if names := listEntryNames(t, dir, testCacheHash); len(names) != 2 {
		t.Fatalf("got entries %v, want the two originals", names)
	}
}

func TestMergeRecoversFromCrashedMerge(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"ccc", "ddd"})

	// A merge that died mid-write: placeholder header plus partial data,
	// never sealed, flock released with the process.
	crashed := cacheFilePath(dir, testCacheHash, 0, 4, 0, 200)
	leftover := make([]byte, 4+4*5+7)
	binary.LittleEndian.PutUint32(leftover, 5)
	copy(leftover[24:], "aaabbbc")
	if err := os.WriteFile(crashed, leftover, 0o644); err != nil {
		t.Fatal(err)
	}

	// Restart: a fresh manager still serves the range from the sources.
	restarted := NewCacheManager(dir, 0)
	restarted.mergeQuiet = time.Hour
	cached, err := openCachedRange(restarted, testCacheHash, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if cached == nil {
		t.Fatal("sources no longer serve the range after crashed merge")
	}
	cached.Done()

	// The first prepare after restart sweeps the crashed leftover.
	restarted.prepare()
	if _, err := os.Stat(crashed); !os.IsNotExist(err) {
		t.Fatalf("crashed merge leftover was not cleaned up: %v", err)
	}
	if names := listEntryNames(t, dir, testCacheHash); len(names) != 2 {
		t.Fatalf("got entries %v, want the two intact sources", names)
	}

	// The next pass completes the interrupted merge.
	if err := mergeHashDir(restarted, testCacheHash); err != nil {
		t.Fatal(err)
	}
	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != cacheFileName(0, 4, 0, 200) {
		t.Fatalf("got entries %v, want single %q", names, cacheFileName(0, 4, 0, 200))
	}
	verifyChunks(t, dir, testCacheHash, []string{"aaa", "bbb", "ccc", "ddd"})
}

func TestMergeWorkerCompactsAfterQuietPeriod(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = 10 * time.Millisecond

	// Publishing hints the merger; no manual mergeHashDir call.
	writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, []string{"aaa", "bbb"})
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, []string{"ccc", "ddd"})

	deadline := time.Now().Add(5 * time.Second)
	for {
		names := listEntryNames(t, dir, testCacheHash)
		if len(names) == 1 && names[0] == cacheFileName(0, 4, 0, 200) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not compact entries, still have %v", names)
		}
		time.Sleep(10 * time.Millisecond)
	}
	verifyChunks(t, dir, testCacheHash, []string{"aaa", "bbb", "ccc", "ddd"})
}

func TestMergeConcurrentReaders(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = time.Hour

	want := []string{"aaa", "bbb", "ccc", "ddd"}
	writeRangeEntry(t, m, testCacheHash, 0, 2, 0, 100, want[:2])
	writeRangeEntry(t, m, testCacheHash, 2, 4, 100, 200, want[2:])

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			buf := make([]byte, 64)
			for range 25 {
				cached, err := openCachedRange(m, testCacheHash, 0, 4)
				if err != nil {
					t.Error(err)
					return
				}
				if cached == nil {
					// Transient miss while sources are being unlinked.
					continue
				}
				for i, w := range want {
					n, err := cached.Chunk(uint32(i), buf)
					if err != nil {
						t.Error(err)
						break
					}
					if got := string(buf[:n]); got != w {
						t.Errorf("chunk %d: got %q, want %q", i, got, w)
						break
					}
				}
				cached.Done()
			}
		})
	}
	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Error(err)
	}
	wg.Wait()

	// Readers may have pinned sources during the first pass; a final pass
	// leaves only the merged entry.
	if err := mergeHashDir(m, testCacheHash); err != nil {
		t.Fatal(err)
	}
	names := listEntryNames(t, dir, testCacheHash)
	if len(names) != 1 || names[0] != cacheFileName(0, 4, 0, 200) {
		t.Fatalf("got entries %v, want single %q", names, cacheFileName(0, 4, 0, 200))
	}
	verifyChunks(t, dir, testCacheHash, want)
}
