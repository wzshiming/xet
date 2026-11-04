package download

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

// testXorbHash returns a distinct xorb hash for each b.
func testXorbHash(b byte) xet.XorbHash {
	var h xet.XorbHash
	for i := range h {
		h[i] = b
	}
	return h
}

// spanChunks returns n chunks with distinct contents and lengths.
func spanChunks(n int) []string {
	chunks := make([]string, n)
	for i := range chunks {
		chunks[i] = fmt.Sprintf("chunk%d:%s", i, strings.Repeat("x", i))
	}
	return chunks
}

// mustLookup fails the test unless m has a sealed entry covering [start, end).
func mustLookup(t *testing.T, m *CacheManager, hash xet.XorbHash, start, end int) *cachedChunks {
	t.Helper()
	c, err := m.lookup(t.Context(), hash, start, end)
	if err != nil || c == nil {
		t.Fatalf("lookup [%d,%d) = %v, %v; want a hit", start, end, c, err)
	}
	return c
}

// readSpan reads chunks [from, to) of c into a buffer of exactly c.size(from, to) bytes.
func readSpan(t *testing.T, c *cachedChunks, from, to int) string {
	t.Helper()
	buf := make([]byte, c.size(from, to))
	if err := c.read(buf, from, to); err != nil {
		t.Fatalf("read [%d,%d): %v", from, to, err)
	}
	return string(buf)
}

func TestCacheLookupNilManagerMisses(t *testing.T) {
	var m *CacheManager
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, ctx := range []context.Context{t.Context(), canceled} {
		c, err := m.lookup(ctx, testXorbHash(1), 0, 1)
		if c != nil || err != nil {
			t.Fatalf("lookup on a nil manager = %v, %v; want a miss", c, err)
		}
	}
}

func TestCacheLookupNeedsEntryCoveringSpan(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa1)
	chunks := spanChunks(10)
	writeRangeEntry(t, m, hash.String(), 0, 10, 0, 1000, chunks)

	c := mustLookup(t, m, hash, 3, 7)
	defer c.release()
	want := strings.Join(chunks[3:7], "")
	if got := c.size(3, 7); got != int64(len(want)) {
		t.Fatalf("size(3, 7) = %d, want %d", got, len(want))
	}
	if got := readSpan(t, c, 3, 7); got != want {
		t.Fatalf("read(3, 7) = %q, want %q", got, want)
	}

	if miss, err := m.lookup(t.Context(), hash, 8, 12); miss != nil || err != nil {
		t.Fatalf("lookup [8,12) over entry [0,10) = %v, %v; want a miss", miss, err)
	}
}

func TestCacheLookupMissesEntryUntilSealed(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa2)
	// Another manager's writer holds the entry's flock until it seals.
	writer, err := newChunkCache(&chunksReader{chunks: []string{"aaa", "bbb"}}, NewCacheManager(m.dir, 0), hash.String(), 4, 6, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Done()
	if _, err := writer.load(); err != nil {
		t.Fatal(err)
	}
	if c, err := m.lookup(t.Context(), hash, 4, 6); c != nil || err != nil {
		t.Fatalf("lookup of an entry still being written = %v, %v; want a miss", c, err)
	}

	if err := writer.LoadAll(); err != nil {
		t.Fatal(err)
	}
	c := mustLookup(t, m, hash, 4, 6)
	defer c.release()
	if got := readSpan(t, c, 4, 6); got != "aaabbb" {
		t.Fatalf("read(4, 6) = %q, want %q", got, "aaabbb")
	}
}

func TestCacheLookupReturnsContextError(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa3)
	writeRangeEntry(t, m, hash.String(), 0, 1, 0, 10, []string{"aaa"})
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	c, err := m.lookup(ctx, hash, 0, 1)
	if c != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("lookup with a canceled context = %v, %v; want context.Canceled", c, err)
	}
}

func TestCachedChunksReadRejectsShortBuffer(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa4)
	writeRangeEntry(t, m, hash.String(), 0, 2, 0, 100, []string{"aaa", "bbb"})
	c := mustLookup(t, m, hash, 0, 2)
	defer c.release()
	if err := c.read(make([]byte, c.size(0, 2)-1), 0, 2); err == nil {
		t.Fatal("read into a buffer one byte short succeeded")
	}
}

func TestCachedChunksPanicsOnSpanOutsideEntry(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa5)
	writeRangeEntry(t, m, hash.String(), 2, 5, 0, 100, []string{"aaa", "bbb", "ccc"})
	c := mustLookup(t, m, hash, 2, 5)
	defer c.release()
	for _, span := range [][2]int{{1, 3}, {3, 6}, {4, 3}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("size(%d, %d) outside entry [2,5) did not panic", span[0], span[1])
				}
			}()
			c.size(span[0], span[1])
		}()
	}
}

func TestCacheLookupVerifiesChecksumOncePerManager(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xa6)
	path := writeRangeEntry(t, NewCacheManager(dir, 0), hash.String(), 0, 2, 0, 100, []string{"aaa", "bbb"})

	m := NewCacheManager(dir, 0)
	mustLookup(t, m, hash, 0, 2).release()
	// Corruption after the first verified lookup goes unnoticed by the same manager.
	corruptDataByte(t, path)
	mustLookup(t, m, hash, 0, 2).release()
}

func TestCacheLookupRemovesCorruptEntry(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xa7)
	path := writeRangeEntry(t, NewCacheManager(dir, 0), hash.String(), 0, 2, 0, 100, []string{"aaa", "bbb"})
	corruptDataByte(t, path)

	if c, err := NewCacheManager(dir, 0).lookup(t.Context(), hash, 0, 2); c != nil || err != nil {
		t.Fatalf("lookup of a corrupt entry = %v, %v; want a miss", c, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupt entry was not removed: %v", err)
	}
}

func TestCacheLookupRemovesEntryWithInvalidOffsets(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xa8)
	// A sealed layout for chunks [0,2) whose offsets run backwards, under a matching checksum.
	data := []byte("abc")
	header := make([]byte, 4+4*3)
	binary.LittleEndian.PutUint32(header[0:], 3)
	binary.LittleEndian.PutUint32(header[8:], 5)
	binary.LittleEndian.PutUint32(header[12:], uint32(len(data)))
	crc := crc32.Update(crc32.ChecksumIEEE(data), crc32.IEEETable, header)
	path := cacheFilePath(dir, hash.String(), 0, 2, 0, 100)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, binary.LittleEndian.AppendUint32(append(header, data...), crc), 0o644); err != nil {
		t.Fatal(err)
	}

	if c, err := NewCacheManager(dir, 0).lookup(t.Context(), hash, 0, 2); c != nil || err != nil {
		t.Fatalf("lookup of an entry with invalid offsets = %v, %v; want a miss", c, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("entry with invalid offsets was not removed: %v", err)
	}
}

func TestCacheLookupHitIsNotEvictedUntilReleased(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xa9)
	payload := "0123456789"
	path := writeRangeEntry(t, m, hash.String(), 0, 1, 0, 10, []string{payload})
	first := mustLookup(t, m, hash, 0, 1)
	second := mustLookup(t, m, hash, 0, 1)

	setCapacity(m, 1)
	m.evaluate()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("in-use entry was evicted: %v", err)
	}
	if got := readSpan(t, first, 0, 1); got != payload {
		t.Fatalf("read(0, 1) = %q, want %q", got, payload)
	}

	// A repeated release must not drop the second hit's pin.
	first.release()
	first.release()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("entry still pinned by a second hit was evicted: %v", err)
	}
	// The last release must re-evaluate on its own.
	second.release()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("released entry was not evicted: %v", err)
	}
}

func TestCachedChunksReadAfterReleaseFails(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xaa)
	writeRangeEntry(t, m, hash.String(), 0, 1, 0, 10, []string{"aaa"})
	c := mustLookup(t, m, hash, 0, 1)
	c.release()
	if err := c.read(make([]byte, 3), 0, 1); err == nil {
		t.Fatal("read after release succeeded")
	}
}
