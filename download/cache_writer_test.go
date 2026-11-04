package download

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

// entryData lays chunks out the way put takes them.
func entryData(chunks []string) ([]int64, []byte) {
	offsets := make([]int64, 1, len(chunks)+1)
	var data []byte
	for _, c := range chunks {
		data = append(data, c...)
		offsets = append(offsets, int64(len(data)))
	}
	return offsets, data
}

// putAndWait writes chunks as entry key through w and waits for the write.
func putAndWait(t *testing.T, w *cacheWriter, key cacheKey, chunks []string) {
	t.Helper()
	offsets, data := entryData(chunks)
	var calls atomic.Int32
	w.put(key, offsets, data, func() { calls.Add(1) })
	w.wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("done ran %d times, want 1", n)
	}
}

// stallWrites holds every write of w in its test hook until the test ends or
// unstall runs; stalled receives once per write held.
func stallWrites(t *testing.T, w *cacheWriter) (stalled <-chan struct{}, unstall func()) {
	ready := make(chan struct{}, 8)
	release := make(chan struct{})
	w.beforeWrite = func() {
		ready <- struct{}{}
		<-release
	}
	unstall = sync.OnceFunc(func() { close(release) })
	t.Cleanup(unstall)
	return ready, unstall
}

// oneChunkKey is the key of the single-chunk entry [0,1) of hash.
func oneChunkKey(hash xet.XorbHash) cacheKey {
	return cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 1, ByteEnd: 10}
}

func TestCacheWriterNilManagerRunsDoneSynchronously(t *testing.T) {
	var m *CacheManager
	w := m.newWriter(4)
	if w != nil {
		t.Fatalf("newWriter on a nil manager = %v, want nil", w)
	}
	ran := 0
	w.put(cacheKey{Hash: testXorbHash(1), ChunkStart: 0, ChunkEnd: 1, ByteEnd: 10}, []int64{0, 3}, []byte("abc"), func() { ran++ })
	if ran != 1 {
		t.Fatalf("nil writer ran done %d times before put returned, want 1", ran)
	}
	w.wait()
}

func TestCacheWriterRoundTrip(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xb1)
	chunks := spanChunks(4)
	if c, err := m.lookup(t.Context(), hash, 0, 4); c != nil || err != nil {
		t.Fatalf("lookup before put = %v, %v; want a miss", c, err)
	}
	putAndWait(t, m.newWriter(1), cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 4, ByteStart: 0, ByteEnd: 400}, chunks)

	c := mustLookup(t, m, hash, 0, 4)
	defer c.release()
	for _, span := range [][2]int{{0, 4}, {1, 3}} {
		want := strings.Join(chunks[span[0]:span[1]], "")
		if got := c.size(span[0], span[1]); got != int64(len(want)) {
			t.Errorf("size(%d, %d) = %d, want %d", span[0], span[1], got, len(want))
		}
		if got := readSpan(t, c, span[0], span[1]); got != want {
			t.Errorf("read(%d, %d) = %q, want %q", span[0], span[1], got, want)
		}
	}
}

func TestCacheWriterEntryMatchesLoadPath(t *testing.T) {
	hash := testXorbHash(0xb2)
	chunks := spanChunks(4)
	loaded := writeRangeEntry(t, NewCacheManager(t.TempDir(), 0), hash.String(), 2, 6, 100, 500, chunks)
	dir := t.TempDir()
	putAndWait(t, NewCacheManager(dir, 0).newWriter(1), cacheKey{Hash: hash, ChunkStart: 2, ChunkEnd: 6, ByteStart: 100, ByteEnd: 500}, chunks)

	if names, want := listEntryNames(t, dir, hash.String()), []string{cacheFileName(2, 6, 100, 500)}; !slices.Equal(names, want) {
		t.Fatalf("put left %v in the hash directory, want %v", names, want)
	}
	want, err := os.ReadFile(loaded)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(cacheFilePath(dir, hash.String(), 2, 6, 100, 500))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("put wrote %x, want the load path's %x", got, want)
	}
}

func TestCacheWriterEntryAdoptedAcrossManagers(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xb3)
	putAndWait(t, NewCacheManager(dir, 0).newWriter(1), cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, []string{"aaa", "bbb"})
	path := cacheFilePath(dir, hash.String(), 0, 2, 0, 100)

	fresh := NewCacheManager(dir, 0)
	c := mustLookup(t, fresh, hash, 0, 2)
	got := readSpan(t, c, 0, 2)
	c.release()
	if got != "aaabbb" {
		t.Fatalf("fresh manager read %q, want %q", got, "aaabbb")
	}
	// Verified once by fresh, so it trusts the entry from now on.
	corruptDataByte(t, path)
	mustLookup(t, fresh, hash, 0, 2).release()

	if c, err := NewCacheManager(dir, 0).lookup(t.Context(), hash, 0, 2); c != nil || err != nil {
		t.Fatalf("another fresh manager's lookup of the corrupted entry = %v, %v; want a miss", c, err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("corrupted entry was not removed: %v", err)
	}
}

func TestCacheWriterPanicsOnMalformedOffsets(t *testing.T) {
	// maxInflight 0 drops every well-formed write.
	w := NewCacheManager(t.TempDir(), 0).newWriter(0)
	key := cacheKey{Hash: testXorbHash(0xb4), ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}
	for name, offsets := range map[string][]int64{
		"empty":     {},
		"short":     {0, 6},
		"long":      {0, 3, 6, 6},
		"nonzero":   {1, 3, 6},
		"wrongEnd":  {0, 3, 5},
		"backwards": {0, 7, 6},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("put with offsets %v for 6 bytes of chunks [0,2) did not panic", offsets)
				}
			}()
			w.put(key, offsets, []byte("aaabbb"), func() {})
		})
	}
}

func TestCacheWriterPutDoesNotWaitForStalledWrite(t *testing.T) {
	dir := t.TempDir()
	w := NewCacheManager(dir, 0).newWriter(2)
	stalled, unstall := stallWrites(t, w)
	offsets, data := entryData([]string{"aaa"})
	first, second := testXorbHash(0xb5), testXorbHash(0xb6)
	var firstDone, secondDone atomic.Int32

	w.put(oneChunkKey(first), offsets, data, func() { firstDone.Add(1) })
	if !within(t, "first write reaching the hook", stalled) {
		t.FailNow()
	}
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		w.put(oneChunkKey(second), offsets, data, func() { secondDone.Add(1) })
	}()
	if !within(t, "second put", returned) {
		t.FailNow()
	}
	if n := secondDone.Load(); n != 0 {
		t.Fatalf("second put ran done %d times before its write, want 0", n)
	}

	unstall()
	w.wait()
	if a, b := firstDone.Load(), secondDone.Load(); a != 1 || b != 1 {
		t.Fatalf("done ran %d and %d times, want once each", a, b)
	}
	if !entryExists(t, dir, first.String()) || !entryExists(t, dir, second.String()) {
		t.Fatal("an accepted write was not completed")
	}
}

func TestCacheWriterDropsPutBeyondMaxInflight(t *testing.T) {
	dir := t.TempDir()
	w := NewCacheManager(dir, 0).newWriter(1)
	stalled, unstall := stallWrites(t, w)
	offsets, data := entryData([]string{"aaa"})
	first, second := testXorbHash(0xb7), testXorbHash(0xb8)
	var firstDone, secondDone atomic.Int32

	w.put(oneChunkKey(first), offsets, data, func() { firstDone.Add(1) })
	if !within(t, "first write reaching the hook", stalled) {
		t.FailNow()
	}
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		w.put(oneChunkKey(second), offsets, data, func() { secondDone.Add(1) })
		if n := secondDone.Load(); n != 1 {
			t.Errorf("dropped put ran done %d times before returning, want 1", n)
		}
	}()
	if !within(t, "dropped put", returned) {
		t.FailNow()
	}

	waited := make(chan struct{})
	go func() {
		defer close(waited)
		w.wait()
	}()
	select {
	case <-waited:
		t.Fatal("wait returned while a write was stalled")
	case <-time.After(50 * time.Millisecond):
	}
	unstall()
	if !within(t, "wait", waited) {
		t.FailNow()
	}
	if a, b := firstDone.Load(), secondDone.Load(); a != 1 || b != 1 {
		t.Fatalf("done ran %d and %d times, want once each", a, b)
	}
	if !entryExists(t, dir, first.String()) {
		t.Fatal("stalled write was not completed")
	}
	if entryExists(t, dir, second.String()) {
		t.Fatal("dropped put wrote an entry")
	}
}

func TestCacheWriterFreesSlotBeforeDone(t *testing.T) {
	dir := t.TempDir()
	w := NewCacheManager(dir, 0).newWriter(1)
	offsets, data := entryData([]string{"aaa"})
	second := testXorbHash(0xba)
	w.put(oneChunkKey(testXorbHash(0xb9)), offsets, data, func() {
		w.put(oneChunkKey(second), offsets, data, func() {})
	})
	w.wait()
	if !entryExists(t, dir, second.String()) {
		t.Fatal("a put from done was dropped: the finished write still counted against maxInflight")
	}
}

func TestCacheWriterSkipsSealedEntry(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	hash := testXorbHash(0xc1)
	path := writeRangeEntry(t, m, hash.String(), 0, 2, 0, 100, []string{"aaa", "bbb"})
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	putAndWait(t, m.newWriter(1), cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, []string{"xxxx", "yyyy"})

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(before, after) || after.Size() != before.Size() || !after.ModTime().Equal(before.ModTime()) || !bytes.Equal(got, want) {
		t.Fatalf("put rewrote the sealed entry: %d bytes at %v, was %d bytes at %v", after.Size(), after.ModTime(), before.Size(), before.ModTime())
	}
}

func TestCacheWriterSkipsEntryLockedByAnotherManager(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xc2)
	// Another manager's streaming writer holds the entry's flock until it seals.
	holder, err := newChunkCache(&chunksReader{chunks: []string{"aaa", "bbb"}}, NewCacheManager(dir, 0), hash.String(), 0, 2, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Done()

	w := NewCacheManager(dir, 0).newWriter(1)
	offsets, data := entryData([]string{"xxxxxxxx", "yyyyyyyy"})
	var calls atomic.Int32
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		w.put(cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, offsets, data, func() { calls.Add(1) })
		w.wait()
	}()
	if !within(t, "put on an entry locked elsewhere", finished) {
		t.FailNow()
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("done ran %d times, want 1", n)
	}

	if err := holder.LoadAll(); err != nil {
		t.Fatal(err)
	}
	c := mustLookup(t, NewCacheManager(dir, 0), hash, 0, 2)
	defer c.release()
	if got := readSpan(t, c, 0, 2); got != "aaabbb" {
		t.Fatalf("lock holder's entry reads %q, want %q", got, "aaabbb")
	}
}

func TestCacheWriterRewritesCrashedLeftover(t *testing.T) {
	dir := t.TempDir()
	hash := testXorbHash(0xc3)
	path := cacheFilePath(dir, hash.String(), 0, 2, 0, 100)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, bytes.Repeat([]byte("junk"), 64), 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewCacheManager(dir, 0)
	putAndWait(t, m.newWriter(1), cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, []string{"aaa", "bbb"})
	c := mustLookup(t, m, hash, 0, 2)
	defer c.release()
	if got := readSpan(t, c, 0, 2); got != "aaabbb" {
		t.Fatalf("rewritten entry reads %q, want %q", got, "aaabbb")
	}
}

func TestCacheWriterEntriesEvictedLeastRecentlyUsed(t *testing.T) {
	dir := t.TempDir()
	payload := "0123456789"
	entrySize := cacheEntryFileSize(payload)
	// Room for exactly two entries.
	m := NewCacheManager(dir, entrySize*2)
	w := m.newWriter(1)
	a, b, c := testXorbHash(0xd1), testXorbHash(0xd2), testXorbHash(0xd3)
	put := func(h xet.XorbHash) { putAndWait(t, w, oneChunkKey(h), []string{payload}) }
	twoEntries := CacheUsage{Count: 2, Bytes: 2 * entrySize}

	put(a)
	put(b)
	if got, err := m.Usage(t.Context()); err != nil || got != twoEntries {
		t.Fatalf("usage = %+v, %v; want %+v", got, err, twoEntries)
	}
	put(c)
	if entryExists(t, dir, a.String()) {
		t.Fatal("oldest entry was not evicted")
	}
	if !entryExists(t, dir, b.String()) || !entryExists(t, dir, c.String()) {
		t.Fatal("an entry within capacity was evicted")
	}

	// Touch B so C becomes the least recently used.
	mustLookup(t, m, b, 0, 1).release()
	put(a)
	if entryExists(t, dir, c.String()) {
		t.Fatal("least recently used entry C was not evicted")
	}
	if !entryExists(t, dir, b.String()) {
		t.Fatal("recently used entry B was evicted")
	}
	if !entryExists(t, dir, a.String()) {
		t.Fatal("just written entry A was evicted")
	}
	if got, err := m.Usage(t.Context()); err != nil || got != twoEntries {
		t.Fatalf("usage = %+v, %v; want %+v", got, err, twoEntries)
	}
}

func TestCacheWriterEntriesGetMerged(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	m.mergeQuiet = 10 * time.Millisecond
	hash := testXorbHash(0xd4)
	w := m.newWriter(2)
	putAndWait(t, w, cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, []string{"aaa", "bbb"})
	putAndWait(t, w, cacheKey{Hash: hash, ChunkStart: 2, ChunkEnd: 4, ByteStart: 100, ByteEnd: 200}, []string{"ccc", "ddd"})

	merged := []string{cacheFileName(0, 4, 0, 200)}
	deadline := time.Now().Add(5 * time.Second)
	for names := listEntryNames(t, dir, hash.String()); !slices.Equal(names, merged); names = listEntryNames(t, dir, hash.String()) {
		if time.Now().After(deadline) {
			t.Fatalf("written entries were not merged, still have %v", names)
		}
		time.Sleep(10 * time.Millisecond)
	}
	verifyChunks(t, dir, hash.String(), []string{"aaa", "bbb", "ccc", "ddd"})
}

func TestCacheWriterMarksEntryVerified(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	hash := testXorbHash(0xd5)
	putAndWait(t, m.newWriter(1), cacheKey{Hash: hash, ChunkStart: 0, ChunkEnd: 2, ByteEnd: 100}, []string{"aaa", "bbb"})
	// The writer checksummed what it wrote, as seal does, so its manager trusts the entry.
	corruptDataByte(t, cacheFilePath(dir, hash.String(), 0, 2, 0, 100))
	mustLookup(t, m, hash, 0, 2).release()
}
