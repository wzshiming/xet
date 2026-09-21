package download

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

const testCacheHash = "0123456789abcdef"

func TestChunkCacheRejectsEmptyHashSuffix(t *testing.T) {
	dir := t.TempDir()
	manager := NewCacheManager(dir, 1)
	cache, err := newChunkCache(bytes.NewReader([]byte("chunk")), manager, "aa11", 0, 1, 10, 20)
	if cache != nil {
		cache.Done()
	}
	if err == nil {
		t.Fatal("accepted a hash with an empty directory suffix")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("invalid hash created cache directories: %v", entries)
	}
}

func TestNewCacheRangeBounds(t *testing.T) {
	cases := []struct {
		chunkStart, chunkEnd uint32
		bytesStart, bytesEnd int64
		ok                   bool
	}{
		{0, 1, 0, 0, true},
		{0, xet.MaxChunksPerXorb, 0, 10, true},
		{xet.MaxChunksPerXorb - 1, xet.MaxChunksPerXorb, 5, 5, true},
		{1, 1, 0, 0, false},
		{2, 1, 0, 0, false},
		{0, xet.MaxChunksPerXorb + 1, 0, 0, false},
		{0, math.MaxUint32, 0, 0, false},
		{math.MaxUint32 - 1, math.MaxUint32, 0, 0, false},
		{0, 1, -1, 0, false},
		{0, 1, 1, 0, false},
	}
	for _, tc := range cases {
		_, err := newCacheRange("cache", testCacheHash, tc.chunkStart, tc.chunkEnd, tc.bytesStart, tc.bytesEnd)
		if (err == nil) != tc.ok {
			t.Errorf("chunks [%d, %d) bytes [%d, %d]: err = %v, want ok=%v", tc.chunkStart, tc.chunkEnd, tc.bytesStart, tc.bytesEnd, err, tc.ok)
		}
	}
}

func TestCacheFileLayoutRejectsOversizedChunkCount(t *testing.T) {
	for name, end := range map[string]uint32{"pastMax": xet.MaxChunksPerXorb + 1, "wrapsCount": math.MaxUint32} {
		t.Run(name, func(t *testing.T) {
			// A sealed layout for [0, end) with all-zero offsets and no data.
			numOffsets := end + 1
			header := make([]byte, 4+4*int(numOffsets))
			binary.LittleEndian.PutUint32(header, numOffsets)
			content := binary.LittleEndian.AppendUint32(header, crc32.ChecksumIEEE(header))
			path := filepath.Join(t.TempDir(), cacheFileName(0, end, 0, 0))
			if err := os.WriteFile(path, content, 0o644); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := readCacheFileLayout(f, 0, end, int64(len(content))); err == nil {
				t.Fatalf("accepted a cache file claiming %d chunks", end)
			}
		})
	}
}

func TestChunkCacheEntryPathUsesTwoLevelFanout(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	hash := strings.Repeat("0123456789abcdef", 4)
	cache, err := newChunkCache(bytes.NewReader([]byte("chunk")), m, hash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	cache.Done()

	want := filepath.Join(dir, hash[:2], hash[2:4], hash[4:], cacheFileName(0, 1, 10, 20))
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("entry not at <root>/2/2/60 path: %v", err)
	}
	if got := cacheFilePath(dir, hash, 0, 1, 10, 20); got != want {
		t.Fatalf("cacheFilePath = %q, want %q", got, want)
	}
}

func TestChunkCacheSealsEntryInPlace(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	cache, err := newChunkCache(bytes.NewReader([]byte("chunk")), m, testCacheHash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	cache.Done()

	entries, err := os.ReadDir(filepath.Join(dir, testCacheHash[:2], testCacheHash[2:4], testCacheHash[4:]))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d files, want the single entry file", len(entries))
	}
	if got, want := entries[0].Name(), cacheFileName(0, 1, 10, 20); got != want {
		t.Fatalf("got entry name %q, want %q", got, want)
	}
	info, err := entries[0].Info()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Size(), cacheEntryFileSize("chunk"); got != want {
		t.Fatalf("got entry size %d, want %d", got, want)
	}

	cached, err := openCachedRange(m, testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached == nil {
		t.Fatal("completed cache was not found")
	}
	defer cached.Done()
	buf := make([]byte, 16)
	n, err := cached.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "chunk" {
		t.Fatalf("got %q, want %q", got, "chunk")
	}
}

func TestChunkCacheIgnoresIncompleteFiles(t *testing.T) {
	dir := t.TempDir()
	path := cacheFilePath(dir, testCacheHash, 0, 1, 10, 20)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	// A crashed download: valid entry name, but not a sealed layout.
	if err := os.WriteFile(path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("incomplete cache was visible")
	}
}

func TestChunkCacheRewritesCrashedLeftoverInPlace(t *testing.T) {
	dir := t.TempDir()
	path := cacheFilePath(dir, testCacheHash, 0, 1, 10, 20)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("crashed-leftover"), 0o644); err != nil {
		t.Fatal(err)
	}

	m := NewCacheManager(dir, 0)
	cache, err := newChunkCache(bytes.NewReader([]byte("chunk")), m, testCacheHash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	if cache.readonly {
		t.Fatal("crashed leftover was adopted instead of rewritten")
	}
	if err := cache.LoadAll(); err != nil {
		t.Fatal(err)
	}
	cache.Done()

	cached, err := openCachedRange(m, testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached == nil {
		t.Fatal("rewritten entry was not adopted")
	}
	defer cached.Done()
	buf := make([]byte, 16)
	n, err := cached.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "chunk" {
		t.Fatalf("got %q, want %q", got, "chunk")
	}
}

func TestChunkCacheInProgressEntryInvisibleToReaders(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	cache, err := newChunkCache(bytes.NewReader([]byte("chunk")), m, testCacheHash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Done()

	// The writer holds the lock and the entry is not sealed yet.
	cached, err := openCachedRange(m, testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("unsealed entry was visible to readers")
	}
}

func TestChunkCacheRejectsEarlyEOF(t *testing.T) {
	dir := t.TempDir()
	cache, err := newChunkCache(bytes.NewReader([]byte("only-one-chunk")), NewCacheManager(dir, 0), testCacheHash, 0, 2, 10, 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.LoadAll(); err == nil || !strings.Contains(err.Error(), "expected 2") {
		t.Fatalf("got %v, want chunk count error", err)
	}
	cache.Done()
	info, err := os.Stat(cache.path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("failed entry was not discarded, size %d", info.Size())
	}
	cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("discarded entry was visible")
	}
}

// TestChunkCacheDoneKeepsCompletedRange releases a writer that decoded every
// expected chunk but never read the decoder's trailing EOF: the entry must be
// sealed and reusable, while a writer one chunk short is still discarded.
func TestChunkCacheDoneKeepsCompletedRange(t *testing.T) {
	chunks := [][]byte{[]byte("first"), []byte("second")}
	encoded := buildTestXorb(t, chunks)
	for _, tc := range []struct {
		name   string
		loaded uint32
		sealed bool
	}{
		{"complete", 1, true},
		{"incomplete", 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dec := xorb.NewDecoder(bytes.NewReader(encoded), false)
			cache, err := newChunkCache(dec, NewCacheManager(dir, 0), testCacheHash, 0, 2, 0, int64(len(encoded)-1))
			if err != nil {
				t.Fatal(err)
			}
			if err := cache.LoadTo(tc.loaded); err != nil {
				t.Fatal(err)
			}
			cache.Done()
			assertEntryUnlocked(t, cache.path)

			// A fresh manager verifies the checksum on its first open.
			cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.sealed {
				if cached != nil {
					cached.Done()
					t.Fatal("incomplete entry was sealed")
				}
				if info, err := os.Stat(cache.path); err != nil || info.Size() != 0 {
					t.Fatalf("incomplete entry was not discarded: %v, %v", info, err)
				}
				return
			}
			if cached == nil {
				t.Fatal("completed entry was discarded")
			}
			defer cached.Done()
			buf := make([]byte, 16)
			for i, want := range chunks {
				n, err := cached.Chunk(uint32(i), buf)
				if err != nil || !bytes.Equal(buf[:n], want) {
					t.Fatalf("chunk %d = %q, %v; want %q", i, buf[:n], err, want)
				}
			}
		})
	}
}

// corruptDataByte flips the last byte of the data region, which sits just
// before the four-byte crc32 trailer.
func corruptDataByte(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], info.Size()-5); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], info.Size()-5); err != nil {
		t.Fatal(err)
	}
}

func TestChunkCacheRemovesEntryOnChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	writeCacheEntry(t, NewCacheManager(dir, 0), testCacheHash, "chunk")
	corruptDataByte(t, findFinalFile(t, dir, testCacheHash))

	// A fresh manager has no verification memory, so the first open must
	// detect the corruption, drop the entry, and report a miss.
	cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("corrupted cache entry was served")
	}
	if entryExists(t, dir, testCacheHash) {
		t.Fatal("corrupted cache entry was not removed")
	}
}

func TestChunkCacheVerifiesChecksumOncePerManager(t *testing.T) {
	dir := t.TempDir()
	writeCacheEntry(t, NewCacheManager(dir, 0), testCacheHash, "chunk")

	m := NewCacheManager(dir, 0)
	cached, err := openCachedRange(m, testCacheHash, 0, 1)
	if err != nil || cached == nil {
		t.Fatalf("open cached range: %v", err)
	}
	cached.Done()

	// Corruption after the first verified open goes unnoticed by the same
	// manager: verification is lazy and runs at most once per entry.
	corruptDataByte(t, findFinalFile(t, dir, testCacheHash))
	cached, err = openCachedRange(m, testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached == nil {
		t.Fatal("verified entry was re-verified and dropped")
	}
	cached.Done()
}

func TestChunkCacheRejectsEntryWithoutChecksumTrailer(t *testing.T) {
	dir := t.TempDir()
	payload := "legacy-chunk"
	// Layout without the crc32 trailer: [numOffsets][offset0][offset1][data].
	header := make([]byte, 12)
	binary.LittleEndian.PutUint32(header[0:], 2)
	binary.LittleEndian.PutUint32(header[8:], uint32(len(payload)))
	content := append(header, payload...)

	path := cacheFilePath(dir, testCacheHash, 0, 1, 0, int64(len(payload)))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cached != nil {
		cached.Done()
		t.Fatal("unsealed entry was adopted")
	}
}

func TestChunkCacheConcurrentWriterReusesPublishedFile(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	first, err := newChunkCache(bytes.NewReader([]byte("first")), m, testCacheHash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan *chunkCache, 1)
	errs := make(chan error, 1)
	go func() {
		cache, err := newChunkCache(bytes.NewReader([]byte("second")), m, testCacheHash, 0, 1, 10, 20)
		result <- cache
		errs <- err
	}()

	select {
	case <-result:
		t.Fatal("second writer did not wait for the lock")
	case <-time.After(150 * time.Millisecond):
	}

	if err := first.LoadAll(); err != nil {
		t.Fatal(err)
	}
	second := <-result
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	defer first.Done()
	defer second.Done()
	if !second.readonly {
		t.Fatal("second writer did not reuse the published cache")
	}
	buf := make([]byte, 16)
	n, err := second.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "first" {
		t.Fatalf("got %q, want first writer data", got)
	}
}

func TestChunkCacheContenderRebuildsAfterWriterFailure(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	first, err := newChunkCache(bytes.NewReader([]byte("first")), m, testCacheHash, 0, 1, 10, 20)
	if err != nil {
		t.Fatal(err)
	}

	type result struct {
		cache *chunkCache
		err   error
	}
	results := make(chan result, 1)
	go func() {
		cache, err := newChunkCache(bytes.NewReader([]byte("second")), m, testCacheHash, 0, 1, 10, 20)
		results <- result{cache, err}
	}()

	select {
	case <-results:
		t.Fatal("second writer did not wait for the lock")
	case <-time.After(150 * time.Millisecond):
	}

	// The first writer gives up without sealing the entry.
	first.Done()

	res := <-results
	if res.err != nil {
		t.Fatal(res.err)
	}
	second := res.cache
	defer second.Done()
	if second.readonly {
		t.Fatal("second writer adopted a discarded entry")
	}
	if err := second.LoadAll(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, err := second.Chunk(0, buf)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buf[:n]); got != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

func TestDefaultCacheDir(t *testing.T) {
	if got, want := defaultCacheDir(""), filepath.Join(os.TempDir(), "xet-cache"); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

type countingDownloadClient struct {
	body    []byte
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (c *countingDownloadClient) DownloadXorbWithURL(context.Context, string, http.Header) (io.ReadCloser, error) {
	if c.calls.Add(1) == 1 {
		close(c.started)
		<-c.release
	}
	return io.NopCloser(bytes.NewReader(c.body)), nil
}

func (*countingDownloadClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

func TestPrefetcherLocksBeforeNetworkRequest(t *testing.T) {
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, false)
	if _, err := encoder.Write([]byte("chunk")); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}

	client := &countingDownloadClient{
		body:    encoded.Bytes(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	task := fetchTask{
		key:        fetchKey{Hash: testCacheHash, Start: 10, End: 20},
		url:        "https://example.invalid/xorb",
		chunkStart: 0,
		chunkEnd:   1,
	}
	entry1 := &prefetchEntry{task: task, ready: make(chan struct{})}
	entry2 := &prefetchEntry{task: task, ready: make(chan struct{})}
	// Separate managers over the same directory simulate two processes.
	dir := t.TempDir()
	p1 := &prefetcher{ctx: context.Background(), client: client, cache: NewCacheManager(dir, 0)}
	p2 := &prefetcher{ctx: context.Background(), client: client, cache: NewCacheManager(dir, 0)}

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() { p1.runJob(entry1); close(done1) }()
	<-client.started
	go func() { p2.runJob(entry2); close(done2) }()

	time.Sleep(150 * time.Millisecond)
	if got := client.calls.Load(); got != 1 {
		t.Fatalf("got %d network requests while first writer held the lock, want 1", got)
	}
	close(client.release)
	<-done1
	<-done2
	if entry1.err != nil || entry2.err != nil {
		t.Fatalf("prefetch errors: first=%v second=%v", entry1.err, entry2.err)
	}
	defer entry1.cache.Done()
	defer entry2.cache.Done()
	if got := client.calls.Load(); got != 1 {
		t.Fatalf("got %d network requests, want 1", got)
	}
}
