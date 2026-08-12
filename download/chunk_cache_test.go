package download

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/xorb"
)

const testCacheHash = "0123456789abcdef"

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

	entries, err := os.ReadDir(filepath.Join(dir, testCacheHash[:2], testCacheHash[2:]))
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
