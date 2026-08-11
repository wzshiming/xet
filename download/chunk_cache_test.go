package download

import (
	"bytes"
	"context"
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

func TestChunkCachePublishesFinalSizeInName(t *testing.T) {
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
	var found bool
	var finalInfo os.FileInfo
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".lock") {
			t.Fatalf("unexpected separate lock file %q", entry.Name())
		}
		_, _, _, _, expectedSize, ok := parseCacheFileName(entry.Name())
		if !ok {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != expectedSize {
			t.Fatalf("filename size %d does not match file size %d", expectedSize, info.Size())
		}
		finalInfo = info
		found = true
	}
	if !found {
		t.Fatal("completed cache file was not published")
	}
	partialInfo, err := os.Stat(cache.partialPath)
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(partialInfo, finalInfo) {
		t.Fatal("partial lock path and final cache path do not reference the same inode")
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

func TestChunkCacheIgnoresPartialAndWrongSizedFiles(t *testing.T) {
	dir := t.TempDir()
	base := cacheFileBasePath(dir, testCacheHash, 0, 1, 10, 20)
	if err := os.MkdirAll(filepath.Dir(base), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+".partial", []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(base+"_999", []byte("short"), 0o644); err != nil {
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
	if _, err := os.Stat(cache.partialPath); !os.IsNotExist(err) {
		t.Fatalf("partial cache was not removed: %v", err)
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
