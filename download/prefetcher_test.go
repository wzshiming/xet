package download

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"sync"
	"testing"

	"github.com/wzshiming/xet/internal/flock"
)

type gatedClient struct {
	data    []byte
	stallAt int
	release chan struct{}
	stalled chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func (c *gatedClient) DownloadXorbWithURL(ctx context.Context, _ string, _ http.Header) (io.ReadCloser, error) {
	return &gatedBody{client: c, ctx: ctx}, nil
}

func (*gatedClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

type gatedBody struct {
	client   *gatedClient
	ctx      context.Context
	pos      int
	released bool
}

func (b *gatedBody) Read(p []byte) (int, error) {
	c := b.client
	if !b.released && b.pos >= c.stallAt {
		c.once.Do(func() { close(c.stalled) })
		select {
		case <-c.release:
			b.released = true
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	limit := c.stallAt
	if b.released {
		limit = len(c.data)
	}
	if b.pos >= limit {
		return 0, io.EOF
	}
	n := copy(p, c.data[b.pos:limit])
	b.pos += n
	return n, nil
}

func (b *gatedBody) Close() error {
	close(b.client.closed)
	return nil
}

func newTestPrefetcher(entry *prefetchEntry) *prefetcher {
	return &prefetcher{
		ctx:     context.Background(),
		entries: map[fetchKey]*prefetchEntry{entry.task.key: entry},
	}
}

func newTestEntry() *prefetchEntry {
	return &prefetchEntry{
		task:  fetchTask{key: fetchKey{Hash: testCacheHash, Start: 0, End: 10}},
		ready: make(chan struct{}),
	}
}

// writerCache opens a locked, unsealed writer for chunk [idx, idx+1).
func writerCache(t *testing.T, m *CacheManager, idx uint32) *chunkCache {
	t.Helper()
	c, err := newChunkCache(bytes.NewReader([]byte("chunk")), m, testCacheHash, idx, idx+1, int64(idx)*10, int64(idx)*10+10)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func assertEntryUnlocked(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := flock.TryLock(f); err != nil {
		t.Fatalf("entry %s still locked: %v", path, err)
	}
	flock.Unlock(f) //nolint:errcheck
}

func assertReleased(t *testing.T, c *chunkCache) {
	t.Helper()
	if c.file != nil {
		t.Fatal("cache still holds its file handle")
	}
	assertEntryUnlocked(t, c.path)
}

func readerV2Fixture(t *testing.T, n int) (chunks [][]byte, encoded []byte, recon *ReconstructionResponseV2) {
	t.Helper()
	chunks = make([][]byte, n)
	total := 0
	for i := range chunks {
		chunks[i] = bytes.Repeat([]byte{byte('a' + i)}, 1000)
		total += len(chunks[i])
	}
	encoded = buildTestXorb(t, chunks)
	recon = &ReconstructionResponseV2{
		Terms: []Term{{
			Hash:           testCacheHash,
			UnpackedLength: uint64(total),
			Range:          ChunkRange{Start: 0, End: uint32(n)},
		}},
		Xorbs: map[string][]XorbMultiRangeFetch{
			testCacheHash: {{
				URL: "test://xorb",
				Ranges: []XorbRangeDescriptor{{
					Chunks: ChunkRange{Start: 0, End: uint32(n)},
					Bytes:  ByteRange{Start: 0, End: int64(len(encoded) - 1)},
				}},
			}},
		},
	}
	return chunks, encoded, recon
}

func TestPrefetcherKeepsFirstCompletion(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	first := writerCache(t, m, 0)
	second := writerCache(t, m, 1)
	entry := newTestEntry()
	p := newTestPrefetcher(entry)
	p.publishEntry(entry, first)

	// A worker that fails or republishes after publication must not disturb consumers.
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.failEntry(entry, errors.New("late failure"))
		p.publishEntry(entry, second)
	}()
	for range 100 {
		if got, err := p.Get(entry.task.key); err != nil || got != first {
			t.Fatalf("Get = %v, %v; want the first cache", got, err)
		}
	}
	<-done
	if got, err := p.Get(entry.task.key); err != nil || got != first {
		t.Fatalf("Get after late completions = %v, %v; want the first cache", got, err)
	}
	assertReleased(t, second)
	p.Close()
	assertReleased(t, first)
	p.Close()
}

func TestPrefetcherCloseFailsUnpublishedEntry(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	cache := writerCache(t, m, 0)
	entry := newTestEntry()
	p := newTestPrefetcher(entry)

	p.Close()
	p.publishEntry(entry, cache)
	if got, err := p.Get(entry.task.key); !errors.Is(err, fs.ErrClosed) || got != nil {
		t.Fatalf("Get after Close = %v, %v; want fs.ErrClosed", got, err)
	}
	assertReleased(t, cache)
}

func TestPrefetcherCloseRacesPublication(t *testing.T) {
	m := NewCacheManager(t.TempDir(), 0)
	for i := range uint32(20) {
		cache := writerCache(t, m, i)
		entry := newTestEntry()
		p := newTestPrefetcher(entry)

		var wg sync.WaitGroup
		wg.Go(func() { p.publishEntry(entry, cache) })
		wg.Go(p.Close)
		wg.Wait()

		got, err := p.Get(entry.task.key)
		switch {
		case err == nil && got == cache:
		case errors.Is(err, fs.ErrClosed) && got == nil:
		default:
			t.Fatalf("Get = %v, %v", got, err)
		}
		assertReleased(t, cache)
		p.Close()
	}
}

func TestReaderV2CancelAfterFirstChunkReleasesEntry(t *testing.T) {
	chunks, encoded, recon := readerV2Fixture(t, 3)
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	path := cacheFilePath(dir, testCacheHash, 0, 3, 0, int64(len(encoded)-1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &gatedClient{
		data:    encoded,
		stallAt: len(buildTestXorb(t, chunks[:1])),
		stalled: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	r, err := NewReaderV2(ctx, client, recon, WithCacheManager(m))
	if err != nil {
		t.Fatal(err)
	}
	// Cancel once the worker has published the first chunk and stalled on the second.
	go func() {
		<-client.stalled
		cancel()
	}()
	if _, err := io.ReadAll(r); err == nil {
		t.Fatal("canceled download returned no error")
	}
	r.Close()
	<-client.closed
	assertEntryUnlocked(t, path)
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("aborted entry was not discarded: %v, %v", info, err)
	}

	r, err = NewReaderV2(context.Background(), &fakeClientAdapter{data: encoded}, recon, WithCacheManager(m))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if want := bytes.Join(chunks, nil); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("retry after cancel: %v, %d bytes, want %d", err, len(got), len(want))
	}
}

func TestReaderV2CloseBeforeFirstChunkReleasesLateResult(t *testing.T) {
	_, encoded, recon := readerV2Fixture(t, 3)
	dir := t.TempDir()
	m := NewCacheManager(dir, 0)
	path := cacheFilePath(dir, testCacheHash, 0, 3, 0, int64(len(encoded)-1))
	client := &gatedClient{
		data:    encoded,
		release: make(chan struct{}),
		stalled: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	r, err := NewReaderV2(context.Background(), client, recon, WithCacheManager(m))
	if err != nil {
		t.Fatal(err)
	}

	<-client.stalled
	r.Close()
	close(client.release)
	<-client.closed
	assertEntryUnlocked(t, path)
	m.mu.Lock()
	v, ok := m.lru.Get(path)
	m.mu.Unlock()
	if ok && v.(*cacheEntry).refs != 0 {
		t.Fatalf("late result pinned %d refs after Close", v.(*cacheEntry).refs)
	}
}
