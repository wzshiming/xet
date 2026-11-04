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
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet/internal/flock"
)

type gatedClient struct {
	data      []byte
	stallAt   int
	ignoreCtx bool // a stalled Read returns only on release or body Close, like a body not bound to ctx
	release   chan struct{}
	stalled   chan struct{}
	closed    chan struct{}
	calls     atomic.Int32
	once      sync.Once
	closeOnce sync.Once
}

func (c *gatedClient) DownloadXorbWithURL(ctx context.Context, _ string, _ http.Header) (io.ReadCloser, error) {
	c.calls.Add(1)
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
		ctxDone := b.ctx.Done()
		if c.ignoreCtx {
			ctxDone = nil
		}
		select {
		case <-c.release:
			b.released = true
		case <-ctxDone:
			return 0, b.ctx.Err()
		case <-c.closed:
			return 0, errors.New("body closed")
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
	b.client.closeOnce.Do(func() { close(b.client.closed) })
	return nil
}

// within fails the test unless done closes promptly, so a stuck Close or
// leaked worker fails the run instead of hanging it.
func within(t *testing.T, what string, done <-chan struct{}) bool {
	t.Helper()
	select {
	case <-done:
		return true
	case <-time.After(3 * time.Second):
		t.Errorf("%s did not finish within 3s", what)
		return false
	}
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
