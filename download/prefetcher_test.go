package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
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

func readerPrefetcher(t *testing.T, r io.ReadCloser) *prefetcher {
	t.Helper()
	switch r := r.(type) {
	case *ReaderV1:
		return r.prefetcher
	case *ReaderV2:
		return r.prefetcher
	}
	t.Fatalf("unexpected reader %T", r)
	return nil
}

// awaitWorkers joins p's goroutines; a closed body no longer implies the
// worker has released its entry, since ctx cancellation closes bodies early.
func awaitWorkers(t *testing.T, p *prefetcher) bool {
	t.Helper()
	idle := make(chan struct{})
	go func() {
		defer close(idle)
		p.workers.Wait()
	}()
	return within(t, "prefetch workers", idle)
}

// answers is one reconstruction in both API shapes.
type answers struct {
	v1 *ReconstructionResponseV1
	v2 *ReconstructionResponseV2
}

// splitAnswers describes one 3-chunk xorb fetched through url as chunks [0,2)
// and [2,3); split is the first range's length.
func splitAnswers(t *testing.T, url string) (chunks [][]byte, encoded []byte, split int64, a *answers) {
	t.Helper()
	chunks = make([][]byte, 3)
	for i := range chunks {
		chunks[i] = bytes.Repeat([]byte{byte('a' + i)}, 1000)
	}
	encoded = buildTestXorb(t, chunks)
	split = int64(len(buildTestXorb(t, chunks[:2])))
	terms := []Term{
		{Hash: testCacheHash, UnpackedLength: 2000, Range: ChunkRange{Start: 0, End: 2}},
		{Hash: testCacheHash, UnpackedLength: 1000, Range: ChunkRange{Start: 2, End: 3}},
	}
	ranges := []XorbRangeDescriptor{
		{Chunks: ChunkRange{Start: 0, End: 2}, Bytes: ByteRange{Start: 0, End: split - 1}},
		{Chunks: ChunkRange{Start: 2, End: 3}, Bytes: ByteRange{Start: split, End: int64(len(encoded) - 1)}},
	}
	fetchInfo := []FetchInfoEntry{
		{Range: ranges[0].Chunks, URL: url, URLRange: ranges[0].Bytes},
		{Range: ranges[1].Chunks, URL: url, URLRange: ranges[1].Bytes},
	}
	a = &answers{
		v1: &ReconstructionResponseV1{Terms: terms, FetchInfo: map[string][]FetchInfoEntry{testCacheHash: fetchInfo}},
		v2: &ReconstructionResponseV2{Terms: slices.Clone(terms), Xorbs: map[string][]XorbMultiRangeFetch{testCacheHash: {{URL: url, Ranges: ranges}}}},
	}
	return chunks, encoded, split, a
}

// splitRangeReaders builds V1 and V2 constructors for splitAnswers' layout, so
// a stall inside the first range leaves the second queued behind it at
// concurrency 1.
func splitRangeReaders(t *testing.T) (chunks [][]byte, encoded []byte, split int64, readers map[string]func(context.Context, ClientAdapter, *CacheManager) (io.ReadCloser, error)) {
	t.Helper()
	chunks, encoded, split, a := splitAnswers(t, "test://xorb")
	readers = map[string]func(context.Context, ClientAdapter, *CacheManager) (io.ReadCloser, error){
		"v1": func(ctx context.Context, client ClientAdapter, m *CacheManager) (io.ReadCloser, error) {
			return NewReaderV1(ctx, client, a.v1, WithCacheManager(m), WithConcurrency(1))
		},
		"v2": func(ctx context.Context, client ClientAdapter, m *CacheManager) (io.ReadCloser, error) {
			return NewReaderV2(ctx, client, a.v2, WithCacheManager(m), WithConcurrency(1))
		},
	}
	return chunks, encoded, split, readers
}

// refreshing gives client the re-query capability: fresh is answered once onRefresh allows it.
type refreshing[T any] struct {
	ClientAdapter
	fresh     *T
	retries   int
	onRefresh func(context.Context) error
}

func (r refreshing[T]) RefreshReconstruction(ctx context.Context) (*T, error) {
	if err := r.onRefresh(ctx); err != nil {
		return nil, err
	}
	return r.fresh, nil
}

func (r refreshing[T]) RefreshRetries() int { return r.retries }

// openRefreshing opens old through client with concurrency workers and
// re-plans from fresh after a 403, calling onRefresh first with the refresh context.
func openRefreshing(ctx context.Context, api string, client ClientAdapter, m *CacheManager, old, fresh *answers, retries, concurrency int, onRefresh func(context.Context) error) (io.ReadCloser, error) {
	opts := []Option{WithCacheManager(m), WithConcurrency(concurrency)}
	if api == "v1" {
		return NewReaderV1(ctx, refreshing[ReconstructionResponseV1]{client, fresh.v1, retries, onRefresh}, old.v1, opts...)
	}
	return NewReaderV2(ctx, refreshing[ReconstructionResponseV2]{client, fresh.v2, retries, onRefresh}, old.v2, opts...)
}

// forbidden403 is what a refused fetch URL produces, like client's statusError.
type forbidden403 struct{}

func (forbidden403) Error() string   { return "API error (status 403 Forbidden)" }
func (forbidden403) StatusCode() int { return http.StatusForbidden }

// expiringClient serves data like fakeClientAdapter but refuses the expired
// URL with 403 and logs each answered request as "<url> <Range>". hold runs
// before a request is answered; refused receives one value per 403.
type expiringClient struct {
	fakeClientAdapter
	expired string
	hold    func(url, rng string)
	refused chan<- struct{}
	mu      sync.Mutex
	gets    []string
}

func (c *expiringClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	rng := header.Get("Range")
	if c.hold != nil {
		c.hold(url, rng)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.gets = append(c.gets, url+" "+rng)
	c.mu.Unlock()
	if url == c.expired {
		if c.refused != nil {
			c.refused <- struct{}{}
		}
		return nil, forbidden403{}
	}
	return c.fakeClientAdapter.DownloadXorbWithURL(ctx, url, header)
}

func (c *expiringClient) requests() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.gets)
}

func byteRange(r ByteRange) string { return fmt.Sprintf("bytes=%d-%d", r.Start, r.End) }

func TestRefreshRejectsChangedRanges(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(a *answers)
		want   string
	}{
		{"byte range", func(a *answers) {
			a.v1.FetchInfo[testCacheHash][0].URLRange.End++
			a.v2.Xorbs[testCacheHash][0].Ranges[0].Bytes.End++
		}, "no longer covers"},
		{"chunk range", func(a *answers) {
			a.v1.FetchInfo[testCacheHash][0].Range.End = 3
			a.v2.Xorbs[testCacheHash][0].Ranges[0].Chunks.End = 3
		}, "no longer covers"},
		{"missing xorb", func(a *answers) {
			delete(a.v1.FetchInfo, testCacheHash)
			delete(a.v2.Xorbs, testCacheHash)
		}, "no fetch info"},
	}
	for _, api := range []string{"v1", "v2"} {
		for _, tc := range cases {
			t.Run(api+"/"+tc.name, func(t *testing.T) {
				_, encoded, _, old := splitAnswers(t, "test://old")
				_, _, _, fresh := splitAnswers(t, "test://new")
				tc.mutate(fresh)
				second := byteRange(old.v1.FetchInfo[testCacheHash][1].URLRange)
				release := make(chan struct{})
				client := &expiringClient{fakeClientAdapter: fakeClientAdapter{data: encoded}, expired: "test://old"}
				client.hold = func(_, rng string) {
					if rng == second {
						<-release
					}
				}
				var refreshes atomic.Int32
				r, err := openRefreshing(t.Context(), api, client, NewCacheManager(t.TempDir(), 0), old, fresh, 2, 1, func(context.Context) error {
					refreshes.Add(1)
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				p := readerPrefetcher(t, r)
				got, err := io.ReadAll(r)
				close(release)
				awaitWorkers(t, p)
				if err == nil || !strings.Contains(err.Error(), tc.want) || !forbidden(err) {
					t.Fatalf("read: %v, want the 403 and %q reported", err, tc.want)
				}
				if len(got) != 0 {
					t.Fatalf("read %d bytes from a refused answer", len(got))
				}
				if n := refreshes.Load(); n != 1 {
					t.Fatalf("refreshed %d times, want once", n)
				}
				for _, req := range client.requests() {
					if strings.HasPrefix(req, "test://new") {
						t.Fatalf("fetched %q from an answer describing other bytes", req)
					}
				}
			})
		}
	}
}

// TestRefreshUsesFreshURLForLaterRanges refuses the first range at the old
// URL: after one refresh the second range must start at the new URL instead
// of paying its own 403.
func TestRefreshUsesFreshURLForLaterRanges(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		t.Run(api, func(t *testing.T) {
			chunks, encoded, _, old := splitAnswers(t, "test://old")
			_, _, _, fresh := splitAnswers(t, "test://new")
			fresh.v1.OffsetIntoFirstRange, fresh.v2.OffsetIntoFirstRange = 1, 1
			fresh.v1.Terms[0].UnpackedLength, fresh.v2.Terms[0].UnpackedLength = 1999, 1999
			client := &expiringClient{fakeClientAdapter: fakeClientAdapter{data: encoded}, expired: "test://old"}
			var refreshes atomic.Int32
			r, err := openRefreshing(t.Context(), api, client, NewCacheManager(t.TempDir(), 0), old, fresh, 1, 1, func(context.Context) error {
				refreshes.Add(1)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			p := readerPrefetcher(t, r)
			got, err := io.ReadAll(r)
			awaitWorkers(t, p)
			if want := bytes.Join(chunks, nil); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, %v; want %d", len(got), err, len(want))
			}
			ranges := old.v1.FetchInfo[testCacheHash]
			want := []string{
				"test://old " + byteRange(ranges[0].URLRange),
				"test://new " + byteRange(ranges[0].URLRange),
				"test://new " + byteRange(ranges[1].URLRange),
			}
			if got := client.requests(); !slices.Equal(got, want) || refreshes.Load() != 1 {
				t.Fatalf("requests = %q after %d refreshes, want %q after one", got, refreshes.Load(), want)
			}
		})
	}
}

// TestRefreshCoalescesConcurrentForbidden refuses both ranges at the old URL
// while they are in flight together, or refuses the second only after the
// first was refreshed: either way one refresh serves both.
func TestRefreshCoalescesConcurrentForbidden(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		for _, late := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/late=%v", api, late), func(t *testing.T) {
				chunks, encoded, _, old := splitAnswers(t, "test://old")
				_, _, _, fresh := splitAnswers(t, "test://new")
				ranges := old.v1.FetchInfo[testCacheHash]
				var arrived sync.WaitGroup
				arrived.Add(2)
				refreshed := make(chan struct{})
				client := &expiringClient{fakeClientAdapter: fakeClientAdapter{data: encoded}, expired: "test://old"}
				client.hold = func(url, rng string) {
					if url != "test://old" {
						return
					}
					arrived.Done()
					arrived.Wait()
					if late && rng == byteRange(ranges[1].URLRange) {
						within(t, "refresh before the late 403", refreshed)
					}
				}
				var refreshes atomic.Int32
				r, err := openRefreshing(t.Context(), api, client, NewCacheManager(t.TempDir(), 0), old, fresh, 1, 2, func(context.Context) error {
					if refreshes.Add(1) == 1 {
						close(refreshed)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				p := readerPrefetcher(t, r)
				got, err := io.ReadAll(r)
				awaitWorkers(t, p)
				if want := bytes.Join(chunks, nil); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("read %d bytes, %v; want %d", len(got), err, len(want))
				}
				want := []string{
					"test://new " + byteRange(ranges[0].URLRange),
					"test://new " + byteRange(ranges[1].URLRange),
					"test://old " + byteRange(ranges[0].URLRange),
					"test://old " + byteRange(ranges[1].URLRange),
				}
				reqs := client.requests()
				slices.Sort(reqs)
				if !slices.Equal(reqs, want) || refreshes.Load() != 1 {
					t.Fatalf("requests = %q after %d refreshes, want each range refused once and served once after one refresh", reqs, refreshes.Load())
				}
			})
		}
	}
}

// TestReaderStopsDuringRefresh closes the reader, or cancels its context,
// while the refresh that followed two concurrent 403s is still pending: the
// leading and the waiting worker must exit without it, and the cache must stay usable.
func TestReaderStopsDuringRefresh(t *testing.T) {
	for _, api := range []string{"v1", "v2"} {
		for _, stop := range []string{"close", "cancel"} {
			t.Run(api+"/"+stop, func(t *testing.T) {
				chunks, encoded, _, old := splitAnswers(t, "test://old")
				_, _, _, fresh := splitAnswers(t, "test://new")
				dir := t.TempDir()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				refused := make(chan struct{}, 2)
				var arrived sync.WaitGroup
				arrived.Add(2)
				client := &expiringClient{fakeClientAdapter: fakeClientAdapter{data: encoded}, expired: "test://old", refused: refused}
				client.hold = func(url, _ string) {
					if url == "test://old" {
						arrived.Done()
						arrived.Wait()
					}
				}
				entered := make(chan struct{})
				enter := sync.OnceFunc(func() { close(entered) })
				r, err := openRefreshing(ctx, api, client, NewCacheManager(dir, 0), old, fresh, 2, 2, func(ctx context.Context) error {
					enter()
					<-ctx.Done()
					return ctx.Err()
				})
				if err != nil {
					t.Fatal(err)
				}
				p := readerPrefetcher(t, r)
				// Both ranges were refused; one worker is inside the refresh, the other waits for it.
				<-refused
				<-refused
				within(t, "first refresh", entered)
				if stop == "close" {
					r.Close()
				} else {
					cancel()
				}
				if !awaitWorkers(t, p) {
					t.FailNow()
				}
				_, err = io.ReadAll(r)
				switch {
				case stop == "close" && !errors.Is(err, fs.ErrClosed):
					t.Fatalf("read after Close: %v, want fs.ErrClosed", err)
				case stop == "cancel" && (!errors.Is(err, context.Canceled) || !forbidden(err)):
					t.Fatalf("read after cancel: %v, want the 403 and context.Canceled", err)
				}
				r.Close()
				for _, req := range client.requests() {
					if strings.HasPrefix(req, "test://new") {
						t.Fatalf("fetched %q after the reader stopped", req)
					}
				}

				r, err = NewReaderV1(t.Context(), &fakeClientAdapter{data: encoded}, fresh.v1, WithCacheManager(NewCacheManager(dir, 0)))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if want := bytes.Join(chunks, nil); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("download after the stop: %v, %d bytes, want %d", err, len(got), len(want))
				}
			})
		}
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
	p := readerPrefetcher(t, r)
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
	if !awaitWorkers(t, p) {
		t.FailNow()
	}
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

// TestReaderReportsCanceledFetchAfterWorkerExit cancels a stalled fetch and
// lets the worker discard its entry before the reader touches it: the read
// must still report the cancellation, not the closed cache file.
func TestReaderReportsCanceledFetchAfterWorkerExit(t *testing.T) {
	chunks, encoded, _, readers := splitRangeReaders(t)
	for name, newReader := range readers {
		t.Run(name, func(t *testing.T) {
			m := NewCacheManager(t.TempDir(), 0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &gatedClient{
				data:    encoded,
				stallAt: len(buildTestXorb(t, chunks[:1])),
				stalled: make(chan struct{}),
				closed:  make(chan struct{}),
			}
			r, err := newReader(ctx, client, m)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			p := readerPrefetcher(t, r)
			// The first chunk is published; the worker stalls on the second.
			<-client.stalled
			cancel()
			if !awaitWorkers(t, p) {
				t.FailNow()
			}
			if _, err := io.ReadAll(r); !errors.Is(err, context.Canceled) {
				t.Fatalf("read after cancel: %v, want context.Canceled", err)
			}
		})
	}
}

// TestReaderCloseAbortsStalledDownload closes a reader while its worker holds
// cache.mut inside a network read that never completes and the parent context
// stays live: Close must cancel the fetch instead of waiting for it, leave no
// queued fetch running, and keep the caller's context and cache usable.
func TestReaderCloseAbortsStalledDownload(t *testing.T) {
	chunks, encoded, split, readers := splitRangeReaders(t)
	for name, newReader := range readers {
		for _, ignoreCtx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignoreCtx=%v", name, ignoreCtx), func(t *testing.T) {
				dir := t.TempDir()
				m := NewCacheManager(dir, 0)
				ctx := t.Context()
				client := &gatedClient{
					data:      encoded,
					stallAt:   len(buildTestXorb(t, chunks[:1])),
					ignoreCtx: ignoreCtx,
					release:   make(chan struct{}),
					stalled:   make(chan struct{}),
					closed:    make(chan struct{}),
				}
				r, err := newReader(ctx, client, m)
				if err != nil {
					t.Fatal(err)
				}
				p := readerPrefetcher(t, r)
				// The first chunk is published; LoadAll now blocks on the second with cache.mut held.
				<-client.stalled

				closed := make(chan struct{})
				go func() {
					defer close(closed)
					r.Close()
				}()
				if !within(t, "Close during a stalled download", closed) || !awaitWorkers(t, p) {
					close(client.release) // let the stalled worker finish so the failed run can exit
					awaitWorkers(t, p)
					t.FailNow()
				}
				if ctx.Err() != nil {
					t.Fatal("Close canceled the caller's context")
				}
				within(t, "response body Close", client.closed)
				if got := client.calls.Load(); got != 1 {
					t.Fatalf("queued range was requested after Close: %d requests, want 1", got)
				}
				path := cacheFilePath(dir, testCacheHash, 0, 2, 0, split-1)
				assertEntryUnlocked(t, path)
				if info, err := os.Stat(path); err != nil || info.Size() != 0 {
					t.Fatalf("aborted entry was not discarded: %v, %v", info, err)
				}

				r, err = newReader(ctx, &fakeClientAdapter{data: encoded}, m)
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if want := bytes.Join(chunks, nil); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("download after Close: %v, %d bytes, want %d", err, len(got), len(want))
				}
			})
		}
	}
}

// TestReaderCloseKeepsCompletedRange closes a reader whose worker has decoded
// every chunk of its range but is still blocked in the read that would return
// EOF: Close must still stop the fetch promptly, and the completed entry must
// survive sealed for reuse instead of being discarded as unfinished.
func TestReaderCloseKeepsCompletedRange(t *testing.T) {
	chunks, encoded, split, readers := splitRangeReaders(t)
	for name, newReader := range readers {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m := NewCacheManager(dir, 0)
			client := &gatedClient{
				data:    encoded,
				stallAt: int(split),
				release: make(chan struct{}),
				stalled: make(chan struct{}),
				closed:  make(chan struct{}),
			}
			r, err := newReader(context.Background(), client, m)
			if err != nil {
				t.Fatal(err)
			}
			p := readerPrefetcher(t, r)
			// The decoder consumed the whole first range; LoadAll now blocks on its trailing read with cache.mut held.
			<-client.stalled

			closed := make(chan struct{})
			go func() {
				defer close(closed)
				r.Close()
			}()
			if !within(t, "Close during the trailing read", closed) || !awaitWorkers(t, p) {
				close(client.release)
				awaitWorkers(t, p)
				t.FailNow()
			}
			within(t, "response body Close", client.closed)
			if got := client.calls.Load(); got != 1 {
				t.Fatalf("queued range was requested after Close: %d requests, want 1", got)
			}
			path := cacheFilePath(dir, testCacheHash, 0, 2, 0, split-1)
			assertEntryUnlocked(t, path)

			// A fresh manager verifies the checksum on its first open.
			cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 2)
			if err != nil {
				t.Fatal(err)
			}
			if cached == nil {
				t.Fatal("completed range was discarded on Close")
			}
			defer cached.Done()
			buf := make([]byte, 2000)
			for i, want := range chunks[:2] {
				n, err := cached.Chunk(uint32(i), buf)
				if err != nil || !bytes.Equal(buf[:n], want) {
					t.Fatalf("chunk %d: %d bytes, %v; want %d bytes", i, n, err, len(want))
				}
			}
			m.mu.Lock()
			v, ok := m.lru.Get(path)
			m.mu.Unlock()
			if !ok || v.(*cacheEntry).refs != 0 {
				t.Fatalf("sealed entry tracked = %v, refs after Close = %v; want tracked with 0 refs", ok, v)
			}
		})
	}
}

// TestReaderCloseAbandonsLockWait holds the first entry's flock for the whole
// test, as another process would: after Close the worker must stop waiting
// for it and never request the range.
func TestReaderCloseAbandonsLockWait(t *testing.T) {
	_, encoded, split, readers := splitRangeReaders(t)
	for name, newReader := range readers {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			m := NewCacheManager(dir, 0)
			path := cacheFilePath(dir, testCacheHash, 0, 2, 0, split-1)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			holder, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if err := flock.TryLock(holder); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				flock.Unlock(holder) //nolint:errcheck
				holder.Close()
			})

			released := make(chan struct{})
			close(released)
			client := &gatedClient{data: encoded, release: released, stalled: make(chan struct{}), closed: make(chan struct{})}
			r, err := newReader(context.Background(), client, m)
			if err != nil {
				t.Fatal(err)
			}
			p := readerPrefetcher(t, r)
			r.Close()
			if !awaitWorkers(t, p) {
				t.FailNow() // Cleanup releases the lock so the waiting worker can exit
			}
			if got := client.calls.Load(); got != 0 {
				t.Fatalf("locked range was requested after Close: %d requests", got)
			}
		})
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
	p := readerPrefetcher(t, r)

	<-client.stalled
	r.Close()
	close(client.release)
	<-client.closed
	if !awaitWorkers(t, p) {
		t.FailNow()
	}
	assertEntryUnlocked(t, path)
	m.mu.Lock()
	v, ok := m.lru.Get(path)
	m.mu.Unlock()
	if ok && v.(*cacheEntry).refs != 0 {
		t.Fatalf("late result pinned %d refs after Close", v.(*cacheEntry).refs)
	}
}
