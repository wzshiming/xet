package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"sort"
	"sync"

	"github.com/wzshiming/xet/internal/flock"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/xorb"
)

type fetchKey struct {
	Hash  string
	Start int64
	End   int64
}

func (k fetchKey) String() string {
	return fmt.Sprintf("%s:%d-%d", k.Hash, k.Start, k.End)
}

func (k fetchKey) Size() int64 {
	return k.End - k.Start
}

type fetchTask struct {
	key        fetchKey
	url        string
	chunkStart uint32
	chunkEnd   uint32
}

type selectedFetch struct {
	key        fetchKey
	chunkStart uint32
	chunkEnd   uint32
	term       ChunkRange // the chunks the term reads, within [chunkStart, chunkEnd)
}

// checkFetchRange rejects server-supplied ranges before any cache file is sized from them.
func checkFetchRange(term *Term, chunks ChunkRange, span ByteRange) error {
	if term.Range.Start >= term.Range.End || !validChunkRange(chunks.Start, chunks.End) || span.Start < 0 || span.End < span.Start {
		return fmt.Errorf("invalid range for term chunks [%d, %d) of xorb %s", term.Range.Start, term.Range.End, term.Hash)
	}
	return nil
}

// chunkSource serves the decoded chunks of one fetched xorb range, indexed
// from the start of the range: a *chunkCache, or a *memoryRange when there is
// no on-disk cache.
type chunkSource interface {
	readChunk(idx uint32, buf []byte) (chunkRef, error)
	Done()
}

type prefetchEntry struct {
	task  fetchTask
	cache chunkSource
	err   error
	ready chan struct{}
	once  sync.Once

	lastTerm  int     // index of the last term reading the range
	reads     []int32 // per chunk of the range, how many terms read it; set without a cache
	requested bool    // the reader asked for the range; touched by the reader only
}

type prefetcher struct {
	ctx          context.Context
	cancel       context.CancelFunc // nil when no fetch was started
	client       ClientAdapter
	entries      map[fetchKey]*prefetchEntry
	progressFunc progress.ProgressFunc
	cache        *CacheManager  // nil serves decoded ranges from memory ranges instead
	budget       *memoryBudget  // shared by the memory ranges; nil without a limit
	workers      sync.WaitGroup // feeder and worker goroutines started by start

	// Without a cache the feeder dispatches each range for one window token.
	// The reader's first request grants lookahead+1 tokens and each request
	// for a further range one more, so at most lookahead ranges are fetched
	// ahead of the one being read and nothing before the first Read.
	window    chan struct{}
	lookahead int
	started   bool // the reader requested a range; touched by the reader only
}

type progressReader struct {
	r            io.Reader
	name         string
	current      int64
	total        int64
	progressFunc progress.ProgressFunc
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 && r.progressFunc != nil {
		r.current += int64(n)
		r.progressFunc(r.name, r.current, r.total)
	}
	return n, err
}

func newPrefetcher(ctx context.Context, client ClientAdapter, termFetches []selectedFetch, tasks []fetchTask, cache *CacheManager, opts *options) (*prefetcher, error) {
	entries := make(map[fetchKey]*prefetchEntry, len(tasks))
	items := make([]*prefetchEntry, 0, len(entries))
	termOrder := make(map[fetchKey]int, len(termFetches))
	for i, selected := range termFetches {
		if _, ok := termOrder[selected.key]; ok {
			continue
		}
		termOrder[selected.key] = i
	}
	for _, task := range tasks {
		if _, ok := entries[task.key]; ok {
			continue
		}
		item := &prefetchEntry{
			task:  task,
			ready: make(chan struct{}),
		}
		entries[task.key] = item
		items = append(items, item)
	}
	for i, selected := range termFetches {
		entry := entries[selected.key]
		entry.lastTerm = i
		if cache != nil {
			continue
		}
		if entry.reads == nil {
			entry.reads = make([]int32, selected.chunkEnd-selected.chunkStart)
		}
		for c := selected.term.Start; c < selected.term.End; c++ {
			entry.reads[c-selected.chunkStart]++
		}
	}

	if len(items) == 0 {
		return &prefetcher{
			ctx:     ctx,
			client:  client,
			entries: entries,
		}, nil
	}

	orderEntry(items, termOrder)

	// Fetches run on a context Close cancels, so callers sharing ctx are unaffected.
	ctx, cancel := context.WithCancel(ctx)
	p := &prefetcher{
		ctx:          ctx,
		cancel:       cancel,
		client:       client,
		entries:      entries,
		progressFunc: opts.progressFunc,
		cache:        cache,
	}
	if cache == nil {
		p.budget = newMemoryBudget(opts.memoryLimit)
	}

	if err := p.start(items, opts.concurrency); err != nil {
		p.Close()
		return nil, err
	}

	return p, nil
}

func (p *prefetcher) start(items []*prefetchEntry, desiredWorkers int) error {
	newItems := make([]*prefetchEntry, 0, len(items))
	for _, item := range items {
		size := item.task.key.Size()
		if p.cache != nil {
			cc, err := openCachedRange(p.cache, item.task.key.Hash, item.task.chunkStart, item.task.chunkEnd)
			if err != nil {
				return fmt.Errorf("check cache for %s: %w", item.task.key.String(), err)
			}
			if cc != nil {
				p.reportProgress(item.task.key, size, size)
				p.publishEntry(item, cc)
				continue
			}
		}
		p.reportProgress(item.task.key, 0, size)
		newItems = append(newItems, item)
	}
	if len(newItems) == 0 {
		return nil
	}

	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}
	if p.cache == nil {
		p.lookahead = desiredWorkers
		p.window = make(chan struct{}, len(newItems)+desiredWorkers)
	}
	desiredWorkers = min(desiredWorkers, len(newItems))

	jobs := make(chan *prefetchEntry)
	p.workers.Go(func() {
		defer close(jobs)
		for _, item := range newItems {
			if p.window != nil {
				// Once canceled, workers fail the remaining ranges without a token.
				select {
				case <-p.window:
				case <-p.ctx.Done():
				}
			}
			jobs <- item
		}
	})

	for i := 0; i < desiredWorkers; i++ {
		p.workers.Go(func() {
			for entry := range jobs {
				p.runJob(entry)
			}
		})
	}

	return nil
}

func orderEntry(entries []*prefetchEntry, termOrder map[fetchKey]int) {
	sort.Slice(entries, func(i, j int) bool {
		a := entries[i].task.key
		b := entries[j].task.key
		orderA, okA := termOrder[a]
		orderB, okB := termOrder[b]
		if okA && okB && orderA != orderB {
			return orderA < orderB
		}
		if okA != okB {
			return okA
		}
		if a.Start != b.Start {
			return a.Start < b.Start
		}
		if a.End != b.End {
			return a.End < b.End
		}
		if entries[i].task.url != entries[j].task.url {
			return entries[i].task.url < entries[j].task.url
		}
		return a.Hash < b.Hash
	})
}

func (p *prefetcher) runJob(entry *prefetchEntry) {
	var cache *chunkCache
	var err error
	key := entry.task.key

	if err = p.ctx.Err(); err != nil {
		p.failEntry(entry, err)
		return
	}
	if p.cache == nil {
		p.fetchToMemory(entry)
		return
	}
	lockFile, err := lockChunkCache(p.ctx, p.cache.dir, key.Hash, entry.task.chunkStart, entry.task.chunkEnd, key.Start, key.End)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	releaseLock := true
	defer func() {
		if releaseLock {
			flock.Unlock(lockFile) //nolint:errcheck
			lockFile.Close()
		}
	}()

	// Another process may have populated the cache while this worker waited.
	cache, err = openCachedRange(p.cache, key.Hash, entry.task.chunkStart, entry.task.chunkEnd)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	if cache != nil {
		p.publishEntry(entry, cache)
		return
	}

	dec, closeBody, err := p.openRange(entry)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	defer closeBody()

	cache, err = newLockedChunkCache(dec, p.cache, key.Hash, entry.task.chunkStart, entry.task.chunkEnd, key.Start, key.End, lockFile)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	releaseLock = false // cache.Done owns the lock from this point.

	err = cache.LoadTo(0)
	if err != nil {
		cache.Done()
		p.failEntry(entry, err)
		return
	}
	p.publishEntry(entry, cache)
	if err := cache.LoadAll(); err != nil {
		// The published result stays; consumers see the failure through the closed cache.
		cache.Done()
	}
}

// fetchToMemory decodes entry's range into memory, publishing it before the
// first chunk so the reader consumes chunks while they arrive.
func (p *prefetcher) fetchToMemory(entry *prefetchEntry) {
	dec, closeBody, err := p.openRange(entry)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	defer closeBody()

	r := newMemoryRange(entry.reads, p.budget)
	p.publishEntry(entry, r)
	r.fill(dec)
}

// openRange requests entry's byte range and returns its chunk decoder.
// closeBody may be called repeatedly; canceling p.ctx calls it too, since
// bodies not bound to ctx only stop a stalled Read when closed.
func (p *prefetcher) openRange(entry *prefetchEntry) (dec *xorb.Decoder, closeBody func(), err error) {
	key := entry.task.key
	header := http.Header{
		"Range": {fmt.Sprintf("bytes=%d-%d", key.Start, key.End)},
	}

	rc, err := p.client.DownloadXorbWithURL(p.ctx, entry.task.url, header)
	if err != nil {
		return nil, nil, err
	}
	closeOnce := sync.OnceValue(rc.Close)
	stop := context.AfterFunc(p.ctx, func() { closeOnce() })

	var reader io.Reader = rc
	if p.progressFunc != nil {
		reader = &progressReader{
			r:            rc,
			name:         key.String(),
			total:        key.Size(),
			progressFunc: p.progressFunc,
		}
	}
	return xorb.NewDecoder(reader, false), func() { stop(); closeOnce() }, nil
}

func (p *prefetcher) publishEntry(entry *prefetchEntry, cache chunkSource) {
	p.completeEntry(entry, cache, nil)
}

func (p *prefetcher) failEntry(entry *prefetchEntry, err error) {
	p.completeEntry(entry, nil, err)
}

func (p *prefetcher) completeEntry(entry *prefetchEntry, cache chunkSource, err error) {
	done := false
	entry.once.Do(func() {
		entry.cache = cache
		entry.err = err
		done = true
		close(entry.ready)
	})
	if !done && cache != nil {
		// Lost to an earlier completion or Close; nobody else will release it.
		cache.Done()
	}
}

func (p *prefetcher) reportProgress(key fetchKey, current, total int64) {
	if p.progressFunc != nil {
		p.progressFunc(key.String(), current, total)
	}
}

// Cancel network reads before waiting for cache cleanup.
func (p *prefetcher) Close() {
	if p.cancel != nil {
		p.cancel()
	}
	for _, entry := range p.entries {
		// once.Do orders any concurrent publication before the read below.
		p.failEntry(entry, fs.ErrClosed)
		if entry.cache != nil {
			entry.cache.Done()
		}
	}
}

// Get returns the source for key, blocking until its fetch task publishes it:
// a cache after its first chunk, a memory range once the response arrives.
// The prefetcher retains ownership of the returned source.
func (p *prefetcher) Get(key fetchKey) (chunkSource, error) {
	entry, ok := p.entries[key]
	if !ok {
		return nil, fmt.Errorf("xorb fetch task %q not found", key)
	}

	if p.window != nil && !entry.requested {
		entry.requested = true
		grant := 1
		if !p.started {
			p.started = true
			grant += p.lookahead
		}
		for range grant {
			p.window <- struct{}{}
		}
	}

	<-entry.ready
	if entry.err != nil {
		return nil, entry.err
	}
	return entry.cache, nil
}

// finishTerm tells p the reader is done with term termIdx, which read key.
// Without a cache the range's memory is released once no later term reads it.
func (p *prefetcher) finishTerm(key fetchKey, termIdx int) {
	if p.cache != nil {
		return
	}
	if entry := p.entries[key]; entry.lastTerm == termIdx {
		entry.cache.Done()
	}
}
