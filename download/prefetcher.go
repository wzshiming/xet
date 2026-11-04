package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"slices"
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

// chunkSource serves the decoded chunks of one xorb range, indexed from the
// start of the range: a *fetchedRange for a range being fetched, or a
// *chunkCache for one found in the cache.
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

	terms     []int // indices of the terms reading the range, ascending
	requested bool  // the reader asked for the range; touched by the reader only
}

type prefetcher struct {
	ctx          context.Context
	cancel       context.CancelFunc // nil when no fetch was started
	client       ClientAdapter
	entries      map[fetchKey]*prefetchEntry
	progressFunc progress.ProgressFunc
	cache        *CacheManager   // nil keeps fetched ranges out of any cache
	store        *chunkStore     // holds the chunks of the fetched ranges
	terms        []selectedFetch // the reader's terms, in reading order
	termBase     []int64         // per term, the chunks the terms before it read
	workers      sync.WaitGroup  // feeder and worker goroutines started by start

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
		entry.terms = append(entry.terms, i)
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
		store:        newChunkStore(opts.memoryLimit),
		terms:        termFetches,
		termBase:     make([]int64, len(termFetches)),
	}
	var read int64
	for i, selected := range termFetches {
		p.termBase[i] = read
		read += int64(selected.term.End - selected.term.Start)
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
	key := entry.task.key
	if err := p.ctx.Err(); err != nil {
		p.failEntry(entry, err)
		return
	}
	if p.cache == nil {
		p.fetch(entry, nil)
		return
	}
	lockFile, err := lockChunkCache(p.ctx, p.cache.dir, key.Hash, entry.task.chunkStart, entry.task.chunkEnd, key.Start, key.End)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	// Another process may have populated the cache while this worker waited.
	cache, err := openCachedRange(p.cache, key.Hash, entry.task.chunkStart, entry.task.chunkEnd)
	if err != nil || cache != nil {
		flock.Unlock(lockFile) //nolint:errcheck
		lockFile.Close()
		if err != nil {
			p.failEntry(entry, err)
		} else {
			p.publishEntry(entry, cache)
		}
		return
	}
	p.fetch(entry, lockFile)
}

// fetch decodes entry's range into a fetched range, published before the
// first chunk so the reader consumes chunks while they arrive. Given the
// locked file of the range's cache entry, fetch owns it and writes the entry.
func (p *prefetcher) fetch(entry *prefetchEntry, lockFile *os.File) {
	key := entry.task.key
	unlock := func() {
		if lockFile != nil {
			flock.Unlock(lockFile) //nolint:errcheck
			lockFile.Close()
		}
	}
	dec, closeBody, err := p.openRange(entry)
	if err != nil {
		unlock()
		p.failEntry(entry, err)
		return
	}
	defer closeBody()

	var cache *chunkCache
	if lockFile != nil {
		cache, err = newLockedChunkCache(nil, p.cache, key.Hash, entry.task.chunkStart, entry.task.chunkEnd, key.Start, key.End, lockFile)
		if err != nil {
			unlock()
			p.failEntry(entry, err)
			return
		}
	}
	r := newFetchedRange(p.store, p.chunkUses(entry), cache)
	p.publishEntry(entry, r)
	r.fill(dec)
}

// chunkUses lists for each chunk of entry's range the positions of its reads
// in reading order, where a position counts the chunk reads before it.
func (p *prefetcher) chunkUses(entry *prefetchEntry) [][]int64 {
	n := int(entry.task.chunkEnd - entry.task.chunkStart)
	each := func(visit func(i int, pos int64)) {
		for _, t := range entry.terms {
			selected := p.terms[t]
			for c := selected.term.Start; c < selected.term.End; c++ {
				if i := int(c) - int(selected.chunkStart); i >= 0 && i < n {
					visit(i, p.termBase[t]+int64(c-selected.term.Start))
				}
			}
		}
	}
	// Lay the positions out chunk by chunk in one slice.
	ends := make([]int, n+1)
	each(func(i int, _ int64) { ends[i+1]++ })
	for i := range n {
		ends[i+1] += ends[i]
	}
	all := make([]int64, ends[n])
	next := slices.Clone(ends[:n])
	each(func(i int, pos int64) {
		all[next[i]] = pos
		next[i]++
	})
	uses := make([][]int64, n)
	for i := range uses {
		uses[i] = all[ends[i]:ends[i+1]:ends[i+1]]
	}
	return uses
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
// a fetched range once the response arrives, or a cached range.
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

// finishTerm tells p the reader is done with term termIdx, which read key; a
// range is released once no later term reads it.
func (p *prefetcher) finishTerm(key fetchKey, termIdx int) {
	entry := p.entries[key]
	if terms := entry.terms; terms[len(terms)-1] == termIdx {
		entry.cache.Done()
	}
}
