package download

import (
	"context"
	"fmt"
	"io"
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
}

type prefetchEntry struct {
	task  fetchTask
	cache *chunkCache
	err   error
	ready chan struct{}
	once  sync.Once
}

type prefetcher struct {
	ctx          context.Context
	client       ClientAdapter
	entries      map[fetchKey]*prefetchEntry
	progressFunc progress.ProgressFunc
	cache        *CacheManager
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

	if len(items) == 0 {
		return &prefetcher{
			ctx:     ctx,
			client:  client,
			entries: entries,
		}, nil
	}

	orderEntry(items, termOrder)

	p := &prefetcher{
		ctx:          ctx,
		client:       client,
		entries:      entries,
		progressFunc: opts.progressFunc,
		cache:        cache,
	}

	if err := p.start(items, opts.concurrency); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *prefetcher) start(items []*prefetchEntry, desiredWorkers int) error {
	newItems := make([]*prefetchEntry, 0, len(items))
	for _, item := range items {
		cc, err := openCachedRange(p.cache, item.task.key.Hash, item.task.chunkStart, item.task.chunkEnd)
		if err != nil {
			return fmt.Errorf("check cache for %s: %w", item.task.key.String(), err)
		}
		size := item.task.key.Size()
		if cc != nil {
			p.reportProgress(item.task.key, size, size)
			p.publishEntry(item, cc)
		} else {
			p.reportProgress(item.task.key, 0, size)
			newItems = append(newItems, item)
		}
	}
	if len(newItems) == 0 {
		return nil
	}

	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}
	desiredWorkers = min(desiredWorkers, len(newItems))

	jobs := make(chan *prefetchEntry)
	go func() {
		defer close(jobs)
		for _, item := range newItems {
			jobs <- item
		}
	}()

	for i := 0; i < desiredWorkers; i++ {
		go func() {
			for entry := range jobs {
				p.runJob(entry)
			}
		}()
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

	lockFile, err := lockChunkCache(p.cache.dir, key.Hash, entry.task.chunkStart, entry.task.chunkEnd, key.Start, key.End)
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

	header := http.Header{
		"Range": {fmt.Sprintf("bytes=%d-%d", key.Start, key.End)},
	}

	rc, err := p.client.DownloadXorbWithURL(p.ctx, entry.task.url, header)
	if err != nil {
		p.failEntry(entry, err)
		return
	}
	defer rc.Close()

	var reader io.Reader = rc
	if p.progressFunc != nil {
		reader = &progressReader{
			r:            rc,
			name:         entry.task.key.String(),
			total:        entry.task.key.Size(),
			progressFunc: p.progressFunc,
		}
	}

	dec := xorb.NewDecoder(reader, false)
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
	err = cache.LoadAll()
	if err != nil {
		cache.Done()
		p.failEntry(entry, err)
		return
	}
}

func (p *prefetcher) publishEntry(entry *prefetchEntry, cache *chunkCache) {
	p.completeEntry(entry, cache, nil)
}

func (p *prefetcher) failEntry(entry *prefetchEntry, err error) {
	p.completeEntry(entry, nil, err)
}

func (p *prefetcher) completeEntry(entry *prefetchEntry, cache *chunkCache, err error) {
	entry.cache = cache
	entry.err = err
	entry.once.Do(func() {
		close(entry.ready)
	})
}

func (p *prefetcher) reportProgress(key fetchKey, current, total int64) {
	if p.progressFunc != nil {
		p.progressFunc(key.String(), current, total)
	}
}

// Close releases all caches owned by the prefetcher.
func (p *prefetcher) Close() {
	for _, entry := range p.entries {
		if entry.cache != nil {
			entry.cache.Done()
		}
	}
}

// Get returns the cache for key, blocking until the fetch task has published its
// first chunk. The prefetcher retains ownership of the returned cache.
func (p *prefetcher) Get(key fetchKey) (*chunkCache, error) {
	entry, ok := p.entries[key]
	if !ok {
		return nil, fmt.Errorf("xorb fetch task %q not found", key)
	}

	<-entry.ready
	if entry.err != nil {
		return nil, entry.err
	}
	return entry.cache, nil
}
