package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

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
	key fetchKey
	url string
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
	retries      int
	progressFunc progress.ProgressFunc

	store *chunkStore
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

func newPrefetcher(ctx context.Context, client ClientAdapter, termFetches []selectedFetch, tasks []fetchTask, opts *options) (*prefetcher, error) {
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
	concurrency := opts.concurrency
	retries := opts.retries

	if len(items) == 0 {
		return &prefetcher{
			ctx:     ctx,
			client:  client,
			entries: entries,
			retries: retries,
		}, nil
	}

	orderEntry(items, termOrder)

	store, err := newChunkStore()
	if err != nil {
		return nil, err
	}

	p := &prefetcher{
		ctx:          ctx,
		client:       client,
		entries:      entries,
		retries:      retries,
		store:        store,
		progressFunc: opts.progressFunc,
	}

	if opts.progressFunc != nil {
		for _, item := range items {
			opts.progressFunc(item.task.key.String(), 0, item.task.key.End-item.task.key.Start)
		}
	}

	desiredWorkers := concurrency
	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}

	jobs := make(chan *prefetchEntry)
	for i := 0; i < desiredWorkers; i++ {
		go func() {
			for entry := range jobs {
				p.runJob(entry)
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, item := range items {
			jobs <- item
		}
	}()

	return p, nil
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
	maxAttempts := max(p.retries+1, 1)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if p.ctx.Err() != nil {
			err = p.ctx.Err()
			break
		}

		header := http.Header{
			"Range": {fmt.Sprintf("bytes=%d-%d", entry.task.key.Start, entry.task.key.End)},
		}

		rc, err := p.client.DownloadXorb(p.ctx, entry.task.url, header)
		if err != nil {
			if attempt < maxAttempts {
				continue
			}
			break
		}

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
		cache, err = newChunkCache(dec, p.store)
		if err != nil {
			rc.Close()
			if attempt < maxAttempts {
				continue
			}
			break
		}

		err = cache.LoadTo(0)
		if err != nil {
			cache.Done()
			rc.Close()
			if attempt < maxAttempts {
				continue
			}
			break
		}
		p.publishEntry(entry, cache)
		err = cache.LoadAll()
		if err != nil {
			rc.Close()
			if attempt < maxAttempts {
				continue
			}
			break
		}

		rc.Close()
		break
	}

	if err != nil {
		p.failEntry(entry, err)
	}
}

func (p *prefetcher) publishEntry(entry *prefetchEntry, cache *chunkCache) {
	entry.cache = cache
	entry.err = nil
	entry.once.Do(func() {
		close(entry.ready)
	})
}

func (p *prefetcher) failEntry(entry *prefetchEntry, err error) {
	entry.err = err
	entry.once.Do(func() {
		close(entry.ready)
	})
}

// Close releases all resources held by the prefetcher: it closes every cached
// entry and drops the prefetcher's own reference to the shared chunkStore so
// that the backing temp file is deleted once all consumers are done with it.
func (p *prefetcher) Close() {
	for _, entry := range p.entries {
		if entry.cache != nil {
			entry.cache.Done()
		}
	}

	if p.store != nil {
		p.store.Close()
	}
}

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
