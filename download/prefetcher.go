package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/xorb"
)

type fetchKey struct {
	Hash  string
	Start int64
	End   int64
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
	ctx     context.Context
	client  ClientAdapter
	entries map[fetchKey]*prefetchEntry
	retries int

	store *chunkStore

	// Progress tracking: total is computed from unique fetch-key byte ranges
	// before any download starts; done is incremented only when an entry
	// successfully completes, so retries never inflate the reported value.
	progressFunc  progress.ProgressFunc
	progressName  string
	progressTotal int64
	progressDone  atomic.Int64
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

	// Compute the total expected transfer bytes from all unique fetch-key byte ranges.
	// This is done before any network activity so the total is always known upfront.
	var progressTotal int64
	if opts.progressFunc != nil {
		seen := make(map[fetchKey]struct{}, len(tasks))
		for _, task := range tasks {
			if _, ok := seen[task.key]; ok {
				continue
			}
			seen[task.key] = struct{}{}
			if task.key.End >= task.key.Start {
				progressTotal += task.key.End - task.key.Start + 1
			}
		}
	}

	if len(items) == 0 {
		return &prefetcher{
			ctx:           ctx,
			client:        client,
			entries:       entries,
			retries:       retries,
			progressFunc:  opts.progressFunc,
			progressName:  opts.progressName,
			progressTotal: progressTotal,
		}, nil
	}

	orderEntry(items, termOrder)

	store, err := newChunkStore()
	if err != nil {
		return nil, err
	}

	p := &prefetcher{
		ctx:           ctx,
		client:        client,
		entries:       entries,
		retries:       retries,
		store:         store,
		progressFunc:  opts.progressFunc,
		progressName:  opts.progressName,
		progressTotal: progressTotal,
	}

	desiredWorkers := concurrency
	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}

	jobs := make(chan []*prefetchEntry)
	for i := 0; i < desiredWorkers; i++ {
		go func() {
			for entries := range jobs {
				p.runJob(entries)
			}
		}()
	}
	go func() {
		defer close(jobs)
		remaining := len(items)
		groupByURL(items, func(entries []*prefetchEntry) {
			for len(entries) > 0 {
				size := max(remaining/concurrency, 1)
				n := min(size, len(entries))
				jobs <- entries[:n]
				entries = entries[n:]
				remaining -= n
			}
		})
	}()

	return p, nil
}

func groupByURL(entries []*prefetchEntry, fn func([]*prefetchEntry)) {
	if len(entries) == 0 {
		return
	}
	start := 0
	for i := 1; i < len(entries); i++ {
		if entries[i].task.url != entries[start].task.url {
			fn(entries[start:i])
			start = i
		}
	}
	fn(entries[start:])
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

func (p *prefetcher) runJob(entries []*prefetchEntry) {
	if len(entries) == 0 {
		return
	}
	if len(entries) > 1 {
		// Try multipart download first, with retries
		maxAttempts := max(p.retries+1, 1)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			ranges := make([]string, len(entries))
			for i, entry := range entries {
				ranges[i] = fmt.Sprintf("%d-%d", entry.task.key.Start, entry.task.key.End)
			}
			header := http.Header{
				"Range": {
					fmt.Sprintf("bytes=%s", strings.Join(ranges, ",")),
				},
			}
			mr, closer, err := p.client.DownloadXorbsMultipart(p.ctx, entries[0].task.url, header)
			if err != nil {
				if attempt < maxAttempts && shouldRetryXorbLoadError(err) {
					continue
				}
				// Fall back to single-range retrieval
				p.runSingleRangeEntries(entries)
				return
			}

			err = p.runStreamJob(entries, mr, closer)
			if err == nil {
				return
			}

			// Stream failed; fall back to single-range if this was the last attempt
			if attempt == maxAttempts || !shouldRetryXorbLoadError(err) {
				p.failEntries(entries, err)
				return
			}
		}
		return
	}

	p.runSingleRangeEntries(entries)
}

func (p *prefetcher) runSingleRangeEntries(entries []*prefetchEntry) {
	for _, entry := range entries {
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
			var rc io.ReadCloser
			rc, err = p.client.DownloadXorb(p.ctx, entry.task.url, header)
			if err != nil {
				if attempt < maxAttempts {
					continue
				}
				break
			}

			dec := xorb.NewDecoder(rc, false)
			cache, err = newChunkCache(dec, p.store)
			if err != nil {
				dec.Close()
				if attempt < maxAttempts {
					continue
				}
				break
			}
			break
		}

		if err != nil {
			p.failEntry(entry, err)
			continue
		}

		p.publishEntry(entry, cache)
		p.warmEntry(entry, cache)
	}
}

func (p *prefetcher) runStreamJob(entries []*prefetchEntry, mr *multipart.Reader, closer io.Closer) error {
	type streamTarget struct {
		entry *prefetchEntry
		cache *chunkCache
		pipeW *io.PipeWriter
	}

	targets := make([]streamTarget, len(entries))
	for i, entry := range entries {
		pipeR, pipeW := io.Pipe()
		dec := xorb.NewDecoder(pipeR, false)
		cache, err := newChunkCache(dec, p.store)
		if err != nil {
			for j := range i {
				targets[j].pipeW.CloseWithError(err)
				targets[j].cache.Done()
			}
			pipeW.CloseWithError(err)
			pipeR.CloseWithError(err)
			closer.Close()
			return err
		}
		targets[i] = streamTarget{entry: entry, cache: cache, pipeW: pipeW}

		p.publishEntry(entry, cache)
	}

	defer closer.Close()
	for i := range targets {
		part, err := mr.NextPart()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			for j := i; j < len(targets); j++ {
				targets[j].pipeW.CloseWithError(err)
			}
			return err
		}

		_, copyErr := io.Copy(targets[i].pipeW, part)
		part.Close()
		if copyErr != nil {
			targets[i].pipeW.CloseWithError(copyErr)
			for j := i + 1; j < len(targets); j++ {
				targets[j].pipeW.CloseWithError(copyErr)
			}
			return copyErr
		}
		targets[i].pipeW.Close()

		p.warmEntry(targets[i].entry, targets[i].cache)
	}

	return nil
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

func (p *prefetcher) warmEntry(entry *prefetchEntry, cache *chunkCache) {
	if err := cache.LoadAll(); err == nil {
		p.markEntryDone(entry.task.key)
	}
}

// markEntryDone adds the byte span of key to the completed-bytes counter and
// fires the progress callback. It is called exactly once per entry, only on
// success, so retries never inflate the reported current value.
func (p *prefetcher) markEntryDone(key fetchKey) {
	if p.progressFunc == nil {
		return
	}
	if key.End >= key.Start {
		done := p.progressDone.Add(key.End - key.Start + 1)
		p.progressFunc(p.progressName, done, p.progressTotal)
	}
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

func shouldRetryXorbLoadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

func (p *prefetcher) failEntries(entries []*prefetchEntry, err error) {
	for _, entry := range entries {
		p.failEntry(entry, err)
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
