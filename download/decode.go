package download

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/wzshiming/xet/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Decoder, error)
}

type options struct {
	concurrency int
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func WithConcurrency(concurrency int) func(*options) {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

func (o *options) concurrencyValue() int {
	if o == nil || o.concurrency <= 0 {
		return 1
	}
	return o.concurrency
}

type xorbFetchTask struct {
	key fetchKey
	url string
}

type selectedFetch struct {
	key        fetchKey
	chunkStart uint32
	chunkEnd   uint32
}

// chunkRef records the position of one decoded chunk in the backing temp file.
type chunkRef struct {
	offset int64
	length int32
}

// xorbChunkCache wraps a Decoder with lazy per-chunk decoding backed by a temp file.
// Decoded chunks are written to disk so that dedup'd xorb chunks (the same chunk
// position referenced by multiple reconstruction terms) can be re-read without a
// second download, while keeping memory usage low.
type xorbChunkCache struct {
	dec         *xorb.Decoder
	file        *os.File
	index       []chunkRef
	writeOffset int64
	done        bool // decoder exhausted or closed
	mut         sync.Mutex
}

func newXorbChunkCache(dec *xorb.Decoder) (*xorbChunkCache, error) {
	f, err := os.CreateTemp("", "xorb-chunk-*")
	if err != nil {
		return nil, err
	}
	return &xorbChunkCache{dec: dec, file: f}, nil
}

// Chunk returns the decoded chunk at idx, decoding forward as needed.
// Already-decoded chunks are served from the backing temp file.
func (c *xorbChunkCache) Chunk(idx uint32) ([]byte, error) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if int(idx) < len(c.index) {
		ref := c.index[idx]
		data := make([]byte, ref.length)
		if _, err := c.file.ReadAt(data, ref.offset); err != nil {
			return nil, err
		}
		return data, nil
	}
	for uint32(len(c.index)) <= idx {
		err := c.load()
		if err != nil {
			return nil, err
		}
	}
	ref := c.index[idx]
	data := make([]byte, ref.length)
	if _, err := c.file.ReadAt(data, ref.offset); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *xorbChunkCache) load() error {
	if c.done {
		return io.EOF
	}
	data, err := c.dec.Decode()
	if err == io.EOF {
		c.Done()
		return io.EOF
	}
	if err != nil {
		return err
	}
	if _, err := c.file.Write(data); err != nil {
		return err
	}
	c.index = append(c.index, chunkRef{offset: c.writeOffset, length: int32(len(data))})
	c.writeOffset += int64(len(data))
	return nil
}

// LoadAll decodes and caches all chunks.
func (c *xorbChunkCache) LoadAll() error {
	for {
		c.mut.Lock()
		err := c.load()
		c.mut.Unlock()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (c *xorbChunkCache) Done() {
	if !c.done {
		c.done = true
		c.dec.Close()
	}
}

// Close closes the underlying decoder and the backing temp file.
func (c *xorbChunkCache) Close() {
	c.Done()
	if c.file != nil {
		c.file.Close()
		os.Remove(c.file.Name())
		c.file = nil
	}
}

type xorbPrefetcher struct {
	ctx     context.Context
	client  ClientAdapter
	entries map[fetchKey]*xorbPrefetchEntry
}

type xorbPrefetchEntry struct {
	task  xorbFetchTask
	cache *xorbChunkCache
	err   error
	ready chan struct{}
}

type fetchKey struct {
	Hash  string
	Start int64
	End   int64
}

func newXorbPrefetcher(ctx context.Context, client ClientAdapter, termFetches []selectedFetch, tasks []xorbFetchTask, concurrency int) *xorbPrefetcher {
	entries := make(map[fetchKey]*xorbPrefetchEntry, len(tasks))
	for _, task := range tasks {
		if _, ok := entries[task.key]; ok {
			continue
		}
		entries[task.key] = &xorbPrefetchEntry{
			task:  task,
			ready: make(chan struct{}),
		}
	}
	p := &xorbPrefetcher{
		ctx:     ctx,
		client:  client,
		entries: entries,
	}

	order := make([]*xorbPrefetchEntry, 0, len(entries))
	seen := make(map[fetchKey]struct{}, len(entries))
	for _, sel := range termFetches {
		if _, ok := seen[sel.key]; ok {
			continue
		}
		entry, ok := p.entries[sel.key]
		if !ok {
			continue
		}
		seen[sel.key] = struct{}{}
		order = append(order, entry)
	}
	for _, task := range tasks {
		if _, ok := seen[task.key]; ok {
			continue
		}
		entry, ok := p.entries[task.key]
		if !ok {
			continue
		}
		seen[task.key] = struct{}{}
		order = append(order, entry)
	}

	workers := concurrency
	if workers <= 0 {
		workers = 1
	}
	if workers > len(order) {
		workers = len(order)
	}
	if workers > 0 {
		jobs := make(chan *xorbPrefetchEntry)
		for i := 0; i < workers; i++ {
			go func() {
				for entry := range jobs {
					p.runDownload(entry)
				}
			}()
		}
		go func() {
			defer close(jobs)
			for _, entry := range order {
				jobs <- entry
			}
		}()
	}
	return p
}

func (p *xorbPrefetcher) runDownload(entry *xorbPrefetchEntry) {
	header := http.Header{
		"Range": {fmt.Sprintf("bytes=%d-%d", entry.task.key.Start, entry.task.key.End)},
	}
	dec, err := p.client.DownloadXorb(p.ctx, entry.task.url, header)
	if err != nil {
		entry.err = err
		close(entry.ready)
		return
	}
	cache, err := newXorbChunkCache(dec)
	if err != nil {
		dec.Close()
		entry.err = err
		close(entry.ready)
		return
	}
	entry.cache = cache
	close(entry.ready)
	entry.err = cache.LoadAll()
}

func (p *xorbPrefetcher) Get(key fetchKey) (*xorbChunkCache, error) {
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
