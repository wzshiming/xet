package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (io.ReadCloser, error)

	DownloadXorbsMultipart(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error)
}

type options struct {
	concurrency int
	retries     int
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func WithConcurrency(concurrency int) func(*options) {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithRetries configures how many times xorb range prefetch should retry when
// stream reads fail with transient truncation errors (for example unexpected EOF).
func WithRetries(retries int) func(*options) {
	return func(o *options) {
		if retries < 0 {
			retries = 0
		}
		o.retries = retries
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
	dec   *xorb.Decoder
	store *xorbChunkStore
	index []chunkRef
	done  bool // decoder exhausted or closed
	mut   sync.Mutex
}

type xorbChunkStore struct {
	file        *os.File
	writeOffset int64
	refCount    int
	mut         sync.RWMutex
}

func newXorbChunkStore() (*xorbChunkStore, error) {
	f, err := os.CreateTemp("", "xorb-chunk-*")
	if err != nil {
		return nil, err
	}
	return &xorbChunkStore{file: f}, nil
}

func (s *xorbChunkStore) acquire() error {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.file == nil {
		return os.ErrClosed
	}
	s.refCount++
	return nil
}

func (s *xorbChunkStore) append(data []byte) (int64, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.file == nil {
		return 0, os.ErrClosed
	}
	offset := s.writeOffset
	n, err := s.file.Write(data)
	if err != nil {
		return 0, err
	}
	if n != len(data) {
		return 0, io.ErrShortWrite
	}
	s.writeOffset += int64(n)
	return offset, nil
}

func (s *xorbChunkStore) readAt(buf []byte, offset int64) error {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if s.file == nil {
		return os.ErrClosed
	}
	_, err := s.file.ReadAt(buf, offset)
	return err
}

func (s *xorbChunkStore) release() {
	s.mut.Lock()
	defer s.mut.Unlock()
	if s.file == nil {
		return
	}
	if s.refCount > 0 {
		s.refCount--
	}
	if s.refCount == 0 {
		name := s.file.Name()
		s.file.Close()
		os.Remove(name)
		s.file = nil
	}
}

func newXorbChunkCache(dec *xorb.Decoder, store *xorbChunkStore) (*xorbChunkCache, error) {
	if err := store.acquire(); err != nil {
		return nil, err
	}
	return &xorbChunkCache{dec: dec, store: store}, nil
}

// Chunk returns the decoded chunk at idx, decoding forward as needed.
// Already-decoded chunks are served from the backing temp file.
func (c *xorbChunkCache) Chunk(idx uint32) ([]byte, error) {
	c.mut.Lock()
	defer c.mut.Unlock()
	if int(idx) < len(c.index) {
		ref := c.index[idx]
		data := make([]byte, ref.length)
		if err := c.store.readAt(data, ref.offset); err != nil {
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
	if err := c.store.readAt(data, ref.offset); err != nil {
		return nil, err
	}
	return data, nil
}

func (c *xorbChunkCache) load() error {
	if c.done {
		return io.EOF
	}

	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)

	n, err := c.dec.Read(tmp[:])
	if err != nil {
		if err == io.EOF {
			c.Done()
			return io.EOF
		}
		return err
	}
	offset, err := c.store.append(tmp[:n])
	if err != nil {
		return err
	}
	c.index = append(c.index, chunkRef{offset: offset, length: int32(n)})
	return nil
}

// LoadAll decodes and caches all chunks.
func (c *xorbChunkCache) LoadAll() error {
	for {
		c.mut.Lock()
		err := c.load()
		c.mut.Unlock()
		if err != nil {
			if err == io.EOF {
				return nil
			}
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
	if c.store != nil {
		c.store.release()
		c.store = nil
	}
}

type xorbPrefetcher struct {
	ctx     context.Context
	client  ClientAdapter
	entries map[fetchKey]*xorbPrefetchEntry
	retries int

	store    *xorbChunkStore
	storeMut sync.Mutex
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

type xorbPrefetchJob struct {
	entries []*xorbPrefetchEntry
}

func newXorbPrefetcher(ctx context.Context, client ClientAdapter, termFetches []selectedFetch, tasks []xorbFetchTask, concurrency int, retries int) *xorbPrefetcher {
	entries := make(map[fetchKey]*xorbPrefetchEntry, len(tasks))
	items := make([]*xorbPrefetchEntry, 0, len(entries))
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
		item := &xorbPrefetchEntry{
			task:  task,
			ready: make(chan struct{}),
		}
		entries[task.key] = item
		items = append(items, item)
	}
	if len(items) == 0 {
		return &xorbPrefetcher{ctx: ctx, client: client, entries: entries, retries: retries}
	}

	orderEntry(items, termOrder)

	p := &xorbPrefetcher{
		ctx:     ctx,
		client:  client,
		entries: entries,
		retries: retries,
	}

	desiredWorkers := concurrency
	if desiredWorkers <= 0 {
		desiredWorkers = 1
	}

	jobs := make(chan *xorbPrefetchJob)
	for i := 0; i < desiredWorkers; i++ {
		go func() {
			for job := range jobs {
				p.runJob(job)
			}
		}()
	}
	go func() {
		defer close(jobs)
		p.buildJobs(items, func(job *xorbPrefetchJob) {
			jobs <- job
		})
	}()

	return p
}

func (p *xorbPrefetcher) buildJobs(entries []*xorbPrefetchEntry, fn func(*xorbPrefetchJob)) {
	if len(entries) == 0 {
		return
	}

	job := &xorbPrefetchJob{entries: []*xorbPrefetchEntry{entries[0]}}
	for _, entry := range entries[1:] {
		if len(job.entries) < 8 &&
			job.entries[len(job.entries)-1].task.url == entry.task.url {
			job.entries = append(job.entries, entry)
			continue
		}
		fn(job)
		job = &xorbPrefetchJob{entries: []*xorbPrefetchEntry{entry}}
	}
	fn(job)
}

func orderEntry(entries []*xorbPrefetchEntry, termOrder map[fetchKey]int) {
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

func (p *xorbPrefetcher) runJob(job *xorbPrefetchJob) {
	if len(job.entries) == 0 {
		return
	}
	if len(job.entries) > 1 {
		// Try multipart download first, with retries
		maxAttempts := max(p.retries+1, 1)
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			ranges := make([]string, len(job.entries))
			for i, entry := range job.entries {
				ranges[i] = fmt.Sprintf("%d-%d", entry.task.key.Start, entry.task.key.End)
			}
			header := http.Header{
				"Range": {
					fmt.Sprintf("bytes=%s", strings.Join(ranges, ",")),
				},
			}
			mr, closer, err := p.client.DownloadXorbsMultipart(p.ctx, job.entries[0].task.url, header)
			if err != nil {
				if attempt < maxAttempts && shouldRetryXorbLoadError(err) {
					continue
				}
				// Fall back to single-range retrieval
				p.runSingleRangeEntries(job.entries)
				return
			}

			err = p.runStreamJob(job.entries, mr, closer)
			if err == nil {
				return
			}

			// Stream failed; fall back to single-range if this was the last attempt
			if attempt == maxAttempts || !shouldRetryXorbLoadError(err) {
				p.failEntries(job.entries, err)
				return
			}
		}
		return
	}

	p.runSingleRangeEntries(job.entries)
}

func (p *xorbPrefetcher) runSingleRangeEntries(entries []*xorbPrefetchEntry) {
	for _, entry := range entries {
		var cache *xorbChunkCache
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
			cache, err = p.newChunkCache(dec)
			if err != nil {
				dec.Close()
				if attempt < maxAttempts {
					continue
				}
				break
			}

			err = cache.LoadAll()
			if err == nil {
				break
			}

			cache.Close()
			cache = nil
			if !shouldRetryXorbLoadError(err) || attempt == maxAttempts {
				break
			}
		}

		entry.cache = cache
		entry.err = err
		close(entry.ready)
	}
}

func (p *xorbPrefetcher) runStreamJob(entries []*xorbPrefetchEntry, mr *multipart.Reader, closer io.Closer) error {
	type streamTarget struct {
		entry *xorbPrefetchEntry
		cache *xorbChunkCache
		pipeW *io.PipeWriter
	}

	targets := make([]streamTarget, len(entries))
	for i, entry := range entries {
		pipeR, pipeW := io.Pipe()
		dec := xorb.NewDecoder(pipeR, false)
		cache, err := p.newChunkCache(dec)
		if err != nil {
			for j := range i {
				targets[j].pipeW.CloseWithError(err)
				targets[j].cache.Close()
			}
			pipeW.CloseWithError(err)
			pipeR.CloseWithError(err)
			closer.Close()
			return err
		}
		targets[i] = streamTarget{entry: entry, cache: cache, pipeW: pipeW}
	}

	go func() {
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
				return
			}

			_, copyErr := io.Copy(targets[i].pipeW, part)
			part.Close()
			if copyErr != nil {
				targets[i].pipeW.CloseWithError(copyErr)
				for j := i + 1; j < len(targets); j++ {
					targets[j].pipeW.CloseWithError(copyErr)
				}
				return
			}
			targets[i].pipeW.Close()
		}
	}()

	for _, target := range targets {
		if err := target.cache.LoadAll(); err != nil {
			for _, t := range targets {
				t.cache.Close()
			}
			return err
		}
	}

	for _, target := range targets {
		target.entry.cache = target.cache
		target.entry.err = nil
		close(target.entry.ready)
	}

	return nil
}

func (p *xorbPrefetcher) newChunkCache(dec *xorb.Decoder) (*xorbChunkCache, error) {
	p.storeMut.Lock()
	defer p.storeMut.Unlock()

	if p.store == nil {
		store, err := newXorbChunkStore()
		if err != nil {
			return nil, err
		}
		p.store = store
	}

	return newXorbChunkCache(dec, p.store)
}

func shouldRetryXorbLoadError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)
}

func (p *xorbPrefetcher) failEntries(entries []*xorbPrefetchEntry, err error) {
	for _, entry := range entries {
		entry.err = err
		close(entry.ready)
	}
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
