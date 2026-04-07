package download

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (io.ReadCloser, error)

	DownloadXorbsMultipart(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error)
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
	buf         [xet.MaxChunkSize]byte
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
	n, err := c.dec.Read(c.buf[:])
	if err == io.EOF {
		c.Done()
		return io.EOF
	}
	if err != nil {
		return err
	}
	if _, err := c.file.Write(c.buf[:n]); err != nil {
		return err
	}
	c.index = append(c.index, chunkRef{offset: c.writeOffset, length: int32(n)})
	c.writeOffset += int64(n)
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

type xorbPrefetchJob struct {
	entries []*xorbPrefetchEntry
}

func newXorbPrefetcher(ctx context.Context, client ClientAdapter, termFetches []selectedFetch, tasks []xorbFetchTask, concurrency int) *xorbPrefetcher {
	entries := make(map[fetchKey]*xorbPrefetchEntry, len(tasks))
	items := make([]*xorbPrefetchEntry, 0, len(entries))
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
	p := &xorbPrefetcher{
		ctx:     ctx,
		client:  client,
		entries: entries,
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
	orderEntry(entries)

	job := &xorbPrefetchJob{entries: []*xorbPrefetchEntry{entries[0]}}
	for _, entry := range entries[1:] {
		if job.entries[len(job.entries)-1].task.url == entry.task.url {
			job.entries = append(job.entries, entry)
			continue
		}
		fn(job)
		job = &xorbPrefetchJob{entries: []*xorbPrefetchEntry{entry}}
	}
	fn(job)
}

func orderEntry(entries []*xorbPrefetchEntry) {
	sort.Slice(entries, func(i, j int) bool {
		a := entries[i].task.key
		b := entries[j].task.key
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
			p.failEntries(job.entries, err)
			return
		}

		if err := p.runStreamJob(job.entries, mr, closer); err != nil {
			p.failEntries(job.entries, err)
			return
		}
		return
	}

	for _, entry := range job.entries {
		header := http.Header{
			"Range": {fmt.Sprintf("bytes=%d-%d", entry.task.key.Start, entry.task.key.End)},
		}
		rc, err := p.client.DownloadXorb(p.ctx, entry.task.url, header)
		if err != nil {
			entry.err = err
			close(entry.ready)
			continue
		}

		dec := xorb.NewDecoder(rc, false)
		cache, err := newXorbChunkCache(dec)
		if err != nil {
			dec.Close()
			entry.err = err
			close(entry.ready)
			continue
		}
		entry.cache = cache
		close(entry.ready)
		go func(entry *xorbPrefetchEntry, cache *xorbChunkCache) {
			entry.err = cache.LoadAll()
		}(entry, cache)
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
		cache, err := newXorbChunkCache(dec)
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

		entry.cache = cache
		close(entry.ready)
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

	go func() {
		for _, entry := range entries {
			entry.err = entry.cache.LoadAll()
		}
	}()

	return nil
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
