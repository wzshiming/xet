package download

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"slices"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/progress"
	"github.com/wzshiming/xet/xorb"
)

// termReader reconstructs a file by delivering its terms in order, as
// xet-core's file reconstructor does. Its first Read or WriteTo starts a
// dispatcher that walks the terms in file order: the first term of each fetch
// waits until the fetch concurrency places earlier has delivered its last
// term, every term then waits for its unpacked length in the memory budget,
// and a fetch starts with its first term. A fetch fills one buffer with the
// decoded chunks its terms read, from the cache when an entry covers the range
// and from the network otherwise, and hands a downloaded range to the cache
// writer. Read and WriteTo deliver each term from its fetch's buffer and
// return its bytes to the budget once it is delivered.
type termReader struct {
	terms   []Term
	fetches []*rangeFetch // fetches[i] serves terms[i]; dropped once it is delivered
	skip    int64         // OffsetIntoFirstRange

	ctx         context.Context
	cancel      context.CancelFunc
	client      ClientAdapter
	cache       *CacheManager
	writer      *cacheWriter
	budget      *MemoryBudget
	concurrency int
	started     bool // the first Read or WriteTo started the dispatcher
	detach      func()
	progress    progress.ProgressFunc
	verify      bool
	admitted    chan struct{}  // one token per term the dispatcher admitted, in order
	workers     sync.WaitGroup // the dispatcher and the fetches

	failed   chan struct{} // closed on the first failure
	failErr  error         // set before failed is closed
	failOnce sync.Once

	mu       sync.Mutex
	held     int64 // budget bytes of the admitted terms not delivered yet
	returned bool  // the budget got every byte back; later admissions return theirs at once

	// Read and WriteTo state.
	next     int    // index of the next term to deliver
	cur      []byte // undelivered rest of term next-1
	verifier *fileVerifier
	eof      bool
	err      error
}

// rangeFetch is one fetch of an xorb range and the decoded chunks its terms read.
type rangeFetch struct {
	key        fetchKey
	url        string
	chunkStart uint32
	chunkEnd   uint32
	xorbHash   xet.XorbHash
	cacheable  bool   // looked up in and written to the cache, so decoded whole
	needed     []bool // local chunks some term reads
	size       int    // decoded length of the needed chunks, from their terms
	last       int    // index of the last term the fetch serves
	started    bool   // the dispatcher started the fetch

	delivered chan struct{} // closed once its last term is delivered

	done    chan struct{}   // closed once the fields below are set
	data    []byte          // decoded chunks in order
	offsets []int64         // local chunk i is data[offsets[i]:offsets[i+1]], empty when skipped
	hashes  []xet.ChunkHash // hashes of the needed chunks, when verifying
}

const defaultConcurrency = 4

type fetchKey struct {
	Hash  string
	Start int64
	End   int64
}

func (k fetchKey) String() string {
	return fmt.Sprintf("%s:%d-%d", k.Hash, k.Start, k.End)
}

// Size is the length of the inclusive byte range [Start, End].
func (k fetchKey) Size() int64 {
	return k.End - k.Start + 1
}

type fetchTask struct {
	key        fetchKey
	url        string
	chunkStart uint32
	chunkEnd   uint32
}

// checkFetchRange rejects server-supplied ranges before any cache file is sized from them.
func checkFetchRange(term *Term, chunks ChunkRange, span ByteRange) error {
	if term.Range.Start >= term.Range.End || !validChunkRange(chunks.Start, chunks.End) || span.Start < 0 || span.End < span.Start {
		return fmt.Errorf("invalid range for term chunks [%d, %d) of xorb %s", term.Range.Start, term.Range.End, term.Hash)
	}
	return nil
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

// newTermReader plans the reconstruction of terms; tasks[i] is the fetch of
// terms[i], and offset bytes of the first term are not delivered.
func newTermReader(ctx context.Context, client ClientAdapter, terms []Term, offset int64, tasks []fetchTask, opts ...Option) (*termReader, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	verifier, err := newFileVerifier(o, offset)
	if err != nil {
		return nil, err
	}
	concurrency := o.concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	fetches, err := planFetches(terms, tasks, o.cache != nil, concurrency)
	if err != nil {
		return nil, err
	}
	budget := o.budget
	if budget == nil {
		budget = NewMemoryBudget(DefaultMemorySizes())
	}
	// Fetches run on a context Close cancels, so callers sharing ctx are unaffected.
	ctx, cancel := context.WithCancel(ctx)
	r := &termReader{
		terms:       terms,
		fetches:     fetches,
		skip:        offset,
		ctx:         ctx,
		cancel:      cancel,
		client:      client,
		cache:       o.cache,
		writer:      o.cache.newWriter(0),
		budget:      budget,
		concurrency: concurrency,
		detach:      func() {},
		progress:    o.progressFunc,
		verify:      verifier != nil,
		admitted:    make(chan struct{}, len(terms)),
		failed:      make(chan struct{}),
		verifier:    verifier,
	}
	if r.progress != nil {
		reported := make(map[fetchKey]bool, len(fetches))
		for _, f := range fetches {
			if !reported[f.key] {
				reported[f.key] = true
				r.progress(f.key.String(), 0, f.key.Size())
			}
		}
	}
	return r, nil
}

// start readies the cache, attaches the download to the budget and starts the
// dispatcher, once.
func (r *termReader) start() {
	if r.started {
		return
	}
	r.started = true
	if r.cache != nil {
		// Adopt pre-existing entries and clean up orphaned partial files before
		// the download starts; only the manager's first prepare walks the cache
		// directory.
		r.cache.prepare()
	}
	r.detach = r.budget.Attach()
	order := slices.Clone(r.fetches)
	r.workers.Go(func() { r.dispatch(order, r.concurrency) })
}

// planFetches returns the fetch serving each term. A term shares the latest
// fetch of its range while fewer than concurrency fetches follow that one in
// fetch order, so every term of a fetch comes before the first term of the
// fetch concurrency places later; a range read again after that is fetched
// again.
func planFetches(terms []Term, tasks []fetchTask, cached bool, concurrency int) ([]*rangeFetch, error) {
	type planned struct {
		fetch *rangeFetch
		seq   int                 // place in fetch order
		sized map[ChunkRange]bool // term ranges counted in fetch.size
	}
	latest := make(map[fetchKey]*planned)
	fetches := make([]*rangeFetch, len(terms))
	plannedFetches := 0
	for i, task := range tasks {
		p := latest[task.key]
		if p != nil && (p.fetch.chunkStart != task.chunkStart || p.fetch.chunkEnd != task.chunkEnd) {
			return nil, fmt.Errorf("xorb range %s holds chunks [%d, %d) and [%d, %d)", task.key, p.fetch.chunkStart, p.fetch.chunkEnd, task.chunkStart, task.chunkEnd)
		}
		if p == nil || p.seq < plannedFetches-concurrency {
			f := &rangeFetch{
				key:        task.key,
				url:        task.url,
				chunkStart: task.chunkStart,
				chunkEnd:   task.chunkEnd,
				needed:     make([]bool, task.chunkEnd-task.chunkStart),
				delivered:  make(chan struct{}),
				done:       make(chan struct{}),
			}
			if cached {
				hash, err := xet.ParseXorbHash(task.key.Hash)
				f.xorbHash, f.cacheable = hash, err == nil
			}
			p = &planned{fetch: f, seq: plannedFetches, sized: map[ChunkRange]bool{}}
			latest[task.key] = p
			plannedFetches++
		}
		f := p.fetch
		rg := terms[i].Range
		for c := rg.Start; c < rg.End; c++ {
			f.needed[c-f.chunkStart] = true
		}
		// Overlapping terms make this an upper bound; a range holds at most an xorb.
		if !p.sized[rg] {
			p.sized[rg] = true
			f.size = int(min(uint64(f.size)+terms[i].UnpackedLength, xet.MaxXorbSize))
		}
		f.last = i
		fetches[i] = f
	}
	return fetches, nil
}

// termBytes is the budget a term holds from its admission to its delivery.
func termBytes(term Term) int64 {
	return int64(min(term.UnpackedLength, math.MaxInt64))
}

// dispatch admits the terms in file order and starts each fetch with its
// first term, once the fetch concurrency places earlier has delivered its
// last term.
func (r *termReader) dispatch(order []*rangeFetch, concurrency int) {
	var window []chan struct{} // delivered of the last concurrency fetches started, in order
	for i, f := range order {
		order[i] = nil
		if err := r.ctx.Err(); err != nil {
			r.fail(err)
			return
		}
		if !f.started && len(window) == concurrency {
			select {
			case <-window[0]:
			case <-r.ctx.Done():
				r.fail(r.ctx.Err())
				return
			}
			window = window[1:]
		}
		n := termBytes(r.terms[i])
		if err := r.budget.Acquire(r.ctx, n); err != nil {
			r.fail(err)
			return
		}
		r.admit(n)
		if !f.started {
			f.started = true
			window = append(window, f.delivered)
			r.workers.Go(func() { r.fetch(f) })
		}
	}
}

// admit records the n acquired bytes of the next term and lets it be delivered.
func (r *termReader) admit(n int64) {
	r.mu.Lock()
	if r.returned {
		r.budget.Release(n)
	} else {
		r.held += n
	}
	r.mu.Unlock()
	r.admitted <- struct{}{}
}

// release returns the n bytes of a delivered term to the budget.
func (r *termReader) release(n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.returned {
		return
	}
	r.held -= n
	r.budget.Release(n)
}

// returnBudget gives the budget back every byte the reader holds.
func (r *termReader) returnBudget() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.budget.Release(r.held)
	r.held = 0
	r.returned = true
}

// fail records the first failure and stops every fetch.
func (r *termReader) fail(err error) {
	r.failOnce.Do(func() {
		r.failErr = err
		close(r.failed)
		r.cancel()
	})
}

// fetch fills f from the cache or the network.
func (r *termReader) fetch(f *rangeFetch) {
	defer close(f.done)
	if err := r.fill(f); err != nil {
		r.fail(fmt.Errorf("download xorb: %w", err))
	}
}

func (r *termReader) fill(f *rangeFetch) error {
	if f.cacheable {
		c, err := r.cache.lookup(r.ctx, f.xorbHash, int(f.chunkStart), int(f.chunkEnd))
		if err != nil {
			return err
		}
		if c != nil {
			defer c.release()
			return r.readCached(f, c)
		}
	}
	return r.download(f)
}

// readCached fills f with its chunks from a cache entry covering them.
func (r *termReader) readCached(f *rangeFetch, c *cachedChunks) error {
	from, to := int(f.chunkStart), int(f.chunkEnd)
	data := make([]byte, c.size(from, to))
	if err := c.read(data, from, to); err != nil {
		return err
	}
	offsets := make([]int64, to-from+1)
	for i := range to - from {
		offsets[i+1] = c.size(from, from+i+1)
	}
	f.data, f.offsets = data, offsets
	if r.verify {
		f.hashes = make([]xet.ChunkHash, to-from)
		for i, needed := range f.needed {
			if needed {
				f.hashes[i] = computeChunkHash(data[offsets[i]:offsets[i+1]])
			}
		}
	}
	if r.progress != nil {
		r.progress(f.key.String(), f.key.Size(), f.key.Size())
	}
	return nil
}

// download fetches f's range, decodes it into f and hands it to the cache writer.
func (r *termReader) download(f *rangeFetch) error {
	header := http.Header{
		"Range": {fmt.Sprintf("bytes=%d-%d", f.key.Start, f.key.End)},
	}
	rc, err := r.client.DownloadXorbWithURL(r.ctx, f.url, header)
	if err != nil {
		return err
	}
	closeBody := sync.OnceValue(rc.Close)
	defer closeBody()
	// Bodies not bound to ctx only stop a stalled Read when closed.
	stop := context.AfterFunc(r.ctx, func() { closeBody() })
	defer stop()

	var body io.Reader = rc
	if r.progress != nil {
		body = &progressReader{
			r:            rc,
			name:         f.key.String(),
			total:        f.key.Size(),
			progressFunc: r.progress,
		}
	}
	if err := r.decode(f, body); err != nil {
		return err
	}
	if f.cacheable {
		key := cacheKey{Hash: f.xorbHash, ChunkStart: int(f.chunkStart), ChunkEnd: int(f.chunkEnd), ByteStart: f.key.Start, ByteEnd: f.key.End}
		// The writer keeps data alive until it is written; nothing waits for that.
		r.writer.put(key, f.offsets, f.data, func() {})
	}
	// The range ends with its last chunk; reading on to EOF also lets the connection be reused.
	var trailing [1]byte
	if _, err := io.ReadFull(body, trailing[:]); err != io.EOF {
		if err == nil {
			err = fmt.Errorf("xorb range %s holds more than chunks [%d, %d)", f.key, f.chunkStart, f.chunkEnd)
		}
		return err
	}
	return nil
}

// decode reads the chunks of f's range from body and decodes those it keeps,
// in order, straight into one buffer; the others are skipped undecoded. The
// needed chunks are hashed when verifying.
func (r *termReader) decode(f *rangeFetch, body io.Reader) error {
	n := int(f.chunkEnd - f.chunkStart)
	offsets := make([]int64, n+1)
	// DecodeChunk may scribble on the byte after a chunk; the next chunk overwrites it.
	data := make([]byte, 0, f.size+1)
	var hashes []xet.ChunkHash
	if r.verify {
		hashes = make([]xet.ChunkHash, n)
	}
	scratch := pool.GetChunkBuf()
	defer pool.PutChunkBuf(scratch)
	var header [xorb.ChunkHeaderSize]byte
	var raw uint64
	for i := range n {
		idx := f.chunkStart + uint32(i)
		if _, err := io.ReadFull(body, header[:]); err != nil {
			return fmt.Errorf("read chunk %d header: %w", idx, err)
		}
		h, err := xorb.ParseChunkHeader(header[:])
		if err != nil {
			return fmt.Errorf("chunk %d: %w", idx, err)
		}
		if int(h.CompressedSize) > len(scratch) {
			return fmt.Errorf("chunk %d: compressed size %d exceeds %d", idx, h.CompressedSize, len(scratch))
		}
		if raw += uint64(h.UncompressedSize); raw > xet.MaxXorbSize {
			return fmt.Errorf("chunk %d: raw payload size exceeds maximum %d", idx, xet.MaxXorbSize)
		}
		if !f.cacheable && !f.needed[i] {
			if _, err := io.ReadFull(body, scratch[:h.CompressedSize]); err != nil {
				return fmt.Errorf("read chunk %d: %w", idx, err)
			}
			offsets[i+1] = int64(len(data))
			continue
		}
		size := int(h.UncompressedSize)
		data = slices.Grow(data, size+1)
		chunk := data[len(data) : len(data)+size]
		if h.Compression == xorb.CompressionNone {
			if h.CompressedSize != h.UncompressedSize {
				return fmt.Errorf("chunk %d: uncompressed chunk of %d bytes stored in %d", idx, h.UncompressedSize, h.CompressedSize)
			}
			if _, err := io.ReadFull(body, chunk); err != nil {
				return fmt.Errorf("read chunk %d: %w", idx, err)
			}
		} else {
			compressed := scratch[:h.CompressedSize]
			if _, err := io.ReadFull(body, compressed); err != nil {
				return fmt.Errorf("read chunk %d: %w", idx, err)
			}
			if _, err := xorb.DecodeChunk(chunk, h, compressed); err != nil {
				return fmt.Errorf("decode chunk %d: %w", idx, err)
			}
		}
		data = data[:len(data)+size]
		offsets[i+1] = int64(len(data))
		if hashes != nil && f.needed[i] {
			hashes[i] = computeChunkHash(chunk)
		}
	}
	f.data, f.offsets, f.hashes = data, offsets, hashes
	return nil
}

// Read implements io.Reader.
func (r *termReader) Read(p []byte) (int, error) {
	if err := r.check(); err != nil {
		return 0, err
	}
	r.start()
	n := 0
	for n < len(p) {
		if len(r.cur) == 0 {
			if err := r.advance(); err != nil {
				if n > 0 && err != io.EOF {
					// The error sticks; the next call returns it.
					return n, nil
				}
				return n, err
			}
		}
		copied := copy(p[n:], r.cur)
		r.consume(copied)
		n += copied
	}
	return n, nil
}

// WriteTo implements io.WriterTo, writing each term, or the rest of the
// current one, with one Write.
func (r *termReader) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for {
		if err := r.check(); err != nil {
			if err == io.EOF {
				return n, nil
			}
			return n, err
		}
		r.start()
		if len(r.cur) == 0 {
			if err := r.advance(); err != nil {
				if err == io.EOF {
					return n, nil
				}
				return n, err
			}
		}
		written, err := writeTerm(w, r.cur)
		r.consume(written)
		n += int64(written)
		if err != nil {
			return n, err
		}
	}
}

// check returns the sticky error, io.EOF after the last term, or the first
// fetch failure, which sticks from then on.
func (r *termReader) check() error {
	if r.err != nil {
		return r.err
	}
	if r.eof {
		return io.EOF
	}
	select {
	case <-r.failed:
		return r.stick(r.failErr)
	default:
		return nil
	}
}

// stick makes err the reader's error and stops it.
func (r *termReader) stick(err error) error {
	r.err = err
	r.cur = nil
	r.fetches = nil
	r.cancel()
	r.returnBudget()
	r.detach()
	return err
}

// consume marks n bytes of the current term delivered, and delivers the term
// once all of it is.
func (r *termReader) consume(n int) {
	r.cur = r.cur[n:]
	if len(r.cur) == 0 {
		r.cur = nil
		r.deliver(r.next - 1)
	}
}

// deliver returns the budget of term i, which is delivered, and closes its
// fetch's delivered after the fetch's last term.
func (r *termReader) deliver(i int) {
	r.release(termBytes(r.terms[i]))
	f := r.fetches[i]
	r.fetches[i] = nil
	if f.last == i {
		close(f.delivered)
	}
}

// advance makes the next nonempty term current, waiting for its admission
// and its fetch, and verifies the file after the last one.
func (r *termReader) advance() error {
	for r.next < len(r.terms) {
		select {
		case <-r.admitted:
		case <-r.failed:
			return r.stick(r.failErr)
		}
		f := r.fetches[r.next]
		select {
		case <-f.done:
		case <-r.failed:
		}
		select {
		case <-r.failed:
			return r.stick(r.failErr)
		default:
		}
		term := r.terms[r.next]
		lo, hi := term.Range.Start-f.chunkStart, term.Range.End-f.chunkStart
		data := f.data[f.offsets[lo]:f.offsets[hi]]
		if r.verifier != nil {
			r.verifier.addChunks(f.hashes[lo:hi], f.offsets[lo:hi+1])
		}
		if r.next == 0 {
			data = data[min(r.skip, int64(len(data))):]
		}
		r.next++
		if len(data) > 0 {
			r.cur = data
			return nil
		}
		r.deliver(r.next - 1)
	}
	if r.verifier != nil {
		if err := r.verifier.verify(); err != nil {
			return r.stick(err)
		}
	}
	r.eof = true
	r.cancel()
	r.detach()
	// A reader read to its end need not be closed, so its cache writes end here.
	r.writer.wait()
	return io.EOF
}

// Close stops the fetches, returns the budget, and waits for the fetches and
// the cache writes. It is safe to call multiple times and after Read has
// returned io.EOF.
func (r *termReader) Close() error {
	r.cancel()
	r.workers.Wait()
	r.returnBudget()
	r.detach()
	r.writer.wait()
	if r.err == nil {
		r.err = fs.ErrClosed
	}
	r.cur = nil
	r.fetches = nil
	return nil
}
