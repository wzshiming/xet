package download

import (
	"bytes"
	"cmp"
	"container/heap"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/xorb"
)

// chunkStore holds the decoded chunks of one reader's fetched ranges, each
// until the last term reading it has read it. Chunks stay in memory up to
// limit; past it, chunks leave memory in the order of leavesBefore: first
// those a file also holds, the range's cache entry or the output WriteTo
// wrote them to, then those read again furthest ahead, which move to their
// range's spill file.
type chunkStore struct {
	mu       sync.Mutex // guards the store and its ranges
	limit    int64      // bytes of chunk data held in memory; <= 0 holds all
	used     int64
	writing  int64     // bytes of used being written to spill files
	heap     chunkHeap // chunks in memory, except those being spilled
	spillErr error     // why spill files take no more chunks
}

func newChunkStore(limit int64) *chunkStore {
	return &chunkStore{limit: limit}
}

// spillFile holds the chunks of one fetched range that left memory with no
// other copy. Each range has its own, so the workers of different ranges
// spill in parallel. Chunks are packed end to end, as writes into a file
// with holes are slower, and the space of dropped chunks is reused.
type spillFile struct {
	f    *os.File
	name string   // set while the file still has a name
	end  int64    // the space past end is free
	free []extent // free space before end, by offset, merged where adjacent
}

type extent struct{ off, n int64 }

// alloc reserves n bytes, at the first free extent that takes them.
func (sf *spillFile) alloc(n int64) int64 {
	for i, e := range sf.free {
		if e.n < n {
			continue
		}
		if e.n == n {
			sf.free = slices.Delete(sf.free, i, i+1)
		} else {
			sf.free[i] = extent{e.off + n, e.n - n}
		}
		return e.off
	}
	sf.end += n
	return sf.end - n
}

// release frees the n bytes at off.
func (sf *spillFile) release(off, n int64) {
	i, _ := slices.BinarySearchFunc(sf.free, off, func(e extent, off int64) int { return cmp.Compare(e.off, off) })
	sf.free = slices.Insert(sf.free, i, extent{off, n})
	if i+1 < len(sf.free) && off+n == sf.free[i+1].off {
		sf.free[i].n += sf.free[i+1].n
		sf.free = slices.Delete(sf.free, i+1, i+2)
	}
	if i > 0 && sf.free[i-1].off+sf.free[i-1].n == off {
		sf.free[i-1].n += sf.free[i].n
		sf.free = slices.Delete(sf.free, i, i+1)
	}
	if last := len(sf.free) - 1; last >= 0 && sf.free[last].off+sf.free[last].n == sf.end {
		sf.end = sf.free[last].off
		sf.free = sf.free[:last]
	}
}

func (sf *spillFile) close() {
	sf.f.Close()
	if sf.name != "" {
		os.Remove(sf.name)
	}
}

// fetchedChunk is one chunk of a fetched range: in data while in memory, and
// at off in file while a file holds it.
type fetchedChunk struct {
	rng     *fetchedRange
	uses    []int64 // positions in reading order of the reads of the chunk
	next    int     // uses[next] is the next read
	size    int32
	hash    xet.ChunkHash
	data    []byte
	file    *os.File
	off     int64
	output  bool // file is the caller's, which may have changed since
	heapIdx int  // index in chunkStore.heap, or -1
}

// fetchedRange serves the chunks of one xorb range as a worker decodes them,
// indexed from the start of the range, out of its reader's chunkStore. With
// a cache manager every chunk is also written to the range's cache entry,
// sealed for later readers once the range is complete; without one, chunks
// no term reads are skipped undecoded.
type fetchedRange struct {
	store  *chunkStore
	more   sync.Cond      // on store.mu; broadcast when shown, err or closed change
	chunks []fetchedChunk // nil once closed
	need   []bool         // whether a term reads each chunk; fixed
	hold   int            // chunks from hold on wait for the end of the body
	shown  int            // leading chunks readers may read
	err    error          // why no further chunk will be shown; io.EOF once complete
	closed bool

	cache     *chunkCache // the entry being written, or nil
	cacheFile *os.File
	spill     *spillFile // created for the first chunk to spill
}

// newFetchedRange makes a range of len(uses) chunks, where uses lists the
// positions in reading order at which terms read each chunk, written to cache
// unless it is nil.
func newFetchedRange(store *chunkStore, uses [][]int64, cache *chunkCache) *fetchedRange {
	r := &fetchedRange{
		store:  store,
		chunks: make([]fetchedChunk, len(uses)),
		need:   make([]bool, len(uses)),
		cache:  cache,
	}
	r.more.L = &store.mu
	for i := range r.chunks {
		r.chunks[i] = fetchedChunk{rng: r, uses: uses[i], heapIdx: -1}
		if r.need[i] = len(uses[i]) > 0; r.need[i] {
			r.hold = i
		}
	}
	if cache != nil {
		r.cacheFile = cache.file
	}
	return r
}

// fill decodes dec into r until the range ends, dec fails, or r is released.
// The last chunk a term reads is shown only once the body is read to its end:
// a reader with every chunk may finish and cancel the fetch, which closes the
// body, and a body must not be closed while it is being read.
func (r *fetchedRange) fill(dec *xorb.Decoder) {
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	n := len(r.need)
	for i := range n {
		if !r.need[i] && r.cache == nil {
			if err := dec.Skip(); err != nil {
				r.finish(i, chunkCountErr(err, i, n))
				return
			}
			continue
		}
		size, err := dec.Read(buf[:])
		if err != nil {
			r.finish(i, chunkCountErr(err, i, n))
			return
		}
		off := int64(-1)
		if r.cache != nil {
			if off, err = r.cache.push(buf[:size]); err != nil {
				r.finish(i, err)
				return
			}
		}
		if !r.need[i] {
			continue // only the cache entry takes it
		}
		hashes, _ := dec.Chunks()
		if !r.add(i, buf[:size], hashes[len(hashes)-1], off) {
			return
		}
	}
	if r.cache != nil {
		// Seal before the trailing read, which Close may interrupt.
		if err := r.cache.finalize(); err != nil {
			r.finish(n, err)
			return
		}
	}
	err := dec.Skip()
	if err == nil {
		err = fmt.Errorf("decoded %d chunks, expected %d", n+1, n)
	}
	r.finish(n, err)
}

func chunkCountErr(err error, decoded, expected int) error {
	if err == io.EOF {
		return fmt.Errorf("decoded %d chunks, expected %d", decoded, expected)
	}
	return err
}

// add stores chunk i, which a term reads, decoded as data with hash and
// written at off in the cache entry unless off is negative, and shows the
// chunks before it. It reports whether r still takes chunks.
func (r *fetchedRange) add(i int, data []byte, hash xet.ChunkHash, off int64) bool {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		return false
	}
	c := &r.chunks[i]
	c.size, c.hash = int32(len(data)), hash
	if off >= 0 {
		c.file, c.off = r.cacheFile, off
	}
	if !r.place(c, data) {
		return false
	}
	if shown := min(i+1, r.hold); shown > r.shown {
		r.shown = shown
		r.more.Broadcast()
	}
	s.evict()
	return true
}

// place keeps new chunk c, decoded as data, in memory if it fits there or a
// chunk in memory would leave before it; otherwise c stays in its cache entry
// alone or goes to r's spill file. The caller holds s.mu, which place
// releases to copy or write data; it reports false if r was released
// meanwhile.
func (r *fetchedRange) place(c *fetchedChunk, data []byte) bool {
	s := r.store
	n := int64(len(data))
	fits := s.limit <= 0 || s.used-s.writing+n <= s.limit || (len(s.heap) > 0 && leavesBefore(s.heap[0], c))
	if !fits && c.file != nil {
		return true
	}
	if !fits {
		if f, off, err := s.alloc(r, n); err == nil {
			s.mu.Unlock()
			_, err = f.WriteAt(data, off)
			s.mu.Lock()
			if r.closed {
				return false
			}
			if err == nil {
				c.file, c.off = f, off
				return true
			}
			r.release(f, off, n)
			if s.spillErr == nil {
				s.spillErr = err
			}
		}
		// Hold the chunk past the limit.
	}
	s.mu.Unlock()
	kept := bytes.Clone(data)
	s.mu.Lock()
	if r.closed {
		return false
	}
	c.data = kept
	s.used += n
	heap.Push(&s.heap, c)
	return true
}

// finish shows the first processed chunks and records why no more follow.
func (r *fetchedRange) finish(processed int, err error) {
	r.store.mu.Lock()
	defer r.store.mu.Unlock()
	r.shown, r.err = processed, err
	r.more.Broadcast()
}

// readChunk copies chunk idx into buf once it is shown and counts one of its
// reads, dropping the chunk after the last.
func (r *fetchedRange) readChunk(idx uint32, buf []byte) (chunkRef, error) {
	s := r.store
	s.mu.Lock()
	for int(idx) >= r.shown && r.err == nil && !r.closed {
		r.more.Wait()
	}
	switch {
	case r.closed:
		s.mu.Unlock()
		return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, fs.ErrClosed)
	case int(idx) >= r.shown && r.err == io.EOF:
		s.mu.Unlock()
		return chunkRef{}, fmt.Errorf("chunk %d out of range (total: %d)", idx, len(r.chunks))
	case int(idx) >= r.shown:
		err := r.err
		s.mu.Unlock()
		return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, err)
	}
	c := &r.chunks[idx]
	// Only this reader drops a chunk or frees its spill space, so the copy can be
	// read without the lock.
	ref := chunkRef{size: c.size, hash: c.hash, hashed: true}
	data, file, off, output := c.data, c.file, c.off, c.output
	s.mu.Unlock()

	if int(ref.size) > len(buf) {
		return chunkRef{}, fmt.Errorf("buffer too small for chunk: need %d bytes", ref.size)
	}
	out := buf[:ref.size]
	switch {
	case data != nil:
		copy(out, data)
	case file != nil:
		if _, err := file.ReadAt(out, off); err != nil {
			return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, err)
		}
		if output && xet.ComputeChunkHash(out) != ref.hash {
			return chunkRef{}, fmt.Errorf("load chunk %d: the output file no longer holds it", idx)
		}
	default:
		return chunkRef{}, fmt.Errorf("load chunk %d: no read left", idx)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed {
		return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, fs.ErrClosed)
	}
	if c.next+1 < len(c.uses) {
		c.next++
		ref.again = true
		if c.heapIdx >= 0 {
			heap.Fix(&s.heap, c.heapIdx)
		}
	} else {
		s.drop(c)
		c.next = len(c.uses)
	}
	return ref, nil
}

// noteOutput records that f holds chunk idx at off, where WriteTo has just
// written it whole, so the chunk leaves memory for free when it must.
func (r *fetchedRange) noteOutput(idx uint32, f *os.File, off int64) {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.closed || int(idx) >= r.shown {
		return
	}
	c := &r.chunks[idx]
	if c.file != nil || c.heapIdx < 0 {
		return
	}
	var probe [1]byte
	if _, err := f.ReadAt(probe[:], off); err != nil {
		return // e.g. a write-only file
	}
	c.file, c.off, c.output = f, off, true
	heap.Fix(&s.heap, c.heapIdx)
}

// Done releases r's chunks, its spill file and its cache entry, which is kept
// if sealed and discarded otherwise. It leaves the response body to the
// worker, as bodies are not safe to close while another goroutine reads them.
func (r *fetchedRange) Done() {
	s := r.store
	s.mu.Lock()
	var spill *spillFile
	if !r.closed {
		r.closed = true
		for i := range r.chunks {
			s.drop(&r.chunks[i])
		}
		r.chunks = nil
		spill, r.spill = r.spill, nil
		r.more.Broadcast()
	}
	s.mu.Unlock()
	if spill != nil {
		// A spill write in progress fails or lands harmlessly.
		spill.close()
	}
	if r.cache != nil {
		// After s.mu: Done waits for a push in progress.
		r.cache.Done()
	}
}

// noteWritten lets a fetched range read chunk idx, just written whole
// through w, back from w's file for the later terms that read it again.
func noteWritten(src chunkSource, idx uint32, w io.Writer, size int64) {
	r, ok := src.(*fetchedRange)
	f, isFile := w.(*os.File)
	if !ok || !isFile || size == 0 {
		return
	}
	// After the write even an O_APPEND file's offset is the chunk's end.
	end, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	r.noteOutput(idx, f, end-size)
}

// evict moves chunks out of memory until it is within the limit; the caller
// holds s.mu, which evict releases while it writes a chunk to a spill file.
func (s *chunkStore) evict() {
	for s.limit > 0 && s.used-s.writing > s.limit && len(s.heap) > 0 {
		c := s.heap[0]
		if c.file != nil {
			heap.Pop(&s.heap)
			s.used -= int64(len(c.data))
			c.data = nil
			continue
		}
		r := c.rng
		data := c.data
		n := int64(len(data))
		f, off, err := s.alloc(r, n)
		if err != nil {
			return // hold the chunks past the limit
		}
		heap.Pop(&s.heap)
		s.writing += n
		s.mu.Unlock()
		_, err = f.WriteAt(data, off)
		s.mu.Lock()
		s.writing -= n
		switch {
		case c.data == nil: // dropped while being written
			r.release(f, off, n)
		case err != nil:
			r.release(f, off, n)
			if s.spillErr == nil {
				s.spillErr = err
			}
			heap.Push(&s.heap, c)
		default:
			s.used -= n
			c.data, c.file, c.off = nil, f, off
		}
	}
}

// alloc reserves n bytes of r's spill file, creating the file on first use.
func (s *chunkStore) alloc(r *fetchedRange, n int64) (*os.File, int64, error) {
	if s.spillErr != nil {
		return nil, 0, s.spillErr
	}
	if r.closed {
		return nil, 0, fs.ErrClosed
	}
	if r.spill == nil {
		f, err := os.CreateTemp("", "xet-download-*")
		if err != nil {
			s.spillErr = err
			return nil, 0, err
		}
		r.spill = &spillFile{f: f}
		// Where an open file cannot lose its name, close removes it.
		if os.Remove(f.Name()) != nil {
			r.spill.name = f.Name()
		}
	}
	return r.spill.f, r.spill.alloc(n), nil
}

// release frees the n bytes at off of r's spill file f.
func (r *fetchedRange) release(f *os.File, off, n int64) {
	if r.spill != nil && r.spill.f == f {
		r.spill.release(off, n)
	}
}

// drop forgets the data and the copies of c.
func (s *chunkStore) drop(c *fetchedChunk) {
	if c.heapIdx >= 0 {
		heap.Remove(&s.heap, c.heapIdx)
	}
	s.used -= int64(len(c.data))
	if c.file != nil {
		c.rng.release(c.file, c.off, int64(c.size))
	}
	c.data, c.file, c.output = nil, nil, false
}

// chunkHeap orders the chunks in memory by leavesBefore.
type chunkHeap []*fetchedChunk

// leavesBefore reports whether a leaves memory before b: first the chunks a
// file also holds, then those read again furthest ahead.
func leavesBefore(a, b *fetchedChunk) bool {
	if (a.file != nil) != (b.file != nil) {
		return a.file != nil
	}
	return a.uses[a.next] > b.uses[b.next]
}

func (h chunkHeap) Len() int { return len(h) }

func (h chunkHeap) Less(i, j int) bool { return leavesBefore(h[i], h[j]) }

func (h chunkHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx, h[j].heapIdx = i, j
}

func (h *chunkHeap) Push(x any) {
	c := x.(*fetchedChunk)
	c.heapIdx = len(*h)
	*h = append(*h, c)
}

func (h *chunkHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	c.heapIdx = -1
	return c
}
