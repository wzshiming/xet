package download

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sync"
	"sync/atomic"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/xorb"
)

// memoryBudget counts the decoded bytes a reader without a cache may still
// hold in memory; a nil budget never runs out.
type memoryBudget struct{ left atomic.Int64 }

func newMemoryBudget(limit int64) *memoryBudget {
	if limit <= 0 {
		return nil
	}
	b := &memoryBudget{}
	b.left.Store(limit)
	return b
}

// take reserves n bytes if they fit.
func (b *memoryBudget) take(n int64) bool {
	if b == nil {
		return true
	}
	for {
		left := b.left.Load()
		if left < n {
			return false
		}
		if b.left.CompareAndSwap(left, left-n) {
			return true
		}
	}
}

// hold reserves n bytes even past the limit.
func (b *memoryBudget) hold(n int64) {
	if b != nil {
		b.left.Add(-n)
	}
}

func (b *memoryBudget) give(n int64) {
	if b != nil {
		b.left.Add(n)
	}
}

// memoryRange holds the decoded chunks of one fetched xorb range for a reader
// without an on-disk cache, each until the last term reading it has read it:
// in memory within the reader's budget, otherwise in a temporary file, and
// once WriteTo has written a chunk later terms read again, in the output file.
type memoryRange struct {
	mu        sync.Mutex
	more      sync.Cond // broadcast whenever chunks or err change
	chunks    []memoryChunk
	reads     []int32 // reads left of each chunk of the range
	budget    *memoryBudget
	spill     *os.File // created for the first chunk past the budget
	spillName string   // set while the spill file still has a name
	spillEnd  int64
	err       error // why no further chunk will arrive; io.EOF once complete
}

// memoryChunk is one decoded chunk, in data or else at off in file: the spill
// file or an output file. Neither is set once the chunk has no reads left.
type memoryChunk struct {
	ref  chunkRef
	data []byte
	file *os.File
	off  int64
}

func newMemoryRange(reads []int32, budget *memoryBudget) *memoryRange {
	r := &memoryRange{
		chunks: make([]memoryChunk, 0, len(reads)),
		reads:  reads,
		budget: budget,
	}
	r.more.L = &r.mu
	return r
}

// fill decodes dec into r until the range ends, dec fails, or r is released;
// a release takes effect once the chunk being read arrives.
func (r *memoryRange) fill(dec *xorb.Decoder) {
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	for i := range len(r.reads) {
		n, err := dec.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				err = fmt.Errorf("decoded %d chunks, expected %d", i, len(r.reads))
			}
			r.finish(err)
			return
		}
		hashes, _ := dec.Chunks()
		ref := chunkRef{size: int32(n), hash: hashes[len(hashes)-1], hashed: true}
		if i < len(r.reads)-1 {
			if !r.add(ref, buf[:n]) {
				return
			}
			continue
		}
		data := bytes.Clone(buf[:n])
		// Finish with the body first: with every chunk the reader may reach
		// EOF and cancel the fetch, which closes the body.
		_, end := dec.Read(buf[:])
		if end == nil {
			end = fmt.Errorf("decoded %d chunks, expected %d", i+2, len(r.reads))
		}
		if r.add(ref, data) {
			r.finish(end)
		}
	}
}

// add appends the next chunk, stored only if a term reads it; it reports
// whether r still takes chunks.
func (r *memoryRange) add(ref chunkRef, data []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil { // released by Done
		return false
	}
	c := memoryChunk{ref: ref}
	if r.reads[len(r.chunks)] > 0 {
		r.store(&c, data)
	}
	r.chunks = append(r.chunks, c)
	r.more.Broadcast()
	return true
}

// store keeps data in memory within the budget and in the spill file past
// it, falling back to memory when the file cannot take it.
func (r *memoryRange) store(c *memoryChunk, data []byte) {
	n := int64(len(data))
	if !r.budget.take(n) {
		if r.spillChunk(c, data) == nil {
			return
		}
		r.budget.hold(n)
	}
	c.data = bytes.Clone(data)
}

func (r *memoryRange) spillChunk(c *memoryChunk, data []byte) error {
	if r.spill == nil {
		f, err := os.CreateTemp("", "xet-download-*")
		if err != nil {
			return err
		}
		// Where an open file cannot lose its name, Done removes it.
		if os.Remove(f.Name()) != nil {
			r.spillName = f.Name()
		}
		r.spill = f
	}
	if _, err := r.spill.WriteAt(data, r.spillEnd); err != nil {
		return err
	}
	c.file, c.off = r.spill, r.spillEnd
	r.spillEnd += int64(len(data))
	return nil
}

func (r *memoryRange) finish(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err == nil {
		r.err = err
		r.more.Broadcast()
	}
}

// readChunk copies chunk idx into buf once the worker has decoded it and
// counts one of its reads, dropping the chunk after the last.
func (r *memoryRange) readChunk(idx uint32, buf []byte) (chunkRef, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for int(idx) >= len(r.chunks) && r.err == nil {
		r.more.Wait()
	}
	if int(idx) >= len(r.chunks) {
		if r.err == io.EOF {
			return chunkRef{}, fmt.Errorf("chunk %d out of range (total: %d)", idx, len(r.chunks))
		}
		return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, r.err)
	}
	c := &r.chunks[idx]
	if int(c.ref.size) > len(buf) {
		return chunkRef{}, fmt.Errorf("buffer too small for chunk: need %d bytes", c.ref.size)
	}
	data := buf[:c.ref.size]
	switch {
	case c.data != nil:
		copy(data, c.data)
	case c.file != nil:
		if _, err := c.file.ReadAt(data, c.off); err != nil {
			return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, err)
		}
		// Unlike the spill file, an output file belongs to the caller.
		if c.file != r.spill && xet.ComputeChunkHash(data) != c.ref.hash {
			return chunkRef{}, fmt.Errorf("load chunk %d: the output file no longer holds it", idx)
		}
	default:
		return chunkRef{}, fmt.Errorf("load chunk %d: no read left", idx)
	}
	r.reads[idx]--
	if r.reads[idx] == 0 {
		r.drop(c)
	}
	ref := c.ref
	ref.again = r.reads[idx] > 0
	return ref, nil
}

func (r *memoryRange) drop(c *memoryChunk) {
	if c.data != nil {
		r.budget.give(int64(len(c.data)))
	}
	c.data, c.file = nil, nil
}

// moveToFile reads chunk idx back from off in f for its later reads, where
// WriteTo has just written it whole, instead of holding it.
func (r *memoryRange) moveToFile(idx uint32, f *os.File, off int64) {
	var probe [1]byte
	if _, err := f.ReadAt(probe[:], off); err != nil {
		return // e.g. a write-only file
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if int(idx) >= len(r.chunks) || r.reads[idx] == 0 || r.chunks[idx].data == nil {
		return
	}
	c := &r.chunks[idx]
	r.drop(c)
	c.file, c.off = f, off
}

// Done drops every chunk and the spill file. It leaves the response body to
// the worker, as bodies are not safe to close while another goroutine reads
// them.
func (r *memoryRange) Done() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.chunks {
		r.drop(&r.chunks[i])
	}
	r.chunks = nil
	if r.spill != nil {
		r.spill.Close()
		if r.spillName != "" {
			os.Remove(r.spillName)
		}
		r.spill = nil
	}
	r.err = fs.ErrClosed
	r.more.Broadcast()
}

// moveWritten lets a memory range read chunk idx, just written whole through
// w, back from w's file for the later terms that read it again.
func moveWritten(src chunkSource, idx uint32, w io.Writer, size int64) {
	m, ok := src.(*memoryRange)
	f, isFile := w.(*os.File)
	if !ok || !isFile || size == 0 {
		return
	}
	// After the write even an O_APPEND file's offset is the chunk's end.
	end, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	m.moveToFile(idx, f, end-size)
}
