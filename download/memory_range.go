package download

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/wzshiming/xet/internal/pool"
	"github.com/wzshiming/xet/xorb"
)

// memoryRange holds the decoded chunks of one fetched xorb range for a reader
// without an on-disk cache. Its prefetch worker decodes into it while the
// reader waits only for the chunk it needs next.
type memoryRange struct {
	mu       sync.Mutex
	more     sync.Cond // broadcast whenever chunks or err change
	chunks   []memoryChunk
	expected int
	err      error // why no further chunk will arrive; io.EOF once complete
}

// memoryChunk is one decoded chunk; its ref carries the decoder's hash.
type memoryChunk struct {
	data []byte
	ref  chunkRef
}

func newMemoryRange(expected uint32) *memoryRange {
	r := &memoryRange{
		chunks:   make([]memoryChunk, 0, expected),
		expected: int(expected),
	}
	r.more.L = &r.mu
	return r
}

// fill decodes dec into r until the range ends, dec fails, or r is released;
// a release takes effect once the chunk being read arrives.
func (r *memoryRange) fill(dec *xorb.Decoder) {
	buf := pool.GetChunkBuf()
	defer pool.PutChunkBuf(buf)
	for i := range r.expected {
		n, err := dec.Read(buf[:])
		if err != nil {
			if err == io.EOF {
				err = fmt.Errorf("decoded %d chunks, expected %d", i, r.expected)
			}
			r.publish(nil, err)
			return
		}
		hashes, _ := dec.Chunks()
		chunk := &memoryChunk{
			data: bytes.Clone(buf[:n]),
			ref:  chunkRef{size: int32(n), hash: hashes[len(hashes)-1], hashed: true},
		}
		var end error
		if i == r.expected-1 {
			// Finish with the body first: with every chunk the reader may
			// reach EOF and cancel the fetch, which closes the body.
			if _, end = dec.Read(buf[:]); end == nil {
				end = fmt.Errorf("decoded %d chunks, expected %d", i+2, r.expected)
			}
		}
		if !r.publish(chunk, end) {
			return
		}
	}
}

// publish appends chunk, if any, and records err; it reports whether the
// worker should go on decoding.
func (r *memoryRange) publish(chunk *memoryChunk, err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil { // released by Done
		return false
	}
	if chunk != nil {
		r.chunks = append(r.chunks, *chunk)
	}
	r.err = err
	r.more.Broadcast()
	return err == nil
}

// readChunk copies the decoded chunk at idx into buf once the worker has
// decoded it.
func (r *memoryRange) readChunk(idx uint32, buf []byte) (chunkRef, error) {
	r.mu.Lock()
	for int(idx) >= len(r.chunks) && r.err == nil {
		r.more.Wait()
	}
	if int(idx) >= len(r.chunks) {
		err, total := r.err, len(r.chunks)
		r.mu.Unlock()
		if err == io.EOF {
			return chunkRef{}, fmt.Errorf("chunk %d out of range (total: %d)", idx, total)
		}
		return chunkRef{}, fmt.Errorf("load chunk %d: %w", idx, err)
	}
	chunk := r.chunks[idx]
	r.mu.Unlock()

	if len(chunk.data) > len(buf) {
		return chunkRef{}, fmt.Errorf("buffer too small for chunk: need %d bytes", len(chunk.data))
	}
	copy(buf, chunk.data)
	return chunk.ref, nil
}

// Done drops the decoded chunks. It leaves the response body to the worker,
// as bodies are not safe to close while another goroutine reads them.
func (r *memoryRange) Done() {
	r.mu.Lock()
	r.chunks = nil
	r.err = fs.ErrClosed
	r.more.Broadcast()
	r.mu.Unlock()
}
