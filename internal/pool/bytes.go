// Package pool provides shared sync.Pool-backed buffers for hot paths.
package pool

import (
	"bytes"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
	"github.com/wzshiming/xet"
)

// chunkBufPool pools *[MaxChunkSize]byte arrays to avoid large stack/heap allocations
// in hot paths (encoder, decoder, validate, upload).
var chunkBufPool = sync.Pool{
	New: func() any {
		return new([xet.MaxChunkSize]byte)
	},
}

// GetChunkBuf returns a *[MaxChunkSize]byte from the pool.
// Callers must call PutChunkBuf when the buffer is no longer needed.
func GetChunkBuf() *[xet.MaxChunkSize]byte {
	return chunkBufPool.Get().(*[xet.MaxChunkSize]byte)
}

// PutChunkBuf returns a *[MaxChunkSize]byte to the pool.
func PutChunkBuf(buf *[xet.MaxChunkSize]byte) {
	chunkBufPool.Put(buf)
}

// lz4WriterPool pools lz4.Writer objects to avoid per-call allocation.
var lz4WriterPool sync.Pool

// GetLZ4Writer returns a pooled *lz4.Writer reset to w, or a new one if the pool is empty.
func GetLZ4Writer(w io.Writer) *lz4.Writer {
	if v := lz4WriterPool.Get(); v != nil {
		lw := v.(*lz4.Writer)
		lw.Reset(w)
		return lw
	}
	return lz4.NewWriter(w)
}

// PutLZ4Writer resets the writer to io.Discard (releasing buffer references) and returns it to the pool.
func PutLZ4Writer(w *lz4.Writer) {
	w.Reset(io.Discard)
	lz4WriterPool.Put(w)
}

// lz4ReaderPool pools lz4.Reader objects to avoid per-call allocation.
var lz4ReaderPool sync.Pool

// GetLZ4Reader returns a pooled *lz4.Reader reset to r, or a new one if the pool is empty.
func GetLZ4Reader(r io.Reader) *lz4.Reader {
	if v := lz4ReaderPool.Get(); v != nil {
		lr := v.(*lz4.Reader)
		lr.Reset(r)
		return lr
	}
	return lz4.NewReader(r)
}

// PutLZ4Reader resets the reader to an empty source (releasing data references) and returns it to the pool.
func PutLZ4Reader(r *lz4.Reader) {
	r.Reset(bytes.NewReader(nil))
	lz4ReaderPool.Put(r)
}
