// Package pool provides shared sync.Pool-backed byte buffers.
package pool

import (
	"sync"

	"github.com/wzshiming/xet"
)

// chunkBufPool pools *[MaxChunkSize]byte arrays to avoid large stack/heap allocations
// in hot paths (encoder, decoder, validate, upload).
var chunkBufPool = sync.Pool{
	New: func() any {
		buf := new([xet.MaxChunkSize]byte)
		return buf
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
