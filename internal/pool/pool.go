// Package pool provides shared sync.Pool-backed buffers for hot paths.
package pool

import (
	"sync"
)

const maxChunkSize = 131072 + 8 // MaxChunkSize + 8 bytes for compression header

// chunkBufPool pools *[MaxChunkSize]byte arrays to avoid large stack/heap allocations
// in hot paths (encoder, decoder, validate, upload).
var chunkBufPool = sync.Pool{
	New: func() any {
		return new([maxChunkSize]byte)
	},
}

// GetChunkBuf returns a *[MaxChunkSize]byte from the pool.
// Callers must call PutChunkBuf when the buffer is no longer needed.
func GetChunkBuf() *[maxChunkSize]byte {
	return chunkBufPool.Get().(*[maxChunkSize]byte)
}

// PutChunkBuf returns a *[MaxChunkSize]byte to the pool.
func PutChunkBuf(buf *[maxChunkSize]byte) {
	chunkBufPool.Put(buf)
}
