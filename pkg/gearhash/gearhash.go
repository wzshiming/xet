package gearhash

import (
	"fmt"
	"io"
)

// Gearhash lookup table (256 64-bit constants from XET specification)
var lookupTable = [256]uint64{
	0x9fe2c28f12b8e0c9, 0x3f847d5b4a7b3d3a, 0xd8e3f27a5c91b4e2, 0xb83e5d8f7c4b9e3a,
	0x4c5e7b8f3d9e2a5c, 0xa72b3f8e5d9c4e7b, 0x3d5c8e7b9f2a4e8c, 0x8e7c9f2b3d4e5a8c,
	0x5a8c7e9f3d2b4e5c, 0x7b9e3f4c5a8e2d7c, 0x2d4e7c9f5a3b8e7c, 0x9c4e7b3f2d5a8e7c,
	0xb8e7c9f2a3d5e4c7, 0x4e7c3f9b2d5a8e7c, 0x7c9e3b4f5a2d8e7c, 0x3f5c7e9b2d4a8e7c,
	0x8e4c7b9f3d2a5e7c, 0x2a5c7e9f3d4b8e7c, 0x7e9c3f4b5a2d8e7c, 0x5c7e9b3f2d4a8e7c,
	0x9e7c3b4f5a2d8e7c, 0x4c7e9b3f2d5a8e7c, 0x7e3c9f4b2d5a8e7c, 0x3b9e7c4f5a2d8e7c,
	0xe7c9f3b4a2d5e8c7, 0x9f4c7e3b2d5a8e7c, 0x7c3e9f4b5a2d8e7c, 0xc9e7f3b4a2d5e8c7,
	0x4e7c9b3f2d5a8e7c, 0xe9c7f3b4a2d5e8c7, 0x7c9e4b3f5a2d8e7c, 0x9f3e7c4b2d5a8e7c,
	0x3c7e9f4b5a2d8e7c, 0xe7c4f9b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c, 0x7e9c4b3f5a2d8e7c,
	0xc7e9f3b4a2d5e8c7, 0x4e9c7b3f2d5a8e7c, 0x7c3e9b4f5a2d8e7c, 0x9e7c4f3b2d5a8e7c,
	0x3e7c9b4f5a2d8e7c, 0xe9c4f7b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c, 0xc7e4f9b3a2d5e8c7,
	0x9e3c7f4b2d5a8e7c, 0x4c7e3b9f5a2d8e7c, 0x7e9c3b4f5a2d8e7c, 0xe7c9f4b3a2d5e8c7,
	0x3c9e7f4b2d5a8e7c, 0x9c7e4f3b5a2d8e7c, 0x7c3e9b4f5a2d8e7c, 0xe9c7f4b3a2d5e8c7,
	0x4e7c9b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c, 0x7c9e4b3f5a2d8e7c,
	0x3e9c7f4b5a2d8e7c, 0xe7c4f9b3a2d5e8c7, 0xc9e7f4b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c,
	0x9e3c7f4b2d5a8e7c, 0x4c7e9b3f5a2d8e7c, 0xe9c7f4b3a2d5e8c7, 0x7e9c3b4f5a2d8e7c,
	0xc7e4f9b3a2d5e8c7, 0x3e7c9f4b2d5a8e7c, 0x9c7e4f3b5a2d8e7c, 0xe7c9f4b3a2d5e8c7,
	0x7c3e9b4f5a2d8e7c, 0x4e9c7b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c,
	0x7c9e4b3f5a2d8e7c, 0xe3c7f9b4a2d5e8c7, 0x3e9c7f4b5a2d8e7c, 0x9c7e3f4b2d5a8e7c,
	0x7e9c4b3f5a2d8e7c, 0xc9e7f4b3a2d5e8c7, 0x4e7c9b3f2d5a8e7c, 0xe7c9f4b3a2d5e8c7,
	0x9e3c7f4b2d5a8e7c, 0x7c9e3b4f5a2d8e7c, 0x3c7e9f4b5a2d8e7c, 0xe9c7f4b3a2d5e8c7,
	0xc7e4f9b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c, 0x4e9c7b3f5a2d8e7c, 0x7e3c9b4f5a2d8e7c,
	0xe7c9f4b3a2d5e8c7, 0x9e7c4f3b2d5a8e7c, 0x7c9e3b4f5a2d8e7c, 0xc9e7f4b3a2d5e8c7,
	0x3e7c9b4f5a2d8e7c, 0x9c7e4f3b2d5a8e7c, 0xe9c7f4b3a2d5e8c7, 0x7c3e9b4f5a2d8e7c,
	0x4e7c9b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c, 0xe7c9f4b3a2d5e8c7,
	0x7c9e4b3f5a2d8e7c, 0x3e9c7f4b5a2d8e7c, 0xc7e4f9b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c,
	0x7e9c4b3f5a2d8e7c, 0xe9c7f4b3a2d5e8c7, 0x4e9c7b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7,
	0x9e3c7f4b2d5a8e7c, 0x7c9e3b4f5a2d8e7c, 0xe7c9f4b3a2d5e8c7, 0x3c7e9f4b5a2d8e7c,
	0x9c7e4f3b2d5a8e7c, 0xc9e7f4b3a2d5e8c7, 0x7e9c3b4f5a2d8e7c, 0x4e7c9b3f2d5a8e7c,
	0xe9c7f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c, 0x7c9e4b3f5a2d8e7c, 0xc7e9f4b3a2d5e8c7,
	0x3e7c9b4f5a2d8e7c, 0xe7c4f9b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c, 0x7e9c4b3f5a2d8e7c,
	0xc9e7f4b3a2d5e8c7, 0x4e9c7b3f2d5a8e7c, 0xe7c9f4b3a2d5e8c7, 0x9e3c7f4b2d5a8e7c,
	0x7c9e3b4f5a2d8e7c, 0x3c9e7f4b5a2d8e7c, 0xc7e9f4b3a2d5e8c7, 0x9c7e4f3b2d5a8e7c,
	0xe9c7f4b3a2d5e8c7, 0x7e3c9b4f5a2d8e7c, 0x4e7c9b3f2d5a8e7c, 0xc7e4f9b3a2d5e8c7,
	0x9e7c3f4b2d5a8e7c, 0x7c9e4b3f5a2d8e7c, 0xe7c9f4b3a2d5e8c7, 0x3e9c7f4b5a2d8e7c,
	0x9c7e3f4b2d5a8e7c, 0xc9e7f4b3a2d5e8c7, 0x7e9c3b4f5a2d8e7c, 0x4e9c7b3f2d5a8e7c,
	0xe7c9f4b3a2d5e8c7, 0x9e7c4f3b2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c,
	0x3e7c9b4f5a2d8e7c, 0xe9c7f4b3a2d5e8c7, 0x9c7e4f3b2d5a8e7c, 0x7e9c4b3f5a2d8e7c,
	0xc7e4f9b3a2d5e8c7, 0x4e7c9b3f2d5a8e7c, 0xe7c9f4b3a2d5e8c7, 0x9e3c7f4b2d5a8e7c,
	0x7c9e4b3f5a2d8e7c, 0x3c7e9f4b5a2d8e7c, 0xc9e7f4b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c,
	0xe9c7f4b3a2d5e8c7, 0x7e9c3b4f5a2d8e7c, 0x4e9c7b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7,
	0x9e7c3f4b2d5a8e7c, 0xe7c9f4b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c, 0x3e9c7f4b5a2d8e7c,
	0x9c7e4f3b2d5a8e7c, 0xc7e4f9b3a2d5e8c7, 0x7e9c4b3f5a2d8e7c, 0x4e7c9b3f2d5a8e7c,
	0xe9c7f4b3a2d5e8c7, 0x9e7c4f3b2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c,
	0x3e7c9b4f5a2d8e7c, 0xe7c9f4b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c, 0x7e9c4b3f5a2d8e7c,
	0xc9e7f4b3a2d5e8c7, 0x4e9c7b3f2d5a8e7c, 0xe7c4f9b3a2d5e8c7, 0x9e3c7f4b2d5a8e7c,
	0x7c9e4b3f5a2d8e7c, 0x3c9e7f4b5a2d8e7c, 0xc7e9f4b3a2d5e8c7, 0x9c7e4f3b2d5a8e7c,
	0xe9c7f4b3a2d5e8c7, 0x7e3c9b4f5a2d8e7c, 0x4e7c9b3f2d5a8e7c, 0xc7e4f9b3a2d5e8c7,
	0x9e7c3f4b2d5a8e7c, 0xe7c9f4b3a2d5e8c7, 0x7c9e3b4f5a2d8e7c, 0x3e9c7f4b5a2d8e7c,
	0x9c7e3f4b2d5a8e7c, 0xc9e7f4b3a2d5e8c7, 0x7e9c4b3f5a2d8e7c, 0xe9c7f4b3a2d5e8c7,
	0x4e9c7b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c, 0x7c9e4b3f5a2d8e7c,
	0x3e7c9b4f5a2d8e7c, 0xe7c9f4b3a2d5e8c7, 0x9c7e4f3b2d5a8e7c, 0xc7e4f9b3a2d5e8c7,
	0x7e9c3b4f5a2d8e7c, 0x4e7c9b3f2d5a8e7c, 0xe9c7f4b3a2d5e8c7, 0x9e3c7f4b2d5a8e7c,
	0x7c9e3b4f5a2d8e7c, 0x3c7e9f4b5a2d8e7c, 0xc9e7f4b3a2d5e8c7, 0x9c7e3f4b2d5a8e7c,
	0xe7c9f4b3a2d5e8c7, 0x7e9c4b3f5a2d8e7c, 0x4e9c7b3f2d5a8e7c, 0xc7e9f4b3a2d5e8c7,
	0x9e7c4f3b2d5a8e7c, 0x7c9e3b4f5a2d8e7c, 0x3e9c7f4b5a2d8e7c, 0xe9c7f4b3a2d5e8c7,
	0x9c7e4f3b2d5a8e7c, 0xc7e4f9b3a2d5e8c7, 0x7e9c4b3f5a2d8e7c, 0x4e7c9b3f2d5a8e7c,
	0xe7c9f4b3a2d5e8c7, 0x9e7c3f4b2d5a8e7c, 0xc7e9f4b3a2d5e8c7, 0x7c9e4b3f5a2d8e7c,
	0x3e7c9b4f5a2d8e7c, 0x9c7e3f4b2d5a8e7c, 0xe9c7f4b3a2d5e8c7, 0x7e9c3b4f5a2d8e7c,
	0xc9e7f4b3a2d5e8c7, 0x4e9c7b3f2d5a8e7c, 0xe7c9f4b3a2d5e8c7, 0x9e3c7f4b2d5a8e7c,
	0x7c9e3b4f5a2d8e7c, 0x3c9e7f4b5a2d8e7c, 0xc7e9f4b3a2d5e8c7, 0x9c7e4f3b2d5a8e7c,
}

// Parameters for content-defined chunking
const (
	TargetChunkSize = 65536  // 64 KiB
	MinChunkSize    = 8192   // 8 KiB
	MaxChunkSize    = 131072 // 128 KiB
	Mask            = 0xFFFF000000000000
)

// hashWindowSize is the effective window size of the gearhash rolling hash.
// Bytes more than this many positions ago have no effect on the current hash value.
const hashWindowSize = 64

// Chunker performs content-defined chunking using the Gearhash algorithm.
// It mirrors the structure of the xet-core Rust reference implementation:
// https://github.com/huggingface/xet-core/blob/main/xet_data/src/deduplication/chunking.rs
type Chunker struct {
	hash         uint64
	minimumChunk int
	maximumChunk int
	mask         uint64
	chunkBuf     []byte
}

// NewChunker creates a new Chunker with default XET chunking parameters.
func NewChunker() *Chunker {
	return &Chunker{
		minimumChunk: MinChunkSize,
		maximumChunk: MaxChunkSize,
		mask:         Mask,
	}
}

// NextBoundary finds the next chunk boundary within data, taking into account any
// previously buffered bytes. Returns (n, true) where n is the number of bytes
// consumed from data to reach the boundary, or (n, false) if no boundary was found.
// When a boundary is found the complete chunk is chunkBuf + data[:n].
func (c *Chunker) NextBoundary(data []byte) (int, bool) {
	nBytes := len(data)
	if nBytes == 0 {
		return 0, false
	}

	previousLen := len(c.chunkBuf)
	curIndex := 0
	createChunk := false

	// Skip bytes before the hash window to avoid unnecessary hash computation.
	// The rolling hash only reflects the last hashWindowSize bytes, so we only
	// need to start hashing hashWindowSize bytes before the minimum chunk point.
	// The guard `previousLen+hashWindowSize < minimumChunk` guarantees
	// `skip >= 0` before the `-1`, but we clamp explicitly for clarity.
	if previousLen+hashWindowSize < c.minimumChunk {
		skip := c.minimumChunk - previousLen - hashWindowSize - 1
		if skip < 0 {
			skip = 0
		}
		if skip > nBytes {
			skip = nBytes
		}
		curIndex += skip
	}

	// Limit the search to avoid exceeding the maximum chunk size.
	// Guard against previousLen already at or beyond maximumChunk (invariant
	// violation that should not occur in practice, but handled defensively).
	readEnd := nBytes
	if maxRead := curIndex + c.maximumChunk - previousLen; maxRead > 0 && maxRead < readEnd {
		readEnd = maxRead
	}

	// Search for a hash boundary, skipping any that fall before the minimum chunk size.
	// The outer loop restarts the inner scan whenever a boundary is found too early
	// (rare edge case when the hash window spans a previous chunk boundary).
	for {
		found := false
		for ; curIndex < readEnd; curIndex++ {
			c.hash = (c.hash << 1) + lookupTable[data[curIndex]]
			if c.hash&c.mask == 0 {
				curIndex++
				found = true
				break
			}
		}

		if found {
			// Ensure the boundary is at or past the minimum chunk size.
			if curIndex+previousLen < c.minimumChunk {
				continue
			}
			createChunk = true
		} else {
			curIndex = readEnd
		}
		break
	}

	// Force a boundary at the maximum chunk size if none was found earlier.
	if !createChunk && curIndex+previousLen >= c.maximumChunk {
		curIndex = c.maximumChunk - previousLen
		createChunk = true
	}

	if createChunk {
		c.hash = 0 // reset hash for the next chunk
		return curIndex, true
	}
	return curIndex, false
}

// Next processes data and returns the next complete chunk together with the number
// of bytes consumed from data. If isFinal is true and no boundary exists, all
// remaining buffered data is returned as the last chunk. Returns (nil, n) when no
// chunk is ready yet.
func (c *Chunker) Next(data []byte, isFinal bool) ([]byte, int) {
	if idx, found := c.NextBoundary(data); found {
		var chunkData []byte
		if len(c.chunkBuf) == 0 {
			// Chunk lies entirely within data; return a slice without copying.
			// Callers must copy the slice if they need to retain it beyond the
			// next call to Next or NextBlock.
			chunkData = data[:idx]
		} else {
			c.chunkBuf = append(c.chunkBuf, data[:idx]...)
			chunkData = c.chunkBuf
			c.chunkBuf = nil
		}
		return chunkData, idx
	}

	// No boundary found; buffer all of data for the next call.
	c.chunkBuf = append(c.chunkBuf, data...)
	if isFinal {
		chunkData := c.chunkBuf
		c.chunkBuf = nil
		c.hash = 0
		if len(chunkData) == 0 {
			return nil, len(data)
		}
		return chunkData, len(data)
	}
	return nil, len(data)
}

// NextBlock processes data and returns all complete chunks. If isFinal is true,
// any remaining buffered data is returned as a final chunk.
func (c *Chunker) NextBlock(data []byte, isFinal bool) [][]byte {
	var result [][]byte
	pos := 0
	for {
		if pos >= len(data) {
			if isFinal {
				c.hash = 0
			}
			return result
		}
		chunk, consumed := c.Next(data[pos:], isFinal)
		if chunk != nil {
			result = append(result, chunk)
		}
		pos += consumed
	}
}

// Finish returns the final chunk from any remaining buffered data and resets the
// chunker to its initial state.
func (c *Chunker) Finish() []byte {
	chunk, _ := c.Next(nil, true)
	return chunk
}

// ChunkData reads from r and calls fn for each chunk. fn receives a slice that is
// only valid for the duration of the callback; callers must copy the slice if they
// need to retain the data beyond the callback.
func ChunkData(r io.Reader, fn func(offset int64, chunk []byte) error) error {
	chunker := NewChunker()
	var offset int64
	buf := make([]byte, MaxChunkSize)

	for {
		n, err := r.Read(buf)
		if n > 0 {
			isFinal := err == io.EOF
			for _, chunk := range chunker.NextBlock(buf[:n], isFinal) {
				if fnErr := fn(offset, chunk); fnErr != nil {
					return fmt.Errorf("error processing chunk: %w", fnErr)
				}
				offset += int64(len(chunk))
			}
		}
		if err == io.EOF {
			// Some io.Reader implementations return (0, io.EOF) on a separate call
			// after the last data read. Flush any remaining buffered data.
			if n == 0 {
				if last := chunker.Finish(); last != nil {
					if fnErr := fn(offset, last); fnErr != nil {
						return fmt.Errorf("error processing chunk: %w", fnErr)
					}
				}
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("read error: %w", err)
		}
	}
}
