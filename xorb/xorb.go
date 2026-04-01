package xorb

import (
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Binary format identifiers
var (
	xorbIdentifier       = [...]byte{'X', 'E', 'T', 'B', 'L', 'O', 'B'}
	hashSectionIdent     = [...]byte{'X', 'B', 'L', 'B', 'H', 'S', 'H'}
	boundarySectionIdent = [...]byte{'X', 'B', 'L', 'B', 'B', 'N', 'D'}
)

// Xorb represents a container of compressed chunks
type Xorb struct {
	chunks []Chunk
	hash   *xet.Hash
}

// NewXorb creates a new empty xorb
func NewXorb() *Xorb {
	return &Xorb{
		chunks: make([]Chunk, 0),
	}
}

// ChunkSize returns the number of chunks in the xorb
func (x *Xorb) ChunkSize() int {
	return len(x.chunks)
}

// Chunk returns a pointer to the chunk at the given index.
func (x *Xorb) Chunk(index int) (*Chunk, error) {
	if index < 0 || index >= len(x.chunks) {
		return nil, fmt.Errorf("chunk index out of bounds: %d", index)
	}
	return &x.chunks[index], nil
}

// Chunks returns all chunks in the xorb
func (x *Xorb) Chunks() []Chunk {
	return x.chunks
}

// AddChunk adds a chunk from raw uncompressed data.
func (x *Xorb) AddChunk(raw []byte) error {
	if x.hash != nil {
		return fmt.Errorf("cannot add chunk to xorb after hash has been computed")
	}
	x.chunks = append(x.chunks, Chunk{uncompressed: raw})
	return nil
}

// AddChunkWithSeeker adds a chunk whose uncompressed data is read lazily from r at the
// given offset and size. The data at [offset, offset+size) must remain valid
// for the lifetime of the xorb.
func (x *Xorb) AddChunkWithSeeker(r io.ReadSeeker, offset int64, size uint32) error {
	if x.hash != nil {
		return fmt.Errorf("cannot add chunk to xorb after hash has been computed")
	}
	x.chunks = append(x.chunks, Chunk{
		uncompressedSrc:    r,
		uncompressedOffset: offset,
		uncompressedSize:   size,
	})
	return nil
}

// EncodedSize returns the total byte length of the serialized xorb.
// If withFooter is false, only the chunk data region size is returned (without footer).
// If withFooter is true, the full XETBLOB format size including the footer is returned.
// This may trigger compression of chunks that have not yet been compressed.
func (x *Xorb) EncodedSize(withFooter bool) (int64, error) {
	var total int64
	for i := range x.chunks {
		n, err := x.chunks[i].encodedChunkSize()
		if err != nil {
			return 0, fmt.Errorf("chunk %d: %w", i, err)
		}
		total += n
	}
	if withFooter {
		numChunks := int64(len(x.chunks))
		// Footer layout: main header (40) + hash section (12 + numChunks*32) +
		//   boundary section (12 + numChunks*8) + trailer (28) + length field (4)
		total += 40 + 12 + numChunks*32 + 12 + numChunks*8 + 28 + 4
	}
	return total, nil
}

func (x *Xorb) Hash() (xet.Hash, error) {
	if x.hash == nil {
		chunkSizes := make([]uint64, len(x.chunks))
		chunkHashes := make([]xet.Hash, len(x.chunks))
		for i := range x.chunks {
			h, err := x.chunks[i].Hash()
			if err != nil {
				return xet.Hash{}, err
			}
			chunkHashes[i] = h
			chunkSizes[i] = uint64(x.chunks[i].size())
		}

		// Compute xorb hash based on chunk hashes and sizes
		hash := xet.ComputeXorbHash(chunkHashes, chunkSizes)
		x.hash = &hash
	}
	return *x.hash, nil
}
