package xorb

import (
	"fmt"

	"github.com/wzshiming/xet"
)

// Binary format identifiers
const (
	XorbIdentifier       = "XETBLOB"
	HashSectionIdent     = "XBLBHSH"
	BoundarySectionIdent = "XBLBBND"
)

// Xorb represents a container of compressed chunks
type Xorb struct {
	Chunks      []ChunkData
	ChunkHashes []xet.Hash
	Hash        xet.Hash
}

// ChunkData represents a single compressed chunk within an xorb
type ChunkData struct {
	UncompressedData []byte
	CompressedData   []byte
	CompressionType  CompressionType
	Hash             xet.Hash
}

// ChunkHeader represents the 8-byte header for each chunk in the xorb
type ChunkHeader struct {
	Version          uint8           // Offset 0, 1 byte
	CompressedSize   uint32          // Offset 1-3, 3 bytes (little-endian)
	CompressionType  CompressionType // Offset 4, 1 byte
	UncompressedSize uint32          // Offset 5-7, 3 bytes (little-endian)
}

// NewXorb creates a new empty xorb
func NewXorb() *Xorb {
	return &Xorb{
		Chunks:      make([]ChunkData, 0),
		ChunkHashes: make([]xet.Hash, 0),
	}
}

// AddChunk adds a chunk to the xorb
func (x *Xorb) AddChunk(chunk xet.ChunkBytes) error {
	// Compute chunk hash
	chunkHash := chunk.Hash()

	data := chunk.Bytes()

	// Compress the chunk
	compressed, compressionType, err := selectBestCompression(data)
	if err != nil {
		return fmt.Errorf("failed to compress chunk: %w", err)
	}

	x.Chunks = append(x.Chunks, ChunkData{
		UncompressedData: data,
		CompressedData:   compressed,
		CompressionType:  compressionType,
		Hash:             chunkHash,
	})
	x.ChunkHashes = append(x.ChunkHashes, chunkHash)

	return nil
}
