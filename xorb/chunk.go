package xorb

import (
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Chunk represents a single compressed chunk within an xorb.
type Chunk struct {
	hash *xet.Hash

	// Uncompressed data. Set directly (AddChunk) or materialized lazily.
	uncompressed []byte

	// Lazy source for uncompressed data (AddChunkWithSeeker path).
	uncompressedSrc    io.ReadSeeker
	uncompressedOffset int64

	// Compressed data. Computed lazily from uncompressed, or read lazily from compressedSrc.
	compressed []byte

	// Lazy source for compressed data (Decode path).
	compressedSrc    io.ReadSeeker
	compressedOffset int64

	// Sizes are grouped at the end to avoid padding between the interface fields above.
	uncompressedSize uint32 // also holds the known size from Decode header
	compressedSize   uint32
	compressedType   compressionType
}

// encodedChunkSize returns the number of bytes this chunk occupies in a serialized xorb
// (8-byte header + compressed payload). For chunks decoded from a reader the size is
// known without reading the compressed data; otherwise compression is triggered.
func (c *Chunk) encodedChunkSize() (int64, error) {
	if c.compressed != nil {
		return 8 + int64(len(c.compressed)), nil
	}
	if c.compressedSrc != nil {
		return 8 + int64(c.compressedSize), nil
	}
	compressed, _, err := c.compressedData()
	if err != nil {
		return 0, err
	}
	return 8 + int64(len(compressed)), nil
}

// size returns the uncompressed size without materializing data.
func (c *Chunk) size() uint32 {
	if c.uncompressed != nil {
		return uint32(len(c.uncompressed))
	}
	return c.uncompressedSize
}

// UncompressedData ensures the uncompressed data is materialized and returns it, reading from the source or decompressing as needed.
func (c *Chunk) UncompressedData() ([]byte, error) {
	if c.uncompressed != nil {
		return c.uncompressed, nil
	}
	if c.uncompressedSrc != nil {
		buf := make([]byte, c.uncompressedSize)
		if _, err := c.uncompressedSrc.Seek(c.uncompressedOffset, io.SeekStart); err != nil {
			return nil, fmt.Errorf("seek to uncompressed chunk data: %w", err)
		}
		if _, err := io.ReadFull(c.uncompressedSrc, buf); err != nil {
			return nil, fmt.Errorf("read uncompressed chunk data: %w", err)
		}
		c.uncompressedSrc = nil
		c.uncompressed = buf
		return c.uncompressed, nil
	}

	compressed, ct, err := c.compressedData()
	if err != nil {
		return nil, err
	}
	uncompressed, err := decompressChunk(compressed, ct, int(c.uncompressedSize))
	if err != nil {
		return nil, fmt.Errorf("decompress chunk: %w", err)
	}
	c.uncompressed = uncompressed
	return c.uncompressed, nil
}

func (c *Chunk) compressedData() ([]byte, compressionType, error) {
	if c.compressed != nil {
		return c.compressed, c.compressedType, nil
	}
	if c.compressedSrc != nil {
		buf := make([]byte, c.compressedSize)
		if _, err := c.compressedSrc.Seek(c.compressedOffset, io.SeekStart); err != nil {
			return nil, 0, fmt.Errorf("seek to compressed chunk data: %w", err)
		}
		if _, err := io.ReadFull(c.compressedSrc, buf); err != nil {
			return nil, 0, fmt.Errorf("read compressed chunk data: %w", err)
		}

		c.compressedSrc = nil
		c.compressed = buf
		// c.compressedType already set from Decode header
		return c.compressed, c.compressedType, nil
	}

	uncompressed, err := c.UncompressedData()
	if err != nil {
		return nil, 0, fmt.Errorf("get uncompressed data for compression: %w", err)
	}
	compressed, ct, err := selectBestCompression(uncompressed)
	if err != nil {
		return nil, 0, fmt.Errorf("compress chunk: %w", err)
	}
	c.compressed = compressed
	c.compressedType = ct
	return c.compressed, c.compressedType, nil
}

// Hash returns the hash of the uncompressed data, materializing it if needed.
func (c *Chunk) Hash() (xet.Hash, error) {
	if c.hash == nil {
		uncompressed, err := c.UncompressedData()
		if err != nil {
			return xet.Hash{}, err
		}

		hash := xet.ComputeChunkHash(uncompressed)
		c.hash = &hash
	}
	return *c.hash, nil
}
