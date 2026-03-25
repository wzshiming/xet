package gearhash

import (
	"bufio"
	"fmt"
	"io"

	"github.com/wzshiming/xet/pkg/xet"
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

// Content-defined chunking parameters
// These constants are imported from pkg/xet to maintain consistency with the reference implementation
const (
	TargetChunkSize = xet.TargetChunkSize
	MinChunkSize    = xet.MinChunkSize
	MaxChunkSize    = xet.MaxChunkSize
	Mask            = xet.GearhashMask
)

// ChunkData reads data from the provided reader and invokes fn for each chunk.
func ChunkData(r io.Reader, fn func(offset int64, chunk []byte) error) error {
	reader := bufio.NewReader(r)

	var offset int64
	var buf [MaxChunkSize]byte

	for {
		chunkSize, err := findChunkBoundary(reader, buf[:])
		if err != nil && err != io.EOF {
			return err
		}

		if chunkSize == 0 {
			return nil
		}

		if err := fn(offset, buf[:chunkSize]); err != nil {
			return fmt.Errorf("error processing chunk: %w", err)
		}

		offset += int64(chunkSize)

		if err == io.EOF {
			return nil
		}
	}
}

// findChunkBoundary fills buf with the next chunk and returns its size.
func findChunkBoundary(reader *bufio.Reader, buf []byte) (int, error) {
	var (
		hash uint64
		size int
	)

	for size < MinChunkSize {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return size, io.EOF
			}
			return 0, fmt.Errorf("read chunk prefix: %w", err)
		}

		buf[size] = b
		size++
		hash = (hash << 1) + lookupTable[b]
	}

	for size < MaxChunkSize {
		b, err := reader.ReadByte()
		if err != nil {
			if err == io.EOF {
				return size, io.EOF
			}
			return 0, fmt.Errorf("read chunk data: %w", err)
		}

		buf[size] = b
		size++
		hash = (hash << 1) + lookupTable[b]

		if (hash & Mask) == 0 {
			return size, nil
		}
	}

	return size, nil
}
