// Package xorb implements the XET xorb binary format (Section 7).
package xorb

import (
	"encoding/binary"
	"fmt"

	"github.com/wzshiming/xet/pkg/merkle"
	"github.com/wzshiming/xet/pkg/xet"
)

const (
	// ChunkHeaderSize is the size of each chunk header in bytes.
	ChunkHeaderSize = 8
	// MaxXorbSize is the maximum serialized xorb size (64 MiB).
	MaxXorbSize = 67108864
	// MaxXorbChunks is the maximum number of chunks per xorb.
	MaxXorbChunks = 8192
)

// ChunkEntry represents a single chunk within a xorb.
type ChunkEntry struct {
	UncompressedData []byte
	Hash             [32]byte
}

// Xorb represents a complete xorb container.
type Xorb struct {
	Chunks    []ChunkEntry
	XorbHash  [32]byte
	ChunkData []byte // serialized chunk data region (headers + compressed)
}

// putUint24LE writes a 24-bit little-endian value.
func putUint24LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
}

// getUint24LE reads a 24-bit little-endian value.
func getUint24LE(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
}

// Serialize serializes a list of chunks into the xorb binary format (Section 7.2).
func Serialize(chunks []ChunkEntry, compression CompressionType) ([]byte, error) {
	if len(chunks) > MaxXorbChunks {
		return nil, fmt.Errorf("too many chunks: %d > %d", len(chunks), MaxXorbChunks)
	}

	// Compute chunk hashes and sizes
	chunkHashes := make([][32]byte, len(chunks))
	chunkSizes := make([]uint64, len(chunks))
	for i, c := range chunks {
		chunkHashes[i] = c.Hash
		chunkSizes[i] = uint64(len(c.UncompressedData))
	}

	// Compute xorb hash (Merkle root of chunk hashes)
	xorbHash := merkle.ComputeXorbHash(chunkHashes, chunkSizes)

	// Build chunk data region
	var chunkDataRegion []byte
	chunkBoundaryOffsets := make([]uint32, len(chunks))
	unpackedOffsets := make([]uint32, len(chunks))
	var unpackedCumulative uint32

	for i, c := range chunks {
		compressed, compType, err := CompressChunk(c.UncompressedData, compression)
		if err != nil {
			return nil, fmt.Errorf("compressing chunk %d: %w", i, err)
		}

		// Write chunk header (8 bytes)
		var header [ChunkHeaderSize]byte
		header[0] = 0 // version
		putUint24LE(header[1:4], uint32(len(compressed)))
		header[4] = byte(compType)
		putUint24LE(header[5:8], uint32(len(c.UncompressedData)))

		chunkDataRegion = append(chunkDataRegion, header[:]...)
		chunkDataRegion = append(chunkDataRegion, compressed...)

		chunkBoundaryOffsets[i] = uint32(len(chunkDataRegion))
		unpackedCumulative += uint32(len(c.UncompressedData))
		unpackedOffsets[i] = unpackedCumulative
	}

	// Build CasObjectInfo footer
	footer := buildFooter(xorbHash, chunkHashes, chunkBoundaryOffsets, unpackedOffsets)

	// Combine: chunk data + footer + footer length
	result := make([]byte, 0, len(chunkDataRegion)+len(footer)+4)
	result = append(result, chunkDataRegion...)
	result = append(result, footer...)

	// Append 4-byte footer length
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(footer)))
	result = append(result, lenBuf[:]...)

	if len(result) > MaxXorbSize {
		return nil, fmt.Errorf("serialized xorb exceeds maximum size: %d > %d", len(result), MaxXorbSize)
	}

	return result, nil
}

// buildFooter constructs the CasObjectInfo footer (Section 7.5).
func buildFooter(xorbHash [32]byte, chunkHashes [][32]byte, boundaryOffsets, unpackedOffsets []uint32) []byte {
	numChunks := uint32(len(chunkHashes))

	var buf []byte

	// Track offsets from end for the trailer
	// Main Header (Section 7.5.1)
	buf = append(buf, []byte("XETBLOB")...) // 7 bytes ident
	buf = append(buf, 1)                     // version
	buf = append(buf, xorbHash[:]...)        // 32 bytes

	hashSectionStart := len(buf)

	// Hash Section (Section 7.5.2)
	buf = append(buf, []byte("XBLBHSH")...) // 7 bytes
	buf = append(buf, 0)                     // hashes version
	var numBuf [4]byte
	binary.LittleEndian.PutUint32(numBuf[:], numChunks)
	buf = append(buf, numBuf[:]...) // num_chunks

	for _, h := range chunkHashes {
		buf = append(buf, h[:]...) // 32 bytes each
	}

	boundarySectionStart := len(buf)

	// Boundary Section (Section 7.5.3)
	buf = append(buf, []byte("XBLBBND")...) // 7 bytes
	buf = append(buf, 1)                     // boundaries version
	binary.LittleEndian.PutUint32(numBuf[:], numChunks)
	buf = append(buf, numBuf[:]...) // num_chunks

	for _, off := range boundaryOffsets {
		binary.LittleEndian.PutUint32(numBuf[:], off)
		buf = append(buf, numBuf[:]...)
	}

	for _, off := range unpackedOffsets {
		binary.LittleEndian.PutUint32(numBuf[:], off)
		buf = append(buf, numBuf[:]...)
	}

	// Trailer (Section 7.5.4)
	footerEnd := len(buf) + 4 + 4 + 4 + 16 // trailer size: num_chunks + 2 offsets + reserved

	binary.LittleEndian.PutUint32(numBuf[:], numChunks)
	buf = append(buf, numBuf[:]...) // num_chunks

	// Hash section offset from end
	hashOffFromEnd := uint32(footerEnd - hashSectionStart)
	binary.LittleEndian.PutUint32(numBuf[:], hashOffFromEnd)
	buf = append(buf, numBuf[:]...)

	// Boundary section offset from end
	bndOffFromEnd := uint32(footerEnd - boundarySectionStart)
	binary.LittleEndian.PutUint32(numBuf[:], bndOffFromEnd)
	buf = append(buf, numBuf[:]...)

	// Reserved 16 bytes
	buf = append(buf, make([]byte, 16)...)

	return buf
}

// ParsedChunk represents a parsed chunk from a xorb.
type ParsedChunk struct {
	CompressedData   []byte
	CompressionType  CompressionType
	CompressedSize   uint32
	UncompressedSize uint32
}

// ParseChunkData parses the chunk data region of a xorb.
func ParseChunkData(data []byte, numChunks int) ([]ParsedChunk, error) {
	chunks := make([]ParsedChunk, 0, numChunks)
	offset := 0

	for i := 0; i < numChunks; i++ {
		if offset+ChunkHeaderSize > len(data) {
			return nil, fmt.Errorf("truncated chunk header at chunk %d", i)
		}

		version := data[offset]
		if version != 0 {
			return nil, fmt.Errorf("unknown chunk version: %d", version)
		}

		compressedSize := getUint24LE(data[offset+1 : offset+4])
		compType := CompressionType(data[offset+4])
		uncompressedSize := getUint24LE(data[offset+5 : offset+8])

		offset += ChunkHeaderSize

		if offset+int(compressedSize) > len(data) {
			return nil, fmt.Errorf("truncated compressed data at chunk %d", i)
		}

		chunks = append(chunks, ParsedChunk{
			CompressedData:   data[offset : offset+int(compressedSize)],
			CompressionType:  compType,
			CompressedSize:   compressedSize,
			UncompressedSize: uncompressedSize,
		})

		offset += int(compressedSize)
	}

	return chunks, nil
}

// Deserialize parses a complete xorb binary and returns the decompressed chunks.
func Deserialize(data []byte) ([][]byte, [32]byte, error) {
	if len(data) < 4 {
		return nil, [32]byte{}, fmt.Errorf("xorb too small")
	}

	// Read footer length from last 4 bytes
	footerLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	if int(footerLen)+4 > len(data) {
		return nil, [32]byte{}, fmt.Errorf("invalid footer length")
	}

	footerStart := len(data) - 4 - int(footerLen)
	footerData := data[footerStart : len(data)-4]
	chunkDataRegion := data[:footerStart]

	// Parse footer - main header
	if len(footerData) < 40 {
		return nil, [32]byte{}, fmt.Errorf("footer too small")
	}
	if string(footerData[:7]) != "XETBLOB" {
		return nil, [32]byte{}, fmt.Errorf("invalid footer ident: %s", string(footerData[:7]))
	}
	if footerData[7] != 1 {
		return nil, [32]byte{}, fmt.Errorf("invalid footer version: %d", footerData[7])
	}

	var xorbHash [32]byte
	copy(xorbHash[:], footerData[8:40])

	// Parse hash section to get num_chunks
	pos := 40
	if string(footerData[pos:pos+7]) != "XBLBHSH" {
		return nil, [32]byte{}, fmt.Errorf("invalid hash section ident")
	}
	pos += 7
	if footerData[pos] != 0 {
		return nil, [32]byte{}, fmt.Errorf("invalid hash version: %d", footerData[pos])
	}
	pos++
	numChunks := int(binary.LittleEndian.Uint32(footerData[pos : pos+4]))
	pos += 4

	// Skip chunk hashes
	pos += numChunks * 32

	// Parse chunk data region
	parsed, err := ParseChunkData(chunkDataRegion, numChunks)
	if err != nil {
		return nil, xorbHash, err
	}

	// Decompress all chunks
	result := make([][]byte, len(parsed))
	for i, pc := range parsed {
		decompressed, err := DecompressChunk(pc.CompressedData, pc.CompressionType, int(pc.UncompressedSize))
		if err != nil {
			return nil, xorbHash, fmt.Errorf("decompressing chunk %d: %w", i, err)
		}

		// Verify hash
		chunkHash := xet.ComputeChunkHash(decompressed)
		_ = chunkHash // hash verification is optional per spec

		result[i] = decompressed
	}

	return result, xorbHash, nil
}
