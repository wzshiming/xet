package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet/pkg/merkle"
	"github.com/wzshiming/xet/pkg/xet"
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
func (x *Xorb) AddChunk(data []byte) error {
	// Compute chunk hash
	chunkHash := xet.ComputeChunkHash(data)

	// Compress the chunk
	compressed, compressionType, err := SelectBestCompression(data)
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

// SerializeChunksOnly serializes only the chunk data region without footer
// This is used for conformance testing with tools that expect raw chunk data
func (x *Xorb) SerializeChunksOnly() ([]byte, error) {
	var buf bytes.Buffer

	// Write chunk data region
	for _, chunk := range x.Chunks {
		// Write chunk header (8 bytes)
		header := ChunkHeader{
			Version:          0,
			CompressedSize:   uint32(len(chunk.CompressedData)),
			CompressionType:  chunk.CompressionType,
			UncompressedSize: uint32(len(chunk.UncompressedData)),
		}

		if err := writeChunkHeader(&buf, header); err != nil {
			return nil, fmt.Errorf("failed to write chunk header: %w", err)
		}

		// Write compressed data
		if _, err := buf.Write(chunk.CompressedData); err != nil {
			return nil, fmt.Errorf("failed to write chunk data: %w", err)
		}
	}

	return buf.Bytes(), nil
}

// Serialize serializes the xorb to binary format with CasObjectInfo footer
func (x *Xorb) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Track chunk offsets for boundary section
	chunkOffsets := make([]uint64, len(x.Chunks))
	unpackedOffsets := make([]uint64, len(x.Chunks))
	currentOffset := uint64(0)
	currentUnpacked := uint64(0)

	// Write chunk data region
	for i, chunk := range x.Chunks {
		// Write chunk header (8 bytes)
		header := ChunkHeader{
			Version:          0,
			CompressedSize:   uint32(len(chunk.CompressedData)),
			CompressionType:  chunk.CompressionType,
			UncompressedSize: uint32(len(chunk.UncompressedData)),
		}

		if err := writeChunkHeader(&buf, header); err != nil {
			return nil, fmt.Errorf("failed to write chunk header: %w", err)
		}

		// Write compressed data
		if _, err := buf.Write(chunk.CompressedData); err != nil {
			return nil, fmt.Errorf("failed to write chunk data: %w", err)
		}

		currentOffset += 8 + uint64(len(chunk.CompressedData))
		currentUnpacked += uint64(len(chunk.UncompressedData))

		// Store END offsets per spec section 7.5.3
		chunkOffsets[i] = currentOffset
		unpackedOffsets[i] = currentUnpacked
	}

	// Build CasObjectInfo footer
	footer, err := x.buildFooter(chunkOffsets, unpackedOffsets)
	if err != nil {
		return nil, fmt.Errorf("failed to build footer: %w", err)
	}

	// Write footer
	if _, err := buf.Write(footer); err != nil {
		return nil, fmt.Errorf("failed to write footer: %w", err)
	}

	// Write footer length (4-byte little-endian)
	footerLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(footerLen, uint32(len(footer)))
	if _, err := buf.Write(footerLen); err != nil {
		return nil, fmt.Errorf("failed to write footer length: %w", err)
	}

	return buf.Bytes(), nil
}

// writeChunkHeader writes a chunk header to the buffer
func writeChunkHeader(w io.Writer, h ChunkHeader) error {
	buf := make([]byte, 8)
	buf[0] = h.Version

	// CompressedSize (3 bytes, little-endian)
	buf[1] = byte(h.CompressedSize & 0xFF)
	buf[2] = byte((h.CompressedSize >> 8) & 0xFF)
	buf[3] = byte((h.CompressedSize >> 16) & 0xFF)

	buf[4] = byte(h.CompressionType)

	// UncompressedSize (3 bytes, little-endian)
	buf[5] = byte(h.UncompressedSize & 0xFF)
	buf[6] = byte((h.UncompressedSize >> 8) & 0xFF)
	buf[7] = byte((h.UncompressedSize >> 16) & 0xFF)

	_, err := w.Write(buf)
	return err
}

// buildFooter builds the CasObjectInfo footer
func (x *Xorb) buildFooter(chunkOffsets, unpackedOffsets []uint64) ([]byte, error) {
	var buf bytes.Buffer

	// Compute xorb hash from chunk hashes
	chunkSizes := make([]uint64, len(x.Chunks))
	for i, chunk := range x.Chunks {
		chunkSizes[i] = uint64(len(chunk.UncompressedData))
	}

	// Compute xorb hash using inline Merkle tree implementation
	x.Hash = merkle.ComputeXorbHash(x.ChunkHashes, chunkSizes)

	// Main Header: XETBLOB ident (7 bytes), version (1), xorb hash (32 bytes)
	buf.Write([]byte("XETBLOB"))
	buf.WriteByte(1) // version - MUST be 1 per spec
	buf.Write(x.Hash[:])

	// Hash Section: XBLBHSH ident (7 bytes), version (1), num_chunks (4 bytes), chunk hashes
	buf.Write([]byte("XBLBHSH"))
	buf.WriteByte(0) // version

	numChunks := uint32(len(x.Chunks))
	binary.Write(&buf, binary.LittleEndian, numChunks)

	for _, hash := range x.ChunkHashes {
		buf.Write(hash[:])
	}

	// Boundary Section: XBLBBND ident (7 bytes), version (1), num_chunks (4 bytes), chunk offsets
	buf.Write([]byte("XBLBBND"))
	buf.WriteByte(1) // version - MUST be 1 per spec
	binary.Write(&buf, binary.LittleEndian, numChunks)

	// Write boundary offsets (packed offsets in xorb)
	for _, offset := range chunkOffsets {
		binary.Write(&buf, binary.LittleEndian, uint32(offset))
	}

	// Write unpacked offsets (offsets in reconstructed file)
	for _, offset := range unpackedOffsets {
		binary.Write(&buf, binary.LittleEndian, uint32(offset))
	}

	// Trailer: num_chunks (4), hash_offset_from_end (4), boundary_offset_from_end (4), reserved (16)
	// Calculate offsets from the end of the footer (not including the 4-byte length trailer)
	footerEndPos := buf.Len() + 4 + 4 + 4 + 16 // current pos + trailer fields

	// Hash section starts at position 40 (after main header)
	hashSectionStart := 40
	hashOffsetFromEnd := uint32(footerEndPos - hashSectionStart)

	// Boundary section starts after hash section
	boundarySectionStart := 40 + 8 + 4 + int(numChunks)*32 // main header + hash header + num_chunks + hashes
	boundaryOffsetFromEnd := uint32(footerEndPos - boundarySectionStart)

	binary.Write(&buf, binary.LittleEndian, numChunks)
	binary.Write(&buf, binary.LittleEndian, hashOffsetFromEnd)
	binary.Write(&buf, binary.LittleEndian, boundaryOffsetFromEnd)

	// Reserved: 16 bytes, zero
	reserved := make([]byte, 16)
	buf.Write(reserved)

	return buf.Bytes(), nil
}

// DeserializeChunksOnly deserializes only the chunk data region without expecting a footer
// This matches the Rust deserialize_chunks() function behavior
func DeserializeChunksOnly(data []byte) (*Xorb, error) {
	xorb := NewXorb()
	offset := 0

	// Parse chunks until we run out of data or hit a footer marker
	for offset < len(data) {
		if offset+8 > len(data) {
			// Not enough data for a chunk header
			break
		}

		// Check if we've hit the start of a footer (XETBLOB identifier)
		if offset+7 <= len(data) && string(data[offset:offset+7]) == "XETBLOB" {
			// We've hit a footer, stop parsing chunks
			break
		}

		// Parse chunk header
		header := ChunkHeader{
			Version:          data[offset],
			CompressedSize:   uint32(data[offset+1]) | uint32(data[offset+2])<<8 | uint32(data[offset+3])<<16,
			CompressionType:  CompressionType(data[offset+4]),
			UncompressedSize: uint32(data[offset+5]) | uint32(data[offset+6])<<8 | uint32(data[offset+7])<<16,
		}
		offset += 8

		if header.Version != 0 {
			return nil, fmt.Errorf("unsupported chunk version: %d", header.Version)
		}

		if offset+int(header.CompressedSize) > len(data) {
			return nil, fmt.Errorf("data too short for chunk data")
		}

		compressedData := data[offset : offset+int(header.CompressedSize)]
		offset += int(header.CompressedSize)

		// Decompress chunk
		uncompressedData, err := DecompressChunk(compressedData, header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress chunk: %w", err)
		}

		// Compute chunk hash
		chunkHash := xet.ComputeChunkHash(uncompressedData)

		xorb.Chunks = append(xorb.Chunks, ChunkData{
			UncompressedData: uncompressedData,
			CompressedData:   compressedData,
			CompressionType:  header.CompressionType,
			Hash:             chunkHash,
		})
		xorb.ChunkHashes = append(xorb.ChunkHashes, chunkHash)
	}

	// Compute xorb hash from chunks
	if len(xorb.Chunks) > 0 {
		chunkSizes := make([]uint64, len(xorb.Chunks))
		for i, chunk := range xorb.Chunks {
			chunkSizes[i] = uint64(len(chunk.UncompressedData))
		}
		xorb.Hash = merkle.ComputeXorbHash(xorb.ChunkHashes, chunkSizes)
	}

	return xorb, nil
}

// Deserialize deserializes an xorb from binary format
func Deserialize(data []byte) (*Xorb, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short for xorb")
	}

	// Read footer length from the last 4 bytes
	footerLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	if int(footerLen)+4 > len(data) {
		return nil, fmt.Errorf("invalid footer length: %d", footerLen)
	}

	// Split data into chunk data region and footer
	chunkDataEnd := len(data) - int(footerLen) - 4
	chunkData := data[:chunkDataEnd]
	footerData := data[chunkDataEnd : len(data)-4]

	// Parse footer
	xorb := NewXorb()
	if err := xorb.parseFooter(footerData); err != nil {
		return nil, fmt.Errorf("failed to parse footer: %w", err)
	}

	// Parse chunks
	if err := xorb.parseChunks(chunkData); err != nil {
		return nil, fmt.Errorf("failed to parse chunks: %w", err)
	}

	return xorb, nil
}

// parseFooter parses the CasObjectInfo footer
func (x *Xorb) parseFooter(data []byte) error {
	offset := 0

	// Main Header: XETBLOB (7), version (1), xorb hash (32)
	if offset+40 > len(data) {
		return fmt.Errorf("footer too short for main header")
	}

	if string(data[offset:offset+7]) != "XETBLOB" {
		return fmt.Errorf("invalid xorb identifier")
	}
	offset += 7

	version := data[offset]
	if version != 1 {
		return fmt.Errorf("unsupported xorb version: %d (expected 1)", version)
	}
	offset++

	copy(x.Hash[:], data[offset:offset+32])
	offset += 32

	// Hash Section: XBLBHSH (7), version (1), num_chunks (4), hashes
	if offset+12 > len(data) {
		return fmt.Errorf("footer too short for hash section header")
	}

	if string(data[offset:offset+7]) != "XBLBHSH" {
		return fmt.Errorf("invalid hash section identifier")
	}
	offset += 7

	version = data[offset]
	if version != 0 {
		return fmt.Errorf("unsupported hash section version: %d", version)
	}
	offset++

	numChunks := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// Read chunk hashes
	if offset+int(numChunks)*32 > len(data) {
		return fmt.Errorf("footer too short for chunk hashes")
	}

	x.ChunkHashes = make([]xet.Hash, numChunks)
	for i := range numChunks {
		copy(x.ChunkHashes[i][:], data[offset:offset+32])
		offset += 32
	}

	// We can skip parsing the boundary section for now
	// It's mainly used for reconstruction and not needed for basic deserialization

	return nil
}

// parseChunks parses the chunk data region
func (x *Xorb) parseChunks(data []byte) error {
	offset := 0
	x.Chunks = make([]ChunkData, len(x.ChunkHashes))

	for i := range x.Chunks {
		if offset+8 > len(data) {
			return fmt.Errorf("data too short for chunk header %d", i)
		}

		// Parse chunk header
		header := ChunkHeader{
			Version:          data[offset],
			CompressedSize:   uint32(data[offset+1]) | uint32(data[offset+2])<<8 | uint32(data[offset+3])<<16,
			CompressionType:  CompressionType(data[offset+4]),
			UncompressedSize: uint32(data[offset+5]) | uint32(data[offset+6])<<8 | uint32(data[offset+7])<<16,
		}
		offset += 8

		if header.Version != 0 {
			return fmt.Errorf("unsupported chunk version: %d", header.Version)
		}

		if offset+int(header.CompressedSize) > len(data) {
			return fmt.Errorf("data too short for chunk %d", i)
		}

		compressedData := data[offset : offset+int(header.CompressedSize)]
		offset += int(header.CompressedSize)

		// Decompress chunk
		uncompressedData, err := DecompressChunk(compressedData, header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return fmt.Errorf("failed to decompress chunk %d: %w", i, err)
		}

		x.Chunks[i] = ChunkData{
			UncompressedData: uncompressedData,
			CompressedData:   compressedData,
			CompressionType:  header.CompressionType,
			Hash:             x.ChunkHashes[i],
		}
	}

	return nil
}

// CompressedDataStream returns a slice containing the raw compressed bytes for
// all chunks in an xorb, concatenated in order.  The 8-byte chunk header that
// precedes each chunk in the on-disk "chunks-only" serialization is stripped;
// only the actual compressed payload is included.
//
// Also returned is a slice of per-chunk byte offsets within the resulting
// stream (i.e. chunkOffsets[i] is the position at which chunk i begins).
//
// The function handles both the full XETBLOB format (with footer) and the
// chunks-only upload format (without footer) by delegating to
// DeserializeChunksOnly, which stops at the XETBLOB magic bytes.
func CompressedDataStream(data []byte) (stream []byte, chunkOffsets []int, err error) {
	x, err := DeserializeChunksOnly(data)
	if err != nil {
		return nil, nil, err
	}

	chunkOffsets = make([]int, len(x.Chunks))
	var buf []byte
	for i, chunk := range x.Chunks {
		chunkOffsets[i] = len(buf)
		buf = append(buf, chunk.CompressedData...)
	}

	return buf, chunkOffsets, nil
}
