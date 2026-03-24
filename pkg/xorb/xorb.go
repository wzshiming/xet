package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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

// Serialize serializes the xorb to binary format
func (x *Xorb) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Track chunk offsets for boundary section
	chunkOffsets := make([]uint64, len(x.Chunks))
	unpackedOffsets := make([]uint64, len(x.Chunks))
	currentOffset := uint64(0)
	currentUnpacked := uint64(0)

	// Write chunk data region
	for i, chunk := range x.Chunks {
		chunkOffsets[i] = currentOffset
		unpackedOffsets[i] = currentUnpacked

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
	x.Hash = computeXorbHashFromChunks(x.ChunkHashes, chunkSizes)

	// Main Header: XETBLOB ident (7 bytes), version (1), xorb hash (32 bytes)
	buf.Write([]byte("XETBLOB"))
	buf.WriteByte(0) // version
	buf.Write(x.Hash[:])

	// Hash Section: XBLBHSH ident (7 bytes), version (1), num_chunks (4 bytes), chunk hashes
	buf.Write([]byte("XBLBHSH"))
	buf.WriteByte(0) // version

	numChunks := uint32(len(x.Chunks))
	binary.Write(&buf, binary.LittleEndian, numChunks)

	for _, hash := range x.ChunkHashes {
		buf.Write(hash[:])
	}

	// Boundary Section: XBLBBND ident (7 bytes), version (1), chunk offsets
	buf.Write([]byte("XBLBBND"))
	buf.WriteByte(0) // version

	// Write boundary offsets (packed offsets in xorb)
	for _, offset := range chunkOffsets {
		binary.Write(&buf, binary.LittleEndian, offset)
	}

	// Write unpacked offsets (offsets in reconstructed file)
	for _, offset := range unpackedOffsets {
		binary.Write(&buf, binary.LittleEndian, offset)
	}

	return buf.Bytes(), nil
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
	if version != 0 {
		return fmt.Errorf("unsupported xorb version: %d", version)
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

// computeXorbHashFromChunks computes xorb hash using Merkle tree with variable fan-out
func computeXorbHashFromChunks(hashes []xet.Hash, sizes []uint64) xet.Hash {
	if len(hashes) != len(sizes) {
		panic("chunk hashes and sizes length mismatch")
	}

	if len(hashes) == 0 {
		return xet.Hash{}
	}

	if len(hashes) == 1 {
		return hashes[0]
	}

	// Build Merkle tree with variable fan-out
	currentLevel := make([]merkleNode, len(hashes))
	for i := range hashes {
		currentLevel[i] = merkleNode{hash: hashes[i], size: sizes[i]}
	}

	// Iteratively build parent levels until we have a single root
	for len(currentLevel) > 1 {
		cutPoints := findCutPoints(currentLevel)
		nextLevel := make([]merkleNode, 0)
		start := 0

		for _, cutPoint := range cutPoints {
			merged := mergeNodes(currentLevel[start:cutPoint])
			nextLevel = append(nextLevel, merged)
			start = cutPoint
		}

		// Handle remaining nodes
		if start < len(currentLevel) {
			merged := mergeNodes(currentLevel[start:])
			nextLevel = append(nextLevel, merged)
		}

		currentLevel = nextLevel
	}

	return currentLevel[0].hash
}

// merkleNode represents a node in the Merkle tree
type merkleNode struct {
	hash xet.Hash
	size uint64
}

// findCutPoints determines where to split the sequence based on hash values
func findCutPoints(nodes []merkleNode) []int {
	if len(nodes) <= xet.MinChildren {
		return nil
	}

	cutPoints := make([]int, 0)
	lastCut := 0

	for i := xet.MinChildren; i < len(nodes); i++ {
		remaining := len(nodes) - i
		if remaining < xet.MinChildren {
			break
		}

		// Use hash value to determine if this is a cut point
		hashValue := binary.LittleEndian.Uint64(nodes[i].hash[:8])
		if hashValue%xet.MeanBranchingFactor == 0 {
			groupSize := i - lastCut
			if groupSize >= xet.MinChildren && groupSize <= xet.MaxChildren {
				cutPoints = append(cutPoints, i)
				lastCut = i
			}
		}
	}

	return cutPoints
}

// mergeNodes merges a sequence of nodes into a single parent node
func mergeNodes(nodes []merkleNode) merkleNode {
	if len(nodes) == 1 {
		return nodes[0]
	}

	// Build input for internal node hash: "{hash_hex} : {size}\n" per child
	var input []byte
	var totalSize uint64

	for _, n := range nodes {
		hashStr := n.hash.String()
		sizeStr := fmt.Sprintf("%d", n.size)
		line := hashStr + " : " + sizeStr + "\n"
		input = append(input, []byte(line)...)
		totalSize += n.size
	}

	parentHash := xet.ComputeInternalNodeHash(input)

	return merkleNode{hash: parentHash, size: totalSize}
}
