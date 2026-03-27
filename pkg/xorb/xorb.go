package xorb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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

// UpdateHash recalculates the xorb hash from the current chunks.
func (x *Xorb) UpdateHash() {
	chunkSizes := make([]uint64, len(x.Chunks))
	for i, chunk := range x.Chunks {
		chunkSizes[i] = uint64(len(chunk.UncompressedData))
	}

	x.Hash = xet.ComputeXorbHash(x.ChunkHashes, chunkSizes)
}

// Serialize streams the xorb. When chunkOnly is true, only the chunk data
// region is emitted without the footer.
func Serialize(x *Xorb, chunkOnly bool) (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		if err := writeXorb(pw, x, chunkOnly); err != nil {
			pw.CloseWithError(err)
			return
		}
		pw.Close()
	}()

	return pr, nil
}

// SerializeBytes materializes the streamed serialization into a byte slice.
func SerializeBytes(x *Xorb, chunkOnly bool) ([]byte, error) {
	reader, err := Serialize(x, chunkOnly)
	if err != nil {
		return nil, err
	}

	return io.ReadAll(reader)
}

// SerializeChunksOnly serializes only the chunk data region without footer
// This is used for conformance testing with tools that expect raw chunk data
func (x *Xorb) SerializeChunksOnly() ([]byte, error) {
	return SerializeBytes(x, true)
}

// Serialize serializes the xorb to binary format with CasObjectInfo footer
func (x *Xorb) Serialize() ([]byte, error) {
	return SerializeBytes(x, false)
}

func writeXorb(w io.Writer, x *Xorb, chunkOnly bool) error {
	// Track chunk offsets for boundary section
	var chunkOffsets []uint64
	var unpackedOffsets []uint64
	if !chunkOnly {
		chunkOffsets = make([]uint64, len(x.Chunks))
		unpackedOffsets = make([]uint64, len(x.Chunks))
	}

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

		if err := writeChunkHeader(w, header); err != nil {
			return fmt.Errorf("failed to write chunk header: %w", err)
		}

		// Write compressed data
		if _, err := w.Write(chunk.CompressedData); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}

		if !chunkOnly {
			currentOffset += 8 + uint64(len(chunk.CompressedData))
			currentUnpacked += uint64(len(chunk.UncompressedData))

			// Store END offsets per spec section 7.5.3
			chunkOffsets[i] = currentOffset
			unpackedOffsets[i] = currentUnpacked
		}
	}

	if chunkOnly {
		if len(x.Chunks) > 0 {
			x.UpdateHash()
		}
		return nil
	}

	// Build CasObjectInfo footer
	footer, err := x.buildFooter(chunkOffsets, unpackedOffsets)
	if err != nil {
		return fmt.Errorf("failed to build footer: %w", err)
	}

	// Write footer
	if _, err := w.Write(footer); err != nil {
		return fmt.Errorf("failed to write footer: %w", err)
	}

	// Write footer length (4-byte little-endian)
	var footerLen [4]byte
	binary.LittleEndian.PutUint32(footerLen[:], uint32(len(footer)))
	if _, err := w.Write(footerLen[:]); err != nil {
		return fmt.Errorf("failed to write footer length: %w", err)
	}

	return nil
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
	x.UpdateHash()

	// Main Header: XETBLOB ident (7 bytes), version (1), xorb hash (32 bytes)
	buf.Write([]byte(XorbIdentifier))
	buf.WriteByte(1) // version - MUST be 1 per spec
	buf.Write(x.Hash[:])

	// Hash Section: XBLBHSH ident (7 bytes), version (1), num_chunks (4 bytes), chunk hashes
	buf.Write([]byte(HashSectionIdent))
	buf.WriteByte(0) // version

	numChunks := uint32(len(x.Chunks))
	binary.Write(&buf, binary.LittleEndian, numChunks)

	for _, hash := range x.ChunkHashes {
		buf.Write(hash[:])
	}

	// Boundary Section: XBLBBND ident (7 bytes), version (1), num_chunks (4 bytes), chunk offsets
	buf.Write([]byte(BoundarySectionIdent))
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
	return DeserializeBytes(data, true)
}

// DeserializeBytes parses an xorb from a byte slice. When chunkOnly is true,
// the footer is not expected.
func DeserializeBytes(data []byte, chunkOnly bool) (*Xorb, error) {
	return Deserialize(bytes.NewReader(data), chunkOnly)
}

// Deserialize deserializes an xorb from a streaming reader.
func Deserialize(r io.Reader, chunkOnly bool) (*Xorb, error) {
	if chunkOnly {
		return deserializeChunksOnlyReader(r)
	}

	return deserializeFull(r)
}

// parseFooter parses the CasObjectInfo footer
func (x *Xorb) parseFooter(data []byte) error {
	offset := 0

	// Main Header: XETBLOB (7), version (1), xorb hash (32)
	if offset+40 > len(data) {
		return fmt.Errorf("footer too short for main header")
	}

	if string(data[offset:offset+7]) != XorbIdentifier {
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

	if string(data[offset:offset+7]) != HashSectionIdent {
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

func deserializeFull(r io.Reader) (*Xorb, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read xorb: %w", err)
	}

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
	if err := xorb.parseChunksFromReader(bytes.NewReader(chunkData), len(xorb.ChunkHashes)); err != nil {
		return nil, fmt.Errorf("failed to parse chunks: %w", err)
	}

	return xorb, nil
}

func deserializeChunksOnlyReader(r io.Reader) (*Xorb, error) {
	xorb := NewXorb()
	br := bufio.NewReader(r)

	var headerBuf [8]byte
	for {
		peek, err := br.Peek(7)
		if err == io.EOF {
			if len(peek) == 0 {
				break
			}
			return nil, fmt.Errorf("data too short for chunk header")
		}
		if err == nil && string(peek) == XorbIdentifier {
			break
		}

		if _, err := io.ReadFull(br, headerBuf[:]); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil, fmt.Errorf("data too short for chunk header")
			}
			return nil, fmt.Errorf("read chunk header: %w", err)
		}

		header := ChunkHeader{
			Version:          headerBuf[0],
			CompressedSize:   uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16,
			CompressionType:  CompressionType(headerBuf[4]),
			UncompressedSize: uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16,
		}

		if header.Version != 0 {
			return nil, fmt.Errorf("unsupported chunk version: %d", header.Version)
		}

		compressedData := make([]byte, header.CompressedSize)
		if _, err := io.ReadFull(br, compressedData); err != nil {
			if err == io.ErrUnexpectedEOF || err == io.EOF {
				return nil, fmt.Errorf("data too short for chunk data")
			}
			return nil, fmt.Errorf("read chunk data: %w", err)
		}

		// Decompress chunk
		uncompressedData, err := DecompressChunk(compressedData, header.CompressionType, int(header.UncompressedSize))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress chunk: %w", err)
		}

		// Compute chunk hash
		chunkHash := xet.ChunkBytes(uncompressedData).Hash()

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
		xorb.UpdateHash()
	}

	return xorb, nil
}

// parseChunksFromReader parses the chunk data region from a streaming reader.
func (x *Xorb) parseChunksFromReader(r io.Reader, numChunks int) error {
	x.Chunks = make([]ChunkData, numChunks)

	var headerBuf [8]byte
	for i := range x.Chunks {
		if _, err := io.ReadFull(r, headerBuf[:]); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("data too short for chunk header %d", i)
			}
			return fmt.Errorf("read chunk header %d: %w", i, err)
		}

		// Parse chunk header
		header := ChunkHeader{
			Version:          headerBuf[0],
			CompressedSize:   uint32(headerBuf[1]) | uint32(headerBuf[2])<<8 | uint32(headerBuf[3])<<16,
			CompressionType:  CompressionType(headerBuf[4]),
			UncompressedSize: uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16,
		}

		if header.Version != 0 {
			return fmt.Errorf("unsupported chunk version: %d", header.Version)
		}

		compressedData := make([]byte, header.CompressedSize)
		if _, err := io.ReadFull(r, compressedData); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return fmt.Errorf("data too short for chunk %d", i)
			}
			return fmt.Errorf("read chunk %d: %w", i, err)
		}

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
