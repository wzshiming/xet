package xorb

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Encoder writes xorb data chunk-by-chunk to an io.Writer.
// Call Encode for each chunk, then Close to flush the footer (if withFooter),
// then SummoryHash to retrieve the overall xorb hash.
type Encoder struct {
	w          io.Writer
	withFooter bool

	// Accumulated per-chunk metadata
	chunkHashes     []xet.Hash
	chunkSizes      []uint64
	chunkOffsets    []uint64 // cumulative packed byte end-positions (footer only)
	unpackedOffsets []uint64 // cumulative unpacked byte end-positions (footer only)

	packedPos   uint64
	unpackedPos uint64

	finalized bool
	xorbHash  *xet.Hash
	err       error
}

func NewEncoder(w io.Writer, withFooter bool) *Encoder {
	return &Encoder{
		w:          w,
		withFooter: withFooter,
	}
}

// Encode compresses chunk and writes the 8-byte header followed by compressed data to w.
func (e *Encoder) Encode(chunk []byte) error {
	if e.err != nil {
		return e.err
	}
	if e.finalized {
		e.err = fmt.Errorf("encoder already finalized")
		return e.err
	}

	compressedData, compressionType, err := selectBestCompression(chunk)
	if err != nil {
		e.err = fmt.Errorf("failed to compress chunk: %w", err)
		return e.err
	}

	compressedSize := uint32(len(compressedData))
	uncompressedSize := uint32(len(chunk))

	// 8-byte chunk header
	header := [8]byte{
		0,
		byte(compressedSize),
		byte(compressedSize >> 8),
		byte(compressedSize >> 16),
		byte(compressionType),
		byte(uncompressedSize),
		byte(uncompressedSize >> 8),
		byte(uncompressedSize >> 16),
	}
	if _, err := e.w.Write(header[:]); err != nil {
		e.err = fmt.Errorf("failed to write chunk header: %w", err)
		return e.err
	}
	if _, err := e.w.Write(compressedData); err != nil {
		e.err = fmt.Errorf("failed to write chunk data: %w", err)
		return e.err
	}

	e.packedPos += 8 + uint64(compressedSize)
	e.unpackedPos += uint64(uncompressedSize)

	h := xet.ComputeChunkHash(chunk)
	e.chunkHashes = append(e.chunkHashes, h)
	e.chunkSizes = append(e.chunkSizes, uint64(uncompressedSize))

	if e.withFooter {
		e.chunkOffsets = append(e.chunkOffsets, e.packedPos)
		e.unpackedOffsets = append(e.unpackedOffsets, e.unpackedPos)
	}

	return nil
}

// Close writes the footer to w (if withFooter was set) and finalizes the encoder.
// It must be called exactly once after all Encode calls.
func (e *Encoder) Close() error {
	if e.err != nil {
		return e.err
	}
	if e.finalized {
		return nil
	}
	e.finalized = true

	if e.withFooter {
		if err := e.writeFooter(); err != nil {
			e.err = err
			return err
		}
	}

	return nil
}

// SummoryHash returns the overall xorb hash computed from all encoded chunks.
func (e *Encoder) SummoryHash() xet.Hash {
	if e.xorbHash == nil {
		hash := xet.ComputeXorbHash(e.chunkHashes, e.chunkSizes)
		e.xorbHash = &hash
	}
	return *e.xorbHash
}

// writeFooter serializes and writes the full CasObjectInfo footer.
func (e *Encoder) writeFooter() error {
	numChunks := uint32(len(e.chunkHashes))

	// Pre-allocate exact footer size to avoid reallocation.
	// Layout: main header (40) + hash section (12 + numChunks*32) +
	//         boundary section (12 + numChunks*8) + trailer (28) + length (4)
	footerSize := 40 + 12 + int(numChunks)*32 + 12 + int(numChunks)*8 + 28 + 4
	buf := make([]byte, 0, footerSize)

	hash := e.SummoryHash()

	// Main Header: XETBLOB ident (7), version (1), xorb hash (32)
	buf = append(buf, xorbIdentifier[:]...)
	buf = append(buf, 1) // version - MUST be 1 per spec
	buf = append(buf, hash[:]...)

	// Hash Section: XBLBHSH ident (7), version (1), num_chunks (4), chunk hashes
	buf = append(buf, hashSectionIdent[:]...)
	buf = append(buf, 0) // version
	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	for _, h := range e.chunkHashes {
		buf = append(buf, h[:]...)
	}

	// Boundary Section: XBLBBND ident (7), version (1), num_chunks (4), packed offsets, unpacked offsets
	buf = append(buf, boundarySectionIdent[:]...)
	buf = append(buf, 1) // version - MUST be 1 per spec
	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	for _, offset := range e.chunkOffsets {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(offset))
	}
	for _, offset := range e.unpackedOffsets {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(offset))
	}

	// Trailer: num_chunks (4), hash_offset_from_end (4), boundary_offset_from_end (4), reserved (16)
	footerEndPos := len(buf) + 4 + 4 + 4 + 16

	hashOffsetFromEnd := uint32(footerEndPos - 40)
	boundarySectionStart := 40 + 8 + 4 + int(numChunks)*32
	boundaryOffsetFromEnd := uint32(footerEndPos - boundarySectionStart)

	buf = binary.LittleEndian.AppendUint32(buf, numChunks)
	buf = binary.LittleEndian.AppendUint32(buf, hashOffsetFromEnd)
	buf = binary.LittleEndian.AppendUint32(buf, boundaryOffsetFromEnd)
	var reserved [16]byte
	buf = append(buf, reserved[:]...)

	// Trailing footer length (4 bytes)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(buf)))

	_, err := e.w.Write(buf)
	return err
}
