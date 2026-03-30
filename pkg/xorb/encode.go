package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// Encode returns a streaming reader for the xorb serialization.
// If chunkOnly is true, only the chunk data region is written (without footer).
// If chunkOnly is false, the full XETBLOB format with footer is written.
//
// The returned io.Reader streams the data directly without buffering everything in memory.
func Encode(x *Xorb, chunkOnly bool) (io.Reader, error) {
	if chunkOnly {
		return &xorbChunksOnlyReader{
			xorb: x,
		}, nil
	}

	// Build footer for full serialization
	footer, chunkOffsets, unpackedOffsets, err := x.buildFooterData()
	if err != nil {
		return nil, err
	}

	return &xorbReader{
		xorb:            x,
		footer:          footer,
		chunkOffsets:    chunkOffsets,
		unpackedOffsets: unpackedOffsets,
	}, nil
}

// xorbChunksOnlyReader implements io.Reader for chunk-only serialization
type xorbChunksOnlyReader struct {
	xorb      *Xorb
	chunkIdx  int
	buffer    []byte
	bufOffset int
}

func (r *xorbChunksOnlyReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Check if we're done with all chunks
		if r.chunkIdx >= len(r.xorb.Chunks) {
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}

		// If buffer is empty or consumed, prepare next chunk
		if len(r.buffer) == 0 || r.bufOffset >= len(r.buffer) {
			chunk := r.xorb.Chunks[r.chunkIdx]

			// Build chunk header (8 bytes)
			header := ChunkHeader{
				Version:          0,
				CompressedSize:   uint32(len(chunk.CompressedData)),
				CompressionType:  chunk.CompressionType,
				UncompressedSize: uint32(len(chunk.UncompressedData)),
			}

			headerBuf := make([]byte, 8)
			headerBuf[0] = header.Version
			headerBuf[1] = byte(header.CompressedSize & 0xFF)
			headerBuf[2] = byte((header.CompressedSize >> 8) & 0xFF)
			headerBuf[3] = byte((header.CompressedSize >> 16) & 0xFF)
			headerBuf[4] = byte(header.CompressionType)
			headerBuf[5] = byte(header.UncompressedSize & 0xFF)
			headerBuf[6] = byte((header.UncompressedSize >> 8) & 0xFF)
			headerBuf[7] = byte((header.UncompressedSize >> 16) & 0xFF)

			// Combine header and compressed data into a new buffer
			r.buffer = make([]byte, 0, 8+len(chunk.CompressedData))
			r.buffer = append(r.buffer, headerBuf...)
			r.buffer = append(r.buffer, chunk.CompressedData...)
			r.bufOffset = 0
		}

		// Copy from buffer to output
		copied := copy(p[n:], r.buffer[r.bufOffset:])
		n += copied
		r.bufOffset += copied

		// If we've consumed the entire buffer, move to next chunk
		if r.bufOffset >= len(r.buffer) {
			r.chunkIdx++
			r.buffer = nil
			r.bufOffset = 0
		}
	}

	return n, nil
}

// xorbReader implements io.Reader for full xorb serialization (with footer)
type xorbReader struct {
	xorb            *Xorb
	footer          []byte
	chunkOffsets    []uint64
	unpackedOffsets []uint64
	chunkIdx        int
	buffer          []byte
	bufOffset       int
	footerWritten   bool
}

func (r *xorbReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Write chunks first
		if r.chunkIdx < len(r.xorb.Chunks) {
			// If buffer is empty or consumed, prepare next chunk
			if len(r.buffer) == 0 || r.bufOffset >= len(r.buffer) {
				chunk := r.xorb.Chunks[r.chunkIdx]

				// Build chunk header (8 bytes)
				header := ChunkHeader{
					Version:          0,
					CompressedSize:   uint32(len(chunk.CompressedData)),
					CompressionType:  chunk.CompressionType,
					UncompressedSize: uint32(len(chunk.UncompressedData)),
				}

				headerBuf := make([]byte, 8)
				headerBuf[0] = header.Version
				headerBuf[1] = byte(header.CompressedSize & 0xFF)
				headerBuf[2] = byte((header.CompressedSize >> 8) & 0xFF)
				headerBuf[3] = byte((header.CompressedSize >> 16) & 0xFF)
				headerBuf[4] = byte(header.CompressionType)
				headerBuf[5] = byte(header.UncompressedSize & 0xFF)
				headerBuf[6] = byte((header.UncompressedSize >> 8) & 0xFF)
				headerBuf[7] = byte((header.UncompressedSize >> 16) & 0xFF)

				// Combine header and compressed data into a new buffer
				r.buffer = make([]byte, 0, 8+len(chunk.CompressedData))
				r.buffer = append(r.buffer, headerBuf...)
				r.buffer = append(r.buffer, chunk.CompressedData...)
				r.bufOffset = 0
			}

			// Copy from buffer to output
			copied := copy(p[n:], r.buffer[r.bufOffset:])
			n += copied
			r.bufOffset += copied

			// If we've consumed the entire buffer, move to next chunk
			if r.bufOffset >= len(r.buffer) {
				r.chunkIdx++
				r.buffer = nil
				r.bufOffset = 0
			}
			continue
		}

		// Write footer
		if !r.footerWritten || (r.footerWritten && r.bufOffset < len(r.buffer)) {
			if !r.footerWritten {
				// Prepare footer buffer
				footerLenBuf := make([]byte, 4)
				binary.LittleEndian.PutUint32(footerLenBuf, uint32(len(r.footer)))

				r.buffer = make([]byte, 0, len(r.footer)+4)
				r.buffer = append(r.buffer, r.footer...)
				r.buffer = append(r.buffer, footerLenBuf...)
				r.bufOffset = 0
				r.footerWritten = true
			}

			// Copy from buffer to output
			copied := copy(p[n:], r.buffer[r.bufOffset:])
			n += copied
			r.bufOffset += copied

			// If we haven't finished the footer buffer, keep going
			if r.bufOffset < len(r.buffer) {
				continue
			}
		}

		// All done - we've written all chunks and footer
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}

	return n, nil
}

// buildFooterData builds the footer bytes and returns them along with offset arrays
func (x *Xorb) buildFooterData() ([]byte, []uint64, []uint64, error) {
	// Track chunk offsets for boundary section
	chunkOffsets := make([]uint64, len(x.Chunks))
	unpackedOffsets := make([]uint64, len(x.Chunks))
	currentOffset := uint64(0)
	currentUnpacked := uint64(0)

	// Calculate offsets
	for i, chunk := range x.Chunks {
		currentOffset += 8 + uint64(len(chunk.CompressedData))
		currentUnpacked += uint64(len(chunk.UncompressedData))

		// Store END offsets per spec section 7.5.3
		chunkOffsets[i] = currentOffset
		unpackedOffsets[i] = currentUnpacked
	}

	// Build CasObjectInfo footer
	footer, err := x.buildFooter(chunkOffsets, unpackedOffsets)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to build footer: %w", err)
	}

	return footer, chunkOffsets, unpackedOffsets, nil
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
	x.Hash = xet.ComputeXorbHash(x.ChunkHashes, chunkSizes)

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
