package xorb

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Encode returns a streaming reader for the xorb serialization.
func (x *Xorb) Encode(withFooter bool) (io.Reader, error) {
	r := &xorbReader{
		xorb:       x,
		withFooter: withFooter,
	}
	if withFooter {
		// Pre-allocate offset slices with known capacity to avoid incremental growth.
		n := len(x.chunks)
		r.chunkOffsets = make([]uint64, 0, n)
		r.unpackedOffsets = make([]uint64, 0, n)
	}
	return r, nil
}

// xorbReader implements io.Reader for xorb serialization.
// If withFooter is true, the full XETBLOB format with footer is written;
// otherwise only the chunk data region is written.
type xorbReader struct {
	xorb            *Xorb
	withFooter      bool
	chunkIdx        int
	buffer          []byte
	bufOffset       int
	footerWritten   bool
	chunkOffsets    []uint64
	unpackedOffsets []uint64
}

func (r *xorbReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Write chunks first
		if r.chunkIdx < len(r.xorb.chunks) {
			// If buffer is empty or consumed, prepare next chunk
			if r.bufOffset >= len(r.buffer) {
				if err := r.loadChunk(); err != nil {
					return 0, err
				}
			}

			// Copy from buffer to output
			copied := copy(p[n:], r.buffer[r.bufOffset:])
			n += copied
			r.bufOffset += copied

			// If we've consumed the entire buffer, move to next chunk.
			if r.bufOffset >= len(r.buffer) {
				r.chunkIdx++
				r.buffer = r.buffer[:0] // retain capacity for next chunk
				r.bufOffset = 0
			}
			continue
		}

		// Write footer if present
		if r.withFooter {
			if !r.footerWritten {
				if err := r.buildFooter(); err != nil {
					return 0, fmt.Errorf("failed to build footer: %w", err)
				}
				r.bufOffset = 0
				r.footerWritten = true
			}
			if r.bufOffset < len(r.buffer) {
				copied := copy(p[n:], r.buffer[r.bufOffset:])
				n += copied
				r.bufOffset += copied
				if r.bufOffset < len(r.buffer) {
					continue
				}
			}
		}

		// All done
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}

	return n, nil
}

// loadChunk compresses the current chunk and writes its header + data into r.buffer.
func (r *xorbReader) loadChunk() error {
	// Use a pointer into the slice so that lazy materialization is cached.
	chunk := &r.xorb.chunks[r.chunkIdx]

	compressedData, compressionType, err := chunk.compressedData()
	if err != nil {
		return fmt.Errorf("failed to compress chunk: %w", err)
	}

	// Record end offsets for footer construction (only needed when writing footer).
	if r.withFooter {
		var prevPacked, prevUnpacked uint64
		if len(r.chunkOffsets) > 0 {
			prevPacked = r.chunkOffsets[len(r.chunkOffsets)-1]
			prevUnpacked = r.unpackedOffsets[len(r.unpackedOffsets)-1]
		}
		r.chunkOffsets = append(r.chunkOffsets, prevPacked+8+uint64(len(compressedData)))
		r.unpackedOffsets = append(r.unpackedOffsets, prevUnpacked+uint64(chunk.size()))
	}

	compressedSize := uint32(len(compressedData))
	uncompressedSize := chunk.size()

	// Reuse buffer capacity; grow only if needed to avoid repeated allocations.
	need := 8 + len(compressedData)
	if cap(r.buffer) < need {
		r.buffer = make([]byte, 0, need)
	} else {
		r.buffer = r.buffer[:0]
	}

	// Build chunk header (8 bytes):
	//   [0]    version (0)
	//   [1-3]  compressed size (little-endian, 3 bytes)
	//   [4]    compression type
	//   [5-7]  uncompressed size (little-endian, 3 bytes)
	r.buffer = append(r.buffer,
		0,
		byte(compressedSize),
		byte(compressedSize>>8),
		byte(compressedSize>>16),
		byte(compressionType),
		byte(uncompressedSize),
		byte(uncompressedSize>>8),
		byte(uncompressedSize>>16),
	)
	r.buffer = append(r.buffer, compressedData...)
	r.bufOffset = 0
	return nil
}

// buildFooter writes the full CasObjectInfo footer (including trailing length) directly into r.buffer.
func (r *xorbReader) buildFooter() error {
	x := r.xorb
	numChunks := uint32(len(x.chunks))

	// Pre-allocate the exact footer size to avoid any reallocations.
	// Layout: main header (40) + hash section (12 + numChunks*32) + boundary section (12 + numChunks*8) + trailer (28) + length (4)
	footerSize := 40 + 12 + int(numChunks)*32 + 12 + int(numChunks)*8 + 28 + 4
	if cap(r.buffer) < footerSize {
		r.buffer = make([]byte, 0, footerSize)
	} else {
		r.buffer = r.buffer[:0]
	}

	// Main Header: XETBLOB ident (7 bytes), version (1), xorb hash (32 bytes)
	hash, err := x.Hash()
	if err != nil {
		return fmt.Errorf("failed to compute xorb hash: %w", err)
	}
	r.buffer = append(r.buffer, xorbIdentifier[:]...)
	r.buffer = append(r.buffer, 1) // version - MUST be 1 per spec
	r.buffer = append(r.buffer, hash[:]...)

	// Hash Section: XBLBHSH ident (7 bytes), version (1), num_chunks (4 bytes), chunk hashes
	r.buffer = append(r.buffer, hashSectionIdent[:]...)
	r.buffer = append(r.buffer, 0) // version
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, numChunks)
	for i := range x.chunks {
		h, err := x.chunks[i].Hash()
		if err != nil {
			return fmt.Errorf("failed to compute chunk hash: %w", err)
		}
		r.buffer = append(r.buffer, h[:]...)
	}

	// Boundary Section: XBLBBND ident (7 bytes), version (1), num_chunks (4 bytes), chunk offsets
	r.buffer = append(r.buffer, boundarySectionIdent[:]...)
	r.buffer = append(r.buffer, 1) // version - MUST be 1 per spec
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, numChunks)
	for _, offset := range r.chunkOffsets {
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(offset))
	}
	for _, offset := range r.unpackedOffsets {
		r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(offset))
	}

	// Trailer: num_chunks (4), hash_offset_from_end (4), boundary_offset_from_end (4), reserved (16)
	footerEndPos := len(r.buffer) + 4 + 4 + 4 + 16 // current pos + trailer fields

	// Hash section starts at position 40 (after main header)
	hashOffsetFromEnd := uint32(footerEndPos - 40)

	// Boundary section starts after hash section
	boundarySectionStart := 40 + 8 + 4 + int(numChunks)*32 // main header + hash header + num_chunks + hashes
	boundaryOffsetFromEnd := uint32(footerEndPos - boundarySectionStart)

	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, numChunks)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, hashOffsetFromEnd)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, boundaryOffsetFromEnd)
	var reserved [16]byte
	r.buffer = append(r.buffer, reserved[:]...) // reserved

	// Trailing footer length (4 bytes)
	r.buffer = binary.LittleEndian.AppendUint32(r.buffer, uint32(len(r.buffer)))

	return nil
}
