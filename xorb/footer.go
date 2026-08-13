package xorb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// ErrNoFooter indicates the xorb stream is chunk-only format (or its trailing
// bytes do not form a parseable footer), so chunk offsets must be recovered by
// scanning chunk headers instead.
var ErrNoFooter = errors.New("xorb has no footer")

// minFooterSize is the serialized footer size for a zero-chunk xorb:
// main header (40) + hash section (12) + boundary section (12) + trailer (28) +
// length field (4).
const minFooterSize = 96

// ReadChunkOffsets extracts the cumulative packed end-offset (including the
// 8-byte per-chunk header) of every chunk from the footer of a full-format
// xorb, without scanning chunk data. The reader may be at any position.
// Returns ErrNoFooter when the stream does not end in a parseable footer.
func ReadChunkOffsets(r io.ReadSeeker) ([]uint64, error) {
	size, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("seek xorb end: %w", err)
	}
	if size < minFooterSize {
		return nil, ErrNoFooter
	}

	var lenBuf [4]byte
	if _, err := r.Seek(size-4, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek footer length: %w", err)
	}
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read footer length: %w", err)
	}
	footerLen := int64(binary.LittleEndian.Uint32(lenBuf[:]))

	// footerLen counts all footer bytes except the length field itself and
	// encodes the chunk count: 92 fixed bytes plus 40 per chunk.
	total := footerLen + 4
	if footerLen < minFooterSize-4 || total > size || (footerLen-92)%40 != 0 {
		return nil, ErrNoFooter
	}
	numChunks := int((footerLen - 92) / 40)
	if numChunks > xet.MaxChunksPerXorb {
		return nil, ErrNoFooter
	}

	footerStart := size - total
	if _, err := r.Seek(footerStart, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek footer start: %w", err)
	}
	buf := make([]byte, total)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}

	// Main header: XETBLOB ident (7) + version (1) + xorb hash (32).
	if !bytes.Equal(buf[:7], xorbIdentifier[:]) || buf[7] != 1 {
		return nil, ErrNoFooter
	}
	// Hash section: XBLBHSH ident (7) + version (1) + num_chunks (4) + hashes (32*N).
	const hOff = 40
	if !bytes.Equal(buf[hOff:hOff+7], hashSectionIdent[:]) || buf[hOff+7] != 0 {
		return nil, ErrNoFooter
	}
	if int(binary.LittleEndian.Uint32(buf[hOff+8:hOff+12])) != numChunks {
		return nil, ErrNoFooter
	}
	// Boundary section: XBLBBND ident (7) + version (1) + num_chunks (4) +
	// packed offsets (4*N) + unpacked offsets (4*N).
	bOff := hOff + 12 + numChunks*32
	if !bytes.Equal(buf[bOff:bOff+7], boundarySectionIdent[:]) || buf[bOff+7] != 1 {
		return nil, ErrNoFooter
	}
	if int(binary.LittleEndian.Uint32(buf[bOff+8:bOff+12])) != numChunks {
		return nil, ErrNoFooter
	}

	packedBase := bOff + 12
	offsets := make([]uint64, numChunks)
	prev := uint64(0)
	for i := range offsets {
		off := uint64(binary.LittleEndian.Uint32(buf[packedBase+i*4:]))
		// Each chunk contributes at least its 8-byte header.
		if off < prev+8 {
			return nil, ErrNoFooter
		}
		offsets[i] = off
		prev = off
	}
	// The final offset must equal the size of the chunk-data region.
	if numChunks > 0 && int64(prev) != footerStart {
		return nil, ErrNoFooter
	}
	return offsets, nil
}
