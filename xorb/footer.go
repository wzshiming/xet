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

// xorbFooter is a located and structurally validated footer: its raw bytes
// plus the offsets of the per-chunk sections inside them.
type xorbFooter struct {
	buf          []byte
	numChunks    int
	hashBase     int   // first chunk hash (32 bytes each)
	packedBase   int   // first cumulative packed end-offset (4 bytes each)
	unpackedBase int   // first cumulative unpacked end-offset (4 bytes each)
	footerStart  int64 // stream offset where the footer begins
}

// readFooter reads and validates the trailing footer of a full-format xorb.
// The reader may be at any position. Returns ErrNoFooter when the stream does
// not end in a parseable footer.
func readFooter(r io.ReadSeeker) (*xorbFooter, error) {
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

	return &xorbFooter{
		buf:          buf,
		numChunks:    numChunks,
		hashBase:     hOff + 12,
		packedBase:   bOff + 12,
		unpackedBase: bOff + 12 + numChunks*4,
		footerStart:  footerStart,
	}, nil
}

// ReadChunkOffsets extracts the cumulative packed end-offset (including the
// 8-byte per-chunk header) of every chunk from the footer of a full-format
// xorb, without scanning chunk data. The reader may be at any position.
// Returns ErrNoFooter when the stream does not end in a parseable footer.
func ReadChunkOffsets(r io.ReadSeeker) ([]uint64, error) {
	f, err := readFooter(r)
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, f.numChunks)
	prev := uint64(0)
	for i := range offsets {
		off := uint64(binary.LittleEndian.Uint32(f.buf[f.packedBase+i*4:]))
		// Each chunk contributes at least its 8-byte header.
		if off < prev+8 {
			return nil, ErrNoFooter
		}
		offsets[i] = off
		prev = off
	}
	// The final offset must equal the size of the chunk-data region.
	if f.numChunks > 0 && int64(prev) != f.footerStart {
		return nil, ErrNoFooter
	}
	return offsets, nil
}

// ReadChunkUnpackedSizes extracts every chunk's uncompressed byte size from
// the footer's cumulative unpacked offsets. The reader may be at any position.
// Returns ErrNoFooter when the stream does not end in a parseable footer.
func ReadChunkUnpackedSizes(r io.ReadSeeker) ([]uint32, error) {
	f, err := readFooter(r)
	if err != nil {
		return nil, err
	}
	sizes := make([]uint32, f.numChunks)
	prev := uint64(0)
	for i := range sizes {
		off := uint64(binary.LittleEndian.Uint32(f.buf[f.unpackedBase+i*4:]))
		if off < prev || off-prev > xet.MaxChunkSize {
			return nil, ErrNoFooter
		}
		sizes[i] = uint32(off - prev)
		prev = off
	}
	return sizes, nil
}

// ReadChunkHashes extracts every chunk's hash from the footer's XBLBHSH
// section. The reader may be at any position. Returns ErrNoFooter when the
// stream does not end in a parseable footer.
func ReadChunkHashes(r io.ReadSeeker) ([]xet.ChunkHash, error) {
	f, err := readFooter(r)
	if err != nil {
		return nil, err
	}
	hashes := make([]xet.ChunkHash, f.numChunks)
	for i := range hashes {
		hashes[i] = *(*xet.ChunkHash)(f.buf[f.hashBase+i*32:])
	}
	return hashes, nil
}
