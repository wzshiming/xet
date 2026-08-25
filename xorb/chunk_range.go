package xorb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
)

// ChunkDataRange scans the chunk-data region of an xorb stream and returns
// the [startByte, endByte] byte range (inclusive) that covers chunks
// [chunkStart, chunkEnd).  The range includes the 8-byte per-chunk header so
// that a recipient can parse compression metadata from a partial download.
// Returns (0, 0, nil) when the requested range is out of bounds.
//
// The scan stops as soon as chunk chunkEnd-1 is found, so it is efficient
// when the requested range is near the beginning of the stream.
func ChunkDataRange(r io.ReadSeeker, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	if chunkStart >= chunkEnd {
		return 0, 0, fmt.Errorf("invalid chunk range: chunkStart (%d) must be less than chunkEnd (%d)", chunkStart, chunkEnd)
	}

	offset := int64(0)
	var headerBuf [8]byte

	for idx := uint32(0); ; idx++ {
		n, err := io.ReadFull(r, headerBuf[:])
		if err != nil {
			return 0, 0, err
		}

		// Stop at XETBLOB footer (full-format stored xorbs)
		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			return 0, 0, fmt.Errorf("reached footer before finding chunkEnd: chunkEnd=%d, totalChunks=%d", chunkEnd, idx)
		}

		headerStart := offset
		compressedSize := int64(headerBuf[1]) | int64(headerBuf[2])<<8 | int64(headerBuf[3])<<16
		offset += 8

		if _, err := r.Seek(compressedSize, io.SeekCurrent); err != nil {
			return 0, 0, err
		}
		offset += compressedSize

		if idx == chunkStart {
			startByte = headerStart
		}
		if idx == chunkEnd-1 {
			return startByte, offset - 1, nil
		}
	}
}

// ScanChunkOffsets scans every chunk header and returns the cumulative packed
// end-offset (including the 8-byte header) of each chunk. It rewinds to the
// stream start first and stops at EOF (chunk-only format) or at the footer.
func ScanChunkOffsets(r io.ReadSeeker) ([]uint64, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind xorb: %w", err)
	}

	var offsets []uint64
	var headerBuf [8]byte
	offset := int64(0)

	for {
		n, err := io.ReadFull(r, headerBuf[:])
		if err == io.EOF {
			return offsets, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}

		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			return offsets, nil
		}

		if len(offsets) >= xet.MaxChunksPerXorb {
			return nil, fmt.Errorf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb)
		}

		compressedSize := int64(headerBuf[1]) | int64(headerBuf[2])<<8 | int64(headerBuf[3])<<16
		if _, err := r.Seek(compressedSize, io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("seek past chunk data: %w", err)
		}
		offset += 8 + compressedSize
		offsets = append(offsets, uint64(offset))
	}
}

// ScanChunkUnpackedSizes scans every chunk header and returns each chunk's
// uncompressed byte size. It rewinds to the stream start first and stops at
// EOF (chunk-only format) or at the footer.
func ScanChunkUnpackedSizes(r io.ReadSeeker) ([]uint32, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind xorb: %w", err)
	}

	var sizes []uint32
	var headerBuf [8]byte

	for {
		n, err := io.ReadFull(r, headerBuf[:])
		if err == io.EOF {
			return sizes, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read chunk header: %w", err)
		}

		if n >= 7 && bytes.Equal(headerBuf[:7], xorbIdentifier[:]) {
			return sizes, nil
		}

		if len(sizes) >= xet.MaxChunksPerXorb {
			return nil, fmt.Errorf("chunk count exceeds maximum %d", xet.MaxChunksPerXorb)
		}

		compressedSize := int64(headerBuf[1]) | int64(headerBuf[2])<<8 | int64(headerBuf[3])<<16
		if _, err := r.Seek(compressedSize, io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("seek past chunk data: %w", err)
		}
		sizes = append(sizes, uint32(headerBuf[5])|uint32(headerBuf[6])<<8|uint32(headerBuf[7])<<16)
	}
}

// ChunkDataRangeFromOffsets computes the same inclusive [startByte, endByte]
// range as ChunkDataRange from cumulative packed end-offsets, without stream
// access.
func ChunkDataRangeFromOffsets(offsets []uint64, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	if chunkStart >= chunkEnd {
		return 0, 0, fmt.Errorf("invalid chunk range: chunkStart (%d) must be less than chunkEnd (%d)", chunkStart, chunkEnd)
	}
	if uint64(chunkEnd) > uint64(len(offsets)) {
		return 0, 0, fmt.Errorf("chunk range [%d, %d) out of bounds: xorb has %d chunks", chunkStart, chunkEnd, len(offsets))
	}
	if chunkStart > 0 {
		startByte = int64(offsets[chunkStart-1])
	}
	return startByte, int64(offsets[chunkEnd-1]) - 1, nil
}
