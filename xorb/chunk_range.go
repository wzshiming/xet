package xorb

import (
	"bytes"
	"fmt"
	"io"
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
