package xorb

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/pierrec/lz4/v4"
)

// compressionType represents the type of compression used
type compressionType uint8

const (
	compressionNone          compressionType = 0 // No compression
	compressionLZ4           compressionType = 1 // LZ4 Frame format
	compressionByteGrouping4 compressionType = 2 // ByteGrouping4 + LZ4
)

// decompressChunk decompresses a chunk using the specified compression type.
// The result is appended to dst; pass nil to allocate a new slice.
func decompressChunk(dst, data []byte, compressionType compressionType, uncompressedSize int) ([]byte, error) {
	switch compressionType {
	case compressionNone:
		return append(dst, data...), nil
	case compressionLZ4:
		return decompressLZ4(dst, data, uncompressedSize)
	case compressionByteGrouping4:
		return decompressByteGrouping4LZ4(dst, data, uncompressedSize)
	default:
		return dst, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// lz4WriterPool pools lz4.Writer objects to avoid per-call allocation.
var lz4WriterPool sync.Pool

// compressLZ4 compresses data using LZ4 Frame format.
// The result is appended to dst; pass nil to allocate a new slice.
func compressLZ4(dst, data []byte) ([]byte, error) {
	buf := bytes.NewBuffer(dst)

	var w *lz4.Writer
	if v := lz4WriterPool.Get(); v != nil {
		w = v.(*lz4.Writer)
		w.Reset(buf)
	} else {
		w = lz4.NewWriter(buf)
	}

	if _, err := w.Write(data); err != nil {
		return dst, fmt.Errorf("failed to write to LZ4 writer: %w", err)
	}

	if err := w.Close(); err != nil {
		return dst, fmt.Errorf("failed to close LZ4 writer: %w", err)
	}

	lz4WriterPool.Put(w)
	return buf.Bytes(), nil
}

// lz4ReaderPool pools lz4.Reader objects to avoid per-call allocation.
var lz4ReaderPool sync.Pool

// decompressLZ4 decompresses LZ4 Frame format data.
// The result is appended to dst; pass nil to allocate a new slice.
func decompressLZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	br := bytes.NewReader(data)

	var r *lz4.Reader
	if v := lz4ReaderPool.Get(); v != nil {
		r = v.(*lz4.Reader)
		r.Reset(br)
	} else {
		r = lz4.NewReader(br)
	}

	off := len(dst)
	dst = slices.Grow(dst, uncompressedSize)[:off+uncompressedSize]

	n, err := io.ReadFull(r, dst[off:])
	lz4ReaderPool.Put(r)

	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return dst[:off], fmt.Errorf("failed to decompress LZ4: %w", err)
	}

	if n != uncompressedSize {
		return dst[:off], fmt.Errorf("decompressed size mismatch: got %d, expected %d", n, uncompressedSize)
	}

	return dst, nil
}

// compressByteGrouping4LZ4 applies ByteGrouping4 transform then LZ4 compression.
// The result is appended to dst; pass nil to allocate a new slice.
func compressByteGrouping4LZ4(dst, data []byte) ([]byte, error) {
	transformed := applyByteGrouping4(nil, data)
	return compressLZ4(dst, transformed)
}

// decompressByteGrouping4LZ4 decompresses LZ4 then reverses ByteGrouping4 transform.
// The result is appended to dst; pass nil to allocate a new slice.
func decompressByteGrouping4LZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	decompressed, err := decompressLZ4(nil, data, uncompressedSize)
	if err != nil {
		return dst, err
	}
	return reverseByteGrouping4(dst, decompressed), nil
}

// applyByteGrouping4 reorganizes bytes by position within 4-byte groups.
// Original: [A0 A1 A2 A3 | B0 B1 B2 B3 | ...]
// Grouped: [A0 B0 C0 ... | A1 B1 C1 ... | A2 B2 C2 ... | A3 B3 C3 ...]
// The result is appended to dst; pass nil to allocate a new slice.
func applyByteGrouping4(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	off := len(dst)
	dst = slices.Grow(dst, len(data))[:off+len(data)]
	result := dst[off:]

	writePos := 0

	// Process each byte position within groups (0-3)
	for bytePos := range 4 {
		// Collect bytes at this position from all groups
		for groupStart := 0; groupStart < len(data); groupStart += 4 {
			idx := groupStart + bytePos
			if idx < len(data) {
				result[writePos] = data[idx]
				writePos++
			}
		}
	}

	return dst
}

// reverseByteGrouping4 reverses the ByteGrouping4 transform.
// The result is appended to dst; pass nil to allocate a new slice.
func reverseByteGrouping4(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	off := len(dst)
	dst = slices.Grow(dst, len(data))[:off+len(data)]
	result := dst[off:]

	numFullGroups := len(data) / 4
	remainder := len(data) % 4

	readPos := 0

	// Process each byte position within groups (0-3)
	for bytePos := range 4 {
		// Calculate how many groups have this byte position
		numGroups := numFullGroups
		if bytePos < remainder {
			numGroups++
		}

		// Scatter bytes at this position to all groups
		for g := 0; g < numGroups; g++ {
			if readPos < len(data) {
				result[g*4+bytePos] = data[readPos]
				readPos++
			}
		}
	}

	return dst
}

// selectBestCompression tries different compression methods and returns the best one.
// The result is appended to dst; pass nil to allocate a new slice.
func selectBestCompression(dst, data []byte) ([]byte, compressionType, error) {
	// Try no compression
	noneSize := len(data)
	bestType := compressionNone
	bestSize := noneSize

	// Try LZ4
	lz4Data, err := compressLZ4(nil, data)
	if err == nil && len(lz4Data) < bestSize {
		bestType = compressionLZ4
		bestSize = len(lz4Data)
	}

	// Try ByteGrouping4 + LZ4
	bg4Data, err := compressByteGrouping4LZ4(nil, data)
	if err == nil && len(bg4Data) < bestSize {
		bestType = compressionByteGrouping4
	}

	// Append the best result to dst
	switch bestType {
	case compressionNone:
		dst = append(dst, data...)
	case compressionLZ4:
		dst = append(dst, lz4Data...)
	case compressionByteGrouping4:
		dst = append(dst, bg4Data...)
	}

	return dst, bestType, nil
}
