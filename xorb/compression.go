package xorb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// CompressionType represents the type of compression used
type CompressionType uint8

const (
	CompressionNone          CompressionType = 0 // No compression
	CompressionLZ4           CompressionType = 1 // LZ4 Frame format
	CompressionByteGrouping4 CompressionType = 2 // ByteGrouping4 + LZ4
)

// decompressChunk decompresses a chunk using the specified compression type
func decompressChunk(data []byte, compressionType CompressionType, uncompressedSize int) ([]byte, error) {
	switch compressionType {
	case CompressionNone:
		return data, nil
	case CompressionLZ4:
		return decompressLZ4(data, uncompressedSize)
	case CompressionByteGrouping4:
		return decompressByteGrouping4LZ4(data, uncompressedSize)
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// compressLZ4 compresses data using LZ4 Frame format
func compressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)

	_, err := w.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to write to LZ4 writer: %w", err)
	}

	err = w.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close LZ4 writer: %w", err)
	}

	return buf.Bytes(), nil
}

// decompressLZ4 decompresses LZ4 Frame format data
func decompressLZ4(data []byte, uncompressedSize int) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	result := make([]byte, uncompressedSize)

	// Use io.ReadFull to ensure we read all expected bytes
	// A single Read() call may return fewer bytes without error
	n, err := io.ReadFull(r, result)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to decompress LZ4: %w", err)
	}

	if n != uncompressedSize {
		return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", n, uncompressedSize)
	}

	return result, nil
}

// compressByteGrouping4LZ4 applies ByteGrouping4 transform then LZ4 compression
func compressByteGrouping4LZ4(data []byte) ([]byte, error) {
	transformed := applyByteGrouping4(data)
	return compressLZ4(transformed)
}

// decompressByteGrouping4LZ4 decompresses LZ4 then reverses ByteGrouping4 transform
func decompressByteGrouping4LZ4(data []byte, uncompressedSize int) ([]byte, error) {
	decompressed, err := decompressLZ4(data, uncompressedSize)
	if err != nil {
		return nil, err
	}
	return reverseByteGrouping4(decompressed), nil
}

// applyByteGrouping4 reorganizes bytes by position within 4-byte groups
// Original: [A0 A1 A2 A3 | B0 B1 B2 B3 | ...]
// Grouped: [A0 B0 C0 ... | A1 B1 C1 ... | A2 B2 C2 ... | A3 B3 C3 ...]
func applyByteGrouping4(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	result := make([]byte, len(data))
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

	return result
}

// reverseByteGrouping4 reverses the ByteGrouping4 transform
func reverseByteGrouping4(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	result := make([]byte, len(data))
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

	return result
}

// selectBestCompression tries different compression methods and returns the best one
func selectBestCompression(data []byte) ([]byte, CompressionType, error) {
	// Try no compression
	noneSize := len(data)
	best := data
	bestType := CompressionNone
	bestSize := noneSize

	// Try LZ4
	lz4Data, err := compressLZ4(data)
	if err == nil && len(lz4Data) < bestSize {
		best = lz4Data
		bestType = CompressionLZ4
		bestSize = len(lz4Data)
	}

	// Try ByteGrouping4 + LZ4
	bg4Data, err := compressByteGrouping4LZ4(data)
	if err == nil && len(bg4Data) < bestSize {
		best = bg4Data
		bestType = CompressionByteGrouping4
		bestSize = len(bg4Data)
	}

	return best, bestType, nil
}
