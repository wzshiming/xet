package xorb

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

// CompressionType represents the compression type used for a chunk.
type CompressionType byte

const (
	// CompressionNone indicates no compression.
	CompressionNone CompressionType = 0
	// CompressionLZ4 indicates LZ4 frame compression.
	CompressionLZ4 CompressionType = 1
	// CompressionByteGrouping4LZ4 indicates byte grouping + LZ4 compression.
	CompressionByteGrouping4LZ4 CompressionType = 2
)

// CompressLZ4 compresses data using LZ4 frame format.
func CompressLZ4(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressLZ4 decompresses LZ4 frame data.
func DecompressLZ4(compressed []byte, uncompressedSize int) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(compressed))
	out := make([]byte, uncompressedSize)
	if _, err := io.ReadFull(r, out); err != nil {
		return nil, err
	}
	return out, nil
}

// ByteGroup4 performs the byte grouping transformation (Section 7.4.3).
func ByteGroup4(data []byte) []byte {
	n := len(data)
	groups := [4][]byte{}

	baseSize := n / 4
	remainder := n % 4
	for i := 0; i < 4; i++ {
		sz := baseSize
		if i < remainder {
			sz++
		}
		groups[i] = make([]byte, 0, sz)
	}

	for i := 0; i < n; i++ {
		groups[i%4] = append(groups[i%4], data[i])
	}

	result := make([]byte, 0, n)
	for i := 0; i < 4; i++ {
		result = append(result, groups[i]...)
	}
	return result
}

// ByteUngroup4 reverses the byte grouping transformation (Section 7.4.3).
func ByteUngroup4(grouped []byte, originalLength int) []byte {
	n := originalLength
	baseSize := n / 4
	remainder := n % 4

	sizes := [4]int{}
	for i := 0; i < 4; i++ {
		sizes[i] = baseSize
		if i < remainder {
			sizes[i]++
		}
	}

	groupData := [4][]byte{}
	offset := 0
	for i := 0; i < 4; i++ {
		groupData[i] = grouped[offset : offset+sizes[i]]
		offset += sizes[i]
	}

	data := make([]byte, n)
	for i := 0; i < n; i++ {
		groupIdx := i % 4
		posInGroup := i / 4
		data[i] = groupData[groupIdx][posInGroup]
	}
	return data
}

// CompressByteGrouping4LZ4 applies byte grouping followed by LZ4 compression.
func CompressByteGrouping4LZ4(data []byte) ([]byte, error) {
	grouped := ByteGroup4(data)
	return CompressLZ4(grouped)
}

// DecompressByteGrouping4LZ4 decompresses ByteGrouping4LZ4 data.
func DecompressByteGrouping4LZ4(compressed []byte, uncompressedSize int) ([]byte, error) {
	grouped, err := DecompressLZ4(compressed, uncompressedSize)
	if err != nil {
		return nil, err
	}
	return ByteUngroup4(grouped, uncompressedSize), nil
}

// CompressChunk compresses chunk data using the specified compression type.
// Returns the compressed data and the actual compression type used.
// If compression increases size, falls back to CompressionNone.
func CompressChunk(data []byte, preferred CompressionType) ([]byte, CompressionType, error) {
	if preferred == CompressionNone {
		return data, CompressionNone, nil
	}

	var compressed []byte
	var err error

	switch preferred {
	case CompressionLZ4:
		compressed, err = CompressLZ4(data)
	case CompressionByteGrouping4LZ4:
		compressed, err = CompressByteGrouping4LZ4(data)
	default:
		return data, CompressionNone, nil
	}

	if err != nil {
		return nil, 0, err
	}

	// Fall back to no compression if it didn't help
	if len(compressed) >= len(data) {
		return data, CompressionNone, nil
	}

	return compressed, preferred, nil
}

// DecompressChunk decompresses chunk data based on compression type.
func DecompressChunk(data []byte, compressionType CompressionType, uncompressedSize int) ([]byte, error) {
	switch compressionType {
	case CompressionNone:
		return data, nil
	case CompressionLZ4:
		return DecompressLZ4(data, uncompressedSize)
	case CompressionByteGrouping4LZ4:
		return DecompressByteGrouping4LZ4(data, uncompressedSize)
	default:
		return nil, fmt.Errorf("unknown compression type: %d", compressionType)
	}
}
