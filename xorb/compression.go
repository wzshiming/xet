package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/pierrec/lz4/v4"
	"github.com/wzshiming/xet/internal/pool"
)

// decompressChunk decompresses a chunk using the specified compression type.
// The result is appended to dst; pass nil to allocate a new slice.
// For CompressionNone with nil dst, data is returned directly without copying;
// the caller must not modify the returned slice as it aliases the input.
func decompressChunk(dst, data []byte, compressionType CompressionType, uncompressedSize int) ([]byte, error) {
	switch compressionType {
	case CompressionNone:
		if dst == nil {
			return data, nil
		}
		return append(dst, data...), nil
	case CompressionLZ4:
		return decompressLZ4(dst, data, uncompressedSize)
	case CompressionByteGrouping4:
		return decompressByteGrouping4LZ4(dst, data, uncompressedSize)
	default:
		return dst, fmt.Errorf("unsupported compression type: %d", compressionType)
	}
}

// compressLZ4 compresses data using LZ4 Frame format.
// The result is appended to dst; pass nil to allocate a new slice.
func compressLZ4(dst, data []byte) ([]byte, error) {
	writers := lz4Writers64Kb
	switch {
	case len(data) > 256<<10:
		writers = lz4Writers4Mb
	case len(data) > 64<<10:
		writers = lz4Writers256Kb
	}

	buf := bytes.NewBuffer(dst)
	w := writers.Get().(*lz4.Writer)
	w.Reset(buf)
	defer func() {
		w.Reset(nil)
		writers.Put(w)
	}()

	if _, err := w.Write(data); err != nil {
		return dst, fmt.Errorf("failed to write to LZ4 writer: %w", err)
	}

	if err := w.Close(); err != nil {
		return dst, fmt.Errorf("failed to close LZ4 writer: %w", err)
	}

	return buf.Bytes(), nil
}

// decompressLZ4 decompresses LZ4 Frame format data.
// The result is appended to dst; pass nil to allocate a new slice.
func decompressLZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	off := len(dst)
	dst = slices.Grow(dst, uncompressedSize)[:off+uncompressedSize]

	n, err := readLZ4Frame(dst[off:], data)
	if err != nil {
		return dst[:off], err
	}

	if n != uncompressedSize {
		return dst[:off], fmt.Errorf("decompressed size mismatch: got %d, expected %d", n, uncompressedSize)
	}

	return dst, nil
}

// compressByteGrouping4LZ4 applies ByteGrouping4 transform then LZ4 compression.
// The result is appended to dst; pass nil to allocate a new slice.
func compressByteGrouping4LZ4(dst, data []byte) ([]byte, error) {
	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	transformed := applyByteGrouping4(tmp[:0], data)
	return compressLZ4(dst, transformed)
}

// decompressByteGrouping4LZ4 decompresses LZ4 then reverses ByteGrouping4 transform.
// The result is appended to dst; pass nil to allocate a new slice.
func decompressByteGrouping4LZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	decompressed, err := decompressLZ4(tmp[:0], data, uncompressedSize)
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

	// Plane k holds byte k of every group; planes below len%4 also hold one byte of the partial group.
	groups, tail := len(data)/4, len(data)%4
	var start [4]int
	for k := 1; k < 4; k++ {
		start[k] = start[k-1] + groups
		if k <= tail {
			start[k]++
		}
	}
	p0 := data[start[0]:][:groups]
	p1 := data[start[1]:][:groups]
	p2 := data[start[2]:][:groups]
	p3 := data[start[3]:][:groups]
	for i := range p0 {
		binary.LittleEndian.PutUint32(result[4*i:], uint32(p0[i])|uint32(p1[i])<<8|uint32(p2[i])<<16|uint32(p3[i])<<24)
	}
	for k := range tail {
		result[4*groups+k] = data[start[k]+groups]
	}

	return dst
}

// selectBestCompression tries different compression methods and returns the best one.
// The result is appended to dst; pass nil to allocate a new slice.
func selectBestCompression(dst, data []byte) ([]byte, CompressionType, error) {
	// Try no compression
	noneSize := len(data)
	bestType := CompressionNone
	bestSize := noneSize

	tmp0 := pool.GetChunkBuf()
	tmp1 := pool.GetChunkBuf()
	defer func() {
		pool.PutChunkBuf(tmp0)
		pool.PutChunkBuf(tmp1)
	}()

	// Try LZ4
	lz4Data, err := compressLZ4(tmp0[:0], data)
	if err == nil && len(lz4Data) < bestSize {
		bestType = CompressionLZ4
		bestSize = len(lz4Data)
	}

	// Try ByteGrouping4 + LZ4
	bg4Data, err := compressByteGrouping4LZ4(tmp1[:0], data)
	if err == nil && len(bg4Data) < bestSize {
		bestType = CompressionByteGrouping4
	}

	// Append the best result to dst
	switch bestType {
	case CompressionNone:
		dst = append(dst, data...)
	case CompressionLZ4:
		dst = append(dst, lz4Data...)
	case CompressionByteGrouping4:
		dst = append(dst, bg4Data...)
	}

	return dst, bestType, nil
}

// Frames match xet-core's lz4_flex defaults: hf_xet allocates a frame's declared block size for every chunk it decodes.
var (
	lz4Writers64Kb  = newLZ4WriterPool(lz4.Block64Kb)
	lz4Writers256Kb = newLZ4WriterPool(lz4.Block256Kb)
	lz4Writers4Mb   = newLZ4WriterPool(lz4.Block4Mb)
)

func newLZ4WriterPool(size lz4.BlockSize) *sync.Pool {
	return &sync.Pool{New: func() any {
		w := lz4.NewWriter(nil)
		_ = w.Apply(lz4.BlockSizeOption(size), lz4.ChecksumOption(false))
		return w
	}}
}

// lz4FrameReader keeps a pooled lz4.Reader together with the bytes.Reader it
// decodes from, so that decoding a frame does not allocate a source reader.
type lz4FrameReader struct {
	src bytes.Reader
	zr  *lz4.Reader
}

var lz4ReaderPool = sync.Pool{
	New: func() any {
		fr := new(lz4FrameReader)
		fr.zr = lz4.NewReader(&fr.src)
		return fr
	},
}

// readLZ4Frame decodes the LZ4 frame in data into buf until buf is full or the
// frame ends, and returns the number of bytes decoded.
func readLZ4Frame(buf, data []byte) (int, error) {
	fr := lz4ReaderPool.Get().(*lz4FrameReader)
	fr.src.Reset(data)
	fr.zr.Reset(&fr.src)

	n, err := io.ReadFull(fr.zr, buf)

	// Release data and the frame's block buffers before pooling.
	fr.zr.Reset(nil)
	fr.src.Reset(nil)
	lz4ReaderPool.Put(fr)

	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return n, fmt.Errorf("failed to decompress LZ4: %w", err)
	}
	return n, nil
}
