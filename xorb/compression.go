package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/pierrec/lz4/v4"
	"github.com/wzshiming/xet/internal/pool"
)

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
// The result is appended to dst, whose spare capacity must hold uncompressedSize bytes plus a scratch byte.
func decompressLZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	off := len(dst)
	// The spare byte makes the reader consume the frame's end mark, catching over-long frames, trailing bytes and a bad content checksum when the frame declares one.
	if cap(dst)-off < uncompressedSize+1 {
		return dst, fmt.Errorf("output buffer too small: need capacity %d, have %d", off+uncompressedSize+1, cap(dst))
	}
	dst = dst[:off+uncompressedSize+1]

	fr := lz4ReaderPool.Get().(*lz4FrameReader)
	fr.src.Reset(data)
	fr.zr.Reset(&fr.src)

	n, err := io.ReadFull(fr.zr, dst[off:])

	// Release data and the frame's block buffers before pooling.
	fr.zr.Reset(nil)
	fr.src.Reset(nil)
	lz4ReaderPool.Put(fr)

	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return dst[:off], fmt.Errorf("failed to decompress LZ4: %w", err)
	}

	if n != uncompressedSize {
		return dst[:off], fmt.Errorf("decompressed size mismatch: got %d, expected %d", n, uncompressedSize)
	}

	return dst[:off+uncompressedSize], nil
}

// compressByteGrouping4LZ4 applies ByteGrouping4 transform then LZ4 compression.
// The result is appended to dst; pass nil to allocate a new slice. data must fit in a pooled chunk buffer.
func compressByteGrouping4LZ4(dst, data []byte) ([]byte, error) {
	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	transformed, err := applyByteGrouping4(tmp[:0], data)
	if err != nil {
		return dst, err
	}
	return compressLZ4(dst, transformed)
}

// decompressByteGrouping4LZ4 decompresses LZ4 then reverses ByteGrouping4 transform.
// The result is appended to dst, whose spare capacity must hold uncompressedSize bytes.
func decompressByteGrouping4LZ4(dst, data []byte, uncompressedSize int) ([]byte, error) {
	tmp := pool.GetChunkBuf()
	defer pool.PutChunkBuf(tmp)
	decompressed, err := decompressLZ4(tmp[:0], data, uncompressedSize)
	if err != nil {
		return dst, err
	}
	return reverseByteGrouping4(dst, decompressed)
}

// applyByteGrouping4 reorganizes bytes by position within 4-byte groups.
// Original: [A0 A1 A2 A3 | B0 B1 B2 B3 | ...]
// Grouped: [A0 B0 C0 ... | A1 B1 C1 ... | A2 B2 C2 ... | A3 B3 C3 ...]
// The result is appended to dst, whose spare capacity must hold len(data) bytes.
func applyByteGrouping4(dst, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return dst, nil
	}

	off := len(dst)
	if cap(dst)-off < len(data) {
		return dst, fmt.Errorf("output buffer too small: need capacity %d, have %d", off+len(data), cap(dst))
	}
	dst = dst[:off+len(data)]
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

	return dst, nil
}

// reverseByteGrouping4 reverses the ByteGrouping4 transform.
// The result is appended to dst, whose spare capacity must hold len(data) bytes.
func reverseByteGrouping4(dst, data []byte) ([]byte, error) {
	if len(data) == 0 {
		return dst, nil
	}

	off := len(dst)
	if cap(dst)-off < len(data) {
		return dst, fmt.Errorf("output buffer too small: need capacity %d, have %d", off+len(data), cap(dst))
	}
	dst = dst[:off+len(data)]
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

	return dst, nil
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
