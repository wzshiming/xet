package xorb

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/pierrec/lz4/v4"
	"github.com/wzshiming/xet/internal/pool"
)

var compressLZ4Sink atomic.Pointer[byte]

// compressLZ4 compresses data using LZ4 Frame format.
// The result is appended to dst; pass nil to allocate a new slice.
func compressLZ4(dst, data []byte) ([]byte, error) {
	// DO NOT MERGE: one extra heap allocation per call, for the benchmark gate's sensitivity check.
	compressLZ4Sink.Store(new(byte))

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

	// Plane k holds byte k of every group and, below tail, one byte of the partial group, so it starts at k*groups+min(k, tail).
	groups, tail := len(data)/4, len(data)%4
	p0 := result[:groups]
	p1 := result[groups+min(1, tail):][:groups]
	p2 := result[2*groups+min(2, tail):][:groups]
	p3 := result[3*groups+min(3, tail):][:groups]
	deinterleave4(p0, p1, p2, p3, data)
	for k := range tail {
		result[(k+1)*groups+k] = data[4*groups+k]
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

	// Plane k holds byte k of every group and, below tail, one byte of the partial group, so it starts at k*groups+min(k, tail).
	groups, tail := len(data)/4, len(data)%4
	p0 := data[:groups]
	p1 := data[groups+min(1, tail):][:groups]
	p2 := data[2*groups+min(2, tail):][:groups]
	p3 := data[3*groups+min(3, tail):][:groups]
	interleave4(result, p0, p1, p2, p3)
	for k := range tail {
		result[4*groups+k] = data[(k+1)*groups+k]
	}

	return dst, nil
}

// schemeCompressors holds the compressor of each scheme chooseCompression picks; tests replace one to make it fail.
var schemeCompressors = [...]func(dst, data []byte) ([]byte, error){
	CompressionLZ4:           compressLZ4,
	CompressionByteGrouping4: compressByteGrouping4LZ4,
}

// compressChunk appends data compressed once with scheme, LZ4 or ByteGrouping4, to dst, or data itself as
// CompressionNone when, as in xet-core's serialize_chunk, that does not make it smaller.
func compressChunk(dst, data []byte, scheme CompressionType) ([]byte, CompressionType, error) {
	out, err := schemeCompressors[scheme](dst, data)
	if err != nil {
		return dst, scheme, err
	}
	if len(out)-len(dst) >= len(data) {
		return append(dst, data...), CompressionNone, nil
	}
	return out, scheme, nil
}

// chooseCompression ports xet-core's CompressionScheme::choose_from_data: ByteGrouping4 when the BG4 predictor
// recommends it, LZ4 otherwise. xet-core chose the 0.02 threshold by logistic regression.
func chooseCompression(data []byte) CompressionType {
	if bg4MaxKLDivergence(data) > 0.02 {
		return CompressionByteGrouping4
	}
	return CompressionLZ4
}

// bg4MaxKLDivergence ports xet-core's BG4Predictor: per byte position mod 4 a histogram of byte popcounts, and the
// largest KL divergence of a position's histogram from the pooled one, with the arithmetic of bg4_recommended.
func bg4MaxKLDivergence(data []byte) float64 {
	histograms := popcountHistograms(data)

	// Each pooled bin starts at 1 so q is never 0; the pooled total leaves those 9 out.
	baseCounts := [9]uint32{1, 1, 1, 1, 1, 1, 1, 1, 1}
	var totals [4]uint32
	for i := range 4 {
		for j := range 9 {
			baseCounts[j] += histograms[i][j]
			totals[i] += histograms[i][j]
		}
	}
	baseTotal := totals[0] + totals[1] + totals[2] + totals[3]

	maxKL := 0.0
	for i := range 4 {
		kl := 0.0
		for j := range 9 {
			p := float64(histograms[i][j]) / float64(totals[i])
			q := float64(baseCounts[j]) / float64(baseTotal)
			// The conversion rounds the product, as Rust does, rather than letting Go fuse it into an FMA.
			kl += float64(p * math.Log(p/q))
		}
		// An empty bin makes kl NaN (0·ln 0); Rust's f64::max skips NaN, math.Max would return it.
		if kl > maxKL {
			maxKL = kl
		}
	}
	return maxKL
}

// popcountFields[b] is 1 in the 7-bit field of b's popcount, so adding up to 127 of them counts each popcount.
var popcountFields = func() (fields [256]uint64) {
	for b := range fields {
		fields[b] = 1 << (7 * bits.OnesCount8(uint8(b)))
	}
	return fields
}()

// popcountHistograms counts the bytes of data with each popcount 0-8 per byte position mod 4, the histograms of
// xet-core's BG4Predictor, adding popcountFields in registers rather than incrementing a counter per byte.
func popcountHistograms(data []byte) (histograms [4][9]uint32) {
	for len(data) >= 4 {
		// At most 127 groups of 4 bytes, so no 7-bit field overflows.
		block := data[:min(len(data)&^3, 127*4)]
		data = data[len(block):]
		var p0, p1, p2, p3 uint64
		for ; len(block) >= 4; block = block[4:] {
			p0 += popcountFields[block[0]]
			p1 += popcountFields[block[1]]
			p2 += popcountFields[block[2]]
			p3 += popcountFields[block[3]]
		}
		for j := range 9 {
			histograms[0][j] += uint32(p0>>(7*j)) & 0x7f
			histograms[1][j] += uint32(p1>>(7*j)) & 0x7f
			histograms[2][j] += uint32(p2>>(7*j)) & 0x7f
			histograms[3][j] += uint32(p3>>(7*j)) & 0x7f
		}
	}
	for i, b := range data {
		histograms[i][bits.OnesCount8(b)]++
	}
	return histograms
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
