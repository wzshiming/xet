package xorb

import (
	"fmt"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/pool"
)

// ChunkHeaderSize is the size of the header that precedes each chunk's compressed data.
const ChunkHeaderSize = 8

// CompressionType identifies how a chunk's data is compressed.
type CompressionType uint8

const (
	CompressionNone          CompressionType = 0 // No compression
	CompressionLZ4           CompressionType = 1 // LZ4 Frame format
	CompressionByteGrouping4 CompressionType = 2 // ByteGrouping4 + LZ4
)

// ChunkHeader describes the compressed data that follows it in an xorb.
type ChunkHeader struct {
	CompressedSize   uint32
	UncompressedSize uint32
	Compression      CompressionType
}

// ParseChunkHeader parses the chunk header at the start of b:
//
//	[0]    version
//	[1-3]  compressed size (little-endian, 3 bytes)
//	[4]    compression type
//	[5-7]  uncompressed size (little-endian, 3 bytes)
//
// The compression type is not validated; DecodeChunk rejects unknown types.
func ParseChunkHeader(b []byte) (ChunkHeader, error) {
	if len(b) < ChunkHeaderSize {
		return ChunkHeader{}, fmt.Errorf("chunk header too short: need %d bytes, have %d", ChunkHeaderSize, len(b))
	}
	if b[0] != 0 {
		return ChunkHeader{}, fmt.Errorf("unsupported chunk version: %d", b[0])
	}
	h := ChunkHeader{
		CompressedSize:   uint32(b[1]) | uint32(b[2])<<8 | uint32(b[3])<<16,
		Compression:      CompressionType(b[4]),
		UncompressedSize: uint32(b[5]) | uint32(b[6])<<8 | uint32(b[7])<<16,
	}
	if h.UncompressedSize > xet.MaxChunkSize {
		return ChunkHeader{}, fmt.Errorf("invalid uncompressed chunk size: %d exceeds maximum %d", h.UncompressedSize, xet.MaxChunkSize)
	}
	return h, nil
}

// DecodeChunk decompresses body, the compressed data described by h. A
// CompressionNone result is body itself; dst is ignored and may be nil. An LZ4
// or ByteGrouping4 result is dst[:h.UncompressedSize], so dst must be at least
// that long. LZ4 decodes straight into dst when cap(dst) > h.UncompressedSize.
// The spare byte makes the reader consume the frame's end mark and is scratch
// that may be written even on success; nothing past it is ever written. On
// failure DecodeChunk returns nil and writes nothing into dst, except partial
// output from a direct decode. It is safe for concurrent use.
func DecodeChunk(dst []byte, h ChunkHeader, body []byte) ([]byte, error) {
	if len(body) != int(h.CompressedSize) {
		return nil, fmt.Errorf("chunk body size mismatch: header declares %d bytes, have %d", h.CompressedSize, len(body))
	}
	if h.Compression != CompressionNone && len(dst) < int(h.UncompressedSize) {
		return nil, fmt.Errorf("output buffer too small: need %d bytes, have %d", h.UncompressedSize, len(dst))
	}
	if h.UncompressedSize > xet.MaxChunkSize {
		return nil, fmt.Errorf("invalid uncompressed chunk size: %d exceeds maximum %d", h.UncompressedSize, xet.MaxChunkSize)
	}
	size := int(h.UncompressedSize)

	switch h.Compression {
	case CompressionNone:
		if len(body) != size {
			return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", len(body), size)
		}
		return body, nil
	case CompressionLZ4, CompressionByteGrouping4:
		direct := h.Compression == CompressionLZ4 && cap(dst) > size
		buf := dst
		if !direct {
			tmp := pool.GetChunkBuf()
			defer pool.PutChunkBuf(tmp)
			buf = tmp[:]
		}
		// The spare byte makes the reader consume the frame's end mark, catching over-long frames, trailing bytes and a bad content checksum when the frame declares one.
		n, err := readLZ4Frame(buf[:size+1], body)
		if err != nil {
			return nil, err
		}
		if n != size {
			return nil, fmt.Errorf("decompressed size mismatch: got %d, expected %d", n, size)
		}
		if h.Compression == CompressionByteGrouping4 {
			return reverseByteGrouping4(dst[:0], buf[:size]), nil
		}
		if !direct {
			copy(dst, buf[:size])
		}
		return dst[:size], nil
	default:
		return nil, fmt.Errorf("unsupported compression type: %d", h.Compression)
	}
}
