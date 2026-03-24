package xorb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/pierrec/lz4/v4"
)

// CompressionScheme represents the compression algorithm used for xorb data.
type CompressionScheme uint8

const (
	CompressionNone              CompressionScheme = 0
	CompressionLZ4               CompressionScheme = 1
	CompressionByteGrouping4LZ4  CompressionScheme = 2
	CompressionAuto              CompressionScheme = 99
)

// String returns the string representation of the compression scheme.
func (c CompressionScheme) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionLZ4:
		return "lz4"
	case CompressionByteGrouping4LZ4:
		return "bg4-lz4"
	case CompressionAuto:
		return "auto"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(c))
	}
}

// ParseCompressionScheme parses a string into a CompressionScheme.
// The input is case-insensitive and leading/trailing spaces are trimmed.
func ParseCompressionScheme(s string) (CompressionScheme, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "", "auto":
		return CompressionAuto, nil
	case "none":
		return CompressionNone, nil
	case "lz4":
		return CompressionLZ4, nil
	case "bg4-lz4":
		return CompressionByteGrouping4LZ4, nil
	default:
		return 0, fmt.Errorf("unknown compression scheme: %q", s)
	}
}

// CompressionSchemeFromByte converts a byte value into a CompressionScheme.
func CompressionSchemeFromByte(b byte) (CompressionScheme, error) {
	switch b {
	case 0:
		return CompressionNone, nil
	case 1:
		return CompressionLZ4, nil
	case 2:
		return CompressionByteGrouping4LZ4, nil
	case 99:
		return CompressionAuto, nil
	default:
		return 0, fmt.Errorf("unknown compression scheme byte: %d", b)
	}
}

// ResolveForData resolves Auto to a concrete compression scheme.
// For Auto, returns LZ4 as a simple default for the Go port.
// Otherwise returns the scheme unchanged.
func (c CompressionScheme) ResolveForData(_ []byte) CompressionScheme {
	if c == CompressionAuto {
		return CompressionLZ4
	}
	return c
}

// Compress compresses data using the compression scheme.
func (c CompressionScheme) Compress(data []byte) ([]byte, error) {
	scheme := c
	if scheme == CompressionAuto {
		scheme = scheme.ResolveForData(data)
	}

	switch scheme {
	case CompressionNone:
		return data, nil
	case CompressionLZ4:
		return lz4Compress(data)
	case CompressionByteGrouping4LZ4:
		grouped := bg4Split(data)
		return lz4Compress(grouped)
	default:
		return nil, fmt.Errorf("cannot compress with scheme: %s", c)
	}
}

// Decompress decompresses data using the compression scheme.
func (c CompressionScheme) Decompress(data []byte) ([]byte, error) {
	switch c {
	case CompressionAuto:
		return nil, fmt.Errorf("cannot decompress with Auto scheme; resolve first")
	case CompressionNone:
		return data, nil
	case CompressionLZ4:
		return lz4Decompress(data)
	case CompressionByteGrouping4LZ4:
		decompressed, err := lz4Decompress(data)
		if err != nil {
			return nil, err
		}
		return bg4Regroup(decompressed)
	default:
		return nil, fmt.Errorf("cannot decompress with scheme: %s", c)
	}
}

// lz4Compress compresses data using lz4 frame encoding.
func lz4Compress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, fmt.Errorf("lz4 compress write: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("lz4 compress close: %w", err)
	}
	return buf.Bytes(), nil
}

// lz4Decompress decompresses lz4 frame encoded data.
func lz4Decompress(data []byte) ([]byte, error) {
	r := lz4.NewReader(bytes.NewReader(data))
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("lz4 decompress: %w", err)
	}
	return out, nil
}

// bg4Split regroups bytes by their position mod 4.
// Group 0: bytes at positions 0, 4, 8, ...
// Group 1: bytes at positions 1, 5, 9, ...
// Group 2: bytes at positions 2, 6, 10, ...
// Group 3: bytes at positions 3, 7, 11, ...
// The output is prefixed with the original length as a little-endian uint32.
func bg4Split(data []byte) []byte {
	n := len(data)
	groupSize := (n + 3) / 4

	// 4 bytes for original length + padded data
	out := make([]byte, 4+groupSize*4)
	binary.LittleEndian.PutUint32(out[0:4], uint32(n))

	for b := 0; b < 4; b++ {
		for g := 0; g < groupSize; g++ {
			srcIdx := g*4 + b
			dstIdx := 4 + b*groupSize + g
			if srcIdx < n {
				out[dstIdx] = data[srcIdx]
			}
			// else: zero-padded (already zeroed by make)
		}
	}

	return out
}

// bg4Regroup is the inverse of bg4Split: reads the original length,
// then reconstructs the original byte order.
func bg4Regroup(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("bg4 regroup: data too short for length header")
	}

	origLen := int(binary.LittleEndian.Uint32(data[0:4]))
	grouped := data[4:]
	groupSize := (origLen + 3) / 4

	if len(grouped) < groupSize*4 {
		return nil, fmt.Errorf("bg4 regroup: data too short: need %d, have %d", groupSize*4, len(grouped))
	}

	out := make([]byte, origLen)
	for b := 0; b < 4; b++ {
		for g := 0; g < groupSize; g++ {
			dstIdx := g*4 + b
			srcIdx := b*groupSize + g
			if dstIdx < origLen {
				out[dstIdx] = grouped[srcIdx]
			}
		}
	}

	return out, nil
}
