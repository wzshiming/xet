package xorb

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/wzshiming/xet"
)

// reverseByteGrouping4Reference is reverseByteGrouping4 as of c4bc4eb, kept to pin its rewrite; dst's spare capacity must hold data.
func reverseByteGrouping4Reference(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	off := len(dst)
	dst = dst[:off+len(data)]
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
		for g := range numGroups {
			if readPos < len(data) {
				result[g*4+bytePos] = data[readPos]
				readPos++
			}
		}
	}

	return dst
}

func TestReverseByteGrouping4MatchesReference(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{4})
	prefix := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	for n := range 1026 {
		data := make([]byte, n)
		rng.Read(data)
		want := reverseByteGrouping4Reference(append(make([]byte, 0, len(prefix)+n), prefix...), data)
		dst := append(make([]byte, 0, len(prefix)+n), prefix...)
		if got, err := reverseByteGrouping4(dst, data); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("reverseByteGrouping4(%d-byte prefix with cap %d, %d random bytes) error = %v, or its result differs from the reference", len(prefix), cap(dst), n, err)
		}
	}
}

// applyByteGrouping4Reference is applyByteGrouping4 as of c3c70a0, kept to pin its rewrite; dst's spare capacity must hold data.
func applyByteGrouping4Reference(dst, data []byte) []byte {
	if len(data) == 0 {
		return dst
	}

	off := len(dst)
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

	return dst
}

func TestApplyByteGrouping4MatchesReference(t *testing.T) {
	rng := rand.NewChaCha8([32]byte{4})
	prefix := []byte{0xde, 0xad, 0xbe, 0xef, 0x01}
	for n := range 1026 {
		data := make([]byte, n)
		rng.Read(data)
		want := applyByteGrouping4Reference(append(make([]byte, 0, len(prefix)+n), prefix...), data)
		dst := append(make([]byte, 0, len(prefix)+n), prefix...)
		if got, err := applyByteGrouping4(dst, data); err != nil || !bytes.Equal(got, want) {
			t.Fatalf("applyByteGrouping4(%d-byte prefix with cap %d, %d random bytes) error = %v, or its result differs from the reference", len(prefix), cap(dst), n, err)
		}
	}
}

func BenchmarkApplyByteGrouping4(b *testing.B) {
	data := make([]byte, xet.TargetChunkSize)
	rand.NewChaCha8([32]byte{4}).Read(data)
	dst := make([]byte, 0, len(data))
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := applyByteGrouping4(dst, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReverseByteGrouping4(b *testing.B) {
	data := make([]byte, xet.TargetChunkSize)
	rand.NewChaCha8([32]byte{4}).Read(data)
	dst := make([]byte, 0, len(data))
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		if _, err := reverseByteGrouping4(dst, data); err != nil {
			b.Fatal(err)
		}
	}
}
