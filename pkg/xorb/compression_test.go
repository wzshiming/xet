package xorb

import (
	"bytes"
	"testing"
)

func TestByteGroup4Ungroup4(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"single", []byte{0x42}},
		{"two", []byte{0x01, 0x02}},
		{"three", []byte{0x01, 0x02, 0x03}},
		{"four", []byte{0xA0, 0xA1, 0xA2, 0xA3}},
		{"eight", []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xB0, 0xB1, 0xB2, 0xB3}},
		{"ten", []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xB0, 0xB1, 0xB2, 0xB3, 0xC0, 0xC1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.data) == 0 {
				return
			}
			grouped := ByteGroup4(tt.data)
			if len(grouped) != len(tt.data) {
				t.Errorf("grouped length mismatch: %d vs %d", len(grouped), len(tt.data))
			}
			ungrouped := ByteUngroup4(grouped, len(tt.data))
			if !bytes.Equal(ungrouped, tt.data) {
				t.Errorf("round-trip failed:\n  original:  %x\n  grouped:   %x\n  ungrouped: %x", tt.data, grouped, ungrouped)
			}
		})
	}
}

func TestByteGroup4Example(t *testing.T) {
	// From spec Section 7.4.3:
	// Original: [A0 A1 A2 A3 | B0 B1 B2 B3 | C0 C1 C2 C3]
	// Grouped:  [A0 B0 C0 | A1 B1 C1 | A2 B2 C2 | A3 B3 C3]
	data := []byte{0xA0, 0xA1, 0xA2, 0xA3, 0xB0, 0xB1, 0xB2, 0xB3, 0xC0, 0xC1, 0xC2, 0xC3}
	grouped := ByteGroup4(data)
	expected := []byte{0xA0, 0xB0, 0xC0, 0xA1, 0xB1, 0xC1, 0xA2, 0xB2, 0xC2, 0xA3, 0xB3, 0xC3}
	if !bytes.Equal(grouped, expected) {
		t.Errorf("byte group example mismatch:\n  got:  %x\n  want: %x", grouped, expected)
	}
}

func TestByteGroup4TenBytes(t *testing.T) {
	// 10 bytes: group sizes 3, 3, 2, 2
	data := make([]byte, 10)
	for i := range data {
		data[i] = byte(i)
	}
	grouped := ByteGroup4(data)
	ungrouped := ByteUngroup4(grouped, 10)
	if !bytes.Equal(ungrouped, data) {
		t.Errorf("10-byte round-trip failed")
	}
}

func TestCompressDecompressLZ4(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 10)
	}

	compressed, err := CompressLZ4(data)
	if err != nil {
		t.Fatalf("compress error: %v", err)
	}

	decompressed, err := DecompressLZ4(compressed, len(data))
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("LZ4 round-trip failed")
	}
}

func TestCompressDecompressByteGrouping4LZ4(t *testing.T) {
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i % 10)
	}

	compressed, err := CompressByteGrouping4LZ4(data)
	if err != nil {
		t.Fatalf("compress error: %v", err)
	}

	decompressed, err := DecompressByteGrouping4LZ4(compressed, len(data))
	if err != nil {
		t.Fatalf("decompress error: %v", err)
	}

	if !bytes.Equal(decompressed, data) {
		t.Error("ByteGrouping4LZ4 round-trip failed")
	}
}

func TestCompressChunkFallback(t *testing.T) {
	// Already "compressed" random-like data should fall back to None
	data := []byte{0x42} // tiny data, LZ4 frame overhead makes it bigger
	compressed, compType, err := CompressChunk(data, CompressionLZ4)
	if err != nil {
		t.Fatalf("compress error: %v", err)
	}
	if compType != CompressionNone {
		// It's OK if LZ4 is still smaller for this input
		if len(compressed) >= len(data) {
			t.Errorf("should have fallen back to none compression")
		}
	}
}
