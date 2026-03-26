package xorb

import (
	"bytes"
	"testing"
)

func TestLZ4CompressionDecompression(t *testing.T) {
	// Test with various sizes to ensure io.ReadFull works correctly
	testSizes := []int{
		100,     // Small
		10000,   // Medium
		100000,  // Large
		1000000, // Very large (1MB)
	}

	for _, size := range testSizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			// Create test data
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Compress
			compressed, err := compressLZ4(data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressLZ4(compressed, size)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if len(decompressed) != size {
				t.Errorf("Size mismatch: got %d, expected %d", len(decompressed), size)
			}

			if !bytes.Equal(data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}

func TestByteGrouping4Compression(t *testing.T) {
	// Test ByteGrouping4 + LZ4 with large data
	testSizes := []int{
		100,
		10000,
		100000,
	}

	for _, size := range testSizes {
		t.Run(string(rune(size)), func(t *testing.T) {
			// Create test data with structured pattern (good for ByteGrouping4)
			data := make([]byte, size)
			for i := 0; i < size; i += 4 {
				data[i] = 0x01
				if i+1 < size {
					data[i+1] = 0x02
				}
				if i+2 < size {
					data[i+2] = 0x03
				}
				if i+3 < size {
					data[i+3] = 0x04
				}
			}

			// Compress with ByteGrouping4
			compressed, err := compressByteGrouping4LZ4(data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressByteGrouping4LZ4(compressed, size)
			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			// Verify
			if len(decompressed) != size {
				t.Errorf("Size mismatch: got %d, expected %d", len(decompressed), size)
			}

			if !bytes.Equal(data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}

func TestByteGrouping4Transform(t *testing.T) {
	// Test the transform and reverse with various sizes
	testData := [][]byte{
		[]byte("Hello"),                                  // Not multiple of 4
		[]byte("Hello World!"),                           // Multiple of 4
		[]byte("ABCDEFGHIJKLMNOP"),                       // 16 bytes
		[]byte{0x01, 0x02, 0x03},                         // 3 bytes
		[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}, // 7 bytes
	}

	for i, data := range testData {
		t.Run(string(rune(i)), func(t *testing.T) {
			// Apply transform
			transformed := applyByteGrouping4(data)

			if len(transformed) != len(data) {
				t.Errorf("Transform changed size: got %d, expected %d", len(transformed), len(data))
			}

			// Reverse transform
			reversed := reverseByteGrouping4(transformed)

			if len(reversed) != len(data) {
				t.Errorf("Reverse changed size: got %d, expected %d", len(reversed), len(data))
			}

			if !bytes.Equal(data, reversed) {
				t.Errorf("Round-trip failed. Original: %v, Got: %v", data, reversed)
			}
		})
	}
}

func TestSelectBestCompression(t *testing.T) {
	// Test that SelectBestCompression works correctly
	testCases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("Hello World!")},
		{"random", make([]byte, 1000)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Fill random data for that test case
			if tc.name == "random" {
				for i := range tc.data {
					tc.data[i] = byte(i % 256)
				}
			}

			compressed, compressionType, err := SelectBestCompression(tc.data)
			if err != nil {
				t.Fatalf("SelectBestCompression failed: %v", err)
			}

			// Decompress based on type
			var decompressed []byte
			switch compressionType {
			case CompressionNone:
				decompressed = compressed
			case CompressionLZ4:
				decompressed, err = decompressLZ4(compressed, len(tc.data))
			case CompressionByteGrouping4:
				decompressed, err = decompressByteGrouping4LZ4(compressed, len(tc.data))
			default:
				t.Fatalf("Unknown compression type: %d", compressionType)
			}

			if err != nil {
				t.Fatalf("Decompression failed: %v", err)
			}

			if !bytes.Equal(tc.data, decompressed) {
				t.Error("Decompressed data doesn't match original")
			}
		})
	}
}
