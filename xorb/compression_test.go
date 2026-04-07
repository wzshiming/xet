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
			compressed, err := compressLZ4(nil, data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressLZ4(nil, compressed, size)
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
			compressed, err := compressByteGrouping4LZ4(nil, data)
			if err != nil {
				t.Fatalf("Compression failed: %v", err)
			}

			// Decompress
			decompressed, err := decompressByteGrouping4LZ4(nil, compressed, size)
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
			transformed := applyByteGrouping4(nil, data)

			if len(transformed) != len(data) {
				t.Errorf("Transform changed size: got %d, expected %d", len(transformed), len(data))
			}

			// Reverse transform
			reversed := reverseByteGrouping4(nil, transformed)

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

			compressed, compressionType, err := selectBestCompression(nil, tc.data)
			if err != nil {
				t.Fatalf("SelectBestCompression failed: %v", err)
			}

			// Decompress based on type
			var decompressed []byte
			switch compressionType {
			case compressionNone:
				decompressed = compressed
			case compressionLZ4:
				decompressed, err = decompressLZ4(nil, compressed, len(tc.data))
			case compressionByteGrouping4:
				decompressed, err = decompressByteGrouping4LZ4(nil, compressed, len(tc.data))
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

func TestAppendToDestBufferReuse(t *testing.T) {
	// Test that append-to-dest mode correctly reuses pre-allocated buffers.
	data := make([]byte, 10000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	t.Run("compressLZ4", func(t *testing.T) {
		dst := make([]byte, 0, 65536)
		result, err := compressLZ4(dst, data)
		if err != nil {
			t.Fatal(err)
		}
		// Decompress and verify
		decompressed, err := decompressLZ4(nil, result, len(data))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, decompressed) {
			t.Error("Decompressed data doesn't match original")
		}
	})

	t.Run("decompressLZ4_reuse", func(t *testing.T) {
		compressed, err := compressLZ4(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		// Reuse a buffer across multiple decompressions
		var buf []byte
		for range 3 {
			buf, err = decompressLZ4(buf[:0], compressed, len(data))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, buf) {
				t.Error("Decompressed data doesn't match original")
			}
		}
	})

	t.Run("byteGrouping4_roundtrip_reuse", func(t *testing.T) {
		var transformBuf, reverseBuf []byte
		for range 3 {
			transformBuf = applyByteGrouping4(transformBuf[:0], data)
			reverseBuf = reverseByteGrouping4(reverseBuf[:0], transformBuf)
			if !bytes.Equal(data, reverseBuf) {
				t.Error("Round-trip with buffer reuse failed")
			}
		}
	})

	t.Run("decompressChunk_reuse", func(t *testing.T) {
		compressed, ct, err := selectBestCompression(nil, data)
		if err != nil {
			t.Fatal(err)
		}
		var buf []byte
		for range 3 {
			buf, err = decompressChunk(buf[:0], compressed, ct, len(data))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, buf) {
				t.Error("Decompressed data doesn't match original")
			}
		}
	})

	t.Run("selectBestCompression_reuse", func(t *testing.T) {
		var buf []byte
		for range 3 {
			var err error
			buf, _, err = selectBestCompression(buf[:0], data)
			if err != nil {
				t.Fatal(err)
			}
		}
	})
}

func BenchmarkCompressLZ4(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		var err error
		buf, err = compressLZ4(buf[:0], data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecompressLZ4(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	compressed, err := compressLZ4(nil, data)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		buf, err = decompressLZ4(buf[:0], compressed, len(data))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSelectBestCompression(b *testing.B) {
	data := make([]byte, 65536)
	for i := range data {
		data[i] = byte(i % 256)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var buf []byte
	for b.Loop() {
		var err error
		buf, _, err = selectBestCompression(buf[:0], data)
		if err != nil {
			b.Fatal(err)
		}
	}
}
