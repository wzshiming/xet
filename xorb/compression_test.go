package xorb

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/wzshiming/xet"
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
			case CompressionNone:
				decompressed = compressed
			case CompressionLZ4:
				decompressed, err = decompressLZ4(nil, compressed, len(tc.data))
			case CompressionByteGrouping4:
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

// TestCompressLZ4WritesXetCoreFrames pins the frames of xet-core's lz4_flex FrameEncoder::new: FrameInfo::default() with the Auto block size.
func TestCompressLZ4WritesXetCoreFrames(t *testing.T) {
	codecs := []struct {
		name       string
		compress   func(dst, data []byte) ([]byte, error)
		decompress func(dst, data []byte, n int) ([]byte, error)
	}{
		{"lz4", compressLZ4, decompressLZ4},
		{"bg4", compressByteGrouping4LZ4, decompressByteGrouping4LZ4},
	}
	// BD bits 4-6 hold the block maximum size id: 4 = 64 KB, 5 = 256 KB, 7 = 4 MB.
	sizes := []struct {
		n       int
		bd      byte
		lz4Only bool
	}{
		{1, 0x40, false},
		{100, 0x40, false},
		{64 << 10, 0x40, false},
		{64<<10 + 1, 0x50, false},
		{xet.MaxChunkSize, 0x50, false},
		{256 << 10, 0x50, false},
		{256<<10 + 1, 0x70, true},
	}
	// Rounds over all sizes make later calls reuse pooled writers, also after other block sizes.
	for call := 1; call <= 3; call++ {
		for _, size := range sizes {
			for _, c := range codecs {
				if size.lz4Only && c.name != "lz4" {
					continue
				}
				t.Run(fmt.Sprintf("%s/n=%d/call=%d", c.name, size.n, call), func(t *testing.T) {
					data := payloadForTest(size.n, byte(size.n))
					frame, err := c.compress(nil, data)
					if err != nil {
						t.Fatalf("compress %d bytes: %v", size.n, err)
					}
					if frame[4] != 0x60 {
						t.Errorf("FLG = %#02x, want 0x60: version 1, independent blocks, no block or content checksum, no content size, no dictionary", frame[4])
					}
					if frame[5] != size.bd {
						t.Errorf("BD = %#02x, want %#02x, the block size lz4_flex picks for %d bytes", frame[5], size.bd, size.n)
					}
					if end := frame[len(frame)-4:]; !bytes.Equal(end, []byte{0, 0, 0, 0}) {
						t.Errorf("frame ends with %x, want the end mark 00000000 and no content checksum", end)
					}
					if got, err := c.decompress(nil, frame, size.n); err != nil || !bytes.Equal(got, data) {
						t.Errorf("round trip = %d bytes, %v; want the %d-byte input", len(got), err, size.n)
					}
				})
			}
		}
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
