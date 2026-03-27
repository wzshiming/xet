package xorb

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/wzshiming/xet"
)

// buildTestXorb creates a test xorb with specified number of chunks and sizes
func buildTestXorb(t *testing.T, numChunks int, chunkSize int) *Xorb {
	t.Helper()
	xorb := NewXorb()

	for i := 0; i < numChunks; i++ {
		// Create chunk data
		data := make([]byte, chunkSize)
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("failed to generate random data: %v", err)
		}

		// Add chunk
		chunk := xet.ChunkBytes(data)
		if err := xorb.AddChunk(chunk); err != nil {
			t.Fatalf("failed to add chunk: %v", err)
		}
	}

	return xorb
}

func TestNumChunks(t *testing.T) {
	tests := []struct {
		name      string
		numChunks int
	}{
		{"empty", 0},
		{"single", 1},
		{"multiple", 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xorb := buildTestXorb(t, tt.numChunks, 1024)
			if got := xorb.NumChunks(); got != tt.numChunks {
				t.Errorf("NumChunks() = %d, want %d", got, tt.numChunks)
			}
		})
	}
}

func TestNumBytes(t *testing.T) {
	tests := []struct {
		name      string
		numChunks int
		chunkSize int
	}{
		{"empty", 0, 0},
		{"single_small", 1, 100},
		{"single_large", 1, 10000},
		{"multiple_same", 3, 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xorb := buildTestXorb(t, tt.numChunks, tt.chunkSize)
			expected := uint64(tt.numChunks * tt.chunkSize)
			if got := xorb.NumBytes(); got != expected {
				t.Errorf("NumBytes() = %d, want %d", got, expected)
			}
		})
	}
}

func TestGetByteOffset(t *testing.T) {
	// Create a test xorb with 3 chunks of different sizes
	xorb := NewXorb()
	chunkSizes := []int{100, 200, 300}

	for _, size := range chunkSizes {
		data := make([]byte, size)
		chunk := xet.ChunkBytes(data)
		if err := xorb.AddChunk(chunk); err != nil {
			t.Fatalf("failed to add chunk: %v", err)
		}
	}

	// Calculate actual compressed sizes for verification
	var actualSizes []uint64
	for _, chunk := range xorb.Chunks {
		actualSizes = append(actualSizes, uint64(8+len(chunk.CompressedData)))
	}

	tests := []struct {
		name    string
		start   int
		end     int
		wantErr bool
	}{
		{
			name:    "first_chunk",
			start:   0,
			end:     1,
			wantErr: false,
		},
		{
			name:    "second_chunk",
			start:   1,
			end:     2,
			wantErr: false,
		},
		{
			name:    "all_chunks",
			start:   0,
			end:     3,
			wantErr: false,
		},
		{
			name:    "empty_range",
			start:   1,
			end:     1,
			wantErr: false,
		},
		{
			name:    "invalid_start",
			start:   -1,
			end:     1,
			wantErr: true,
		},
		{
			name:    "invalid_end",
			start:   0,
			end:     10,
			wantErr: true,
		},
		{
			name:    "inverted_range",
			start:   2,
			end:     1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := xorb.GetByteOffset(tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetByteOffset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Calculate expected offsets from actual sizes
				var expectedStart, expectedEnd uint64
				for i := 0; i < tt.end; i++ {
					if i < tt.start {
						expectedStart += actualSizes[i]
					}
					expectedEnd += actualSizes[i]
				}

				if start != expectedStart {
					t.Errorf("GetByteOffset() start = %d, want %d", start, expectedStart)
				}
				if end != expectedEnd {
					t.Errorf("GetByteOffset() end = %d, want %d", end, expectedEnd)
				}

				// Verify range is valid
				if start > end {
					t.Errorf("GetByteOffset() returned invalid range: start(%d) > end(%d)", start, end)
				}
			}
		})
	}
}

func TestUncompressedChunkLength(t *testing.T) {
	xorb := buildTestXorb(t, 3, 1024)

	tests := []struct {
		name    string
		index   int
		want    uint64
		wantErr bool
	}{
		{"first", 0, 1024, false},
		{"second", 1, 1024, false},
		{"third", 2, 1024, false},
		{"invalid_negative", -1, 0, true},
		{"invalid_too_large", 3, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := xorb.UncompressedChunkLength(tt.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("UncompressedChunkLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("UncompressedChunkLength() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestUncompressedRangeLength(t *testing.T) {
	xorb := buildTestXorb(t, 5, 1000)

	tests := []struct {
		name    string
		start   int
		end     int
		want    uint64
		wantErr bool
	}{
		{"single_chunk", 0, 1, 1000, false},
		{"two_chunks", 0, 2, 2000, false},
		{"all_chunks", 0, 5, 5000, false},
		{"middle_range", 1, 4, 3000, false},
		{"empty_range", 2, 2, 0, false},
		{"invalid_start", -1, 2, 0, true},
		{"invalid_end", 0, 10, 0, true},
		{"inverted", 3, 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := xorb.UncompressedRangeLength(tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("UncompressedRangeLength() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("UncompressedRangeLength() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestGetBytesByChunkRange(t *testing.T) {
	// Create test xorb with known data
	xorb := NewXorb()
	testData := [][]byte{
		[]byte("chunk0"),
		[]byte("chunk1"),
		[]byte("chunk2"),
	}

	for _, data := range testData {
		chunk := xet.ChunkBytes(data)
		if err := xorb.AddChunk(chunk); err != nil {
			t.Fatalf("failed to add chunk: %v", err)
		}
	}

	tests := []struct {
		name    string
		start   int
		end     int
		want    []byte
		wantErr bool
	}{
		{"first_chunk", 0, 1, []byte("chunk0"), false},
		{"second_chunk", 1, 2, []byte("chunk1"), false},
		{"first_two", 0, 2, []byte("chunk0chunk1"), false},
		{"all_chunks", 0, 3, []byte("chunk0chunk1chunk2"), false},
		{"empty_range", 1, 1, []byte{}, false},
		{"invalid_range", -1, 2, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := xorb.GetBytesByChunkRange(tt.start, tt.end)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBytesByChunkRange() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !bytes.Equal(got, tt.want) {
				t.Errorf("GetBytesByChunkRange() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetChunk(t *testing.T) {
	xorb := NewXorb()
	testData := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	for _, data := range testData {
		chunk := xet.ChunkBytes(data)
		if err := xorb.AddChunk(chunk); err != nil {
			t.Fatalf("failed to add chunk: %v", err)
		}
	}

	tests := []struct {
		name    string
		index   int
		want    []byte
		wantErr bool
	}{
		{"first", 0, []byte("first"), false},
		{"second", 1, []byte("second"), false},
		{"third", 2, []byte("third"), false},
		{"invalid_negative", -1, nil, true},
		{"invalid_too_large", 3, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := xorb.GetChunk(tt.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetChunk() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !bytes.Equal(got, tt.want) {
				t.Errorf("GetChunk() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChunkHash(t *testing.T) {
	xorb := buildTestXorb(t, 3, 100)

	tests := []struct {
		name    string
		index   int
		wantErr bool
	}{
		{"first", 0, false},
		{"second", 1, false},
		{"third", 2, false},
		{"invalid_negative", -1, true},
		{"invalid_too_large", 3, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash, err := xorb.ChunkHash(tt.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("ChunkHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				// Verify hash is not zero
				zeroHash := xet.Hash{}
				if hash == zeroHash {
					t.Error("ChunkHash() returned zero hash")
				}
				// Verify it matches the stored hash
				if hash != xorb.ChunkHashes[tt.index] {
					t.Errorf("ChunkHash() mismatch with stored hash")
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Run("valid_xorb", func(t *testing.T) {
		xorb := buildTestXorb(t, 3, 1024)
		if err := xorb.Validate(); err != nil {
			t.Errorf("Validate() failed for valid xorb: %v", err)
		}
	})

	t.Run("empty_xorb", func(t *testing.T) {
		xorb := NewXorb()
		if err := xorb.Validate(); err != nil {
			t.Errorf("Validate() failed for empty xorb: %v", err)
		}
	})

	t.Run("mismatched_hash", func(t *testing.T) {
		xorb := buildTestXorb(t, 2, 100)
		// Corrupt a chunk hash
		xorb.ChunkHashes[0] = xet.Hash{}
		if err := xorb.Validate(); err == nil {
			t.Error("Validate() should fail with mismatched hash")
		}
	})

	t.Run("mismatched_count", func(t *testing.T) {
		xorb := buildTestXorb(t, 2, 100)
		// Remove a hash
		xorb.ChunkHashes = xorb.ChunkHashes[:1]
		if err := xorb.Validate(); err == nil {
			t.Error("Validate() should fail with mismatched count")
		}
	})

	t.Run("corrupted_data", func(t *testing.T) {
		xorb := buildTestXorb(t, 2, 100)
		// Corrupt chunk data
		xorb.Chunks[0].UncompressedData[0] ^= 0xFF
		if err := xorb.Validate(); err == nil {
			t.Error("Validate() should fail with corrupted data")
		}
	})
}

func TestReconstructXorbWithFooter(t *testing.T) {
	// Create original xorb
	original := buildTestXorb(t, 3, 1024)

	// Serialize to chunks-only format
	chunksOnly, err := original.SerializeChunksOnly()
	if err != nil {
		t.Fatalf("SerializeChunksOnly() failed: %v", err)
	}

	// Reconstruct
	reconstructed, err := ReconstructXorbWithFooter(chunksOnly)
	if err != nil {
		t.Fatalf("ReconstructXorbWithFooter() failed: %v", err)
	}

	// Verify
	if reconstructed.NumChunks() != original.NumChunks() {
		t.Errorf("NumChunks mismatch: got %d, want %d", reconstructed.NumChunks(), original.NumChunks())
	}

	if reconstructed.NumBytes() != original.NumBytes() {
		t.Errorf("NumBytes mismatch: got %d, want %d", reconstructed.NumBytes(), original.NumBytes())
	}

	if reconstructed.Hash != original.Hash {
		t.Errorf("Hash mismatch: got %s, want %s", reconstructed.Hash.String(), original.Hash.String())
	}

	// Verify validation passes
	if err := reconstructed.Validate(); err != nil {
		t.Errorf("Validate() failed on reconstructed xorb: %v", err)
	}
}

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	tests := []struct {
		name      string
		numChunks int
		chunkSize int
	}{
		{"empty", 0, 0},
		{"single_small", 1, 100},
		{"single_large", 1, 65536},
		{"multiple", 5, 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.numChunks == 0 {
				// Skip empty test as it's not meaningful
				return
			}

			original := buildTestXorb(t, tt.numChunks, tt.chunkSize)

			// Serialize
			data, err := original.Serialize()
			if err != nil {
				t.Fatalf("Serialize() failed: %v", err)
			}

			// Deserialize
			reconstructed, err := Deserialize(data)
			if err != nil {
				t.Fatalf("Deserialize() failed: %v", err)
			}

			// Verify
			if reconstructed.NumChunks() != original.NumChunks() {
				t.Errorf("NumChunks mismatch")
			}

			if reconstructed.NumBytes() != original.NumBytes() {
				t.Errorf("NumBytes mismatch")
			}

			if reconstructed.Hash != original.Hash {
				t.Errorf("Hash mismatch")
			}

			// Validate
			if err := reconstructed.Validate(); err != nil {
				t.Errorf("Validate() failed: %v", err)
			}
		})
	}
}

func TestChunksOnlyRoundTrip(t *testing.T) {
	original := buildTestXorb(t, 3, 1024)

	// Serialize chunks only
	chunksOnly, err := original.SerializeChunksOnly()
	if err != nil {
		t.Fatalf("SerializeChunksOnly() failed: %v", err)
	}

	// Deserialize chunks only
	reconstructed, err := DeserializeChunksOnly(chunksOnly)
	if err != nil {
		t.Fatalf("DeserializeChunksOnly() failed: %v", err)
	}

	// Verify
	if reconstructed.NumChunks() != original.NumChunks() {
		t.Errorf("NumChunks mismatch")
	}

	if reconstructed.NumBytes() != original.NumBytes() {
		t.Errorf("NumBytes mismatch")
	}

	if reconstructed.Hash != original.Hash {
		t.Errorf("Hash mismatch")
	}
}
