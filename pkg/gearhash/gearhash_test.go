package gearhash

import (
	"testing"
)

func TestChunkFileSmall(t *testing.T) {
	// Small data (less than MinChunkSize) should produce a single chunk
	data := []byte("Hello World!")
	chunks := ChunkFile(data)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if string(chunks[0]) != "Hello World!" {
		t.Errorf("chunk content mismatch")
	}
}

func TestChunkFileEmpty(t *testing.T) {
	chunks := ChunkFile(nil)
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks for empty input, got %d", len(chunks))
	}
}

func TestChunkFileDeterministic(t *testing.T) {
	// Generate some data and verify same input always produces same chunks
	data := make([]byte, MaxChunkSize*3)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks1 := ChunkFile(data)
	chunks2 := ChunkFile(data)

	if len(chunks1) != len(chunks2) {
		t.Fatalf("determinism: chunk count mismatch: %d vs %d", len(chunks1), len(chunks2))
	}

	for i := range chunks1 {
		if len(chunks1[i]) != len(chunks2[i]) {
			t.Errorf("determinism: chunk %d size mismatch: %d vs %d", i, len(chunks1[i]), len(chunks2[i]))
		}
	}
}

func TestChunkFileMaxChunkSize(t *testing.T) {
	// Create data larger than MaxChunkSize to force boundary
	data := make([]byte, MaxChunkSize+1)
	for i := range data {
		data[i] = 0 // Use zero bytes which may or may not hit a boundary
	}

	chunks := ChunkFile(data)
	for i, chunk := range chunks {
		if len(chunk) > MaxChunkSize {
			t.Errorf("chunk %d exceeds MaxChunkSize: %d > %d", i, len(chunk), MaxChunkSize)
		}
	}

	// Verify total size matches
	total := 0
	for _, chunk := range chunks {
		total += len(chunk)
	}
	if total != len(data) {
		t.Errorf("total chunk size mismatch: %d vs %d", total, len(data))
	}
}

func TestChunkBoundaries(t *testing.T) {
	data := make([]byte, MaxChunkSize*2)
	for i := range data {
		data[i] = byte(i * 7 % 256)
	}

	chunks := ChunkFile(data)
	boundaries := ChunkBoundaries(data)

	if len(chunks) != len(boundaries) {
		t.Fatalf("chunk count vs boundary count mismatch: %d vs %d", len(chunks), len(boundaries))
	}

	// Verify boundaries match chunk sizes
	offset := 0
	for i, chunk := range chunks {
		offset += len(chunk)
		if boundaries[i] != offset {
			t.Errorf("boundary %d mismatch: %d vs expected %d", i, boundaries[i], offset)
		}
	}
}
