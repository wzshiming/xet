package gearhash

import (
	"bytes"
	"testing"
)

func TestChunkData(t *testing.T) {
	// Test with a simple piece of data
	data := []byte("Hello, World! This is a test of the Gearhash chunking algorithm.")

	var chunks []Chunk
	err := ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		buf := make([]byte, len(chunk))
		copy(buf, chunk)
		chunks = append(chunks, Chunk{Data: buf, Offset: offset})
		return nil
	})
	if err != nil {
		t.Fatalf("ChunkData failed: %v", err)
	}

	if len(chunks) == 0 {
		t.Fatal("Expected at least one chunk")
	}

	// Verify that chunks cover all data
	totalSize := 0
	for _, chunk := range chunks {
		totalSize += len(chunk.Data)
	}

	if totalSize != len(data) {
		t.Errorf("Chunks don't cover all data: got %d, expected %d", totalSize, len(data))
	}

	t.Logf("Chunked %d bytes into %d chunks", len(data), len(chunks))
	for i, chunk := range chunks {
		t.Logf("Chunk %d: offset=%d, size=%d", i, chunk.Offset, len(chunk.Data))
	}
}

func TestSmallFile(t *testing.T) {
	// Files smaller than MinChunkSize should produce a single chunk
	data := make([]byte, MinChunkSize-1)
	chunks := collectChunks(t, data)

	if len(chunks) != 1 {
		t.Errorf("Small file should produce 1 chunk, got %d", len(chunks))
	}

	if len(chunks[0].Data) != len(data) {
		t.Errorf("Chunk size mismatch: got %d, expected %d", len(chunks[0].Data), len(data))
	}
}

func TestEmptyData(t *testing.T) {
	chunks := collectChunks(t, []byte{})
	if len(chunks) != 0 {
		t.Errorf("Empty data should produce 0 chunks, got %d", len(chunks))
	}
}

func TestChunkSizeBounds(t *testing.T) {
	// Create a large piece of data to test chunk size bounds
	data := make([]byte, MaxChunkSize*3)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks := collectChunks(t, data)

	for i, chunk := range chunks {
		size := len(chunk.Data)

		// Check that chunks respect size bounds (except possibly the last chunk)
		if i < len(chunks)-1 {
			if size < MinChunkSize {
				t.Errorf("Chunk %d is smaller than MinChunkSize: %d < %d", i, size, MinChunkSize)
			}
			if size > MaxChunkSize {
				t.Errorf("Chunk %d is larger than MaxChunkSize: %d > %d", i, size, MaxChunkSize)
			}
		}

		t.Logf("Chunk %d: size=%d", i, size)
	}
}

func TestDeterminism(t *testing.T) {
	// The same data should always produce the same chunks
	data := make([]byte, 100000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks1 := collectChunks(t, data)
	chunks2 := collectChunks(t, data)

	if len(chunks1) != len(chunks2) {
		t.Errorf("Chunking is not deterministic: got %d and %d chunks", len(chunks1), len(chunks2))
	}

	for i := range chunks1 {
		if chunks1[i].Offset != chunks2[i].Offset {
			t.Errorf("Chunk %d offset mismatch: %d vs %d", i, chunks1[i].Offset, chunks2[i].Offset)
		}
		if len(chunks1[i].Data) != len(chunks2[i].Data) {
			t.Errorf("Chunk %d size mismatch: %d vs %d", i, len(chunks1[i].Data), len(chunks2[i].Data))
		}
	}
}

func collectChunks(t *testing.T, data []byte) []Chunk {
	t.Helper()

	chunks, err := ChunkBytes(data)
	if err != nil {
		t.Fatalf("ChunkBytes failed: %v", err)
	}

	return chunks
}
