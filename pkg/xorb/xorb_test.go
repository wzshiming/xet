package xorb

import (
	"bytes"
	"testing"

	"github.com/wzshiming/xet/pkg/xet"
)

func TestSerializeDeserialize(t *testing.T) {
	// Create some test chunks
	chunks := []ChunkEntry{
		{UncompressedData: bytes.Repeat([]byte("hello "), 100)},
		{UncompressedData: bytes.Repeat([]byte("world "), 100)},
		{UncompressedData: bytes.Repeat([]byte("test! "), 100)},
	}

	// Compute hashes
	for i := range chunks {
		chunks[i].Hash = xet.ComputeChunkHash(chunks[i].UncompressedData)
	}

	// Serialize with LZ4 compression
	serialized, err := Serialize(chunks, CompressionLZ4)
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}

	// Deserialize
	decompressed, xorbHash, err := Deserialize(serialized)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}

	if xorbHash == [32]byte{} {
		t.Error("xorb hash should not be zero")
	}

	if len(decompressed) != len(chunks) {
		t.Fatalf("chunk count mismatch: %d vs %d", len(decompressed), len(chunks))
	}

	for i, d := range decompressed {
		if !bytes.Equal(d, chunks[i].UncompressedData) {
			t.Errorf("chunk %d data mismatch", i)
		}
	}
}

func TestSerializeDeserializeNoCompression(t *testing.T) {
	chunks := []ChunkEntry{
		{UncompressedData: []byte("small data")},
	}
	chunks[0].Hash = xet.ComputeChunkHash(chunks[0].UncompressedData)

	serialized, err := Serialize(chunks, CompressionNone)
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}

	decompressed, _, err := Deserialize(serialized)
	if err != nil {
		t.Fatalf("deserialize error: %v", err)
	}

	if !bytes.Equal(decompressed[0], chunks[0].UncompressedData) {
		t.Error("data mismatch")
	}
}
