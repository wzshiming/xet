package xorb

import (
	"bytes"
	"testing"

	"github.com/wzshiming/xet"
)

func TestSerializeDeserializeStreamingFull(t *testing.T) {
	x := NewXorb()
	chunks := [][]byte{
		[]byte("hello "),
		[]byte("streaming "),
		[]byte("xorb"),
	}

	for _, data := range chunks {
		if err := x.AddChunk(xet.ChunkBytes(data)); err != nil {
			t.Fatalf("add chunk: %v", err)
		}
	}

	reader, err := Serialize(x, false)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	roundTrip, err := Deserialize(reader, false)
	if err != nil {
		t.Fatalf("deserialize: %v", err)
	}

	if len(roundTrip.Chunks) != len(chunks) {
		t.Fatalf("chunk count mismatch: got %d want %d", len(roundTrip.Chunks), len(chunks))
	}

	for i, data := range chunks {
		if !bytes.Equal(roundTrip.Chunks[i].UncompressedData, data) {
			t.Fatalf("chunk %d data mismatch", i)
		}
	}

	// Hash in the footer should be preserved through streaming round-trip.
	if x.Hash != roundTrip.Hash {
		t.Fatalf("hash mismatch after round trip: got %s want %s", roundTrip.Hash.String(), x.Hash.String())
	}
}

func TestSerializeDeserializeStreamingChunksOnly(t *testing.T) {
	x := NewXorb()
	chunks := [][]byte{
		[]byte("chunk-only "),
		[]byte("serialization"),
	}

	for _, data := range chunks {
		if err := x.AddChunk(xet.ChunkBytes(data)); err != nil {
			t.Fatalf("add chunk: %v", err)
		}
	}
	x.UpdateHash()

	reader, err := Serialize(x, true)
	if err != nil {
		t.Fatalf("serialize chunks-only: %v", err)
	}

	roundTrip, err := Deserialize(reader, true)
	if err != nil {
		t.Fatalf("deserialize chunks-only: %v", err)
	}

	if len(roundTrip.Chunks) != len(chunks) {
		t.Fatalf("chunk count mismatch: got %d want %d", len(roundTrip.Chunks), len(chunks))
	}

	for i, data := range chunks {
		if !bytes.Equal(roundTrip.Chunks[i].UncompressedData, data) {
			t.Fatalf("chunk %d data mismatch", i)
		}
	}

	if roundTrip.Hash != x.Hash {
		t.Fatalf("hash mismatch after chunks-only round trip: got %s want %s", roundTrip.Hash.String(), x.Hash.String())
	}
}
