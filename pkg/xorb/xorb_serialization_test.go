package xorb

import (
	"bytes"
	"io"
	"testing"

	"github.com/wzshiming/xet"
)

func TestSerializeDeserializeWithFooter(t *testing.T) {
	x := NewXorb()
	chunkA := []byte("hello world")
	chunkB := []byte("goodbye world")

	if err := x.AddChunk(chunkA); err != nil {
		t.Fatalf("add chunkA: %v", err)
	}
	if err := x.AddChunk(chunkB); err != nil {
		t.Fatalf("add chunkB: %v", err)
	}

	reader, err := Serialize(x, false)
	if err != nil {
		t.Fatalf("serialize with footer: %v", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read serialized data: %v", err)
	}

	roundTrip, err := Deserialize(bytes.NewReader(data), false)
	if err != nil {
		t.Fatalf("deserialize with footer: %v", err)
	}

	if got, want := len(roundTrip.Chunks), 2; got != want {
		t.Fatalf("expected %d chunks, got %d", want, got)
	}

	if !bytes.Equal(roundTrip.Chunks[0].UncompressedData, chunkA) {
		t.Fatalf("chunkA mismatch after round trip")
	}
	if !bytes.Equal(roundTrip.Chunks[1].UncompressedData, chunkB) {
		t.Fatalf("chunkB mismatch after round trip")
	}

	if roundTrip.Hash != x.Hash {
		t.Fatalf("expected hash %s, got %s", x.Hash.String(), roundTrip.Hash.String())
	}
}

func TestSerializeDeserializeChunksOnly(t *testing.T) {
	x := NewXorb()
	chunkA := []byte("foo")
	chunkB := []byte("barbaz")

	if err := x.AddChunk(chunkA); err != nil {
		t.Fatalf("add chunkA: %v", err)
	}
	if err := x.AddChunk(chunkB); err != nil {
		t.Fatalf("add chunkB: %v", err)
	}

	reader, err := Serialize(x, true)
	if err != nil {
		t.Fatalf("serialize chunks-only: %v", err)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read serialized data: %v", err)
	}

	roundTrip, err := Deserialize(bytes.NewReader(data), true)
	if err != nil {
		t.Fatalf("deserialize chunks-only: %v", err)
	}

	if got, want := len(roundTrip.Chunks), 2; got != want {
		t.Fatalf("expected %d chunks, got %d", want, got)
	}

	if !bytes.Equal(roundTrip.Chunks[0].UncompressedData, chunkA) {
		t.Fatalf("chunkA mismatch after round trip")
	}
	if !bytes.Equal(roundTrip.Chunks[1].UncompressedData, chunkB) {
		t.Fatalf("chunkB mismatch after round trip")
	}

	expectedHash := xet.ComputeXorbHash(roundTrip.ChunkHashes, []uint64{uint64(len(chunkA)), uint64(len(chunkB))})
	if roundTrip.Hash != expectedHash {
		t.Fatalf("expected hash %s, got %s", expectedHash.String(), roundTrip.Hash.String())
	}
}
