package xorb

import (
	"bytes"
	"testing"

	"github.com/wzshiming/xet"
)

func TestEncoderChunkHashesAndSizes(t *testing.T) {
	chunks := testFooterChunks(5)

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, false)
	for _, chunk := range chunks {
		if _, err := encoder.Write(chunk); err != nil {
			t.Fatalf("Encoder.Write() failed: %v", err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("Encoder.Close() failed: %v", err)
	}

	hashes := encoder.ChunkHashes()
	sizes := encoder.ChunkSizes()
	if len(hashes) != len(chunks) {
		t.Fatalf("ChunkHashes() has %d entries, want %d", len(hashes), len(chunks))
	}
	if len(sizes) != len(chunks) {
		t.Fatalf("ChunkSizes() has %d entries, want %d", len(sizes), len(chunks))
	}
	for i, chunk := range chunks {
		if want := xet.ComputeChunkHash(chunk); hashes[i] != want {
			t.Fatalf("chunk %d hash = %s, want %s", i, hashes[i].String(), want.String())
		}
		if sizes[i] != uint64(len(chunk)) {
			t.Fatalf("chunk %d size = %d, want %d", i, sizes[i], len(chunk))
		}
	}
}
