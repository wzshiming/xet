package xorb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"reflect"
	"testing"
)

// testFooterChunks returns deterministic chunks with mixed compressibility.
func testFooterChunks(n int) [][]byte {
	rng := rand.New(rand.NewSource(42))
	chunks := make([][]byte, n)
	for i := range chunks {
		b := make([]byte, 1+rng.Intn(4096))
		if i%2 == 0 {
			rng.Read(b)
		}
		chunks[i] = b
	}
	return chunks
}

func encodeChunkOnlyXorbForTest(t *testing.T, chunks ...[]byte) []byte {
	t.Helper()
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
	return buf.Bytes()
}

// assertOffsetsMatchChunkDataRange checks every [start, end) pair against the
// scanning ChunkDataRange reference implementation.
func assertOffsetsMatchChunkDataRange(t *testing.T, data []byte, offsets []uint64, numChunks int) {
	t.Helper()
	for start := uint32(0); start < uint32(numChunks); start++ {
		for end := start + 1; end <= uint32(numChunks); end++ {
			wantStart, wantEnd, err := ChunkDataRange(bytes.NewReader(data), start, end)
			if err != nil {
				t.Fatalf("ChunkDataRange(%d, %d) failed: %v", start, end, err)
			}
			gotStart, gotEnd, err := ChunkDataRangeFromOffsets(offsets, start, end)
			if err != nil {
				t.Fatalf("ChunkDataRangeFromOffsets(%d, %d) failed: %v", start, end, err)
			}
			if gotStart != wantStart || gotEnd != wantEnd {
				t.Fatalf("range [%d, %d) = [%d, %d], want [%d, %d]", start, end, gotStart, gotEnd, wantStart, wantEnd)
			}
		}
	}
}

func TestReadChunkOffsetsMatchesScanAndChunkDataRange(t *testing.T) {
	chunks := testFooterChunks(7)
	data, _ := encodeXorbForTest(t, [4]byte{}, chunks...)

	fromFooter, err := ReadChunkOffsets(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ReadChunkOffsets() failed: %v", err)
	}
	if len(fromFooter) != len(chunks) {
		t.Fatalf("ReadChunkOffsets() returned %d offsets, want %d", len(fromFooter), len(chunks))
	}

	fromScan, err := ScanChunkOffsets(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ScanChunkOffsets() failed: %v", err)
	}
	if !reflect.DeepEqual(fromFooter, fromScan) {
		t.Fatalf("footer offsets %v != scanned offsets %v", fromFooter, fromScan)
	}

	assertOffsetsMatchChunkDataRange(t, data, fromFooter, len(chunks))
}

func TestScanChunkOffsetsChunkOnlyFormat(t *testing.T) {
	chunks := testFooterChunks(5)
	data := encodeChunkOnlyXorbForTest(t, chunks...)

	if _, err := ReadChunkOffsets(bytes.NewReader(data)); !errors.Is(err, ErrNoFooter) {
		t.Fatalf("ReadChunkOffsets() error = %v, want ErrNoFooter", err)
	}

	offsets, err := ScanChunkOffsets(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("ScanChunkOffsets() failed: %v", err)
	}
	if len(offsets) != len(chunks) {
		t.Fatalf("ScanChunkOffsets() returned %d offsets, want %d", len(offsets), len(chunks))
	}
	if offsets[len(offsets)-1] != uint64(len(data)) {
		t.Fatalf("last offset = %d, want stream size %d", offsets[len(offsets)-1], len(data))
	}

	assertOffsetsMatchChunkDataRange(t, data, offsets, len(chunks))
}

func TestReadChunkOffsetsRejectsUnparseableFooters(t *testing.T) {
	data, _ := encodeXorbForTest(t, [4]byte{}, testFooterChunks(3)...)
	footerLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	footerStart := len(data) - int(footerLen) - 4

	corruptIdent := bytes.Clone(data)
	corruptIdent[footerStart] ^= 0xFF

	badLength := bytes.Clone(data)
	binary.LittleEndian.PutUint32(badLength[len(badLength)-4:], uint32(len(badLength)+100))

	tests := []struct {
		name string
		data []byte
	}{
		{"corrupt identifier", corruptIdent},
		{"footer length beyond stream", badLength},
		{"smaller than minimal footer", data[:minFooterSize-1]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ReadChunkOffsets(bytes.NewReader(tt.data)); !errors.Is(err, ErrNoFooter) {
				t.Fatalf("ReadChunkOffsets() error = %v, want ErrNoFooter", err)
			}
		})
	}
}

func TestChunkDataRangeFromOffsetsBounds(t *testing.T) {
	offsets := []uint64{10, 30, 60}

	if _, _, err := ChunkDataRangeFromOffsets(offsets, 1, 1); err == nil {
		t.Fatal("ChunkDataRangeFromOffsets() accepted empty range")
	}
	if _, _, err := ChunkDataRangeFromOffsets(offsets, 0, 4); err == nil {
		t.Fatal("ChunkDataRangeFromOffsets() accepted out-of-bounds range")
	}
}
