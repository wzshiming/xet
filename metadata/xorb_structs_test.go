package metadata

import (
	"bytes"
	"testing"

	"github.com/wzshiming/xet/merklehash"
)

func TestXorbChunkSequenceHeaderSerializeDeserialize(t *testing.T) {
	h := NewXorbChunkSequenceHeader(testHash(42), 10, 65536)
	h.XorbFlags = 0xABCD
	h.NumBytesOnDisk = 32768

	var buf bytes.Buffer
	n, err := h.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != MDBFileInfoEntrySize {
		t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
	}
	got, err := DeserializeXorbChunkSequenceHeader(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("mismatch: got %+v, want %+v", got, h)
	}
}

func TestXorbChunkSequenceHeaderBookend(t *testing.T) {
	h := BookendXorbHeader()
	if !h.IsBookend() {
		t.Fatal("expected bookend")
	}
	normal := NewXorbChunkSequenceHeader(testHash(1), 1, 100)
	if normal.IsBookend() {
		t.Fatal("expected non-bookend")
	}
}

func TestXorbChunkSequenceEntrySerializeDeserialize(t *testing.T) {
	e := XorbChunkSequenceEntry{
		ChunkHash:            testHash(100),
		ChunkByteRangeStart:  1024,
		UnpackedSegmentBytes: 4096,
		Flags:                0x12345678,
		Unused:               0,
	}
	var buf bytes.Buffer
	n, err := e.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != MDBFileInfoEntrySize {
		t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
	}
	got, err := DeserializeXorbChunkSequenceEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestXorbChunkSequenceEntryGlobalDedup(t *testing.T) {
	e := NewXorbChunkSequenceEntry(testHash(999), 4096, 0)

	// Initially no flag
	if e.Flags&MDBChunkWithGlobalDedupFlag != 0 {
		t.Fatal("expected no global dedup flag initially")
	}

	// Set flag
	e2 := e.WithGlobalDedupFlag(true)
	if e2.Flags&MDBChunkWithGlobalDedupFlag == 0 {
		t.Fatal("expected global dedup flag to be set")
	}
	if !e2.IsGlobalDedupEligible() {
		t.Fatal("expected global dedup eligible")
	}

	// Clear flag
	e3 := e2.WithGlobalDedupFlag(false)
	if e3.Flags&MDBChunkWithGlobalDedupFlag != 0 {
		t.Fatal("expected global dedup flag to be cleared")
	}

	// Test hash-based eligibility: construct a hash where d[3] % 1024 == 0
	eligibleHash := merklehash.DataHash{1, 2, 3, 1024}
	eEligible := NewXorbChunkSequenceEntry(eligibleHash, 4096, 0)
	if !eEligible.IsGlobalDedupEligible() {
		t.Fatal("expected hash-based global dedup eligible")
	}

	// Test hash-based ineligibility: construct a hash where d[3] % 1024 != 0
	ineligibleHash := merklehash.DataHash{1, 2, 3, 1025}
	eIneligible := NewXorbChunkSequenceEntry(ineligibleHash, 4096, 0)
	if eIneligible.IsGlobalDedupEligible() {
		t.Fatal("expected hash-based global dedup ineligible")
	}
}

func TestMDBXorbInfoSerializeDeserialize(t *testing.T) {
	chunks := []XorbChunkSequenceEntry{
		{ChunkHash: testHash(10), ChunkByteRangeStart: 0, UnpackedSegmentBytes: 1000, Flags: 0},
		{ChunkHash: testHash(20), ChunkByteRangeStart: 1000, UnpackedSegmentBytes: 2000, Flags: 0},
		{ChunkHash: testHash(30), ChunkByteRangeStart: 3000, UnpackedSegmentBytes: 1500, Flags: 0},
	}

	info := &MDBXorbInfo{
		Metadata: NewXorbChunkSequenceHeader(testHash(1), uint32(len(chunks)), 4500),
		Chunks:   chunks,
	}

	var buf bytes.Buffer
	n, err := info.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(n) != info.NumBytes() {
		t.Fatalf("expected %d bytes, got %d", info.NumBytes(), n)
	}

	got, err := DeserializeMDBXorbInfo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected non-nil")
	}
	if got.Metadata != info.Metadata {
		t.Fatalf("header mismatch: got %+v, want %+v", got.Metadata, info.Metadata)
	}
	if len(got.Chunks) != len(info.Chunks) {
		t.Fatalf("chunks length: got %d, want %d", len(got.Chunks), len(info.Chunks))
	}
	for i := range info.Chunks {
		if got.Chunks[i] != info.Chunks[i] {
			t.Fatalf("chunk %d mismatch: got %+v, want %+v", i, got.Chunks[i], info.Chunks[i])
		}
	}

	// Test ChunksAndBoundaries
	boundaries := got.ChunksAndBoundaries()
	if len(boundaries) != 3 {
		t.Fatalf("expected 3 boundaries, got %d", len(boundaries))
	}
	if boundaries[0].Start != 0 || boundaries[0].End != 1000 {
		t.Fatalf("boundary 0: got [%d, %d), want [0, 1000)", boundaries[0].Start, boundaries[0].End)
	}
	if boundaries[1].Start != 1000 || boundaries[1].End != 3000 {
		t.Fatalf("boundary 1: got [%d, %d), want [1000, 3000)", boundaries[1].Start, boundaries[1].End)
	}
	if boundaries[2].Start != 3000 || boundaries[2].End != 4500 {
		t.Fatalf("boundary 2: got [%d, %d), want [3000, 4500)", boundaries[2].Start, boundaries[2].End)
	}
}

func TestMDBXorbInfoBookend(t *testing.T) {
	h := BookendXorbHeader()
	var buf bytes.Buffer
	if _, err := h.Serialize(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := DeserializeMDBXorbInfo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("expected nil for bookend")
	}
}
