package metadata

import (
	"bytes"
	"testing"

	"github.com/wzshiming/xet/merklehash"
)

func testHash(seed uint64) merklehash.DataHash {
	return merklehash.DataHash{seed, seed + 1, seed + 2, seed + 3}
}

func TestFileDataSequenceHeaderSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name                 string
		containsVerification bool
		containsMetadataExt  bool
	}{
		{"no flags", false, false},
		{"with verification", true, false},
		{"with metadata ext", false, true},
		{"with both", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewFileDataSequenceHeader(testHash(42), 5, tt.containsVerification, tt.containsMetadataExt)
			var buf bytes.Buffer
			n, err := h.Serialize(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if n != MDBFileInfoEntrySize {
				t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
			}
			got, err := DeserializeFileDataSequenceHeader(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if got != h {
				t.Fatalf("mismatch: got %+v, want %+v", got, h)
			}
			if got.ContainsVerification() != tt.containsVerification {
				t.Fatalf("containsVerification: got %v, want %v", got.ContainsVerification(), tt.containsVerification)
			}
			if got.ContainsMetadataExt() != tt.containsMetadataExt {
				t.Fatalf("containsMetadataExt: got %v, want %v", got.ContainsMetadataExt(), tt.containsMetadataExt)
			}
		})
	}
}

func TestFileDataSequenceHeaderBookend(t *testing.T) {
	h := BookendFileHeader()
	if !h.IsBookend() {
		t.Fatal("expected bookend")
	}
	normal := NewFileDataSequenceHeader(testHash(1), 1, false, false)
	if normal.IsBookend() {
		t.Fatal("expected non-bookend")
	}
}

func TestFileDataSequenceEntrySerializeDeserialize(t *testing.T) {
	e := FileDataSequenceEntry{
		XorbHash:             testHash(100),
		XorbFlags:            0x12345678,
		UnpackedSegmentBytes: 65536,
		ChunkIndexStart:      10,
		ChunkIndexEnd:        20,
	}
	var buf bytes.Buffer
	n, err := e.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != MDBFileInfoEntrySize {
		t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
	}
	got, err := DeserializeFileDataSequenceEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestFileVerificationEntrySerializeDeserialize(t *testing.T) {
	e := FileVerificationEntry{
		RangeHash: testHash(200),
		Unused:    [2]uint64{111, 222},
	}
	var buf bytes.Buffer
	n, err := e.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != MDBFileInfoEntrySize {
		t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
	}
	got, err := DeserializeFileVerificationEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestFileMetadataExtSerializeDeserialize(t *testing.T) {
	e := FileMetadataExt{
		SHA256: testHash(300),
		Unused: [2]uint64{333, 444},
	}
	var buf bytes.Buffer
	n, err := e.Serialize(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != MDBFileInfoEntrySize {
		t.Fatalf("expected %d bytes, got %d", MDBFileInfoEntrySize, n)
	}
	got, err := DeserializeFileMetadataExt(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != e {
		t.Fatalf("mismatch: got %+v, want %+v", got, e)
	}
}

func TestMDBFileInfoSerializeDeserialize(t *testing.T) {
	tests := []struct {
		name                 string
		containsVerification bool
		containsMetadataExt  bool
	}{
		{"no flags", false, false},
		{"with verification", true, false},
		{"with metadata ext", false, true},
		{"with both", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			segments := []FileDataSequenceEntry{
				{XorbHash: testHash(10), XorbFlags: 0, UnpackedSegmentBytes: 1000, ChunkIndexStart: 0, ChunkIndexEnd: 5},
				{XorbHash: testHash(20), XorbFlags: 1, UnpackedSegmentBytes: 2000, ChunkIndexStart: 5, ChunkIndexEnd: 10},
			}

			var verification []FileVerificationEntry
			if tt.containsVerification {
				verification = []FileVerificationEntry{
					{RangeHash: testHash(30)},
					{RangeHash: testHash(40)},
				}
			}

			var metadataExt *FileMetadataExt
			if tt.containsMetadataExt {
				metadataExt = &FileMetadataExt{SHA256: testHash(50)}
			}

			info := &MDBFileInfo{
				Metadata:     NewFileDataSequenceHeader(testHash(1), uint32(len(segments)), tt.containsVerification, tt.containsMetadataExt),
				Segments:     segments,
				Verification: verification,
				MetadataExt:  metadataExt,
			}

			var buf bytes.Buffer
			n, err := info.Serialize(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if uint64(n) != info.NumBytes() {
				t.Fatalf("expected %d bytes, got %d", info.NumBytes(), n)
			}

			got, err := DeserializeMDBFileInfo(&buf)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("expected non-nil")
			}

			// Verify header
			if got.Metadata != info.Metadata {
				t.Fatalf("header mismatch: got %+v, want %+v", got.Metadata, info.Metadata)
			}

			// Verify segments
			if len(got.Segments) != len(info.Segments) {
				t.Fatalf("segments length: got %d, want %d", len(got.Segments), len(info.Segments))
			}
			for i := range info.Segments {
				if got.Segments[i] != info.Segments[i] {
					t.Fatalf("segment %d mismatch: got %+v, want %+v", i, got.Segments[i], info.Segments[i])
				}
			}

			// Verify verification
			if len(got.Verification) != len(info.Verification) {
				t.Fatalf("verification length: got %d, want %d", len(got.Verification), len(info.Verification))
			}
			for i := range info.Verification {
				if got.Verification[i] != info.Verification[i] {
					t.Fatalf("verification %d mismatch: got %+v, want %+v", i, got.Verification[i], info.Verification[i])
				}
			}

			// Verify metadata ext
			if tt.containsMetadataExt {
				if got.MetadataExt == nil {
					t.Fatal("expected metadata ext")
				}
				if *got.MetadataExt != *info.MetadataExt {
					t.Fatalf("metadata ext mismatch: got %+v, want %+v", *got.MetadataExt, *info.MetadataExt)
				}
			} else {
				if got.MetadataExt != nil {
					t.Fatal("expected nil metadata ext")
				}
			}

			// Verify file size
			if got.FileSize() != 3000 {
				t.Fatalf("file size: got %d, want 3000", got.FileSize())
			}
		})
	}
}

func TestMDBFileInfoBookend(t *testing.T) {
	h := BookendFileHeader()
	var buf bytes.Buffer
	if _, err := h.Serialize(&buf); err != nil {
		t.Fatal(err)
	}
	got, err := DeserializeMDBFileInfo(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("expected nil for bookend")
	}
}
