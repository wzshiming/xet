package shard

import (
	"testing"
)

func TestSerializeForUpload(t *testing.T) {
	s := &Shard{
		ApplicationID: HFApplicationID,
		FileBlocks:    nil,
		CASBlocks:     nil,
	}

	data, err := s.SerializeForUpload()
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}

	// Should have header (48) + file bookend (48) + CAS bookend (48) = 144
	expectedSize := HeaderSize + BookendSize + BookendSize
	if len(data) != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, len(data))
	}

	// Verify header
	appID, version, footerSize, err := ParseShardHeader(data)
	if err != nil {
		t.Fatalf("parse header error: %v", err)
	}

	if appID != HFApplicationID {
		t.Error("application ID mismatch")
	}
	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}
	if footerSize != 0 {
		t.Errorf("expected footer size 0, got %d", footerSize)
	}
}

func TestSerializeWithFileBlock(t *testing.T) {
	var fileHash [32]byte
	fileHash[0] = 0x42

	var casHash [32]byte
	casHash[0] = 0x43

	var rangeHash [32]byte
	rangeHash[0] = 0x44

	var sha256Hash [32]byte
	sha256Hash[0] = 0x45

	s := &Shard{
		ApplicationID: HFApplicationID,
		FileBlocks: []FileBlock{
			{
				FileHash: fileHash,
				Flags:    FlagWithVerification | FlagWithMetadataExt,
				Entries: []FileDataSequenceEntry{
					{
						CASHash:             casHash,
						UnpackedSegmentSize: 1000,
						ChunkIndexStart:     0,
						ChunkIndexEnd:       5,
					},
				},
				Verification: []FileVerificationEntry{
					{RangeHash: rangeHash},
				},
				MetadataExt: &FileMetadataExt{SHA256Hash: sha256Hash},
			},
		},
		CASBlocks: nil,
	}

	data, err := s.SerializeForUpload()
	if err != nil {
		t.Fatalf("serialize error: %v", err)
	}

	// Verify we can parse the header
	_, version, _, err := ParseShardHeader(data)
	if err != nil {
		t.Fatalf("parse header error: %v", err)
	}
	if version != 2 {
		t.Errorf("expected version 2, got %d", version)
	}

	// Check minimum expected size:
	// Header (48) + FileDataSequenceHeader (48) + 1 entry (48) + 1 verification (48) + metadata (48) + file bookend (48) + CAS bookend (48)
	expectedMinSize := 48 + 48 + 48 + 48 + 48 + 48 + 48
	if len(data) < expectedMinSize {
		t.Errorf("serialized data too small: %d < %d", len(data), expectedMinSize)
	}
}

func TestParseInvalidHeader(t *testing.T) {
	// Too small
	_, _, _, err := ParseShardHeader([]byte{0x00})
	if err == nil {
		t.Error("expected error for small header")
	}

	// Invalid magic
	data := make([]byte, 48)
	_, _, _, err = ParseShardHeader(data)
	if err == nil {
		t.Error("expected error for invalid magic")
	}
}

func TestBookendFormat(t *testing.T) {
	bookend := makeBookend()
	if len(bookend) != BookendSize {
		t.Errorf("bookend size: %d != %d", len(bookend), BookendSize)
	}

	// First 32 bytes should be 0xFF
	for i := 0; i < 32; i++ {
		if bookend[i] != 0xFF {
			t.Errorf("bookend byte %d: expected 0xFF, got 0x%02X", i, bookend[i])
		}
	}

	// Last 16 bytes should be 0x00
	for i := 32; i < 48; i++ {
		if bookend[i] != 0x00 {
			t.Errorf("bookend byte %d: expected 0x00, got 0x%02X", i, bookend[i])
		}
	}
}
