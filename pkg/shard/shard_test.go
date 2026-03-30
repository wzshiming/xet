package shard

import (
	"bytes"
	"io"
	"testing"

	"github.com/wzshiming/xet"
)

// TestNewShard tests creating a new shard with default values
func TestNewShard(t *testing.T) {
	s := NewShard()

	if s == nil {
		t.Fatal("NewShard returned nil")
	}

	// Verify header version
	if s.Header.Version != 2 {
		t.Errorf("expected version 2, got %d", s.Header.Version)
	}

	// Verify footer size is 0 (no footer)
	if s.Header.FooterSize != 0 {
		t.Errorf("expected footer size 0, got %d", s.Header.FooterSize)
	}

	// Verify magic sequence
	if !bytes.Equal(s.Header.Tag[15:32], shardMagicSequence[:]) {
		t.Error("magic sequence mismatch")
	}

	// Verify HF application ID
	if !bytes.Equal(s.Header.Tag[:14], hfApplicationID[:]) {
		t.Error("application ID mismatch")
	}

	// Verify null byte at position 14
	if s.Header.Tag[14] != 0x00 {
		t.Error("expected null byte at position 14")
	}
}

// TestSerializeEmptyShard tests serializing an empty shard
func TestSerializeEmptyShard(t *testing.T) {
	s := NewShard()

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read serialized data: %v", err)
	}

	// Expected size: 48 (header) + 48 (file bookend) + 48 (CAS bookend) = 144 bytes
	expectedSize := 48 + 48 + 48
	if len(data) != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, len(data))
	}

	// Verify header
	if !bytes.Equal(data[:32], s.Header.Tag[:]) {
		t.Error("header tag mismatch")
	}
}

// TestSerializeDeserializeEmptyShard tests round-trip serialization
func TestSerializeDeserializeEmptyShard(t *testing.T) {
	s := NewShard()

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	s2, err := Deserialize(r)
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Verify header
	if s2.Header.Version != s.Header.Version {
		t.Errorf("version mismatch: expected %d, got %d", s.Header.Version, s2.Header.Version)
	}

	if !bytes.Equal(s2.Header.Tag[:], s.Header.Tag[:]) {
		t.Error("tag mismatch")
	}

	// Verify empty file and CAS info
	if len(s2.Files) != 0 {
		t.Errorf("expected 0 files, got %d", len(s2.Files))
	}

	if len(s2.CASInfos) != 0 {
		t.Errorf("expected 0 CAS blocks, got %d", len(s2.CASInfos))
	}
}

// TestSerializeWithFileBlock tests serializing a shard with a file block
func TestSerializeWithFileBlock(t *testing.T) {
	s := NewShard()

	// Create a file block
	fileHash := xet.Hash{}
	for i := range fileHash {
		fileHash[i] = byte(i)
	}

	casHash := xet.Hash{}
	for i := range casHash {
		casHash[i] = byte(i + 32)
	}

	fb := FileBlock{
		FileHash: fileHash,
		Flags:    0,
		Entries: []FileDataSequenceEntry{
			{
				CASHash:          casHash,
				CASFlags:         0,
				UnpackedSegBytes: 1024,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    5,
			},
		},
	}

	s.AddFile(fb)

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if len(s2.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(s2.Files))
	}

	// Verify file block
	if !bytes.Equal(s2.Files[0].FileHash[:], fileHash[:]) {
		t.Error("file hash mismatch")
	}

	if len(s2.Files[0].Entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(s2.Files[0].Entries))
	}

	entry := s2.Files[0].Entries[0]
	if !bytes.Equal(entry.CASHash[:], casHash[:]) {
		t.Error("CAS hash mismatch")
	}

	if entry.UnpackedSegBytes != 1024 {
		t.Errorf("expected unpacked seg bytes 1024, got %d", entry.UnpackedSegBytes)
	}

	if entry.ChunkIndexStart != 0 {
		t.Errorf("expected chunk index start 0, got %d", entry.ChunkIndexStart)
	}

	if entry.ChunkIndexEnd != 5 {
		t.Errorf("expected chunk index end 5, got %d", entry.ChunkIndexEnd)
	}
}

// TestSerializeWithVerification tests serializing a shard with verification entries
func TestSerializeWithVerification(t *testing.T) {
	s := NewShard()

	verifHash := xet.Hash{}
	for i := range verifHash {
		verifHash[i] = byte(i * 2)
	}

	fb := FileBlock{
		FileHash: xet.Hash{},
		Flags:    FileWithVerification,
		Entries: []FileDataSequenceEntry{
			{
				CASHash:          xet.Hash{},
				CASFlags:         0,
				UnpackedSegBytes: 512,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    3,
			},
		},
		Verification: []xet.Hash{verifHash},
	}

	s.AddFile(fb)

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if len(s2.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(s2.Files))
	}

	// Verify verification flag is set
	if s2.Files[0].Flags&FileWithVerification == 0 {
		t.Error("verification flag not set")
	}

	// Verify verification hash
	if len(s2.Files[0].Verification) != 1 {
		t.Fatalf("expected 1 verification entry, got %d", len(s2.Files[0].Verification))
	}

	if !bytes.Equal(s2.Files[0].Verification[0][:], verifHash[:]) {
		t.Error("verification hash mismatch")
	}
}

// TestSerializeWithMetadataExt tests serializing a shard with metadata extension
func TestSerializeWithMetadataExt(t *testing.T) {
	s := NewShard()

	sha256Hash := [32]byte{}
	for i := range sha256Hash {
		sha256Hash[i] = byte(i * 3)
	}

	fb := FileBlock{
		FileHash: xet.Hash{},
		Flags:    FileWithMetadataExt,
		Entries: []FileDataSequenceEntry{
			{
				CASHash:          xet.Hash{},
				CASFlags:         0,
				UnpackedSegBytes: 256,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    2,
			},
		},
		MetadataExt: &FileMetadataExt{
			SHA256Hash: sha256Hash,
		},
	}

	s.AddFile(fb)

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if len(s2.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(s2.Files))
	}

	// Verify metadata ext flag is set
	if s2.Files[0].Flags&FileWithMetadataExt == 0 {
		t.Error("metadata ext flag not set")
	}

	// Verify metadata ext
	if s2.Files[0].MetadataExt == nil {
		t.Fatal("metadata ext is nil")
	}

	if !bytes.Equal(s2.Files[0].MetadataExt.SHA256Hash[:], sha256Hash[:]) {
		t.Error("SHA-256 hash mismatch")
	}
}

// TestSerializeWithCASBlock tests serializing a shard with a CAS block
func TestSerializeWithCASBlock(t *testing.T) {
	s := NewShard()

	casHash := xet.Hash{}
	for i := range casHash {
		casHash[i] = byte(i + 64)
	}

	chunkHash1 := xet.Hash{}
	for i := range chunkHash1 {
		chunkHash1[i] = byte(i)
	}

	chunkHash2 := xet.Hash{}
	for i := range chunkHash2 {
		chunkHash2[i] = byte(i + 32)
	}

	cb := CASBlock{
		CASHash:        casHash,
		CASFlags:       0,
		NumBytesInCAS:  2048,
		NumBytesOnDisk: 1024,
		Chunks: []CASChunkSequenceEntry{
			{
				ChunkHash:        chunkHash1,
				ByteRangeStart:   0,
				UnpackedSegBytes: 1024,
				Flags:            ChunkGlobalDedupEligible,
			},
			{
				ChunkHash:        chunkHash2,
				ByteRangeStart:   1024,
				UnpackedSegBytes: 1024,
				Flags:            0,
			},
		},
	}

	s.AddCASBlock(cb)

	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	if len(s2.CASInfos) != 1 {
		t.Fatalf("expected 1 CAS block, got %d", len(s2.CASInfos))
	}

	// Verify CAS block
	if !bytes.Equal(s2.CASInfos[0].CASHash[:], casHash[:]) {
		t.Error("CAS hash mismatch")
	}

	if s2.CASInfos[0].NumBytesInCAS != 2048 {
		t.Errorf("expected num bytes in CAS 2048, got %d", s2.CASInfos[0].NumBytesInCAS)
	}

	if s2.CASInfos[0].NumBytesOnDisk != 1024 {
		t.Errorf("expected num bytes on disk 1024, got %d", s2.CASInfos[0].NumBytesOnDisk)
	}

	if len(s2.CASInfos[0].Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(s2.CASInfos[0].Chunks))
	}

	// Verify chunk 1
	chunk1 := s2.CASInfos[0].Chunks[0]
	if !bytes.Equal(chunk1.ChunkHash[:], chunkHash1[:]) {
		t.Error("chunk 1 hash mismatch")
	}

	if chunk1.ByteRangeStart != 0 {
		t.Errorf("chunk 1: expected byte range start 0, got %d", chunk1.ByteRangeStart)
	}

	if chunk1.UnpackedSegBytes != 1024 {
		t.Errorf("chunk 1: expected unpacked seg bytes 1024, got %d", chunk1.UnpackedSegBytes)
	}

	if chunk1.Flags&ChunkGlobalDedupEligible == 0 {
		t.Error("chunk 1: global dedup eligible flag not set")
	}

	// Verify chunk 2
	chunk2 := s2.CASInfos[0].Chunks[1]
	if !bytes.Equal(chunk2.ChunkHash[:], chunkHash2[:]) {
		t.Error("chunk 2 hash mismatch")
	}

	if chunk2.ByteRangeStart != 1024 {
		t.Errorf("chunk 2: expected byte range start 1024, got %d", chunk2.ByteRangeStart)
	}
}

// TestSerializeWithFooter tests serializing a shard with footer
func TestSerializeWithFooter(t *testing.T) {
	s := NewShard()

	// Add some data
	fb := FileBlock{
		FileHash: xet.Hash{},
		Flags:    0,
		Entries: []FileDataSequenceEntry{
			{
				CASHash:          xet.Hash{},
				CASFlags:         0,
				UnpackedSegBytes: 100,
				ChunkIndexStart:  0,
				ChunkIndexEnd:    1,
			},
		},
	}
	s.AddFile(fb)

	// Create footer
	s.Footer = &Footer{
		Version:                1,
		FileLookupOffset:       0,
		FileLookupNumEntries:   0,
		CASLookupOffset:        0,
		CASLookupNumEntries:    0,
		ChunkLookupOffset:      0,
		ChunkLookupNumEntries:  0,
		ShardCreationTimestamp: 1234567890,
		ShardKeyExpiry:         1234567900,
		StoredBytesOnDisk:      100,
		MaterializedBytes:      100,
		StoredBytes:            100,
	}

	r, err := s.Serialize(true)
	if err != nil {
		t.Fatalf("failed to serialize with footer: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to read serialized data: %v", err)
	}

	// Verify footer is present (should end with 200 bytes)
	if len(data) < 200 {
		t.Fatalf("data too short to contain footer: %d", len(data))
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Verify footer was read
	if s2.Footer == nil {
		t.Fatal("footer is nil after deserialization")
	}

	if s2.Footer.Version != 1 {
		t.Errorf("expected footer version 1, got %d", s2.Footer.Version)
	}

	if s2.Footer.ShardCreationTimestamp != 1234567890 {
		t.Errorf("expected creation timestamp 1234567890, got %d", s2.Footer.ShardCreationTimestamp)
	}

	if s2.Footer.ShardKeyExpiry != 1234567900 {
		t.Errorf("expected key expiry 1234567900, got %d", s2.Footer.ShardKeyExpiry)
	}
}

// TestSerializeComplexShard tests serializing a complex shard with multiple blocks
func TestSerializeComplexShard(t *testing.T) {
	s := NewShard()

	// Add multiple file blocks
	for i := range 3 {
		fileHash := xet.Hash{}
		for j := range fileHash {
			fileHash[j] = byte(i*32 + j)
		}

		fb := FileBlock{
			FileHash: fileHash,
			Flags:    0,
			Entries: []FileDataSequenceEntry{
				{
					CASHash:          xet.Hash{},
					CASFlags:         0,
					UnpackedSegBytes: uint32(100 * (i + 1)),
					ChunkIndexStart:  uint32(i * 5),
					ChunkIndexEnd:    uint32((i + 1) * 5),
				},
			},
		}
		s.AddFile(fb)
	}

	// Add multiple CAS blocks
	for i := range 2 {
		casHash := xet.Hash{}
		for j := range casHash {
			casHash[j] = byte(i*64 + j)
		}

		cb := CASBlock{
			CASHash:        casHash,
			CASFlags:       0,
			NumBytesInCAS:  uint32(1000 * (i + 1)),
			NumBytesOnDisk: uint32(500 * (i + 1)),
			Chunks: []CASChunkSequenceEntry{
				{
					ChunkHash:        xet.Hash{},
					ByteRangeStart:   0,
					UnpackedSegBytes: uint32(200 * (i + 1)),
					Flags:            0,
				},
			},
		}
		s.AddCASBlock(cb)
	}

	// Serialize
	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Serialize and verify
	s2, err := Deserialize(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("failed to deserialize: %v", err)
	}

	// Verify counts
	if len(s2.Files) != 3 {
		t.Errorf("expected 3 files, got %d", len(s2.Files))
	}

	if len(s2.CASInfos) != 2 {
		t.Errorf("expected 2 CAS blocks, got %d", len(s2.CASInfos))
	}
}

// TestIsBookend tests the bookend detection function
func TestIsBookend(t *testing.T) {
	// Create a valid bookend
	bookend := make([]byte, 48)
	for i := range 32 {
		bookend[i] = 0xFF
	}
	for i := 32; i < 48; i++ {
		bookend[i] = 0x00
	}

	if !isBookend(bookend) {
		t.Error("failed to detect valid bookend")
	}

	// Test invalid bookend (wrong length)
	if isBookend(bookend[:47]) {
		t.Error("detected bookend with wrong length")
	}

	// Test invalid bookend (wrong pattern)
	invalid := make([]byte, 48)
	for i := range invalid {
		invalid[i] = 0xFF
	}
	if isBookend(invalid) {
		t.Error("detected invalid bookend pattern")
	}
}

// TestMagicSequence tests that the magic sequence matches the spec
func TestMagicSequence(t *testing.T) {
	expected := []byte{
		0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
		0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
	}

	if !bytes.Equal(shardMagicSequence[:], expected) {
		t.Error("magic sequence does not match spec")
	}
}

// TestApplicationID tests that the HF application ID matches the spec
func TestApplicationID(t *testing.T) {
	expected := []byte{
		0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65,
		0x74, 0x61, 0x44, 0x61, 0x74, 0x61,
	}

	if !bytes.Equal(hfApplicationID[:], expected) {
		t.Error("HF application ID does not match spec")
	}

	// Verify it's "HFRepoMetaData"
	expectedStr := "HFRepoMetaData"
	if string(hfApplicationID[:]) != expectedStr {
		t.Errorf("expected %q, got %q", expectedStr, string(hfApplicationID[:]))
	}
}

// TestDeserializeInvalidMagicSequence tests deserializing with invalid magic sequence
func TestDeserializeInvalidMagicSequence(t *testing.T) {
	s := NewShard()

	// Serialize a valid shard
	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Corrupt the magic sequence
	data[20] = 0x00

	// Try to deserialize
	_, err = Deserialize(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for invalid magic sequence, got nil")
	}
}

// TestDeserializeInvalidVersion tests deserializing with invalid version
func TestDeserializeInvalidVersion(t *testing.T) {
	s := NewShard()

	// Serialize a valid shard
	r, err := s.Serialize(false)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("failed to serialize: %v", err)
	}

	// Change the version to 99
	data[32] = 99

	// Try to deserialize
	_, err = Deserialize(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for invalid version, got nil")
	}
}

// TestDeserializeTooShort tests deserializing data that's too short
func TestDeserializeTooShort(t *testing.T) {
	data := make([]byte, 10)

	_, err := Deserialize(bytes.NewReader(data))
	if err == nil {
		t.Error("expected error for data too short, got nil")
	}
}
