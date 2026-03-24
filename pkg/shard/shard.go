// Package shard implements the XET shard binary format (Section 9).
package shard

import (
	"encoding/binary"
	"fmt"
)

// Magic sequence bytes 15-31 (Section 9.3.1).
var ShardMagicSequence = [17]byte{
	0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
	0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
}

// HFApplicationID is the Hugging Face application identifier.
var HFApplicationID = [14]byte{
	0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65,
	0x74, 0x61, 0x44, 0x61, 0x74, 0x61,
}

const (
	// HeaderSize is the shard header size in bytes.
	HeaderSize = 48
	// BookendSize is the bookend entry size.
	BookendSize = 48
	// FooterSize is the shard footer size.
	FooterSize = 200

	// File flags
	FlagWithVerification = 1 << 31
	FlagWithMetadataExt  = 1 << 30

	// Chunk flags
	FlagGlobalDedupEligible = 1 << 31
)

// FileDataSequenceEntry represents a term in a file reconstruction (Section 9.4.3).
type FileDataSequenceEntry struct {
	CASHash             [32]byte
	CASFlags            uint32
	UnpackedSegmentSize uint32
	ChunkIndexStart     uint32
	ChunkIndexEnd       uint32
}

// FileVerificationEntry represents a verification entry (Section 9.4.4).
type FileVerificationEntry struct {
	RangeHash [32]byte
}

// FileBlock represents a complete file block (Section 9.4.1).
type FileBlock struct {
	FileHash     [32]byte
	Flags        uint32
	Entries      []FileDataSequenceEntry
	Verification []FileVerificationEntry
	MetadataExt  *FileMetadataExt
}

// FileMetadataExt represents optional file metadata (Section 9.4.5).
type FileMetadataExt struct {
	SHA256Hash [32]byte
}

// CASChunkSequenceEntry represents a chunk entry in a CAS block (Section 9.5.3).
type CASChunkSequenceEntry struct {
	ChunkHash          [32]byte
	ChunkByteRangeStart uint32
	UnpackedSegmentSize uint32
	Flags              uint32
}

// CASBlock represents a CAS info block (Section 9.5.1).
type CASBlock struct {
	CASHash       [32]byte
	CASFlags      uint32
	NumBytesInCAS uint32
	NumBytesOnDisk uint32
	Entries       []CASChunkSequenceEntry
}

// Shard represents the complete shard structure (Section 9.1).
type Shard struct {
	ApplicationID [14]byte
	FileBlocks    []FileBlock
	CASBlocks     []CASBlock
}

// SerializeForUpload serializes a shard without footer for the upload API (Section 11.6).
func (s *Shard) SerializeForUpload() ([]byte, error) {
	var buf []byte

	// Header (48 bytes, Section 9.3)
	buf = append(buf, s.buildTag()...)
	var version [8]byte
	binary.LittleEndian.PutUint64(version[:], 2) // Version MUST be 2
	buf = append(buf, version[:]...)
	var footerSize [8]byte
	binary.LittleEndian.PutUint64(footerSize[:], 0) // Footer omitted
	buf = append(buf, footerSize[:]...)

	// File Info Section (Section 9.4)
	for _, fb := range s.FileBlocks {
		buf = append(buf, serializeFileBlock(fb)...)
	}
	buf = append(buf, makeBookend()...)

	// CAS Info Section (Section 9.5)
	for _, cb := range s.CASBlocks {
		buf = append(buf, serializeCASBlock(cb)...)
	}
	buf = append(buf, makeBookend()...)

	return buf, nil
}

// buildTag constructs the 32-byte magic tag.
func (s *Shard) buildTag() []byte {
	tag := make([]byte, 32)
	copy(tag[0:14], s.ApplicationID[:])
	tag[14] = 0x00
	copy(tag[15:32], ShardMagicSequence[:])
	return tag
}

// makeBookend creates a 48-byte bookend entry (Section 9.4.6).
func makeBookend() []byte {
	b := make([]byte, BookendSize)
	for i := 0; i < 32; i++ {
		b[i] = 0xFF
	}
	// bytes 32-47 are already zero
	return b
}

// serializeFileBlock serializes a file block.
func serializeFileBlock(fb FileBlock) []byte {
	var buf []byte

	// FileDataSequenceHeader (48 bytes, Section 9.4.2)
	buf = append(buf, fb.FileHash[:]...)          // 32 bytes
	var flagsBuf [4]byte
	binary.LittleEndian.PutUint32(flagsBuf[:], fb.Flags)
	buf = append(buf, flagsBuf[:]...)             // 4 bytes
	var numEntries [4]byte
	binary.LittleEndian.PutUint32(numEntries[:], uint32(len(fb.Entries)))
	buf = append(buf, numEntries[:]...)           // 4 bytes
	buf = append(buf, make([]byte, 8)...)         // 8 bytes reserved

	// FileDataSequenceEntry entries (48 bytes each, Section 9.4.3)
	for _, e := range fb.Entries {
		buf = append(buf, e.CASHash[:]...)        // 32 bytes
		binary.LittleEndian.PutUint32(flagsBuf[:], e.CASFlags)
		buf = append(buf, flagsBuf[:]...)         // 4 bytes
		var sizeBuf [4]byte
		binary.LittleEndian.PutUint32(sizeBuf[:], e.UnpackedSegmentSize)
		buf = append(buf, sizeBuf[:]...)          // 4 bytes
		var startBuf [4]byte
		binary.LittleEndian.PutUint32(startBuf[:], e.ChunkIndexStart)
		buf = append(buf, startBuf[:]...)         // 4 bytes
		var endBuf [4]byte
		binary.LittleEndian.PutUint32(endBuf[:], e.ChunkIndexEnd)
		buf = append(buf, endBuf[:]...)           // 4 bytes
	}

	// FileVerificationEntry entries (48 bytes each, Section 9.4.4)
	if fb.Flags&FlagWithVerification != 0 {
		for _, v := range fb.Verification {
			buf = append(buf, v.RangeHash[:]...)  // 32 bytes
			buf = append(buf, make([]byte, 16)...) // 16 bytes reserved
		}
	}

	// FileMetadataExt (48 bytes, Section 9.4.5)
	if fb.Flags&FlagWithMetadataExt != 0 && fb.MetadataExt != nil {
		buf = append(buf, fb.MetadataExt.SHA256Hash[:]...) // 32 bytes
		buf = append(buf, make([]byte, 16)...)              // 16 bytes reserved
	}

	return buf
}

// serializeCASBlock serializes a CAS info block.
func serializeCASBlock(cb CASBlock) []byte {
	var buf []byte

	// CASChunkSequenceHeader (48 bytes, Section 9.5.2)
	buf = append(buf, cb.CASHash[:]...)           // 32 bytes
	var flagsBuf [4]byte
	binary.LittleEndian.PutUint32(flagsBuf[:], cb.CASFlags)
	buf = append(buf, flagsBuf[:]...)             // 4 bytes
	var numEntries [4]byte
	binary.LittleEndian.PutUint32(numEntries[:], uint32(len(cb.Entries)))
	buf = append(buf, numEntries[:]...)           // 4 bytes
	var numBytesCAS [4]byte
	binary.LittleEndian.PutUint32(numBytesCAS[:], cb.NumBytesInCAS)
	buf = append(buf, numBytesCAS[:]...)          // 4 bytes
	var numBytesDisk [4]byte
	binary.LittleEndian.PutUint32(numBytesDisk[:], cb.NumBytesOnDisk)
	buf = append(buf, numBytesDisk[:]...)         // 4 bytes

	// CASChunkSequenceEntry entries (48 bytes each, Section 9.5.3)
	for _, e := range cb.Entries {
		buf = append(buf, e.ChunkHash[:]...)      // 32 bytes
		var startBuf [4]byte
		binary.LittleEndian.PutUint32(startBuf[:], e.ChunkByteRangeStart)
		buf = append(buf, startBuf[:]...)         // 4 bytes
		var sizeBuf [4]byte
		binary.LittleEndian.PutUint32(sizeBuf[:], e.UnpackedSegmentSize)
		buf = append(buf, sizeBuf[:]...)          // 4 bytes
		binary.LittleEndian.PutUint32(flagsBuf[:], e.Flags)
		buf = append(buf, flagsBuf[:]...)         // 4 bytes
		buf = append(buf, make([]byte, 4)...)     // 4 bytes reserved
	}

	return buf
}

// ParseShardHeader parses and validates a shard header (Section 9.3).
func ParseShardHeader(data []byte) (appID [14]byte, version uint64, footerSize uint64, err error) {
	if len(data) < HeaderSize {
		return appID, 0, 0, fmt.Errorf("shard too small for header")
	}

	copy(appID[:], data[0:14])

	// Verify magic sequence
	if data[14] != 0x00 {
		return appID, 0, 0, fmt.Errorf("missing null separator in tag")
	}

	for i := 0; i < 17; i++ {
		if data[15+i] != ShardMagicSequence[i] {
			return appID, 0, 0, fmt.Errorf("invalid magic sequence at byte %d", 15+i)
		}
	}

	version = binary.LittleEndian.Uint64(data[32:40])
	if version != 2 {
		return appID, version, 0, fmt.Errorf("unsupported shard version: %d", version)
	}

	footerSize = binary.LittleEndian.Uint64(data[40:48])
	return appID, version, footerSize, nil
}
