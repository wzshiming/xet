package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/wzshiming/xet"
)

// Shard represents a binary metadata structure that describes file reconstructions and xorb contents
type Shard struct {
	Header   Header
	Files    []FileBlock
	CASInfos []CASBlock
	Footer   *Footer // Optional, omitted for upload API
}

// Header represents the 48-byte shard header
type Header struct {
	Tag        [32]byte // Magic identifier (application ID + magic sequence)
	Version    uint64   // Must be 2
	FooterSize uint64   // 0 if footer omitted
}

// FileFlags represents flags in the file data sequence header
type FileFlags uint32

const (
	FileWithVerification FileFlags = 1 << 31 // FileVerificationEntry present for each entry
	FileWithMetadataExt  FileFlags = 1 << 30 // FileMetadataExt present at end
)

// FileBlock represents a file reconstruction block
type FileBlock struct {
	FileHash     xet.Hash
	Flags        FileFlags
	Entries      []FileDataSequenceEntry
	Verification []xet.Hash       // Present if FileWithVerification flag set
	MetadataExt  *FileMetadataExt // Present if FileWithMetadataExt flag set
}

// FileDataSequenceEntry describes a term in the file reconstruction
type FileDataSequenceEntry struct {
	CASHash          xet.Hash // xorb hash
	CASFlags         uint32   // Reserved, must be 0
	UnpackedSegBytes uint32
	ChunkIndexStart  uint32
	ChunkIndexEnd    uint32 // Exclusive
}

// FileMetadataExt contains optional metadata (SHA-256 hash)
type FileMetadataExt struct {
	SHA256Hash [32]byte
}

// CASBlock represents a xorb and its chunks
type CASBlock struct {
	CASHash        xet.Hash
	CASFlags       uint32 // Reserved, must be 0
	Chunks         []CASChunkSequenceEntry
	NumBytesInCAS  uint32 // Total uncompressed bytes
	NumBytesOnDisk uint32 // Serialized xorb size
}

// CASChunkSequenceEntry describes a chunk in a xorb
type CASChunkSequenceEntry struct {
	ChunkHash        xet.Hash
	ByteRangeStart   uint32 // Cumulative byte offset within uncompressed xorb
	UnpackedSegBytes uint32
	Flags            ChunkFlags
}

// ChunkFlags represents flags in chunk entries
type ChunkFlags uint32

const (
	ChunkGlobalDedupEligible ChunkFlags = 1 << 31 // Chunk is eligible for global deduplication
)

// Footer represents the 200-byte shard footer
type Footer struct {
	Version                uint64 // Must be 1
	FileInfoOffset         uint64
	CASInfoOffset          uint64
	FileLookupOffset       uint64
	FileLookupNumEntries   uint64
	CASLookupOffset        uint64
	CASLookupNumEntries    uint64
	ChunkLookupOffset      uint64
	ChunkLookupNumEntries  uint64
	ChunkHashKey           [32]byte
	ShardCreationTimestamp uint64 // Unix epoch seconds
	ShardKeyExpiry         uint64 // Unix epoch seconds
	Reserved               [48]byte
	StoredBytesOnDisk      uint64
	MaterializedBytes      uint64
	StoredBytes            uint64
	FooterOffset           uint64
}

// Shard magic sequence (bytes 15-31 of the tag)
var shardMagicSequence = [17]byte{
	0x55, 0x69, 0x67, 0x45, 0x6a, 0x7b, 0x81, 0x57,
	0x83, 0xa5, 0xbd, 0xd9, 0x5c, 0xcd, 0xd1, 0x4a, 0xa9,
}

// Default application identifier for Hugging Face deployments
var hfApplicationID = [14]byte{
	0x48, 0x46, 0x52, 0x65, 0x70, 0x6f, 0x4d, 0x65,
	0x74, 0x61, 0x44, 0x61, 0x74, 0x61,
}

// NewShard creates a new empty shard with default header
func NewShard() *Shard {
	// Build default tag with HF application ID
	var tag [32]byte
	copy(tag[:14], hfApplicationID[:])
	tag[14] = 0x00 // Null byte
	copy(tag[15:], shardMagicSequence[:])

	return &Shard{
		Header: Header{
			Tag:        tag,
			Version:    2,
			FooterSize: 0,
		},
		Files:    make([]FileBlock, 0),
		CASInfos: make([]CASBlock, 0),
	}
}

// AddFile adds a file block to the shard
func (s *Shard) AddFile(fb FileBlock) {
	s.Files = append(s.Files, fb)
}

// AddCASBlock adds a CAS block to the shard
func (s *Shard) AddCASBlock(cb CASBlock) {
	s.CASInfos = append(s.CASInfos, cb)
}

// Serialize serializes the shard to binary format (without footer for upload API)
func (s *Shard) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	// Write header
	if err := s.writeHeader(&buf); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Write file info section
	if err := s.writeFileInfoSection(&buf); err != nil {
		return nil, fmt.Errorf("failed to write file info section: %w", err)
	}

	// Write CAS info section
	if err := s.writeCASInfoSection(&buf); err != nil {
		return nil, fmt.Errorf("failed to write CAS info section: %w", err)
	}

	// Note: Footer is omitted for upload API

	return buf.Bytes(), nil
}

// SerializeWithFooter serializes the shard with footer (for stored shards)
func (s *Shard) SerializeWithFooter() ([]byte, error) {
	if s.Footer == nil {
		return nil, fmt.Errorf("footer is required but not set")
	}

	var buf bytes.Buffer

	// Update header with footer size
	s.Header.FooterSize = 200

	// Write header
	if err := s.writeHeader(&buf); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	// Write file info section
	fileInfoOffset := uint64(buf.Len())
	if err := s.writeFileInfoSection(&buf); err != nil {
		return nil, fmt.Errorf("failed to write file info section: %w", err)
	}

	// Write CAS info section
	casInfoOffset := uint64(buf.Len())
	if err := s.writeCASInfoSection(&buf); err != nil {
		return nil, fmt.Errorf("failed to write CAS info section: %w", err)
	}

	// Update footer with offsets
	s.Footer.FileInfoOffset = fileInfoOffset
	s.Footer.CASInfoOffset = casInfoOffset
	s.Footer.FooterOffset = uint64(buf.Len())

	// Write footer
	if err := s.writeFooter(&buf); err != nil {
		return nil, fmt.Errorf("failed to write footer: %w", err)
	}

	return buf.Bytes(), nil
}

// writeHeader writes the 48-byte header
func (s *Shard) writeHeader(buf *bytes.Buffer) error {
	// Tag (32 bytes)
	buf.Write(s.Header.Tag[:])

	// Version (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, s.Header.Version); err != nil {
		return err
	}

	// FooterSize (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, s.Header.FooterSize); err != nil {
		return err
	}

	return nil
}

// writeFileInfoSection writes the file info section
func (s *Shard) writeFileInfoSection(buf *bytes.Buffer) error {
	for _, fb := range s.Files {
		if err := s.writeFileBlock(buf, fb); err != nil {
			return fmt.Errorf("failed to write file block: %w", err)
		}
	}

	// Write bookend entry (48 bytes)
	s.writeBookend(buf)

	return nil
}

// writeFileBlock writes a single file block
func (s *Shard) writeFileBlock(buf *bytes.Buffer, fb FileBlock) error {
	// FileDataSequenceHeader (48 bytes)
	buf.Write(fb.FileHash[:])                                       // 32 bytes
	binary.Write(buf, binary.LittleEndian, uint32(fb.Flags))        // 4 bytes
	binary.Write(buf, binary.LittleEndian, uint32(len(fb.Entries))) // 4 bytes
	buf.Write(make([]byte, 8))                                      // 8 bytes reserved

	// FileDataSequenceEntry entries (48 bytes each)
	for _, entry := range fb.Entries {
		buf.Write(entry.CASHash[:])                                    // 32 bytes
		binary.Write(buf, binary.LittleEndian, entry.CASFlags)         // 4 bytes
		binary.Write(buf, binary.LittleEndian, entry.UnpackedSegBytes) // 4 bytes
		binary.Write(buf, binary.LittleEndian, entry.ChunkIndexStart)  // 4 bytes
		binary.Write(buf, binary.LittleEndian, entry.ChunkIndexEnd)    // 4 bytes
	}

	// FileVerificationEntry entries (48 bytes each) if flag set
	if fb.Flags&FileWithVerification != 0 {
		for _, verif := range fb.Verification {
			buf.Write(verif[:])         // 32 bytes
			buf.Write(make([]byte, 16)) // 16 bytes reserved
		}
	}

	// FileMetadataExt (48 bytes) if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if fb.MetadataExt != nil {
			buf.Write(fb.MetadataExt.SHA256Hash[:]) // 32 bytes
			buf.Write(make([]byte, 16))             // 16 bytes reserved
		}
	}

	return nil
}

// writeCASInfoSection writes the CAS info section
func (s *Shard) writeCASInfoSection(buf *bytes.Buffer) error {
	for _, cb := range s.CASInfos {
		if err := s.writeCASBlock(buf, cb); err != nil {
			return fmt.Errorf("failed to write CAS block: %w", err)
		}
	}

	// Write bookend entry (48 bytes)
	s.writeBookend(buf)

	return nil
}

// writeCASBlock writes a single CAS block
func (s *Shard) writeCASBlock(buf *bytes.Buffer, cb CASBlock) error {
	// CASChunkSequenceHeader (48 bytes)
	buf.Write(cb.CASHash[:])                                       // 32 bytes
	binary.Write(buf, binary.LittleEndian, cb.CASFlags)            // 4 bytes
	binary.Write(buf, binary.LittleEndian, uint32(len(cb.Chunks))) // 4 bytes
	binary.Write(buf, binary.LittleEndian, cb.NumBytesInCAS)       // 4 bytes
	binary.Write(buf, binary.LittleEndian, cb.NumBytesOnDisk)      // 4 bytes

	// CASChunkSequenceEntry entries (48 bytes each)
	for _, chunk := range cb.Chunks {
		buf.Write(chunk.ChunkHash[:])                                  // 32 bytes
		binary.Write(buf, binary.LittleEndian, chunk.ByteRangeStart)   // 4 bytes
		binary.Write(buf, binary.LittleEndian, chunk.UnpackedSegBytes) // 4 bytes
		binary.Write(buf, binary.LittleEndian, uint32(chunk.Flags))    // 4 bytes
		binary.Write(buf, binary.LittleEndian, uint32(0))              // 4 bytes reserved
	}

	return nil
}

// writeBookend writes a 48-byte bookend entry
func (s *Shard) writeBookend(buf *bytes.Buffer) {
	// Bytes 0-31: All 0xFF
	buf.Write(bytes.Repeat([]byte{0xFF}, 32))
	// Bytes 32-47: All 0x00
	buf.Write(make([]byte, 16))
}

// writeFooter writes the 200-byte footer
func (s *Shard) writeFooter(buf *bytes.Buffer) error {
	if s.Footer == nil {
		return fmt.Errorf("footer is nil")
	}

	f := s.Footer

	// Write all footer fields in order (200 bytes total)
	binary.Write(buf, binary.LittleEndian, f.Version)                // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.FileInfoOffset)         // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.CASInfoOffset)          // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.FileLookupOffset)       // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.FileLookupNumEntries)   // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.CASLookupOffset)        // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.CASLookupNumEntries)    // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.ChunkLookupOffset)      // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.ChunkLookupNumEntries)  // 8 bytes
	buf.Write(f.ChunkHashKey[:])                                     // 32 bytes
	binary.Write(buf, binary.LittleEndian, f.ShardCreationTimestamp) // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.ShardKeyExpiry)         // 8 bytes
	buf.Write(f.Reserved[:])                                         // 48 bytes
	binary.Write(buf, binary.LittleEndian, f.StoredBytesOnDisk)      // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.MaterializedBytes)      // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.StoredBytes)            // 8 bytes
	binary.Write(buf, binary.LittleEndian, f.FooterOffset)           // 8 bytes

	return nil
}

// Deserialize deserializes a shard from binary format
func Deserialize(data []byte) (*Shard, error) {
	if len(data) < 48 {
		return nil, fmt.Errorf("data too short for shard header")
	}

	s := &Shard{}
	offset := 0

	// Read header
	if err := s.readHeader(data, &offset); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Check if footer is present
	hasFooter := s.Header.FooterSize > 0

	// Calculate section boundaries
	var casInfoEnd int
	if hasFooter {
		if len(data) < int(s.Header.FooterSize) {
			return nil, fmt.Errorf("data too short for footer")
		}
		casInfoEnd = len(data) - int(s.Header.FooterSize)
	} else {
		casInfoEnd = len(data)
	}

	// Read file info section
	if err := s.readFileInfoSection(data, &offset); err != nil {
		return nil, fmt.Errorf("failed to read file info section: %w", err)
	}

	// Read CAS info section
	if err := s.readCASInfoSection(data, &offset, casInfoEnd); err != nil {
		return nil, fmt.Errorf("failed to read CAS info section: %w", err)
	}

	// Read footer if present
	if hasFooter {
		if err := s.readFooter(data, len(data)-int(s.Header.FooterSize)); err != nil {
			return nil, fmt.Errorf("failed to read footer: %w", err)
		}
	}

	return s, nil
}

// readHeader reads the 48-byte header
func (s *Shard) readHeader(data []byte, offset *int) error {
	if *offset+48 > len(data) {
		return fmt.Errorf("data too short for header")
	}

	// Read tag (32 bytes)
	copy(s.Header.Tag[:], data[*offset:*offset+32])
	*offset += 32

	// Verify magic sequence (bytes 15-31 of tag)
	if !bytes.Equal(s.Header.Tag[15:32], shardMagicSequence[:]) {
		return fmt.Errorf("invalid shard magic sequence")
	}

	// Read version (8 bytes)
	s.Header.Version = binary.LittleEndian.Uint64(data[*offset : *offset+8])
	*offset += 8

	if s.Header.Version != 2 {
		return fmt.Errorf("unsupported shard version: %d", s.Header.Version)
	}

	// Read footer size (8 bytes)
	s.Header.FooterSize = binary.LittleEndian.Uint64(data[*offset : *offset+8])
	*offset += 8

	return nil
}

// readFileInfoSection reads the file info section
func (s *Shard) readFileInfoSection(data []byte, offset *int) error {
	s.Files = make([]FileBlock, 0)

	for {
		if *offset+48 > len(data) {
			return fmt.Errorf("data too short for file block header")
		}

		// Check for bookend (32 bytes of 0xFF followed by 16 bytes of 0x00)
		if isBookend(data[*offset : *offset+48]) {
			*offset += 48
			break
		}

		fb, err := s.readFileBlock(data, offset)
		if err != nil {
			return err
		}
		s.Files = append(s.Files, fb)
	}

	return nil
}

// readFileBlock reads a single file block
func (s *Shard) readFileBlock(data []byte, offset *int) (FileBlock, error) {
	fb := FileBlock{}

	if *offset+48 > len(data) {
		return fb, fmt.Errorf("data too short for file data sequence header")
	}

	// Read FileDataSequenceHeader
	copy(fb.FileHash[:], data[*offset:*offset+32])
	*offset += 32

	fb.Flags = FileFlags(binary.LittleEndian.Uint32(data[*offset : *offset+4]))
	*offset += 4

	numEntries := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4

	*offset += 8 // Skip reserved

	// Read FileDataSequenceEntry entries
	fb.Entries = make([]FileDataSequenceEntry, numEntries)
	for i := range numEntries {
		if *offset+48 > len(data) {
			return fb, fmt.Errorf("data too short for file data sequence entry %d", i)
		}

		entry := FileDataSequenceEntry{}
		copy(entry.CASHash[:], data[*offset:*offset+32])
		*offset += 32

		entry.CASFlags = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		entry.UnpackedSegBytes = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		entry.ChunkIndexStart = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		entry.ChunkIndexEnd = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		fb.Entries[i] = entry
	}

	// Read FileVerificationEntry entries if flag set
	if fb.Flags&FileWithVerification != 0 {
		fb.Verification = make([]xet.Hash, numEntries)
		for i := range numEntries {
			if *offset+48 > len(data) {
				return fb, fmt.Errorf("data too short for file verification entry %d", i)
			}

			copy(fb.Verification[i][:], data[*offset:*offset+32])
			*offset += 32
			*offset += 16 // Skip reserved
		}
	}

	// Read FileMetadataExt if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if *offset+48 > len(data) {
			return fb, fmt.Errorf("data too short for file metadata ext")
		}

		fb.MetadataExt = &FileMetadataExt{}
		copy(fb.MetadataExt.SHA256Hash[:], data[*offset:*offset+32])
		*offset += 32
		*offset += 16 // Skip reserved
	}

	return fb, nil
}

// readCASInfoSection reads the CAS info section
func (s *Shard) readCASInfoSection(data []byte, offset *int, end int) error {
	s.CASInfos = make([]CASBlock, 0)

	for *offset < end {
		if *offset+48 > end {
			return fmt.Errorf("data too short for CAS block header")
		}

		// Check for bookend
		if isBookend(data[*offset : *offset+48]) {
			*offset += 48
			break
		}

		cb, err := s.readCASBlock(data, offset)
		if err != nil {
			return err
		}
		s.CASInfos = append(s.CASInfos, cb)
	}

	return nil
}

// readCASBlock reads a single CAS block
func (s *Shard) readCASBlock(data []byte, offset *int) (CASBlock, error) {
	cb := CASBlock{}

	if *offset+48 > len(data) {
		return cb, fmt.Errorf("data too short for CAS chunk sequence header")
	}

	// Read CASChunkSequenceHeader
	copy(cb.CASHash[:], data[*offset:*offset+32])
	*offset += 32

	cb.CASFlags = binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4

	numEntries := binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4

	cb.NumBytesInCAS = binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4

	cb.NumBytesOnDisk = binary.LittleEndian.Uint32(data[*offset : *offset+4])
	*offset += 4

	// Read CASChunkSequenceEntry entries
	cb.Chunks = make([]CASChunkSequenceEntry, numEntries)
	for i := range numEntries {
		if *offset+48 > len(data) {
			return cb, fmt.Errorf("data too short for CAS chunk sequence entry %d", i)
		}

		chunk := CASChunkSequenceEntry{}
		copy(chunk.ChunkHash[:], data[*offset:*offset+32])
		*offset += 32

		chunk.ByteRangeStart = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		chunk.UnpackedSegBytes = binary.LittleEndian.Uint32(data[*offset : *offset+4])
		*offset += 4

		chunk.Flags = ChunkFlags(binary.LittleEndian.Uint32(data[*offset : *offset+4]))
		*offset += 4

		*offset += 4 // Skip reserved

		cb.Chunks[i] = chunk
	}

	return cb, nil
}

// readFooter reads the 200-byte footer
func (s *Shard) readFooter(data []byte, offset int) error {
	if offset+200 > len(data) {
		return fmt.Errorf("data too short for footer")
	}

	s.Footer = &Footer{}
	f := s.Footer

	// Read all footer fields in order
	f.Version = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	if f.Version != 1 {
		return fmt.Errorf("unsupported footer version: %d", f.Version)
	}

	f.FileInfoOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASInfoOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FileLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FileLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.CASLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ChunkLookupOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ChunkLookupNumEntries = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	copy(f.ChunkHashKey[:], data[offset:offset+32])
	offset += 32

	f.ShardCreationTimestamp = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.ShardKeyExpiry = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	copy(f.Reserved[:], data[offset:offset+48])
	offset += 48

	f.StoredBytesOnDisk = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.MaterializedBytes = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.StoredBytes = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	f.FooterOffset = binary.LittleEndian.Uint64(data[offset : offset+8])
	offset += 8

	return nil
}

// isBookend checks if a 48-byte slice is a bookend entry
func isBookend(data []byte) bool {
	if len(data) != 48 {
		return false
	}

	// Check bytes 0-31 are all 0xFF
	for i := range 32 {
		if data[i] != 0xFF {
			return false
		}
	}

	// Check bytes 32-47 are all 0x00
	for i := 32; i < 48; i++ {
		if data[i] != 0x00 {
			return false
		}
	}

	return true
}
