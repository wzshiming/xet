package shard

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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

// Serialize serializes the shard to binary format (without footer for upload API).
// Returns an io.Reader that streams the data directly without buffering everything in memory.
func (s *Shard) Serialize() (io.Reader, error) {
	pr, pw := io.Pipe()

	go func() {
		err := s.serializeToWriter(pw, false)
		pw.CloseWithError(err)
	}()

	return pr, nil
}

// SerializeBytes is a helper that returns the serialized bytes.
// This wraps Serialize and reads all data into memory for backward compatibility.
func (s *Shard) SerializeBytes() ([]byte, error) {
	r, err := s.Serialize()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// SerializeWithFooter serializes the shard with footer (for stored shards).
// Returns an io.Reader that streams the data directly without buffering everything in memory.
func (s *Shard) SerializeWithFooter() (io.Reader, error) {
	if s.Footer == nil {
		return nil, fmt.Errorf("footer is required but not set")
	}

	pr, pw := io.Pipe()

	go func() {
		err := s.serializeToWriter(pw, true)
		pw.CloseWithError(err)
	}()

	return pr, nil
}

// SerializeWithFooterBytes is a helper that returns the serialized bytes with footer.
// This wraps SerializeWithFooter and reads all data into memory for backward compatibility.
func (s *Shard) SerializeWithFooterBytes() ([]byte, error) {
	r, err := s.SerializeWithFooter()
	if err != nil {
		return nil, err
	}
	return io.ReadAll(r)
}

// serializeToWriter writes the shard data to the provided writer.
// If includeFooter is true, the footer is written; otherwise it's omitted.
func (s *Shard) serializeToWriter(w io.Writer, includeFooter bool) error {
	// Use a counting writer to track offsets for footer
	cw := &countingWriter{w: w}

	if includeFooter {
		// Update header with footer size
		s.Header.FooterSize = 200
	} else {
		s.Header.FooterSize = 0
	}

	// Write header
	if err := s.writeHeader(cw); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Track file info offset
	fileInfoOffset := uint64(cw.n)

	// Write file info section
	if err := s.writeFileInfoSection(cw); err != nil {
		return fmt.Errorf("failed to write file info section: %w", err)
	}

	// Track CAS info offset
	casInfoOffset := uint64(cw.n)

	// Write CAS info section
	if err := s.writeCASInfoSection(cw); err != nil {
		return fmt.Errorf("failed to write CAS info section: %w", err)
	}

	// Write footer if requested
	if includeFooter {
		// Update footer with offsets
		s.Footer.FileInfoOffset = fileInfoOffset
		s.Footer.CASInfoOffset = casInfoOffset
		s.Footer.FooterOffset = uint64(cw.n)

		if err := s.writeFooter(cw); err != nil {
			return fmt.Errorf("failed to write footer: %w", err)
		}
	}

	return nil
}

// countingWriter wraps an io.Writer and counts bytes written
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// writeHeader writes the 48-byte header
func (s *Shard) writeHeader(w io.Writer) error {
	// Tag (32 bytes)
	if _, err := w.Write(s.Header.Tag[:]); err != nil {
		return err
	}

	// Version (8 bytes)
	if err := binary.Write(w, binary.LittleEndian, s.Header.Version); err != nil {
		return err
	}

	// FooterSize (8 bytes)
	if err := binary.Write(w, binary.LittleEndian, s.Header.FooterSize); err != nil {
		return err
	}

	return nil
}

// writeFileInfoSection writes the file info section
func (s *Shard) writeFileInfoSection(w io.Writer) error {
	for _, fb := range s.Files {
		if err := s.writeFileBlock(w, fb); err != nil {
			return fmt.Errorf("failed to write file block: %w", err)
		}
	}

	// Write bookend entry (48 bytes)
	s.writeBookend(w)

	return nil
}

// writeFileBlock writes a single file block
func (s *Shard) writeFileBlock(w io.Writer, fb FileBlock) error {
	// FileDataSequenceHeader (48 bytes)
	if _, err := w.Write(fb.FileHash[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(fb.Flags)); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(fb.Entries))); err != nil {
		return err
	}
	if _, err := w.Write(make([]byte, 8)); err != nil {
		return err
	}

	// FileDataSequenceEntry entries (48 bytes each)
	for _, entry := range fb.Entries {
		if _, err := w.Write(entry.CASHash[:]); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, entry.CASFlags); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, entry.UnpackedSegBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, entry.ChunkIndexStart); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, entry.ChunkIndexEnd); err != nil {
			return err
		}
	}

	// FileVerificationEntry entries (48 bytes each) if flag set
	if fb.Flags&FileWithVerification != 0 {
		for _, verif := range fb.Verification {
			if _, err := w.Write(verif[:]); err != nil {
				return err
			}
			if _, err := w.Write(make([]byte, 16)); err != nil {
				return err
			}
		}
	}

	// FileMetadataExt (48 bytes) if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if fb.MetadataExt != nil {
			if _, err := w.Write(fb.MetadataExt.SHA256Hash[:]); err != nil {
				return err
			}
			if _, err := w.Write(make([]byte, 16)); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeCASInfoSection writes the CAS info section
func (s *Shard) writeCASInfoSection(w io.Writer) error {
	for _, cb := range s.CASInfos {
		if err := s.writeCASBlock(w, cb); err != nil {
			return fmt.Errorf("failed to write CAS block: %w", err)
		}
	}

	// Write bookend entry (48 bytes)
	s.writeBookend(w)

	return nil
}

// writeCASBlock writes a single CAS block
func (s *Shard) writeCASBlock(w io.Writer, cb CASBlock) error {
	// CASChunkSequenceHeader (48 bytes)
	if _, err := w.Write(cb.CASHash[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, cb.CASFlags); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(cb.Chunks))); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, cb.NumBytesInCAS); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, cb.NumBytesOnDisk); err != nil {
		return err
	}

	// CASChunkSequenceEntry entries (48 bytes each)
	for _, chunk := range cb.Chunks {
		if _, err := w.Write(chunk.ChunkHash[:]); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, chunk.ByteRangeStart); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, chunk.UnpackedSegBytes); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(chunk.Flags)); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, uint32(0)); err != nil {
			return err
		}
	}

	return nil
}

// writeBookend writes a 48-byte bookend entry
func (s *Shard) writeBookend(w io.Writer) error {
	// Bytes 0-31: All 0xFF
	if _, err := w.Write(bytes.Repeat([]byte{0xFF}, 32)); err != nil {
		return err
	}
	// Bytes 32-47: All 0x00
	if _, err := w.Write(make([]byte, 16)); err != nil {
		return err
	}
	return nil
}

// writeFooter writes the 200-byte footer
func (s *Shard) writeFooter(w io.Writer) error {
	if s.Footer == nil {
		return fmt.Errorf("footer is nil")
	}

	f := s.Footer

	// Write all footer fields in order (200 bytes total)
	if err := binary.Write(w, binary.LittleEndian, f.Version); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.FileInfoOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.CASInfoOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.FileLookupOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.FileLookupNumEntries); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.CASLookupOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.CASLookupNumEntries); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.ChunkLookupOffset); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.ChunkLookupNumEntries); err != nil {
		return err
	}
	if _, err := w.Write(f.ChunkHashKey[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.ShardCreationTimestamp); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.ShardKeyExpiry); err != nil {
		return err
	}
	if _, err := w.Write(f.Reserved[:]); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.StoredBytesOnDisk); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.MaterializedBytes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.StoredBytes); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, f.FooterOffset); err != nil {
		return err
	}

	return nil
}

// Deserialize deserializes a shard from an io.Reader.
// The reader is fully read into memory for parsing.
func Deserialize(r io.Reader) (*Shard, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}
	return DeserializeBytes(data)
}

// DeserializeBytes deserializes a shard from binary format.
// This is a helper for backward compatibility.
func DeserializeBytes(data []byte) (*Shard, error) {
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
