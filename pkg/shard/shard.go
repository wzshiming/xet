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
	return &shardReader{
		shard: s,
		state: stateHeader,
	}, nil
}

// SerializeWithFooter serializes the shard with footer (for stored shards).
// Returns an io.Reader that streams the data directly without buffering everything in memory.
func (s *Shard) SerializeWithFooter() (io.Reader, error) {
	if s.Footer == nil {
		return nil, fmt.Errorf("footer is required but not set")
	}

	return &shardReader{
		shard:         s,
		state:         stateHeader,
		includeFooter: true,
	}, nil
}

// shardReader implements io.Reader for shard serialization
type shardReader struct {
	shard         *Shard
	state         readerState
	includeFooter bool
	fileIdx       int
	casIdx        int
	buffer        []byte
	bufOffset     int
	bytesWritten  int64
	// Footer tracking
	fileInfoOffset uint64
	casInfoOffset  uint64
	footerOffset   uint64
}

// readerState represents the current state of the reader
type readerState int

const (
	stateHeader readerState = iota
	stateFileBlocks
	stateFileBookend
	stateCASBlocks
	stateCASBookend
	stateFooter
	stateDone
)

func (r *shardReader) Read(p []byte) (n int, err error) {
	for n < len(p) {
		// Check if we're done
		if r.state == stateDone {
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}

		// If buffer is empty or consumed, prepare next data based on state
		if len(r.buffer) == 0 || r.bufOffset >= len(r.buffer) {
			if err := r.prepareNextBuffer(); err != nil {
				if err == io.EOF {
					r.state = stateDone
					if n > 0 {
						return n, nil
					}
					return 0, io.EOF
				}
				return n, err
			}
		}

		// Copy from buffer to output
		copied := copy(p[n:], r.buffer[r.bufOffset:])
		n += copied
		r.bufOffset += copied
		r.bytesWritten += int64(copied)
	}

	return n, nil
}

func (r *shardReader) prepareNextBuffer() error {
	switch r.state {
	case stateHeader:
		// Set footer size based on includeFooter
		if r.includeFooter {
			r.shard.Header.FooterSize = 200
		} else {
			r.shard.Header.FooterSize = 0
		}

		r.buffer = make([]byte, 48)
		r.buildHeader(r.buffer)
		r.bufOffset = 0
		r.fileInfoOffset = uint64(r.bytesWritten) + 48
		r.state = stateFileBlocks
		return nil

	case stateFileBlocks:
		if r.fileIdx < len(r.shard.Files) {
			fb := r.shard.Files[r.fileIdx]
			r.buffer = r.buildFileBlock(fb)
			r.bufOffset = 0
			r.fileIdx++
			return nil
		}
		r.state = stateFileBookend
		return r.prepareNextBuffer()

	case stateFileBookend:
		r.buffer = r.buildBookend()
		r.bufOffset = 0
		r.casInfoOffset = uint64(r.bytesWritten) + 48
		r.state = stateCASBlocks
		return nil

	case stateCASBlocks:
		if r.casIdx < len(r.shard.CASInfos) {
			cb := r.shard.CASInfos[r.casIdx]
			r.buffer = r.buildCASBlock(cb)
			r.bufOffset = 0
			r.casIdx++
			return nil
		}
		r.state = stateCASBookend
		return r.prepareNextBuffer()

	case stateCASBookend:
		r.buffer = r.buildBookend()
		r.bufOffset = 0
		if r.includeFooter {
			r.footerOffset = uint64(r.bytesWritten) + 48
			r.state = stateFooter
		} else {
			r.state = stateDone
		}
		return nil

	case stateFooter:
		// Update footer with offsets
		r.shard.Footer.FileInfoOffset = r.fileInfoOffset
		r.shard.Footer.CASInfoOffset = r.casInfoOffset
		r.shard.Footer.FooterOffset = r.footerOffset

		r.buffer = make([]byte, 200)
		r.buildFooter(r.buffer)
		r.bufOffset = 0
		r.state = stateDone
		return nil

	case stateDone:
		return io.EOF

	default:
		return fmt.Errorf("invalid reader state: %d", r.state)
	}
}

func (r *shardReader) buildHeader(buf []byte) {
	// Tag (32 bytes)
	copy(buf[0:32], r.shard.Header.Tag[:])

	// Version (8 bytes)
	binary.LittleEndian.PutUint64(buf[32:40], r.shard.Header.Version)

	// FooterSize (8 bytes)
	binary.LittleEndian.PutUint64(buf[40:48], r.shard.Header.FooterSize)
}

func (r *shardReader) buildFileBlock(fb FileBlock) []byte {
	// Calculate size: header (48) + entries (48 each) + verification (48 each if flag set) + metadata ext (48 if flag set)
	size := 48 + len(fb.Entries)*48
	if fb.Flags&FileWithVerification != 0 {
		size += len(fb.Verification) * 48
	}
	if fb.Flags&FileWithMetadataExt != 0 {
		size += 48
	}

	buf := make([]byte, size)
	offset := 0

	// FileDataSequenceHeader (48 bytes)
	copy(buf[offset:offset+32], fb.FileHash[:])
	offset += 32
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(fb.Flags))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(fb.Entries)))
	offset += 4
	// 8 bytes reserved (already zeroed)
	offset += 8

	// FileDataSequenceEntry entries (48 bytes each)
	for _, entry := range fb.Entries {
		copy(buf[offset:offset+32], entry.CASHash[:])
		offset += 32
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.CASFlags)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.UnpackedSegBytes)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.ChunkIndexStart)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], entry.ChunkIndexEnd)
		offset += 4
	}

	// FileVerificationEntry entries (48 bytes each) if flag set
	if fb.Flags&FileWithVerification != 0 {
		for _, verif := range fb.Verification {
			copy(buf[offset:offset+32], verif[:])
			offset += 32
			// 16 bytes reserved (already zeroed)
			offset += 16
		}
	}

	// FileMetadataExt (48 bytes) if flag set
	if fb.Flags&FileWithMetadataExt != 0 {
		if fb.MetadataExt != nil {
			copy(buf[offset:offset+32], fb.MetadataExt.SHA256Hash[:])
			offset += 32
			// 16 bytes reserved (already zeroed)
			offset += 16
		}
	}

	return buf
}

func (r *shardReader) buildCASBlock(cb CASBlock) []byte {
	// Calculate size: header (48) + entries (48 each)
	size := 48 + len(cb.Chunks)*48

	buf := make([]byte, size)
	offset := 0

	// CASChunkSequenceHeader (48 bytes)
	copy(buf[offset:offset+32], cb.CASHash[:])
	offset += 32
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.CASFlags)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(cb.Chunks)))
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.NumBytesInCAS)
	offset += 4
	binary.LittleEndian.PutUint32(buf[offset:offset+4], cb.NumBytesOnDisk)
	offset += 4

	// CASChunkSequenceEntry entries (48 bytes each)
	for _, chunk := range cb.Chunks {
		copy(buf[offset:offset+32], chunk.ChunkHash[:])
		offset += 32
		binary.LittleEndian.PutUint32(buf[offset:offset+4], chunk.ByteRangeStart)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], chunk.UnpackedSegBytes)
		offset += 4
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(chunk.Flags))
		offset += 4
		// 4 bytes reserved (already zeroed)
		offset += 4
	}

	return buf
}

func (r *shardReader) buildBookend() []byte {
	buf := make([]byte, 48)
	// Bytes 0-31: All 0xFF
	for i := 0; i < 32; i++ {
		buf[i] = 0xFF
	}
	// Bytes 32-47: All 0x00 (already zeroed)
	return buf
}

func (r *shardReader) buildFooter(buf []byte) {
	f := r.shard.Footer
	offset := 0

	// Write all footer fields in order (200 bytes total)
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.Version)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileInfoOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASInfoOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FileLookupNumEntries)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.CASLookupNumEntries)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ChunkLookupOffset)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ChunkLookupNumEntries)
	offset += 8
	copy(buf[offset:offset+32], f.ChunkHashKey[:])
	offset += 32
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ShardCreationTimestamp)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.ShardKeyExpiry)
	offset += 8
	copy(buf[offset:offset+48], f.Reserved[:])
	offset += 48
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.StoredBytesOnDisk)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.MaterializedBytes)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.StoredBytes)
	offset += 8
	binary.LittleEndian.PutUint64(buf[offset:offset+8], f.FooterOffset)
	offset += 8
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
