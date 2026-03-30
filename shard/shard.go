package shard

import (
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
