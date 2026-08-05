package shard

import (
	"encoding/binary"
	"encoding/hex"
	"time"

	"github.com/wzshiming/xet"
)

// Shard represents a binary metadata structure that describes file reconstructions and xorb contents
type Shard struct {
	FooterSize uint64 // 0 if footer omitted
	Files      []FileBlock
	CASInfos   []CASBlock
	Footer     *Footer // Optional, omitted for upload API
}

// FileFlags represents flags in the file data sequence header
type FileFlags uint32

const (
	FileWithVerification FileFlags = 1 << 31 // FileVerificationEntry present for each entry
	FileWithMetadataExt  FileFlags = 1 << 30 // FileMetadataExt present at end
)

// FileBlock represents a file reconstruction block
type FileBlock struct {
	FileHash     xet.FileHash
	Flags        FileFlags
	Entries      []FileDataSequenceEntry
	Verification []xet.VerificationHash // Present if FileWithVerification flag set
	MetadataExt  *FileMetadataExt       // Present if FileWithMetadataExt flag set
}

// FileDataSequenceEntry describes a term in the file reconstruction
type FileDataSequenceEntry struct {
	CASHash          xet.XorbHash
	CASFlags         uint32 // Reserved, must be 0
	UnpackedSegBytes uint32
	ChunkIndexStart  uint32
	ChunkIndexEnd    uint32 // Exclusive
}

// SHA256Hash is a SHA-256 digest in standard byte order. The shard codec is
// responsible for converting it to and from xet-core's wire representation.
type SHA256Hash [32]byte

// NewSHA256Hash converts a raw SHA-256 digest to the Shard API type.
func NewSHA256Hash(raw [32]byte) SHA256Hash {
	return SHA256Hash(raw)
}

// transformSHA256ByteOrder converts between standard digest byte order and
// the xet-core wire representation. The transformation is its own inverse.
func transformSHA256ByteOrder(hash SHA256Hash) SHA256Hash {
	var transformed SHA256Hash
	for segment := range len(hash) / 8 {
		start := segment * 8
		for offset := range 8 {
			transformed[start+offset] = hash[start+7-offset]
		}
	}
	return transformed
}

func (h SHA256Hash) String() string {
	return hex.EncodeToString(h[:])
}

// FileMetadataExt contains optional metadata (SHA-256 hash)
type FileMetadataExt struct {
	SHA256Hash SHA256Hash
}

// CASBlock represents a xorb and its chunks
type CASBlock struct {
	CASHash        xet.XorbHash
	CASFlags       uint32 // Reserved, must be 0
	Chunks         []CASChunkSequenceEntry
	NumBytesInCAS  uint32 // Total uncompressed bytes
	NumBytesOnDisk uint32 // Serialized xorb size
}

// CASChunkSequenceEntry describes a chunk in a xorb
type CASChunkSequenceEntry struct {
	ChunkHash        xet.ChunkHash
	ByteRangeStart   uint32 // Cumulative byte offset within uncompressed xorb
	UnpackedSegBytes uint32
	Flags            ChunkFlags
}

// ChunkFlags represents flags in chunk entries
type ChunkFlags uint32

const (
	ChunkGlobalDedupEligible ChunkFlags = 1 << 31 // Chunk is eligible for global deduplication
)

// IsChunkGlobalDedupEligible reports whether a chunk may be used for a global
// deduplication query under draft-05. A chunk is eligible when it is the first
// chunk of a file, its shard entry carries the eligibility flag, or its hash
// suffix satisfies the protocol sampling rule.
func IsChunkGlobalDedupEligible(chunkHash xet.ChunkHash, isFirstChunk bool, flags ChunkFlags) bool {
	if isFirstChunk || flags&ChunkGlobalDedupEligible != 0 {
		return true
	}

	const dedupModulus uint64 = 1024
	value := binary.LittleEndian.Uint64(chunkHash[24:32])
	return value%dedupModulus == 0
}

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

// NewShard creates a new empty shard.
func NewShard() *Shard {
	return &Shard{
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

// EncodedSize returns the exact number of bytes that Encode will produce.
//
// Layout:
//
//	48 bytes  header
//	per file: 48 (block header) + 48*len(Entries) [+ 48*len(Verification) if FileWithVerification] [+ 48 if FileWithMetadataExt]
//	48 bytes  file bookend
//	per CAS:  48 (block header) + 48*len(Chunks)
//	48 bytes  CAS bookend
//	200 bytes footer (only when withFooter is true)
func (s *Shard) EncodedSize(withFooter bool) int64 {
	const (
		headerSize    = 48
		bookendSize   = 48
		fileHdrSize   = 48
		fileEntrySize = 48
		verifSize     = 48
		metaExtSize   = 48
		casHdrSize    = 48
		chunkSize     = 48
		footerSize    = 200
	)

	size := int64(headerSize)

	for _, fb := range s.Files {
		size += fileHdrSize
		size += int64(len(fb.Entries)) * fileEntrySize
		if fb.Flags&FileWithVerification != 0 {
			size += int64(len(fb.Verification)) * verifSize
		}
		if fb.Flags&FileWithMetadataExt != 0 {
			size += metaExtSize
		}
	}

	size += bookendSize

	for _, cb := range s.CASInfos {
		size += casHdrSize
		size += int64(len(cb.Chunks)) * chunkSize
	}

	size += bookendSize

	if withFooter {
		size += footerSize
	}

	return size
}

// SetFooter creates a Footer for this shard, computing StoredBytesOnDisk,
// StoredBytes, and MaterializedBytes from the current CAS and file data.
// Offset fields (FileInfoOffset, CASInfoOffset, FooterOffset) are set
// automatically during Encode and do not need to be supplied here.
func (s *Shard) SetFooter() {
	var storedBytesOnDisk, storedBytes, materializedBytes uint64
	for _, cas := range s.CASInfos {
		storedBytesOnDisk += uint64(cas.NumBytesOnDisk)
		storedBytes += uint64(cas.NumBytesInCAS)
	}
	for _, file := range s.Files {
		for _, entry := range file.Entries {
			materializedBytes += uint64(entry.UnpackedSegBytes)
		}
	}
	s.Footer = &Footer{
		Version:                1,
		StoredBytesOnDisk:      storedBytesOnDisk,
		StoredBytes:            storedBytes,
		MaterializedBytes:      materializedBytes,
		ShardCreationTimestamp: uint64(time.Now().Unix()),
	}
}
