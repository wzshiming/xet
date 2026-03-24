package metadata

import (
	"time"

	"github.com/wzshiming/xet/merklehash"
)

// Global dedup constants
const (
	// MDBShardGlobalDedupChunkModulus is the modulus for global dedup eligibility.
	MDBShardGlobalDedupChunkModulus uint64 = 1024

	// MDBShardExpirationBuffer is the amount of time a shard should be expired by before deletion (7 days).
	MDBShardExpirationBuffer = 7 * 24 * time.Hour

	// MDBShardLocalCacheExpiration is the expiration time of a local shard cache entry (3 weeks).
	MDBShardLocalCacheExpiration = 3 * 7 * 24 * time.Hour
)

// Chunk and XORB size constants
const (
	TargetChunkSize        = 64 * 1024           // 64 KB
	MinimumChunkDivisor    = 8
	MaximumChunkMultiplier = 2
	MaxChunkSize           = TargetChunkSize * MaximumChunkMultiplier // 128 KB
	MaxXorbBytes           = 64 * 1024 * 1024                        // 64 MB
	MaxXorbChunks          = 8 * 1024
	XorbBlockSize          = 64 * 1024 * 1024 // 64 MB
)

// File flags
const (
	MDBDefaultFileFlag          uint32 = 0
	MDBFileFlagWithVerification uint32 = 1 << 31
	MDBFileFlagVerificationMask uint32 = 1 << 31
	MDBFileFlagWithMetadataExt  uint32 = 1 << 30
	MDBFileFlagMetadataExtMask  uint32 = 1 << 30
)

// XORB flags
const (
	MDBDefaultXorbFlag          uint32 = 0
	MDBChunkWithGlobalDedupFlag uint32 = 1 << 31
)

// MDBFileInfoEntrySize is the size of each entry in the file info (48 bytes).
const MDBFileInfoEntrySize = 48

// HashIsGlobalDedupEligible returns true if the hash is eligible for global dedup.
func HashIsGlobalDedupEligible(h merklehash.DataHash) bool {
	return h.Rem(MDBShardGlobalDedupChunkModulus) == 0
}
