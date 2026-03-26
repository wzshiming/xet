package xet

import (
	"encoding/binary"
	"fmt"

	"github.com/zeebo/blake3"
)

// Hash represents a 32-byte BLAKE3 hash
type Hash [HashSize]byte

// ParseHash converts an XET hash string back to a 32-byte hash
func ParseHash(hexStr string) (Hash, error) {
	var hash [32]byte
	if len(hexStr) != 64 {
		return hash, fmt.Errorf("invalid hash string length: %d", len(hexStr))
	}
	for seg := range 4 {
		start := seg * 16
		var val uint64
		_, err := fmt.Sscanf(hexStr[start:start+16], "%016x", &val)
		if err != nil {
			return hash, fmt.Errorf("invalid hex segment %d: %w", seg, err)
		}
		binary.LittleEndian.PutUint64(hash[seg*8:], val)
	}
	return hash, nil
}

// String returns the hash as a hex string (byte-swapped for XET format)
func (h Hash) String() string {
	return hashToString(h)
}

// ComputeChunkHash computes the hash of a chunk using DATA_KEY
func ComputeChunkHash(data []byte) Hash {
	hasher, err := blake3.NewKeyed(DataKey)
	if err != nil {
		panic("failed to create keyed hasher: " + err.Error())
	}
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	return result
}

// computeInternalNodeHash computes the hash of an internal node using INTERNAL_NODE_KEY
func computeInternalNodeHash(data []byte) Hash {
	hasher, err := blake3.NewKeyed(InternalNodeKey)
	if err != nil {
		panic("failed to create keyed hasher: " + err.Error())
	}
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	return result
}

// computeFileHash computes the final file hash using ZERO_KEY
func computeFileHash(data []byte) Hash {
	hasher, err := blake3.NewKeyed(ZeroKey)
	if err != nil {
		panic("failed to create keyed hasher: " + err.Error())
	}
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	return result
}

// ComputeVerificationHash computes a term verification hash using VERIFICATION_KEY
func ComputeVerificationHash(chunkHashes []Hash) Hash {
	hasher, err := blake3.NewKeyed(VerificationKey)
	if err != nil {
		panic("failed to create keyed hasher: " + err.Error())
	}

	// Write raw concatenation of chunk hashes (not hex-encoded)
	for _, h := range chunkHashes {
		hasher.Write(h[:])
	}

	var result Hash
	hasher.Sum(result[:0])
	return result
}

// hashToString converts a 32-byte hash to XET string format
// The hash is interpreted as four little-endian 64-bit values,
// each formatted as 16 lowercase hex digits
func hashToString(hash Hash) string {
	var out [64]byte
	for seg := range 4 {
		offset := seg * 8
		val := binary.LittleEndian.Uint64(hash[offset : offset+8])
		s := fmt.Sprintf("%016x", val)
		copy(out[seg*16:], s)
	}
	return string(out[:])
}
