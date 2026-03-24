package xet

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/zeebo/blake3"
)

// Hash represents a 32-byte BLAKE3 hash
type Hash [HashSize]byte

// String returns the hash as a hex string (byte-swapped for XET format)
func (h Hash) String() string {
	return HashToString(h[:])
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

// ComputeInternalNodeHash computes the hash of an internal node using INTERNAL_NODE_KEY
func ComputeInternalNodeHash(data []byte) Hash {
	hasher, err := blake3.NewKeyed(InternalNodeKey)
	if err != nil {
		panic("failed to create keyed hasher: " + err.Error())
	}
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	return result
}

// ComputeFileHash computes the final file hash using ZERO_KEY
func ComputeFileHash(data []byte) Hash {
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

// HashToString converts a 32-byte hash to XET string format
// The hash is interpreted as four little-endian 64-bit values,
// each formatted as 16 lowercase hex digits
func HashToString(hash []byte) string {
	if len(hash) != HashSize {
		panic(fmt.Sprintf("invalid hash size: %d, expected %d", len(hash), HashSize))
	}

	var parts [4]string
	for i := range 4 {
		offset := i * 8
		val := binary.LittleEndian.Uint64(hash[offset : offset+8])
		parts[i] = fmt.Sprintf("%016x", val)
	}

	return parts[0] + parts[1] + parts[2] + parts[3]
}

// StringToHash converts an XET hash string back to a 32-byte hash
func StringToHash(s string) (Hash, error) {
	if len(s) != 64 {
		return Hash{}, fmt.Errorf("invalid hash string length: %d, expected 64", len(s))
	}

	var result Hash
	for i := range 4 {
		part := s[i*16 : (i+1)*16]
		val, err := parseHexUint64(part)
		if err != nil {
			return Hash{}, fmt.Errorf("invalid hex in part %d: %w", i, err)
		}
		offset := i * 8
		binary.LittleEndian.PutUint64(result[offset:offset+8], val)
	}

	return result, nil
}

// parseHexUint64 parses a 16-character hex string as uint64
func parseHexUint64(s string) (uint64, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("expected 8 bytes, got %d", len(b))
	}
	// The hex string represents the value in big-endian
	return binary.BigEndian.Uint64(b), nil
}
