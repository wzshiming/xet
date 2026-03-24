package xet

import (
	"encoding/binary"
	"fmt"

	"github.com/zeebo/blake3"
)

// Blake3KeyedHash computes a BLAKE3 keyed hash with the given 32-byte key.
func Blake3KeyedHash(key *[32]byte, data []byte) [32]byte {
	h, err := blake3.NewKeyed(key[:])
	if err != nil {
		panic("blake3: invalid key: " + err.Error())
	}
	h.Write(data)
	var out [32]byte
	digest := h.Sum(nil)
	copy(out[:], digest[:32])
	return out
}

// ComputeChunkHash computes the hash of a chunk using DATA_KEY (Section 6.1).
func ComputeChunkHash(chunkData []byte) [32]byte {
	return Blake3KeyedHash(&DATA_KEY, chunkData)
}

// ComputeVerificationHash computes the verification hash for a range of chunk hashes (Section 6.4).
// The range is [startIndex, endIndex) - end is exclusive.
func ComputeVerificationHash(chunkHashes [][32]byte, startIndex, endIndex int) [32]byte {
	buf := make([]byte, 0, (endIndex-startIndex)*32)
	for i := startIndex; i < endIndex; i++ {
		buf = append(buf, chunkHashes[i][:]...)
	}
	return Blake3KeyedHash(&VERIFICATION_KEY, buf)
}

// HashToString converts a 32-byte hash to the XET string representation (Section 6.5).
// The hash is interpreted as four little-endian 64-bit values, each printed as 16 hex digits.
func HashToString(hash [32]byte) string {
	var out [64]byte
	for seg := 0; seg < 4; seg++ {
		offset := seg * 8
		val := binary.LittleEndian.Uint64(hash[offset : offset+8])
		s := fmt.Sprintf("%016x", val)
		copy(out[seg*16:], s)
	}
	return string(out[:])
}

// StringToHash converts an XET hex string back to a 32-byte hash (Section 6.5).
func StringToHash(hexStr string) ([32]byte, error) {
	var hash [32]byte
	if len(hexStr) != 64 {
		return hash, fmt.Errorf("invalid hash string length: %d", len(hexStr))
	}
	for seg := 0; seg < 4; seg++ {
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
