package xet

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/zeebo/blake3"
)

// Hash represents a 32-byte BLAKE3 hash
type Hash [hashSize]byte

var (
	dataKeyPool = sync.Pool{New: func() any {
		h, err := blake3.NewKeyed(dataKey[:])
		if err != nil {
			panic("failed to create keyed hasher: " + err.Error())
		}
		return h
	}}
	internalNodeKeyPool = sync.Pool{New: func() any {
		h, err := blake3.NewKeyed(internalNodeKey[:])
		if err != nil {
			panic("failed to create keyed hasher: " + err.Error())
		}
		return h
	}}
	zeroKeyPool = sync.Pool{New: func() any {
		h, err := blake3.NewKeyed(zeroKey[:])
		if err != nil {
			panic("failed to create keyed hasher: " + err.Error())
		}
		return h
	}}
	verificationKeyPool = sync.Pool{New: func() any {
		h, err := blake3.NewKeyed(verificationKey[:])
		if err != nil {
			panic("failed to create keyed hasher: " + err.Error())
		}
		return h
	}}
)

// ParseHash converts an XET hash string back to a 32-byte hash
func ParseHash(hexStr string) (Hash, error) {
	var hash [32]byte
	if len(hexStr) != 64 {
		return hash, fmt.Errorf("invalid hash string length: %d", len(hexStr))
	}
	for seg := range 4 {
		start := seg * 16
		val, err := strconv.ParseUint(hexStr[start:start+16], 16, 64)
		if err != nil {
			return hash, fmt.Errorf("invalid hex segment %d: %w", seg, err)
		}
		binary.LittleEndian.PutUint64(hash[seg*8:], val)
	}
	return hash, nil
}

// String returns the hash as a hex string (byte-swapped for XET format)
func (h Hash) String() string {
	var out [64]byte
	for i := range out {
		out[i] = '0'
	}

	var tmp [8]byte
	for seg := range 4 {
		offset := seg * 8
		val := binary.LittleEndian.Uint64(h[offset : offset+8])
		s := strconv.AppendUint(tmp[:0], val, 16)
		copy(out[seg*16+16-len(s):], s)
	}
	return string(out[:])
}

// ComputeChunkHash computes the hash of a chunk using DATA_KEY
func ComputeChunkHash(data []byte) Hash {
	hasher := dataKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	dataKeyPool.Put(hasher)
	return result
}

// computeInternalNodeHash computes the hash of an internal node using INTERNAL_NODE_KEY
func computeInternalNodeHash(data []byte) Hash {
	hasher := internalNodeKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	internalNodeKeyPool.Put(hasher)
	return result
}

// computeFileHash computes the final file hash using ZERO_KEY
func computeFileHash(data []byte) Hash {
	hasher := zeroKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result Hash
	hasher.Sum(result[:0])
	zeroKeyPool.Put(hasher)
	return result
}

// ComputeVerificationHash computes a term verification hash using VERIFICATION_KEY
func ComputeVerificationHash(chunkHashes []Hash) Hash {
	hasher := verificationKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()

	// Write raw concatenation of chunk hashes (not hex-encoded)
	for _, h := range chunkHashes {
		hasher.Write(h[:])
	}

	var result Hash
	hasher.Sum(result[:0])
	verificationKeyPool.Put(hasher)
	return result
}
