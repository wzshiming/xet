package xet

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"sync"

	"github.com/zeebo/blake3"
)

// hash represents a 32-byte BLAKE3 hash
type hash [hashSize]byte

// FileHash identifies a reconstructed file. It is intentionally a distinct
// type from ChunkHash so the two cannot be mixed accidentally.
type FileHash hash

// ChunkHash identifies an individual content-defined chunk.
type ChunkHash hash

// XorbHash identifies a content-addressed xorb containing one or more chunks.
type XorbHash hash

// VerificationHash identifies a term verification hash for a file reconstruction term.
type VerificationHash hash

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

// parseHash converts an XET hash string back to a 32-byte hash
func parseHash(hexStr string) (hash, error) {
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

// ParseFileHash parses an XET hash string as a file hash.
func ParseFileHash(s string) (FileHash, error) {
	h, err := parseHash(s)
	return FileHash(h), err
}

// ParseChunkHash parses an XET hash string as a chunk hash.
func ParseChunkHash(s string) (ChunkHash, error) {
	h, err := parseHash(s)
	return ChunkHash(h), err
}

// ParseXorbHash parses an XET hash string as an xorb hash.
func ParseXorbHash(s string) (XorbHash, error) {
	h, err := parseHash(s)
	return XorbHash(h), err
}

// ParseVerificationHash parses an XET hash string as a verification hash.
func ParseVerificationHash(s string) (VerificationHash, error) {
	h, err := parseHash(s)
	return VerificationHash(h), err
}

// String returns the hash as a hex string (byte-swapped for XET format)
func (h hash) String() string {
	return formatHash(h)
}

func (h FileHash) String() string {
	return formatHash(hash(h))
}

func (h ChunkHash) String() string {
	return formatHash(hash(h))
}

func (h XorbHash) String() string {
	return formatHash(hash(h))
}

func (h VerificationHash) String() string {
	return formatHash(hash(h))
}

func formatHash(h hash) string {
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
func ComputeChunkHash(data []byte) ChunkHash {
	hasher := dataKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result ChunkHash
	hasher.Sum(result[:0])
	dataKeyPool.Put(hasher)
	return result
}

// computeInternalNodeHash computes the hash of an internal node using INTERNAL_NODE_KEY
func computeInternalNodeHash(data []byte) hash {
	hasher := internalNodeKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result hash
	hasher.Sum(result[:0])
	internalNodeKeyPool.Put(hasher)
	return result
}

// computeFileHash computes the final file hash using ZERO_KEY
func computeFileHash(data []byte) hash {
	hasher := zeroKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()
	hasher.Write(data)
	var result hash
	hasher.Sum(result[:0])
	zeroKeyPool.Put(hasher)
	return result
}

// ComputeVerificationHash computes a term verification hash using VERIFICATION_KEY
func ComputeVerificationHash(chunkHashes []ChunkHash) VerificationHash {
	hasher := verificationKeyPool.Get().(*blake3.Hasher)
	hasher.Reset()

	// Write raw concatenation of chunk hashes (not hex-encoded)
	for _, h := range chunkHashes {
		hasher.Write(h[:])
	}

	var result VerificationHash
	hasher.Sum(result[:0])
	verificationKeyPool.Put(hasher)
	return result
}
