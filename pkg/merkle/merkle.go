// Package merkle implements the XET Merkle tree construction (Section 6.2.2).
package merkle

import (
	"encoding/binary"
	"fmt"

	"github.com/wzshiming/xet/pkg/xet"
)

const (
	// MeanBranchingFactor is the target number of children per node.
	MeanBranchingFactor = 4
	// MinChildren is the minimum number of children for an internal node.
	MinChildren = 2
	// MaxChildren is the maximum number of children per node.
	MaxChildren = 2*MeanBranchingFactor + 1 // 9
)

// HashEntry represents a (hash, size) pair used in Merkle tree construction.
type HashEntry struct {
	Hash [32]byte
	Size uint64
}

// nextMergeCut determines how many entries to merge at the current position (Section 6.2.2.2).
func nextMergeCut(hashes []HashEntry) int {
	n := len(hashes)
	if n <= 2 {
		return n
	}

	end := MaxChildren
	if n < end {
		end = n
	}

	// Check indices 2 through end-1
	for i := 2; i < end; i++ {
		h := hashes[i].Hash
		// Interpret last 8 bytes as little-endian uint64
		hashValue := binary.LittleEndian.Uint64(h[24:32])
		if hashValue%MeanBranchingFactor == 0 {
			return i + 1
		}
	}

	return end
}

// mergedHashOfSequence computes the merged hash for a sequence of hash entries (Section 6.2.2.3).
func mergedHashOfSequence(pairs []HashEntry) HashEntry {
	var totalSize uint64
	buf := make([]byte, 0, len(pairs)*80)

	for _, p := range pairs {
		line := fmt.Sprintf("%s : %d\n", xet.HashToString(p.Hash), p.Size)
		buf = append(buf, line...)
		totalSize += p.Size
	}

	newHash := xet.Blake3KeyedHash(&xet.INTERNAL_NODE_KEY, buf)
	return HashEntry{Hash: newHash, Size: totalSize}
}

// ComputeMerkleRoot computes the Merkle root hash from a list of entries (Section 6.2.2.4).
func ComputeMerkleRoot(entries []HashEntry) [32]byte {
	if len(entries) == 0 {
		return xet.ZERO_HASH
	}

	hv := make([]HashEntry, len(entries))
	copy(hv, entries)

	for len(hv) > 1 {
		writeIdx := 0
		readIdx := 0

		for readIdx < len(hv) {
			cut := readIdx + nextMergeCut(hv[readIdx:])
			hv[writeIdx] = mergedHashOfSequence(hv[readIdx:cut])
			writeIdx++
			readIdx = cut
		}

		hv = hv[:writeIdx]
	}

	return hv[0].Hash
}

// ComputeXorbHash computes the xorb hash from chunk hashes and sizes (Section 6.2.3).
func ComputeXorbHash(chunkHashes [][32]byte, chunkSizes []uint64) [32]byte {
	entries := make([]HashEntry, len(chunkHashes))
	for i := range chunkHashes {
		entries[i] = HashEntry{Hash: chunkHashes[i], Size: chunkSizes[i]}
	}
	return ComputeMerkleRoot(entries)
}

// ComputeFileHash computes the file hash from chunk hashes and sizes (Section 6.3).
func ComputeFileHash(chunkHashes [][32]byte, chunkSizes []uint64) [32]byte {
	entries := make([]HashEntry, len(chunkHashes))
	for i := range chunkHashes {
		entries[i] = HashEntry{Hash: chunkHashes[i], Size: chunkSizes[i]}
	}
	merkleRoot := ComputeMerkleRoot(entries)
	return xet.Blake3KeyedHash(&xet.ZERO_KEY, merkleRoot[:])
}
