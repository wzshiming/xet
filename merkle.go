package xet

import (
	"encoding/binary"
	"fmt"
)

// Tree represents a Merkle tree with variable fan-out
type Tree struct {
	leaves []Hash
	sizes  []uint64 // Size in bytes of each leaf chunk
}

// newTree creates a new Merkle tree
func newTree() *Tree {
	return &Tree{
		leaves: make([]Hash, 0),
		sizes:  make([]uint64, 0),
	}
}

// AddLeaf adds a leaf (chunk hash and size) to the tree
func (t *Tree) AddLeaf(hash Hash, size uint64) {
	t.leaves = append(t.leaves, hash)
	t.sizes = append(t.sizes, size)
}

// ComputeRoot computes the Merkle tree root hash
func (t *Tree) ComputeRoot() Hash {
	if len(t.leaves) == 0 {
		// Return ZERO_HASH (32 zero bytes)
		return Hash{}
	}

	// Build initial list of entries
	entries := make([]node, len(t.leaves))
	for i := range t.leaves {
		entries[i] = node{
			hash: t.leaves[i],
			size: t.sizes[i],
		}
	}

	// Iteratively collapse until single root remains
	for len(entries) > 1 {
		nextLevel := make([]node, 0)
		readIdx := 0

		for readIdx < len(entries) {
			// Determine how many entries to merge
			cutSize := nextMergeCut(entries[readIdx:])
			cutEnd := readIdx + cutSize

			// Merge this group
			merged := t.mergeNodes(entries[readIdx:cutEnd])
			nextLevel = append(nextLevel, merged)

			readIdx = cutEnd
		}

		entries = nextLevel
	}

	return entries[0].hash
}

// node represents a node in the Merkle tree
type node struct {
	hash Hash
	size uint64
}

// nextMergeCut determines how many entries to merge based on the XET specification
// Returns the number of entries to merge (cut point)
func nextMergeCut(nodes []node) int {
	n := len(nodes)

	// If 2 or fewer, merge all
	if n <= 2 {
		return n
	}

	// Maximum we can merge is MAX_CHILDREN or all remaining
	end := min(MaxChildren, n)

	// Check indices 2 through end-1 (0-based indexing)
	// Minimum merge is 3 children when input has more than 2 hashes
	for i := 2; i < end; i++ {
		// Use last 8 bytes of hash as little-endian 64-bit unsigned int
		// Per spec: hash[24:32] are the last 8 bytes
		hashValue := binary.LittleEndian.Uint64(nodes[i].hash[24:32])
		if hashValue%MeanBranchingFactor == 0 {
			return i + 1 // Cut after element i (include i+1 elements)
		}
	}

	// No cut point found, merge up to MAX_CHILDREN or all remaining
	return end
}

// mergeNodes merges a sequence of nodes into a single parent node
func (t *Tree) mergeNodes(nodes []node) node {
	// Build the input for the internal node hash
	// Format: "{hash_hex} : {size}\n" for each child
	var input []byte
	var totalSize uint64

	for _, n := range nodes {
		hashStr := n.hash.String()
		sizeStr := fmt.Sprintf("%d", n.size)
		line := hashStr + " : " + sizeStr + "\n"
		input = append(input, []byte(line)...)
		totalSize += n.size
	}

	// Compute the parent hash
	parentHash := computeInternalNodeHash(input)

	return node{
		hash: parentHash,
		size: totalSize,
	}
}

// ComputeXorbHash computes the xorb hash from chunk hashes and sizes
func ComputeXorbHash(chunkHashes []Hash, chunkSizes []uint64) Hash {
	if len(chunkHashes) != len(chunkSizes) {
		panic("chunk hashes and sizes length mismatch")
	}

	tree := newTree()
	for i := range chunkHashes {
		tree.AddLeaf(chunkHashes[i], chunkSizes[i])
	}

	return tree.ComputeRoot()
}

// ComputeFileHash computes the file hash from chunk hashes and sizes
// This is the same as xorb hash but with an additional keyed hash step
func ComputeFileHash(chunkHashes []Hash, chunkSizes []uint64) Hash {
	// First compute the Merkle root (same as xorb hash)
	xorbHash := ComputeXorbHash(chunkHashes, chunkSizes)
	if xorbHash == (Hash{}) {
		// If xorb hash is ZERO_HASH, return ZERO_HASH for file hash as well
		return Hash{}
	}

	// Then apply the final keyed hash with ZERO_KEY
	return computeFileHash(xorbHash[:])
}
