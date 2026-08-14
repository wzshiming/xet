package xet

import (
	"encoding/binary"
	"strconv"
	"sync"
)

var (
	nodesPool = sync.Pool{New: func() any {
		return []node{}
	}}
)

// merkleTree represents a Merkle tree with variable fan-out
type merkleTree struct {
	node []node
}

// newTree creates a new Merkle tree
func newTree() *merkleTree {
	return &merkleTree{
		node: nodesPool.Get().([]node)[:0],
	}
}

// AddLeaf adds a leaf (chunk hash and size) to the tree
func (t *merkleTree) AddLeaf(hash hash, size uint64) {
	t.node = append(t.node, node{
		hash: hash,
		size: size,
	})
}

// ComputeRoot computes the Merkle tree root hash
func (t *merkleTree) ComputeRoot() hash {
	if len(t.node) == 0 {
		// Return ZERO_HASH (32 zero bytes)
		return hash{}
	}

	// Build initial list of entries
	entries := t.node

	// Scratch buffer shared by every merge; sized so appends never grow it.
	buf := make([]byte, 0, MaxChildren*mergeLineMax)

	// Iteratively collapse until single root remains
	for len(entries) > 1 {
		nextLevel := nodesPool.Get().([]node)[:0]
		readIdx := 0

		for readIdx < len(entries) {
			// Determine how many entries to merge
			cutSize := nextMergeCut(entries[readIdx:])
			cutEnd := readIdx + cutSize

			// Merge this group
			merged := mergeNodes(entries[readIdx:cutEnd], buf)
			nextLevel = append(nextLevel, merged)

			readIdx = cutEnd
		}

		nodesPool.Put(entries)
		entries = nextLevel
	}

	hash := entries[0].hash
	t.node = nil

	nodesPool.Put(entries)
	return hash
}

// node represents a node in the Merkle tree
type node struct {
	hash hash
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

// mergeLineMax is the longest encoded child line in an internal node input:
// 64 (hash hex) + 3 (" : ") + 20 (max uint64 digits) + 1 ("\n").
const mergeLineMax = 64 + 3 + 20 + 1

// mergeNodes merges a sequence of nodes into a single parent node. buf is a
// scratch buffer reused across calls; it needs capacity for
// MaxChildren*mergeLineMax bytes to stay allocation-free.
func mergeNodes(nodes []node, buf []byte) node {
	// Build the input for the internal node hash
	// Format: "{hash_hex} : {size}\n" for each child
	input := buf[:0]
	var totalSize uint64

	for _, n := range nodes {
		input = appendHash(input, n.hash)
		input = append(input, " : "...)
		input = strconv.AppendUint(input, n.size, 10)
		input = append(input, '\n')
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
func ComputeXorbHash(chunkHashes []ChunkHash, chunkSizes []uint64) XorbHash {
	if len(chunkHashes) != len(chunkSizes) {
		panic("chunk hashes and sizes length mismatch")
	}

	tree := newTree()
	for i := range chunkHashes {
		tree.AddLeaf(hash(chunkHashes[i]), chunkSizes[i])
	}

	return XorbHash(tree.ComputeRoot())
}

// ComputeFileHash computes the file hash from chunk hashes and sizes. Non-empty
// files apply an additional keyed hash step to the Merkle root; an empty file
// is represented directly by ZERO_HASH.
func ComputeFileHash(chunkHashes []ChunkHash, chunkSizes []uint64) FileHash {
	if len(chunkHashes) == 0 {
		return FileHash{}
	}

	// First compute the Merkle root (same as xorb hash)
	xorbHash := ComputeXorbHash(chunkHashes, chunkSizes)

	// Then apply the final keyed hash with ZERO_KEY
	return FileHash(computeFileHash(xorbHash[:]))
}
