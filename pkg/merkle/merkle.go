package merkle

import (
	"encoding/binary"
	"fmt"

	"github.com/wzshiming/xet/pkg/xet"
)

// Tree represents a Merkle tree with variable fan-out
type Tree struct {
	leaves []xet.Hash
	sizes  []uint64 // Size in bytes of each leaf chunk
}

// NewTree creates a new Merkle tree
func NewTree() *Tree {
	return &Tree{
		leaves: make([]xet.Hash, 0),
		sizes:  make([]uint64, 0),
	}
}

// AddLeaf adds a leaf (chunk hash and size) to the tree
func (t *Tree) AddLeaf(hash xet.Hash, size uint64) {
	t.leaves = append(t.leaves, hash)
	t.sizes = append(t.sizes, size)
}

// ComputeRoot computes the Merkle tree root hash
func (t *Tree) ComputeRoot() xet.Hash {
	if len(t.leaves) == 0 {
		return xet.Hash{}
	}

	if len(t.leaves) == 1 {
		return t.leaves[0]
	}

	// Build the tree level by level
	currentLevel := make([]node, len(t.leaves))
	for i := range t.leaves {
		currentLevel[i] = node{
			hash: t.leaves[i],
			size: t.sizes[i],
		}
	}

	for len(currentLevel) > 1 {
		currentLevel = t.buildNextLevel(currentLevel)
	}

	return currentLevel[0].hash
}

// node represents a node in the Merkle tree
type node struct {
	hash xet.Hash
	size uint64
}

// buildNextLevel builds the next level of the tree from the current level
func (t *Tree) buildNextLevel(currentLevel []node) []node {
	cutPoints := findCutPoints(currentLevel)

	nextLevel := make([]node, 0, len(cutPoints))
	start := 0

	for _, cutPoint := range cutPoints {
		// Merge nodes from start to cutPoint (exclusive)
		mergedNode := t.mergeNodes(currentLevel[start:cutPoint])
		nextLevel = append(nextLevel, mergedNode)
		start = cutPoint
	}

	// Handle remaining nodes
	if start < len(currentLevel) {
		mergedNode := t.mergeNodes(currentLevel[start:])
		nextLevel = append(nextLevel, mergedNode)
	}

	return nextLevel
}

// findCutPoints determines where to split the sequence based on hash values
func findCutPoints(nodes []node) []int {
	if len(nodes) <= xet.MinChildren {
		return nil
	}

	cutPoints := make([]int, 0)

	for i := xet.MinChildren; i < len(nodes); i++ {
		if i >= len(nodes)-xet.MinChildren && len(nodes)-i < xet.MinChildren {
			// Don't create a cut point that would leave too few children at the end
			break
		}

		// Use the hash value to determine if this is a cut point
		hashValue := binary.LittleEndian.Uint64(nodes[i].hash[:8])
		if hashValue%xet.MeanBranchingFactor == 0 {
			cutPoints = append(cutPoints, i)

			// Check if we've reached MaxChildren
			if i-getLastCutPoint(cutPoints, 0) >= xet.MaxChildren {
				break
			}
		}
	}

	return cutPoints
}

// getLastCutPoint returns the last cut point or the default value
func getLastCutPoint(cutPoints []int, defaultVal int) int {
	if len(cutPoints) == 0 {
		return defaultVal
	}
	return cutPoints[len(cutPoints)-1]
}

// mergeNodes merges a sequence of nodes into a single parent node
func (t *Tree) mergeNodes(nodes []node) node {
	if len(nodes) == 1 {
		return nodes[0]
	}

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
	parentHash := xet.ComputeInternalNodeHash(input)

	return node{
		hash: parentHash,
		size: totalSize,
	}
}

// ComputeXorbHash computes the xorb hash from chunk hashes and sizes
func ComputeXorbHash(chunkHashes []xet.Hash, chunkSizes []uint64) xet.Hash {
	if len(chunkHashes) != len(chunkSizes) {
		panic("chunk hashes and sizes length mismatch")
	}

	tree := NewTree()
	for i := range chunkHashes {
		tree.AddLeaf(chunkHashes[i], chunkSizes[i])
	}

	return tree.ComputeRoot()
}

// ComputeFileHash computes the file hash from chunk hashes and sizes
// This is the same as xorb hash but with an additional keyed hash step
func ComputeFileHash(chunkHashes []xet.Hash, chunkSizes []uint64) xet.Hash {
	// First compute the Merkle root (same as xorb hash)
	xorbHash := ComputeXorbHash(chunkHashes, chunkSizes)

	// Then apply the final keyed hash with ZERO_KEY
	return xet.ComputeFileHash(xorbHash[:])
}
