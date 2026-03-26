package xet

import (
	"testing"
)

func TestMerkleTreeConstruction(t *testing.T) {
	// Test with various numbers of chunks to ensure the algorithm works correctly
	testCases := []struct {
		name      string
		numChunks int
		chunkSize uint64
	}{
		{"Single chunk", 1, 100},
		{"Two chunks", 2, 100},
		{"Three chunks", 3, 100},
		{"Five chunks", 5, 100},
		{"Ten chunks", 10, 100},
		{"Twenty chunks", 20, 100},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tree := newTree()

			// Add dummy chunks
			for i := 0; i < tc.numChunks; i++ {
				// Create a dummy hash for each chunk
				var hash Hash
				hash[0] = byte(i)
				hash[1] = byte(i >> 8)
				tree.AddLeaf(hash, tc.chunkSize)
			}

			// Compute root - should not panic
			root := tree.ComputeRoot()
			t.Logf("Root hash for %d chunks: %s", tc.numChunks, root.String())

			// Verify it's deterministic
			tree2 := newTree()
			for i := 0; i < tc.numChunks; i++ {
				var hash Hash
				hash[0] = byte(i)
				hash[1] = byte(i >> 8)
				tree2.AddLeaf(hash, tc.chunkSize)
			}
			root2 := tree2.ComputeRoot()

			if root != root2 {
				t.Error("Merkle tree computation is not deterministic")
			}
		})
	}
}

func TestNextMergeCut(t *testing.T) {
	// Test the nextMergeCut function logic
	testCases := []struct {
		name     string
		numNodes int
		expected int
	}{
		{"Zero nodes", 0, 0},
		{"One node", 1, 1},
		{"Two nodes", 2, 2},
		{"Three nodes", 3, 3}, // Will return 3 or based on hash
		{"Ten nodes", 10, 0},  // Will vary based on hash values
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodes := make([]node, tc.numNodes)
			for i := 0; i < tc.numNodes; i++ {
				// Create dummy nodes
				nodes[i] = node{
					hash: Hash{},
					size: 100,
				}
			}

			cut := nextMergeCut(nodes)
			t.Logf("Cut for %d nodes: %d", tc.numNodes, cut)

			// Verify basic constraints
			if cut < 0 || cut > tc.numNodes {
				t.Errorf("Invalid cut: %d for %d nodes", cut, tc.numNodes)
			}

			if tc.numNodes <= 2 && cut != tc.numNodes {
				t.Errorf("For <= 2 nodes, should return all nodes. Got %d, expected %d", cut, tc.numNodes)
			}

			if cut > MaxChildren {
				t.Errorf("Cut should not exceed MAX_CHILDREN (%d), got %d", MaxChildren, cut)
			}
		})
	}
}
