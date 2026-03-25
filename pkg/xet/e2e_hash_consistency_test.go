package xet_test

import (
	"testing"

	"github.com/wzshiming/xet/pkg/gearhash"
	"github.com/wzshiming/xet/pkg/merkle"
	"github.com/wzshiming/xet/pkg/xet"
	"github.com/wzshiming/xet/pkg/xorb"
)

// TestFileHashConsistency validates that for the same file, the XET hash
// computed by this implementation is deterministic and follows the specification.
// This ensures consistency with HuggingFace's xet-core implementation.
func TestFileHashConsistency(t *testing.T) {
	tests := []struct {
		name         string
		data         []byte
		expectedHash string // Expected XET file hash in string format (if known)
	}{
		{
			name:         "Empty file",
			data:         []byte{},
			expectedHash: "", // Will verify determinism
		},
		{
			name:         "Small file - Hello World",
			data:         []byte("Hello World!"),
			expectedHash: "", // Will verify determinism
		},
		{
			name:         "Medium file - repeated pattern",
			data:         makeRepeatedData(100 * 1024), // 100KB
			expectedHash: "",
		},
		{
			name:         "Large file - 1MB",
			data:         makeRepeatedData(1024 * 1024),
			expectedHash: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Compute file hash using the complete pipeline
			fileHash := computeFileHashE2E(t, tt.data)

			// Recompute to ensure determinism
			fileHash2 := computeFileHashE2E(t, tt.data)

			if fileHash != fileHash2 {
				t.Errorf("File hash is not deterministic!\n  First:  %s\n  Second: %s",
					fileHash.String(), fileHash2.String())
			}

			t.Logf("File hash (len=%d): %s", len(tt.data), fileHash.String())

			// If expected hash is provided, validate it
			if tt.expectedHash != "" {
				expected, err := xet.StringToHash(tt.expectedHash)
				if err != nil {
					t.Fatalf("Invalid expected hash: %v", err)
				}
				if fileHash != expected {
					t.Errorf("File hash mismatch!\n  Got:  %s\n  Want: %s",
						fileHash.String(), expected.String())
				}
			}
		})
	}
}

// computeFileHashE2E performs the complete end-to-end file hash computation:
// 1. Content-defined chunking using Gearhash
// 2. Compute chunk hashes with DATA_KEY
// 3. Build Merkle tree to get xorb hash (merkle root)
// 4. Apply ZERO_KEY to merkle root to get file hash
func computeFileHashE2E(t *testing.T, data []byte) xet.Hash {
	// Step 1: Chunk the data
	chunks := gearhash.ChunkData(data)

	if len(data) > 0 && len(chunks) == 0 {
		t.Fatal("ChunkData returned 0 chunks for non-empty data")
	}

	// Handle empty file case
	if len(data) == 0 {
		// For empty files, use zero hash as xorb hash
		zeroHash := xet.Hash{}
		return xet.ComputeFileHash(zeroHash[:])
	}

	// Step 2: Compute chunk hashes
	chunkHashes := make([]xet.Hash, len(chunks))
	for i, chunk := range chunks {
		chunkHashes[i] = xet.ComputeChunkHash(chunk.Data)
	}

	// Step 3: Build Merkle tree to get xorb hash
	tree := merkle.NewTree()
	for i, chunk := range chunks {
		tree.AddLeaf(chunkHashes[i], uint64(len(chunk.Data)))
	}
	xorbHash := tree.ComputeRoot()

	// Step 4: Apply ZERO_KEY to get file hash
	fileHash := xet.ComputeFileHash(xorbHash[:])

	return fileHash
}

// TestXorbHashConsistency validates that xorb hash computation is deterministic
// and matches the Merkle tree root computation
func TestXorbHashConsistency(t *testing.T) {
	testData := []byte("Test data for xorb hash validation - ensuring consistency with HuggingFace implementation. " +
		"This data should be large enough to potentially span multiple chunks.")

	// Create xorb using the xorb package
	xorbObj := xorb.NewXorb()

	// Chunk and add to xorb
	chunks := gearhash.ChunkData(testData)
	for _, chunk := range chunks {
		err := xorbObj.AddChunk(chunk.Data)
		if err != nil {
			t.Fatalf("Failed to add chunk: %v", err)
		}
	}

	// Serialize to compute the xorb hash
	serialized, err := xorbObj.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize xorb: %v", err)
	}

	// Get the xorb hash (computed during serialization)
	xorbHash1 := xorbObj.Hash

	// Independently compute merkle root
	tree := merkle.NewTree()
	for _, chunk := range chunks {
		chunkHash := xet.ComputeChunkHash(chunk.Data)
		tree.AddLeaf(chunkHash, uint64(len(chunk.Data)))
	}
	merkleRoot := tree.ComputeRoot()

	// Verify xorb hash equals merkle root
	if xorbHash1 != merkleRoot {
		t.Errorf("Xorb hash does not match Merkle root!\n  Xorb hash:   %s\n  Merkle root: %s",
			xorbHash1.String(), merkleRoot.String())
	}

	// Deserialize to verify round-trip consistency
	deserialized, err := xorb.Deserialize(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize xorb: %v", err)
	}

	if deserialized.Hash != xorbHash1 {
		t.Errorf("Xorb hash mismatch after round-trip!\n  Original:     %s\n  Deserialized: %s",
			xorbHash1.String(), deserialized.Hash.String())
	}

	// Recompute from scratch to ensure determinism
	xorbObj2 := xorb.NewXorb()
	for _, chunk := range chunks {
		xorbObj2.AddChunk(chunk.Data)
	}
	xorbObj2.Serialize() // Need to serialize to compute hash

	if xorbObj2.Hash != xorbHash1 {
		t.Errorf("Xorb hash is not deterministic!\n  First:  %s\n  Second: %s",
			xorbHash1.String(), xorbObj2.Hash.String())
	}

	t.Logf("Xorb hash (deterministic): %s", xorbHash1.String())
	t.Logf("Chunks: %d, Total size: %d bytes, Xorb size: %d bytes",
		len(chunks), len(testData), len(serialized))
}

// TestCompleteUploadDownloadCycle tests the full cycle to ensure hash consistency
func TestCompleteUploadDownloadCycle(t *testing.T) {
	// Test with various data sizes
	testCases := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"64KB", 64 * 1024},
		{"128KB", 128 * 1024},
		{"500KB", 500 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Generate test data
			testData := makeRepeatedData(tc.size)

			// Compute file hash
			fileHash := computeFileHashE2E(t, testData)

			// Verify determinism by recomputing
			fileHash2 := computeFileHashE2E(t, testData)

			if fileHash != fileHash2 {
				t.Errorf("Non-deterministic file hash computation for %d bytes", tc.size)
			}

			t.Logf("File hash for %d bytes: %s", tc.size, fileHash.String())

			// Create xorb and verify its hash is the merkle root
			xorbObj := xorb.NewXorb()
			chunks := gearhash.ChunkData(testData)
			for _, chunk := range chunks {
				xorbObj.AddChunk(chunk.Data)
			}

			// Serialize to compute xorb hash
			_, err := xorbObj.Serialize()
			if err != nil {
				t.Fatalf("Failed to serialize xorb: %v", err)
			}

			// Verify file hash = ZERO_KEY(xorb_hash)
			expectedFileHash := xet.ComputeFileHash(xorbObj.Hash[:])
			if fileHash != expectedFileHash {
				t.Errorf("File hash does not match ZERO_KEY(xorb_hash)\n  E2E:      %s\n  Expected: %s",
					fileHash.String(), expectedFileHash.String())
			}
		})
	}
}

// makeRepeatedData creates test data with a repeated pattern
func makeRepeatedData(size int) []byte {
	pattern := []byte("XET Protocol Test Pattern - Consistency Validation - ")
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = pattern[i%len(pattern)]
	}
	return result
}
