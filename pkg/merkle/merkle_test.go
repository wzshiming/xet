package merkle

import (
	"testing"

	"github.com/wzshiming/xet/pkg/xet"
)

func TestComputeMerkleRootEmpty(t *testing.T) {
	root := ComputeMerkleRoot(nil)
	if root != xet.ZERO_HASH {
		t.Errorf("expected zero hash for empty entries, got %x", root)
	}
}

func TestComputeMerkleRootSingle(t *testing.T) {
	// A single entry should produce a merged hash of itself
	hash1, _ := xet.StringToHash("c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69")
	entries := []HashEntry{{Hash: hash1, Size: 100}}
	root := ComputeMerkleRoot(entries)
	// Root should be the internal node hash of the single entry
	if root == xet.ZERO_HASH {
		t.Error("root should not be zero hash for single entry")
	}
}

func TestComputeMerkleRootTwo(t *testing.T) {
	// Two entries should be merged together (since n <= 2, merge all)
	hash1, _ := xet.StringToHash("c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69")
	hash2, _ := xet.StringToHash("6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22")

	entries := []HashEntry{
		{Hash: hash1, Size: 100},
		{Hash: hash2, Size: 200},
	}
	root := ComputeMerkleRoot(entries)

	// This should match the internal node hash test vector from the spec
	expected := "be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14"
	got := xet.HashToString(root)
	if got != expected {
		t.Errorf("merkle root mismatch:\n  got:  %s\n  want: %s", got, expected)
	}
}

func TestComputeFileHashEmpty(t *testing.T) {
	// Empty file hash: blake3_keyed_hash(ZERO_KEY, ZERO_HASH)
	hash := ComputeFileHash(nil, nil)
	if hash == xet.ZERO_HASH {
		t.Error("empty file hash should not be zero hash (it's the keyed hash of zero hash)")
	}
}

func TestComputeFileHashDeterministic(t *testing.T) {
	hash1, _ := xet.StringToHash("c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69")
	hash2, _ := xet.StringToHash("6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22")

	hashes := [][32]byte{hash1, hash2}
	sizes := []uint64{100, 200}

	result1 := ComputeFileHash(hashes, sizes)
	result2 := ComputeFileHash(hashes, sizes)
	if result1 != result2 {
		t.Error("file hash should be deterministic")
	}
}
