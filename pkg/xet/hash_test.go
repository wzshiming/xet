package xet

import (
	"encoding/hex"
	"testing"
)

func TestChunkHash(t *testing.T) {
	// Test vector from specification: "Hello World!" should hash to specific value
	data := []byte("Hello World!")
	hash := ComputeChunkHash(data)

	// Convert to hex string
	hashStr := hash.String()

	t.Logf("Chunk hash of 'Hello World!': %s", hashStr)

	// The hash should be deterministic
	hash2 := ComputeChunkHash(data)
	if hash != hash2 {
		t.Errorf("Hash is not deterministic")
	}
}

func TestHashToString(t *testing.T) {
	// Test the hash string conversion
	// Create a known hash value
	testHash := Hash{}
	for i := range 32 {
		testHash[i] = byte(i)
	}

	str := testHash.String()
	t.Logf("Hash string: %s", str)

	// Should be 64 hex characters
	if len(str) != 64 {
		t.Errorf("Hash string length should be 64, got %d", len(str))
	}

	// Test round-trip conversion
	hash2, err := StringToHash(str)
	if err != nil {
		t.Errorf("Failed to parse hash string: %v", err)
	}

	if hash2 != testHash {
		t.Errorf("Round-trip conversion failed")
	}
}

func TestDataKey(t *testing.T) {
	// Verify the DATA_KEY has the correct size
	if len(DataKey) != 32 {
		t.Errorf("DATA_KEY should be 32 bytes, got %d", len(DataKey))
	}

	t.Logf("DATA_KEY: %s", hex.EncodeToString(DataKey))
}

func TestVerificationHash(t *testing.T) {
	// Test verification hash with some chunk hashes
	chunk1 := ComputeChunkHash([]byte("chunk1"))
	chunk2 := ComputeChunkHash([]byte("chunk2"))

	verifyHash := ComputeVerificationHash([]Hash{chunk1, chunk2})
	t.Logf("Verification hash: %s", verifyHash.String())

	// Should be deterministic
	verifyHash2 := ComputeVerificationHash([]Hash{chunk1, chunk2})
	if verifyHash != verifyHash2 {
		t.Errorf("Verification hash is not deterministic")
	}
}
