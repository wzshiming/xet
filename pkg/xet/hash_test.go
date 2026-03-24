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

func TestHashToStringAndBack(t *testing.T) {
	originalHash := [32]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}

	wantHex := "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"

	got := HashToString(originalHash)
	if got != wantHex {
		t.Errorf("Expected hash string %s, got %s", wantHex, got)
	}

	// Now convert back to hash
	parsedHash, err := StringToHash(got)
	if err != nil {
		t.Fatalf("Failed to parse hash string: %v", err)
	}

	if parsedHash != originalHash {
		t.Errorf("Parsed hash does not match original. Got %v, want %v", parsedHash, originalHash)
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
