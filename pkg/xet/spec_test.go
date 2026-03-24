package xet

import (
	"encoding/hex"
	"testing"
)

// TestChunkHashVector tests the chunk hash computation against the spec test vector (Appendix C.1).
func TestChunkHashVector(t *testing.T) {
	input := []byte("Hello World!")

	hash := ComputeChunkHash(input)

	// Expected raw hex from spec:
	// a29cfb08e608d4d8726dd8659a90b9134b3240d5d8e42d5fcb28e2a6e763a3e8
	expectedRawHex := "a29cfb08e608d4d8726dd8659a90b9134b3240d5d8e42d5fcb28e2a6e763a3e8"
	gotRawHex := hex.EncodeToString(hash[:])
	if gotRawHex != expectedRawHex {
		t.Errorf("raw hex mismatch:\n  got:  %s\n  want: %s", gotRawHex, expectedRawHex)
	}

	// Expected XET string from spec:
	// d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb
	expectedXETString := "d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb"
	gotXETString := HashToString(hash)
	if gotXETString != expectedXETString {
		t.Errorf("XET string mismatch:\n  got:  %s\n  want: %s", gotXETString, expectedXETString)
	}
}

// TestHashStringConversion tests the hash string conversion (Appendix C.2).
func TestHashStringConversion(t *testing.T) {
	// Hash bytes 0-31
	var hash [32]byte
	for i := 0; i < 32; i++ {
		hash[i] = byte(i)
	}

	expected := "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"
	got := HashToString(hash)
	if got != expected {
		t.Errorf("hash_to_string mismatch:\n  got:  %s\n  want: %s", got, expected)
	}

	// Round-trip
	recovered, err := StringToHash(got)
	if err != nil {
		t.Fatalf("string_to_hash error: %v", err)
	}
	if recovered != hash {
		t.Errorf("round-trip mismatch:\n  got:  %x\n  want: %x", recovered, hash)
	}
}

// TestInternalNodeHash tests the internal node hash (Appendix C.3).
func TestInternalNodeHash(t *testing.T) {
	// Child 1: hash (XET string): c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69
	hash1, err := StringToHash("c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69")
	if err != nil {
		t.Fatal(err)
	}

	// Child 2: hash (XET string): 6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22
	hash2, err := StringToHash("6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22")
	if err != nil {
		t.Fatal(err)
	}

	// Build the buffer: "{hash_hex} : {size}\n"
	buf := HashToString(hash1) + " : 100\n" + HashToString(hash2) + " : 200\n"

	result := Blake3KeyedHash(&INTERNAL_NODE_KEY, []byte(buf))

	// Expected result (XET string): be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14
	expectedStr := "be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14"
	gotStr := HashToString(result)
	if gotStr != expectedStr {
		t.Errorf("internal node hash mismatch:\n  got:  %s\n  want: %s", gotStr, expectedStr)
	}
}

// TestVerificationRangeHash tests the verification hash (Appendix C.4).
func TestVerificationRangeHash(t *testing.T) {
	// Chunk hash 1 (raw hex): aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad
	chunkHash1Raw, _ := hex.DecodeString("aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad")
	var chunkHash1 [32]byte
	copy(chunkHash1[:], chunkHash1Raw)

	// Chunk hash 2 (raw hex): 2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2
	chunkHash2Raw, _ := hex.DecodeString("2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2")
	var chunkHash2 [32]byte
	copy(chunkHash2[:], chunkHash2Raw)

	hashes := [][32]byte{chunkHash1, chunkHash2}
	result := ComputeVerificationHash(hashes, 0, 2)

	// Expected (XET string): eb06a8ad81d588ac05d1d9a079232d9c1e7d0b07232fa58091caa7bf333a2768
	expectedStr := "eb06a8ad81d588ac05d1d9a079232d9c1e7d0b07232fa58091caa7bf333a2768"
	gotStr := HashToString(result)
	if gotStr != expectedStr {
		t.Errorf("verification hash mismatch:\n  got:  %s\n  want: %s", gotStr, expectedStr)
	}
}
