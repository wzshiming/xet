package xet

import (
	"encoding/hex"
	"testing"
)

/*
Child 1:

	hash (XET string): c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69
	size: 100

Child 2:

	hash (XET string): 6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22
	size: 200

Buffer being hashed (ASCII, with literal \n newlines):

	c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69 : 100\n
	6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22 : 200\n

Result (XET string):

	be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14
*/
func TestInternalNodeHash(t *testing.T) {
	// Create two child nodes with specified hashes and sizes
	child1Hash, _ := parseHash("c28f58387a60d4aa200c311cda7c7f77f686614864f5869eadebf765d0a14a69")
	child2Hash, _ := parseHash("6e4e3263e073ce2c0e78cc770c361e2778db3b054b98ab65e277fc084fa70f22")

	child1 := node{
		hash: child1Hash,
		size: 100,
	}
	child2 := node{
		hash: child2Hash,
		size: 200,
	}

	// Create a tree and add these as leaves
	tree := newTree()
	tree.AddLeaf(child1.hash, child1.size)
	tree.AddLeaf(child2.hash, child2.size)

	// Compute the root hash (which will merge these two nodes)
	root := tree.ComputeRoot()

	expectedRoot, _ := parseHash("be64c7003ccd3cf4357364750e04c9592b3c36705dee76a71590c011766b6c14")

	if root != expectedRoot {
		t.Errorf("Internal node hash does not match expected value. Got %s, want %s", root.String(), expectedRoot.String())
	}
}

/*
Input: Two chunk hashes from the Internal Node Hash Test Vector

	above, concatenated as raw bytes (not XET string format).

	Chunk hash 1 (raw hex):
	  aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad

	Chunk hash 2 (raw hex):
	  2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2

	Concatenated input (64 bytes, raw hex):
	  aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad
	  2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2

	Verification hash (XET string):
	  eb06a8ad81d588ac05d1d9a079232d9c1e7d0b07232fa58091caa7bf333a2768
*/
func TestVerificationHash(t *testing.T) {
	chunk1, _ := decodeRawHash("aad4607a38588fc2777f7cda1c310c209e86f564486186f6694aa1d065f7ebad")
	chunk2, _ := decodeRawHash("2cce73e063324e6e271e360c77cc780e65ab984b053bdb78220fa74f08fc77e2")

	// Compute verification hash

	verificationHash := ComputeVerificationHash([]ChunkHash{ChunkHash(chunk1), ChunkHash(chunk2)})

	expectedHash, _ := ParseVerificationHash("eb06a8ad81d588ac05d1d9a079232d9c1e7d0b07232fa58091caa7bf333a2768")

	if verificationHash != expectedHash {
		t.Errorf("Verification hash does not match expected value. Got %s, want %s", verificationHash.String(), expectedHash.String())
	}
}

func decodeRawHash(hexStr string) (hash, error) {
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return hash{}, err
	}
	var hash hash
	copy(hash[:], bytes)
	return hash, nil
}
