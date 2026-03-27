package xet

import (
	"testing"
)

/*
Input (ASCII): Hello World!

	Input (hex): 48656c6c6f20576f726c6421

	Hash (raw hex, bytes 0-31):
	  a29cfb08e608d4d8726dd8659a90b9134b3240d5d8e42d5fcb28e2a6e763a3e8

	Hash (XET string representation):
	  d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb
*/
func TestChunkHashWithKnownInput(t *testing.T) {
	data := ChunkBytes("Hello World!")
	hash := data.Hash()

	expectedHash, _ := ParseHash("d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb")

	if hash != expectedHash {
		t.Errorf("Chunk hash does not match expected value. Got %s, want %s", hash.String(), expectedHash.String())
	}
}

/*
The XET hash string format interprets the 32-byte hash as four
little-endian 64-bit unsigned values and prints each as 16
hexadecimal digits.

Hash bytes [0..31]:

	00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f
	10 11 12 13 14 15 16 17 18 19 1a 1b 1c 1d 1e 1f

Expected XET string:

	07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918
*/
func TestHashToStringFormat(t *testing.T) {
	hash := [32]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}

	expected := "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"

	got := hashToString(hash)
	if got != expected {
		t.Errorf("HashToString output does not match expected format. Got %s, want %s", got, expected)
	}
}
