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
func TestComputeChunkHash(t *testing.T) {
	data := []byte("Hello World!")
	hash := ComputeChunkHash(data)

	expected, _ := ParseChunkHash("d8d408e608fb9ca213b9909a65d86d725f2de4d8d540324be8a363e7a6e228cb")

	if hash != expected {
		t.Errorf("Chunk hash does not match expected value. Got %s, want %s", hash, expected)
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
func TestHashString(t *testing.T) {
	hash := hash{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
		0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
		0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	}

	expected := "07060504030201000f0e0d0c0b0a090817161514131211101f1e1d1c1b1a1918"

	got := hash.String()
	if got != expected {
		t.Errorf("HashToString output does not match expected format. Got %s, want %s", got, expected)
	}
}
