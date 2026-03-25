package gearhash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

// Verify the lookup table exactly matches the XET specification (Appendix B).
func TestLookupTableMatchesSpec(t *testing.T) {
	const expectedSHA256 = "e1d3936666d7ae7a977c958e9afcc75f90aaca758ce5fbe4ece61dffefe1912c"

	hasher := sha256.New()
	var buf [8]byte

	for _, v := range lookupTable {
		binary.LittleEndian.PutUint64(buf[:], v)
		hasher.Write(buf[:])
	}

	sum := hex.EncodeToString(hasher.Sum(nil))
	if sum != expectedSHA256 {
		t.Fatalf("gearhash lookup table mismatch: got %s, want %s", sum, expectedSHA256)
	}
}
