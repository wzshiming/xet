package gearhash

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

// TestLookupTableChecksum verifies the gearhash lookup table matches Appendix B
// of the XET specification by checking its SHA-256 checksum.
func TestLookupTableChecksum(t *testing.T) {
	h := sha256.New()
	for _, v := range lookupTable {
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], v)
		h.Write(buf[:])
	}
	got := hex.EncodeToString(h.Sum(nil))
	want := "e1d3936666d7ae7a977c958e9afcc75f90aaca758ce5fbe4ece61dffefe1912c"
	if got != want {
		t.Errorf("lookup table SHA-256 = %s, want %s", got, want)
	}
}
