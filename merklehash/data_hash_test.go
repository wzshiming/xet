package merklehash

import (
	"bytes"
	"testing"
)

var hashRaw = []byte{
	22, 175, 58, 132, 4, 75, 131, 214,
	190, 153, 138, 66, 226, 3, 153, 242,
	204, 86, 80, 234, 249, 153, 80, 99,
	159, 80, 65, 138, 236, 231, 149, 78,
}

const expectedHex = "d6834b04843aaf16f29903e2428a99be635099f9ea5056cc4e95e7ec8a41509f"

func TestFromSlice(t *testing.T) {
	h, err := FromSlice(hashRaw)
	if err != nil {
		t.Fatalf("FromSlice: %v", err)
	}
	got := h.AsBytes()
	if !bytes.Equal(got, hashRaw) {
		t.Errorf("round-trip failed:\n  got  %v\n  want %v", got, hashRaw)
	}
}

func TestTryFromBytesImproper(t *testing.T) {
	_, err := FromSlice(make([]byte, 31))
	if err == nil {
		t.Error("expected error for 31-byte slice")
	}
}

func TestHex(t *testing.T) {
	h, err := FromSlice(hashRaw)
	if err != nil {
		t.Fatalf("FromSlice: %v", err)
	}
	got := h.Hex()
	if got != expectedHex {
		t.Errorf("Hex()\n  got  %s\n  want %s", got, expectedHex)
	}
}

func TestFromHex(t *testing.T) {
	h, err := FromHex(expectedHex)
	if err != nil {
		t.Fatalf("FromHex: %v", err)
	}
	if !bytes.Equal(h.AsBytes(), hashRaw) {
		t.Errorf("FromHex bytes mismatch:\n  got  %v\n  want %v", h.AsBytes(), hashRaw)
	}
}

func TestHexRoundTrip(t *testing.T) {
	h, err := FromSlice(hashRaw)
	if err != nil {
		t.Fatal(err)
	}
	hexStr := h.Hex()
	h2, err := FromHex(hexStr)
	if err != nil {
		t.Fatal(err)
	}
	if h != h2 {
		t.Errorf("hex round-trip mismatch: %v != %v", h, h2)
	}
}

func TestBase64RoundTrip(t *testing.T) {
	h, err := FromSlice(hashRaw)
	if err != nil {
		t.Fatal(err)
	}
	b64 := h.Base64()
	h2, err := FromBase64(b64)
	if err != nil {
		t.Fatal(err)
	}
	if h != h2 {
		t.Errorf("base64 round-trip mismatch: %v != %v", h, h2)
	}
}

func TestHMACKeyChange(t *testing.T) {
	msg, _ := FromSlice(hashRaw)
	key1 := DataHash{1, 2, 3, 4}
	key2 := DataHash{5, 6, 7, 8}
	r1 := msg.HMAC(key1)
	r2 := msg.HMAC(key2)
	if r1 == r2 {
		t.Error("different keys should produce different HMAC outputs")
	}
}

func TestHMACMessageChange(t *testing.T) {
	key := DataHash{1, 2, 3, 4}
	msg1, _ := FromSlice(hashRaw)
	msg2 := DataHash{0, 0, 0, 0}
	r1 := msg1.HMAC(key)
	r2 := msg2.HMAC(key)
	if r1 == r2 {
		t.Error("different messages should produce different HMAC outputs")
	}
}

func TestMarker(t *testing.T) {
	m := Marker()
	for i := 0; i < 4; i++ {
		if m[i] != ^uint64(0) {
			t.Errorf("Marker()[%d] = %x, want all 1s", i, m[i])
		}
	}
}

func TestHashHexStringEndianness(t *testing.T) {
	h, err := FromSlice(hashRaw)
	if err != nil {
		t.Fatal(err)
	}
	got := h.Hex()
	if got != expectedHex {
		t.Errorf("endianness test:\n  got  %s\n  want %s", got, expectedHex)
	}
	// Also verify String() matches Hex()
	if h.String() != got {
		t.Errorf("String() != Hex()")
	}
}

func TestIsZero(t *testing.T) {
	var zero DataHash
	if !zero.IsZero() {
		t.Error("zero hash should be zero")
	}
	nonZero := DataHash{1, 0, 0, 0}
	if nonZero.IsZero() {
		t.Error("non-zero hash should not be zero")
	}
}

func TestCompare(t *testing.T) {
	a, _ := FromSlice(hashRaw)
	b := DataHash{}
	if a.Compare(a) != 0 {
		t.Error("Compare(self) should be 0")
	}
	if a.Compare(b) == 0 {
		t.Error("different hashes should not compare equal")
	}
	if a.Less(a) {
		t.Error("a should not be less than itself")
	}
}

func TestComputeDataHash(t *testing.T) {
	h := ComputeDataHash([]byte("hello world"))
	if h.IsZero() {
		t.Error("data hash of non-empty input should not be zero")
	}
	// Same input should produce same hash.
	h2 := ComputeDataHash([]byte("hello world"))
	if h != h2 {
		t.Error("determinism failure")
	}
}

func TestComputeInternalNodeHash(t *testing.T) {
	h := ComputeInternalNodeHash([]byte("test data"))
	if h.IsZero() {
		t.Error("internal node hash should not be zero")
	}
	// Data hash and internal node hash should differ for same input.
	h2 := ComputeDataHash([]byte("test data"))
	if h == h2 {
		t.Error("data hash and internal node hash should differ")
	}
}
