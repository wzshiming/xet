package crc64nvme

import (
	"encoding/base64"
	"hash/crc64"
	"math/rand/v2"
	"testing"
)

// TestCheckValue pins the CRC-64/NVME check value from xet-core v1.8.0's cas_types tests.
func TestCheckValue(t *testing.T) {
	input := []byte("123456789")
	const want uint64 = 0xAE8B14860A799888
	ecma := crc64.Checksum(input, crc64.MakeTable(crc64.ECMA))
	iso := crc64.Checksum(input, crc64.MakeTable(crc64.ISO))
	check := func(expr string, got uint64) {
		t.Helper()
		switch got {
		case want:
		case ecma:
			t.Fatalf("%s = %#x (the crc64.ECMA value), want CRC-64/NVME %#x", expr, got, want)
		case iso:
			t.Fatalf("%s = %#x (the crc64.ISO value), want CRC-64/NVME %#x", expr, got, want)
		default:
			t.Fatalf("%s = %#x, want %#x", expr, got, want)
		}
	}

	check(`Checksum("123456789")`, Checksum(input))
	h := New()
	h.Write(input)
	check(`New() fed "123456789": Sum64()`, h.Sum64())
}

func TestEmptyInput(t *testing.T) {
	if got := Checksum(nil); got != 0 {
		t.Fatalf("Checksum(nil) = %#x, want 0", got)
	}
	if got := Checksum([]byte{}); got != 0 {
		t.Fatalf("Checksum([]byte{}) = %#x, want 0", got)
	}
	if got := New().Sum64(); got != 0 {
		t.Fatalf("New().Sum64() before any Write = %#x, want 0", got)
	}
}

// The splits straddle hash/crc64's 64- and 2048-byte slicing-by-8 thresholds.
func TestIncrementalMatchesOneShot(t *testing.T) {
	data := make([]byte, 8<<10)
	rand.NewChaCha8([32]byte{1}).Read(data)
	want := Checksum(data)

	var sevens [][]byte
	for i := 0; i < len(data); i += 7 {
		sevens = append(sevens, data[i:min(i+7, len(data))])
	}
	for _, c := range []struct {
		name   string
		pieces [][]byte
	}{
		{"1+rest", [][]byte{data[:1], data[1:]}},
		{"63+rest", [][]byte{data[:63], data[63:]}},
		{"2047+rest", [][]byte{data[:2047], data[2047:]}},
		{"7-byte pieces", sevens},
	} {
		h := New()
		for _, p := range c.pieces {
			if n, err := h.Write(p); n != len(p) || err != nil {
				t.Fatalf("%s: Write(%d bytes) = %d, %v; want %d, nil", c.name, len(p), n, err, len(p))
			}
		}
		if got := h.Sum64(); got != want {
			t.Fatalf("%s: Sum64() = %#x, want Checksum(data) %#x", c.name, got, want)
		}
	}
}

func TestBase64(t *testing.T) {
	for _, c := range []struct {
		sum  uint64
		want string
	}{
		{0xAE8B14860A799888, "rosUhgp5mIg="},
		{0, "AAAAAAAAAAA="},
	} {
		if got := Base64(c.sum); got != c.want {
			t.Fatalf("Base64(%#x) = %q, want %q", c.sum, got, c.want)
		}
	}
	for _, s := range []string{"", "a", "123456789", "The quick brown fox jumps over the lazy dog"} {
		h := New()
		h.Write([]byte(s))
		if got, want := Base64(h.Sum64()), base64.StdEncoding.EncodeToString(h.Sum(nil)); got != want {
			t.Fatalf("Base64(Sum64()) of %q = %q, want base64 of Sum(nil) %q", s, got, want)
		}
	}
	if HeaderName != "x-amz-checksum-crc64nvme" {
		t.Fatalf("HeaderName = %q, want %q", HeaderName, "x-amz-checksum-crc64nvme")
	}
}
