package xorb

import (
	"bytes"
	"testing"
)

func TestCompressionSchemeString(t *testing.T) {
	tests := []struct {
		scheme CompressionScheme
		want   string
	}{
		{CompressionNone, "none"},
		{CompressionLZ4, "lz4"},
		{CompressionByteGrouping4LZ4, "bg4-lz4"},
		{CompressionAuto, "auto"},
	}
	for _, tt := range tests {
		if got := tt.scheme.String(); got != tt.want {
			t.Errorf("CompressionScheme(%d).String() = %q, want %q", tt.scheme, got, tt.want)
		}
	}
}

func TestParseCompressionScheme(t *testing.T) {
	valid := []struct {
		input string
		want  CompressionScheme
	}{
		{"", CompressionAuto},
		{"auto", CompressionAuto},
		{"AUTO", CompressionAuto},
		{"  Auto  ", CompressionAuto},
		{"none", CompressionNone},
		{"NONE", CompressionNone},
		{"lz4", CompressionLZ4},
		{"LZ4", CompressionLZ4},
		{"bg4-lz4", CompressionByteGrouping4LZ4},
		{"BG4-LZ4", CompressionByteGrouping4LZ4},
	}
	for _, tt := range valid {
		got, err := ParseCompressionScheme(tt.input)
		if err != nil {
			t.Errorf("ParseCompressionScheme(%q) unexpected error: %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseCompressionScheme(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}

	invalid := []string{"zstd", "gzip", "unknown"}
	for _, s := range invalid {
		_, err := ParseCompressionScheme(s)
		if err == nil {
			t.Errorf("ParseCompressionScheme(%q) expected error, got nil", s)
		}
	}
}

func TestCompressionSchemeFromByte(t *testing.T) {
	valid := []struct {
		b    byte
		want CompressionScheme
	}{
		{0, CompressionNone},
		{1, CompressionLZ4},
		{2, CompressionByteGrouping4LZ4},
		{99, CompressionAuto},
	}
	for _, tt := range valid {
		got, err := CompressionSchemeFromByte(tt.b)
		if err != nil {
			t.Errorf("CompressionSchemeFromByte(%d) unexpected error: %v", tt.b, err)
			continue
		}
		if got != tt.want {
			t.Errorf("CompressionSchemeFromByte(%d) = %v, want %v", tt.b, got, tt.want)
		}
	}

	invalid := []byte{3, 50, 100, 255}
	for _, b := range invalid {
		_, err := CompressionSchemeFromByte(b)
		if err == nil {
			t.Errorf("CompressionSchemeFromByte(%d) expected error, got nil", b)
		}
	}
}

func TestLZ4Compress(t *testing.T) {
	original := []byte("Hello, this is a test of LZ4 compression in the xorb package! Repeated data helps compression: AAAAAAAAAA")
	compressed, err := CompressionLZ4.Compress(original)
	if err != nil {
		t.Fatalf("LZ4 Compress error: %v", err)
	}

	decompressed, err := CompressionLZ4.Decompress(compressed)
	if err != nil {
		t.Fatalf("LZ4 Decompress error: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("LZ4 round-trip mismatch: got %d bytes, want %d bytes", len(decompressed), len(original))
	}
}

func TestBG4Split(t *testing.T) {
	tests := [][]byte{
		{},
		{1},
		{1, 2, 3},
		{1, 2, 3, 4},
		{1, 2, 3, 4, 5},
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		bytes.Repeat([]byte{0xAB, 0xCD, 0xEF, 0x01}, 100),
	}
	for i, original := range tests {
		split := bg4Split(original)
		regrouped, err := bg4Regroup(split)
		if err != nil {
			t.Errorf("test %d: bg4Regroup error: %v", i, err)
			continue
		}
		if !bytes.Equal(original, regrouped) {
			t.Errorf("test %d: bg4 round-trip mismatch: got %v, want %v", i, regrouped, original)
		}
	}
}

func TestBG4LZ4Compress(t *testing.T) {
	original := bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 256)
	compressed, err := CompressionByteGrouping4LZ4.Compress(original)
	if err != nil {
		t.Fatalf("BG4-LZ4 Compress error: %v", err)
	}

	decompressed, err := CompressionByteGrouping4LZ4.Decompress(compressed)
	if err != nil {
		t.Fatalf("BG4-LZ4 Decompress error: %v", err)
	}

	if !bytes.Equal(original, decompressed) {
		t.Fatalf("BG4-LZ4 round-trip mismatch: got %d bytes, want %d bytes", len(decompressed), len(original))
	}
}

func TestCompressionNone(t *testing.T) {
	original := []byte("pass through unchanged")
	compressed, err := CompressionNone.Compress(original)
	if err != nil {
		t.Fatalf("None Compress error: %v", err)
	}
	if !bytes.Equal(original, compressed) {
		t.Fatal("None Compress should return data unchanged")
	}

	decompressed, err := CompressionNone.Decompress(compressed)
	if err != nil {
		t.Fatalf("None Decompress error: %v", err)
	}
	if !bytes.Equal(original, decompressed) {
		t.Fatal("None Decompress should return data unchanged")
	}
}

func TestAutoResolve(t *testing.T) {
	data := []byte("test data")
	resolved := CompressionAuto.ResolveForData(data)
	if resolved == CompressionAuto {
		t.Fatal("ResolveForData should not return Auto")
	}
	if resolved != CompressionLZ4 {
		t.Fatalf("ResolveForData returned %v, want LZ4", resolved)
	}
}

func TestAutoDecompressError(t *testing.T) {
	_, err := CompressionAuto.Decompress([]byte("data"))
	if err == nil {
		t.Fatal("Decompress with Auto should return error")
	}
}
