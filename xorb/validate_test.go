package xorb

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

func TestValidateRejectsTruncatedChunkHeader(t *testing.T) {
	err := Validate(bytes.NewReader([]byte{0, 1, 2}), xet.XorbHash{})
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("Validate() error = %v, want unexpected EOF", err)
	}
}

func TestValidateRejectsOversizedChunkLengths(t *testing.T) {
	tests := []struct {
		name   string
		header [8]byte
		want   string
	}{
		{
			name:   "compressed",
			header: [8]byte{0, 1, 0, 3}, // 196609 bytes, larger than MaxChunkSize.
			want:   "invalid compressed chunk size",
		},
		{
			name:   "uncompressed",
			header: [8]byte{0, 0, 0, 0, 0, 1, 0, 3},
			want:   "invalid uncompressed chunk size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(bytes.NewReader(tt.header[:]), xet.XorbHash{})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestDecoderRejectsChunkLargerThanCallerBuffer(t *testing.T) {
	header := [8]byte{0, 2, 0, 0, 0, 2, 0, 0}
	decoder := NewDecoder(bytes.NewReader(header[:]), false)

	if _, err := decoder.Read(make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "input buffer too small") {
		t.Fatalf("Decoder.Read() error = %v, want input-buffer error", err)
	}
}

func TestValidateRejectsFooterChunkCountAboveProtocolLimit(t *testing.T) {
	var data bytes.Buffer
	data.Write(xorbIdentifier[:])
	data.WriteByte(1)
	data.Write(make([]byte, 32))
	data.Write(hashSectionIdent[:])
	data.WriteByte(0)
	if err := binary.Write(&data, binary.LittleEndian, uint32(xet.MaxChunksPerXorb+1)); err != nil {
		t.Fatal(err)
	}

	err := Validate(&data, xet.XorbHash{})
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("Validate() error = %v, want chunk-count limit error", err)
	}
}

func TestFooterUniquenessNonceDoesNotChangeXorbHash(t *testing.T) {
	nonce := [4]byte{1, 2, 3, 4}
	chunks := [][]byte{[]byte("first chunk"), []byte("second chunk")}

	withoutNonce, hashWithoutNonce := encodeXorbForTest(t, [4]byte{}, chunks...)
	withNonce, hashWithNonce := encodeXorbForTest(t, nonce, chunks...)

	if bytes.Equal(withoutNonce, withNonce) {
		t.Fatal("serialized xorbs should differ when the uniqueness nonce differs")
	}
	if hashWithoutNonce != hashWithNonce {
		t.Fatalf("xorb hash changed with nonce: %s != %s", hashWithoutNonce, hashWithNonce)
	}
	if got := withNonce[len(withNonce)-20 : len(withNonce)-16]; !bytes.Equal(got, nonce[:]) {
		t.Fatalf("serialized nonce = %x, want %x", got, nonce)
	}
	if err := Validate(bytes.NewReader(withNonce), hashWithNonce); err != nil {
		t.Fatalf("Validate() rejected uniqueness nonce: %v", err)
	}

	decoder := NewDecoder(bytes.NewReader(withNonce), true)
	decoded, err := io.ReadAll(decoder)
	if err != nil {
		t.Fatalf("Decoder rejected uniqueness nonce: %v", err)
	}
	if want := bytes.Join(chunks, nil); !bytes.Equal(decoded, want) {
		t.Fatalf("decoded data = %q, want %q", decoded, want)
	}
	if got := decoder.SummoryHash(); got != hashWithNonce {
		t.Fatalf("decoded xorb hash = %s, want %s", got, hashWithNonce)
	}
}

func TestValidateRejectsNonZeroReservedFooterBuffer(t *testing.T) {
	data, xorbHash := encodeXorbForTest(t, [4]byte{1}, []byte("chunk"))
	data[len(data)-16] = 1 // First reserved byte after nonce; final 4 bytes are footer length.

	err := Validate(bytes.NewReader(data), xorbHash)
	if err == nil || !strings.Contains(err.Error(), "reserved footer buffer") {
		t.Fatalf("Validate() error = %v, want reserved-footer error", err)
	}
}

func TestEncoderEnforcesDraft05XorbLimits(t *testing.T) {
	t.Run("raw payload", func(t *testing.T) {
		encoder := NewEncoder(io.Discard, false)
		encoder.unpackedPos = xet.MaxXorbSize
		if _, err := encoder.Write([]byte{1}); err == nil || !strings.Contains(err.Error(), "raw payload") {
			t.Fatalf("Encoder.Write() error = %v, want raw-payload limit error", err)
		}
	})

	t.Run("chunk count", func(t *testing.T) {
		encoder := NewEncoder(io.Discard, false)
		encoder.chunkHashes = make([]xet.ChunkHash, xet.MaxChunksPerXorb)
		if _, err := encoder.Write([]byte{1}); err == nil || !strings.Contains(err.Error(), "chunk count") {
			t.Fatalf("Encoder.Write() error = %v, want chunk-count limit error", err)
		}
	})
}

func TestValidateBindsContentToExpectedHash(t *testing.T) {
	var chunkOnly bytes.Buffer
	encoder := NewEncoder(&chunkOnly, false)
	if _, err := encoder.Write([]byte("chunk-only content")); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}

	if err := Validate(bytes.NewReader(chunkOnly.Bytes()), encoder.SummoryHash()); err != nil {
		t.Fatalf("Validate() with correct hash: %v", err)
	}
	err := Validate(bytes.NewReader(chunkOnly.Bytes()), xet.XorbHash{42})
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("Validate() chunk-only wrong hash error = %v, want hash mismatch", err)
	}
}

func TestValidateRejectsForgedFooterHash(t *testing.T) {
	// An internally consistent footer claiming the wrong xorb hash must not
	// validate: the hash has to be recomputed from the chunks themselves.
	data, _ := encodeXorbForTest(t, [4]byte{}, []byte("first"), []byte("second"))
	claimed := xet.XorbHash{7}
	footerLen := binary.LittleEndian.Uint32(data[len(data)-4:])
	footerStart := len(data) - int(footerLen) - 4
	copy(data[footerStart+8:footerStart+40], claimed[:])

	err := Validate(bytes.NewReader(data), claimed)
	if err == nil || !strings.Contains(err.Error(), "hash mismatch") {
		t.Fatalf("Validate() forged footer error = %v, want hash mismatch", err)
	}
}

func encodeXorbForTest(t *testing.T, nonce [4]byte, chunks ...[]byte) ([]byte, xet.XorbHash) {
	t.Helper()

	var buf bytes.Buffer
	encoder := NewEncoder(&buf, true)
	if err := encoder.SetUniquenessNonce(nonce); err != nil {
		t.Fatalf("Encoder.SetUniquenessNonce() failed: %v", err)
	}
	for _, chunk := range chunks {
		if _, err := encoder.Write(chunk); err != nil {
			t.Fatalf("Encoder.Write() failed: %v", err)
		}
	}
	if err := encoder.Close(); err != nil {
		t.Fatalf("Encoder.Close() failed: %v", err)
	}
	return buf.Bytes(), encoder.SummoryHash()
}
