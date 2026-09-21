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

func TestValidateRejectsClaimedHashNotMatchingChunks(t *testing.T) {
	chunks := [][]byte{[]byte("first chunk"), []byte("second chunk")}
	bogus := xet.XorbHash{42}
	chunkOnly := encodeChunkOnlyXorbForTest(t, chunks...)
	forged, xorbHash := encodeXorbForTest(t, [4]byte{}, chunks...)
	// Footer length field excludes itself; the embedded hash follows the 8-byte main header.
	footerStart := len(forged) - int(binary.LittleEndian.Uint32(forged[len(forged)-4:])) - 4
	copy(forged[footerStart+8:footerStart+40], bogus[:])

	if err := Validate(bytes.NewReader(chunkOnly), xorbHash); err != nil {
		t.Fatalf("Validate(chunk-only, computed hash) error = %v", err)
	}
	if err := Validate(bytes.NewReader(nil), xet.XorbHash{}); err != nil {
		t.Fatalf("Validate(empty, zero hash) error = %v", err)
	}

	for _, tt := range []struct {
		name    string
		data    []byte
		claimed xet.XorbHash
	}{
		{"chunk only wrong claim", chunkOnly, bogus},
		{"forged footer matching wrong claim", forged, bogus},
		{"forged footer honest claim", forged, xorbHash},
		{"empty wrong claim", nil, bogus},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(bytes.NewReader(tt.data), tt.claimed)
			if err == nil || !strings.Contains(err.Error(), "xorb hash mismatch") {
				t.Fatalf("Validate() error = %v, want xorb hash mismatch", err)
			}
		})
	}
}

func TestValidateRejectsChunkSizeNotMatchingPayload(t *testing.T) {
	payloads := [][]byte{{'a'}, {'b'}}
	honestChunkOnly, honestFootered, honest := rawXorbForTest(t, 1, payloads...)
	forgedChunkOnly, forgedFootered, forged := rawXorbForTest(t, 2, payloads...)
	if honest == forged {
		t.Fatal("declared sizes should change the root")
	}
	if want, _ := encodeXorbForTest(t, [4]byte{}, payloads...); !bytes.Equal(honestFootered, want) {
		t.Fatal("hand-built honest xorb should match Encoder output")
	}
	if err := Validate(bytes.NewReader(honestChunkOnly), honest); err != nil {
		t.Fatalf("Validate(honest chunk-only) error = %v", err)
	}
	if err := Validate(bytes.NewReader(honestFootered), honest); err != nil {
		t.Fatalf("Validate(honest footer) error = %v", err)
	}

	for _, tt := range []struct {
		name string
		data []byte
	}{
		{"chunk only", forgedChunkOnly},
		{"footer", forgedFootered},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(bytes.NewReader(tt.data), forged)
			if err == nil || !strings.Contains(err.Error(), "chunk size mismatch") {
				t.Fatalf("Validate() error = %v, want chunk size mismatch", err)
			}
		})
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

// rawXorbForTest stores payloads uncompressed while every chunk header and the footer declare declared bytes per chunk.
func rawXorbForTest(t *testing.T, declared int, payloads ...[]byte) (chunkOnly, footered []byte, root xet.XorbHash) {
	t.Helper()

	var stream bytes.Buffer
	encoder := NewEncoder(&stream, true)
	for _, p := range payloads {
		stream.Write([]byte{0, byte(len(p)), 0, 0, byte(compressionNone), byte(declared), 0, 0})
		stream.Write(p)
		encoder.chunkHashes = append(encoder.chunkHashes, xet.ComputeChunkHash(p))
		encoder.chunkSizes = append(encoder.chunkSizes, uint64(declared))
		encoder.packedPos += 8 + uint64(len(p))
		encoder.unpackedPos += uint64(declared)
		encoder.chunkOffsets = append(encoder.chunkOffsets, encoder.packedPos)
		encoder.unpackedOffsets = append(encoder.unpackedOffsets, encoder.unpackedPos)
	}
	chunkOnly = bytes.Clone(stream.Bytes())
	if err := encoder.writeFooter(); err != nil {
		t.Fatalf("Encoder.writeFooter() failed: %v", err)
	}
	return chunkOnly, stream.Bytes(), encoder.SummoryHash()
}
