package xorb

import (
	"bytes"
	"encoding/binary"
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
