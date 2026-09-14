package xet

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"
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

// TestChunkerEOFAfterEmptyInput checks that no bytes yield no chunks and that
// Chunk keeps reporting io.EOF once the input is exhausted.
func TestChunkerEOFAfterEmptyInput(t *testing.T) {
	err := ChunkData(bytes.NewReader(nil), func(offset int64, chunk []byte) error {
		return fmt.Errorf("unexpected %d-byte chunk at offset %d", len(chunk), offset)
	})
	if err != nil {
		t.Fatalf("ChunkData: %v", err)
	}
	c := NewChunker(bytes.NewReader(nil))
	for i := range 2 {
		if _, _, err := c.Chunk(); err != io.EOF {
			t.Fatalf("Chunk() call %d error = %v, want io.EOF", i+1, err)
		}
	}
}

// TestChunkerPropagatesErrors checks that read errors, before or after data,
// surface through both entry points and that a callback error stops ChunkData
// instead of ending the stream early.
func TestChunkerPropagatesErrors(t *testing.T) {
	data := bytes.Repeat([]byte{59}, 2*MaxChunkSize)
	errRead := errors.New("read failed")
	readers := map[string]struct {
		r    func() io.Reader
		want error
	}{
		"before any data": {func() io.Reader { return iotest.ErrReader(errRead) }, errRead},
		"after some data": {func() io.Reader { return iotest.TimeoutReader(bytes.NewReader(data)) }, iotest.ErrTimeout},
	}
	for name, tt := range readers {
		t.Run(name, func(t *testing.T) {
			err := ChunkData(tt.r(), func(int64, []byte) error { return nil })
			if !errors.Is(err, tt.want) {
				t.Fatalf("ChunkData error = %v, want %v", err, tt.want)
			}
			c := NewChunker(tt.r())
			for {
				if _, _, err = c.Chunk(); err != nil {
					break
				}
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("Chunker.Chunk error = %v, want %v", err, tt.want)
			}
		})
	}

	errCallback := errors.New("callback failed")
	calls := 0
	err := ChunkData(bytes.NewReader(data), func(int64, []byte) error {
		calls++
		return errCallback
	})
	if !errors.Is(err, errCallback) || calls != 1 {
		t.Fatalf("ChunkData error = %v after %d callbacks, want %v after 1", err, calls, errCallback)
	}
}
