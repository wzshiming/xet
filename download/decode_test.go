package download

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/wzshiming/xet/xorb"
)

// mockClient implements ClientAdapter and returns a fixed xorb for any URL.
type mockClient struct {
	xorbObj *xorb.Xorb
}

func (m *mockClient) DownloadXorb(_ context.Context, _ string, _ http.Header) (*xorb.Xorb, error) {
	return m.xorbObj, nil
}

// makeXorb builds a simple xorb whose chunks contain the given byte slices.
func makeXorb(chunks [][]byte) *xorb.Xorb {
	x := xorb.NewXorb()
	for _, data := range chunks {
		x.Chunks = append(x.Chunks, xorb.ChunkData{
			UncompressedData: data,
			CompressedData:   data,
		})
	}
	return x
}

// TestReaderV1_NonZeroChunkStart exercises the bug that caused
// "chunk range out of bounds: [590, 593) vs 3 chunks".
// When a xorb is downloaded via a byte-range request the returned xorb
// has only the requested chunks, indexed locally from 0.  The global chunk
// indices stored in the reconstruction term (e.g. 590-593) must not be used
// to access the local Chunks slice.
func TestReaderV1_NonZeroChunkStart(t *testing.T) {
	chunk0 := []byte("hello ")
	chunk1 := []byte("world")
	chunk2 := []byte("!")
	partialXorb := makeXorb([][]byte{chunk0, chunk1, chunk2})

	client := &mockClient{xorbObj: partialXorb}

	// Simulate a reconstruction where the 3 chunks live at global indices
	// [590, 593) inside a much larger xorb.
	reconstruction := &ReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms: []Term{
			{
				Hash:           "xorb1",
				UnpackedLength: uint64(len(chunk0) + len(chunk1) + len(chunk2)),
				Range:          ChunkRange{Start: 590, End: 593},
			},
		},
		FetchInfo: map[string][]FetchInfoEntry{
			"xorb1": {
				{
					Range:    ChunkRange{Start: 590, End: 593},
					URL:      "http://example.com/xorb1",
					URLRange: ByteRange{Start: 1000, End: 2000},
				},
			},
		},
	}

	reader := NewReaderV1(context.Background(), client, reconstruction)

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	want := append(append(chunk0, chunk1...), chunk2...)
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestReaderV2_NonZeroChunkStart is the V2 equivalent of the test above.
func TestReaderV2_NonZeroChunkStart(t *testing.T) {
	chunk0 := []byte("hello ")
	chunk1 := []byte("world")
	chunk2 := []byte("!")
	partialXorb := makeXorb([][]byte{chunk0, chunk1, chunk2})

	client := &mockClient{xorbObj: partialXorb}

	reconstruction := &ReconstructionResponseV2{
		OffsetIntoFirstRange: 0,
		Terms: []Term{
			{
				Hash:           "xorb1",
				UnpackedLength: uint64(len(chunk0) + len(chunk1) + len(chunk2)),
				Range:          ChunkRange{Start: 590, End: 593},
			},
		},
		Xorbs: map[string][]XorbMultiRangeFetch{
			"xorb1": {
				{
					URL: "http://example.com/xorb1",
					Ranges: []XorbRangeDescriptor{
						{
							Chunks: ChunkRange{Start: 590, End: 593},
							Bytes:  ByteRange{Start: 1000, End: 2000},
						},
					},
				},
			},
		},
	}

	reader := NewReaderV2(context.Background(), client, reconstruction)

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	want := append(append(chunk0, chunk1...), chunk2...)
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestReaderV1_ZeroChunkStart ensures the common case (global start == 0)
// still works correctly after the fix.
func TestReaderV1_ZeroChunkStart(t *testing.T) {
	chunk0 := []byte("abc")
	chunk1 := []byte("def")
	partialXorb := makeXorb([][]byte{chunk0, chunk1})

	client := &mockClient{xorbObj: partialXorb}

	reconstruction := &ReconstructionResponse{
		OffsetIntoFirstRange: 0,
		Terms: []Term{
			{
				Hash:           "xorb1",
				UnpackedLength: uint64(len(chunk0) + len(chunk1)),
				Range:          ChunkRange{Start: 0, End: 2},
			},
		},
		FetchInfo: map[string][]FetchInfoEntry{
			"xorb1": {
				{
					Range:    ChunkRange{Start: 0, End: 2},
					URL:      "http://example.com/xorb1",
					URLRange: ByteRange{Start: 0, End: 100},
				},
			},
		},
	}

	reader := NewReaderV1(context.Background(), client, reconstruction)

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	want := append(chunk0, chunk1...)
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestReaderV1_SkipBytes verifies that OffsetIntoFirstRange is applied
// correctly when the first term starts at a non-zero global chunk index.
func TestReaderV1_SkipBytes(t *testing.T) {
	// Three-byte chunk: we will skip the first 2 bytes.
	chunk0 := []byte("abc")
	chunk1 := []byte("def")
	partialXorb := makeXorb([][]byte{chunk0, chunk1})

	client := &mockClient{xorbObj: partialXorb}

	reconstruction := &ReconstructionResponse{
		OffsetIntoFirstRange: 2, // skip first 2 bytes of chunk0
		Terms: []Term{
			{
				Hash:           "xorb1",
				UnpackedLength: uint64(len(chunk0) + len(chunk1)),
				Range:          ChunkRange{Start: 100, End: 102},
			},
		},
		FetchInfo: map[string][]FetchInfoEntry{
			"xorb1": {
				{
					Range:    ChunkRange{Start: 100, End: 102},
					URL:      "http://example.com/xorb1",
					URLRange: ByteRange{Start: 500, End: 600},
				},
			},
		},
	}

	reader := NewReaderV1(context.Background(), client, reconstruction)

	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	// First 2 bytes of "abc" are skipped; remainder is "c" + "def"
	want := []byte("cdef")
	if !bytes.Equal(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}
