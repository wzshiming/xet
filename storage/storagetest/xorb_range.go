package storagetest

import (
	"bytes"
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// Every chunk range of a stored xorb is where a scan of its encoding puts it,
// and reads back as those encoded bytes through a Seek to its start and a read
// of end-start+1 bytes.
func testXorbChunkRangesReadBack(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	chunks := [][]byte{[]byte("first"), []byte("a second, longer chunk"), []byte("3"), bytes.Repeat([]byte("fourth "), 300)}
	encoded, xorbHash := EncodeXorb(t, true, chunks...)
	if _, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}

	offsets, err := st.GetXorbChunkOffsets(ctx, xorbHash)
	if err != nil {
		t.Fatalf("GetXorbChunkOffsets: %v", err)
	}
	n := uint32(len(chunks))
	if len(offsets) != int(n) {
		t.Fatalf("GetXorbChunkOffsets returned %d offsets for %d chunks", len(offsets), n)
	}
	for i := range n {
		for j := i + 1; j <= n; j++ {
			wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(encoded), i, j)
			if err != nil {
				t.Fatalf("ChunkDataRange(%d, %d): %v", i, j, err)
			}
			start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, i, j)
			if err != nil || start != wantStart || end != wantEnd {
				t.Fatalf("ChunkDataRangeFromOffsets(%d, %d) = [%d, %d], %v; want [%d, %d]", i, j, start, end, err, wantStart, wantEnd)
			}
			rc, err := st.GetXorbReadSeekCloser(ctx, xorbHash)
			if err != nil {
				t.Fatalf("GetXorbReadSeekCloser: %v", err)
			}
			got := make([]byte, end-start+1)
			if _, err = rc.Seek(start, io.SeekStart); err == nil {
				_, err = io.ReadFull(rc, got)
			}
			closeErr := rc.Close()
			if err != nil || !bytes.Equal(got, encoded[start:end+1]) {
				t.Fatalf("chunks [%d, %d): Seek(%d) and a read of %d bytes = %v; want the encoded bytes [%d, %d]", i, j, start, len(got), err, start, end)
			}
			if closeErr != nil {
				t.Fatalf("Close after reading chunks [%d, %d) = %v", i, j, closeErr)
			}
		}
	}

	if _, err := st.GetXorbReadSeekCloser(ctx, xet.XorbHash{0xde, 0xad}); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetXorbReadSeekCloser(never stored) = %v, want fs.ErrNotExist", err)
	}
}
