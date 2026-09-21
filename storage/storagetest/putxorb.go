package storagetest

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
)

// An xorb whose claimed hash is not the Merkle root of its chunks must never be
// published, in either encoding, and the rejection must not block the honest upload.
func testPutXorbRejectsWrongIdentity(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	chunks := [][]byte{[]byte("identity chunk one"), []byte("identity chunk two")}
	bogus := xet.XorbHash{42}

	for _, withFooter := range []bool{false, true} {
		encoded, _ := EncodeXorb(t, withFooter, chunks...)
		if _, err := st.PutXorb(ctx, "default", bogus, bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "xorb hash mismatch") {
			t.Fatalf("PutXorb(bogus hash, footer=%v) error = %v, want xorb hash mismatch", withFooter, err)
		}
		if ok, err := st.HasXorb(ctx, "default", bogus); err != nil || ok {
			t.Fatalf("HasXorb(bogus, footer=%v) = %v, %v; want absent", withFooter, ok, err)
		}
	}

	encoded, xorbHash := EncodeXorb(t, false, chunks...)
	if inserted, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil || !inserted {
		t.Fatalf("PutXorb(computed hash) = %v, %v; want inserted", inserted, err)
	}
	if ok, err := st.HasXorb(ctx, "default", xorbHash); err != nil || !ok {
		t.Fatalf("HasXorb(computed hash) = %v, %v; want present", ok, err)
	}
}

// Raw chunks whose headers overstate their payload must not be published under
// the root those declared sizes produce, and must not block the honest upload.
func testPutXorbRejectsForgedChunkSizes(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	payloads := [][]byte{{'a'}, {'b'}}
	var encoded []byte
	var hashes []xet.ChunkHash
	var sizes []uint64
	for _, p := range payloads {
		// Header: version, compressed size (3), compression none, declared size 2 (3).
		encoded = append(encoded, 0, byte(len(p)), 0, 0, 0, 2, 0, 0)
		encoded = append(encoded, p...)
		hashes = append(hashes, xet.ComputeChunkHash(p))
		sizes = append(sizes, 2)
	}
	forged := xet.ComputeXorbHash(hashes, sizes)

	if _, err := st.PutXorb(ctx, "default", forged, bytes.NewReader(encoded)); err == nil || !strings.Contains(err.Error(), "chunk size mismatch") {
		t.Fatalf("PutXorb(forged sizes) error = %v, want chunk size mismatch", err)
	}
	if ok, err := st.HasXorb(ctx, "default", forged); err != nil || ok {
		t.Fatalf("HasXorb(forged) = %v, %v; want absent", ok, err)
	}

	honest, xorbHash := EncodeXorb(t, false, payloads...)
	if inserted, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(honest)); err != nil || !inserted {
		t.Fatalf("PutXorb(honest) = %v, %v; want inserted", inserted, err)
	}
}
