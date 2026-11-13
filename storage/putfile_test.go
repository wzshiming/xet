package storage_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
)

var errStoreFailed = errors.New("store failed")

// failingXorbStore reads the first n bytes of an xorb, then fails.
type failingXorbStore struct {
	storage.Storage
	n int64
}

func (s failingXorbStore) PutXorb(_ context.Context, _ xet.XorbHash, r io.Reader) (bool, error) {
	if _, err := io.CopyN(io.Discard, r, s.n); err != nil {
		return false, err
	}
	return false, errStoreFailed
}

type unreadReader struct{ t *testing.T }

func (r unreadReader) Read([]byte) (int, error) {
	r.t.Error("xorb stream read for an xorb already stored")
	return 0, io.EOF
}

func TestPutXorbHandsTheStoreOnlyValidatedBytes(t *testing.T) {
	ctx := t.Context()
	encoded, xorbHash := storagetest.EncodeXorb(t, true, []byte("first validated chunk"), []byte("second validated chunk"))

	t.Run("bytes after the footer", func(t *testing.T) {
		st := memory.NewStorage()
		if inserted, err := storage.PutXorb(ctx, st, xorbHash, strings.NewReader(string(encoded)+"trailing bytes")); err != nil || !inserted {
			t.Fatalf("PutXorb() = %v, %v; want inserted", inserted, err)
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, xorbHash)
		if err != nil {
			t.Fatal(err)
		}
		stored, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(stored, encoded) {
			t.Fatalf("stored %d bytes, %v; want the %d validated bytes", len(stored), err, len(encoded))
		}
	})

	t.Run("xorb already stored", func(t *testing.T) {
		st := memory.NewStorage()
		if _, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		if inserted, err := storage.PutXorb(ctx, st, xorbHash, unreadReader{t}); err != nil || inserted {
			t.Fatalf("PutXorb() = %v, %v; want a no-op", inserted, err)
		}
	})

	t.Run("store failing mid-stream", func(t *testing.T) {
		_, err := storage.PutXorb(ctx, failingXorbStore{n: 10}, xorbHash, bytes.NewReader(encoded))
		if !errors.Is(err, errStoreFailed) || strings.Contains(err.Error(), "validate xorb") {
			t.Fatalf("PutXorb() error = %v, want the store's own error", err)
		}
	})
}
