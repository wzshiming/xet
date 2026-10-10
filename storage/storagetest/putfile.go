package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// storeFile stores data with storage.PutFile, from a reader already past
// unrelated bytes, and checks the file commits under the hash an independent
// chunking of data predicts.
func storeFile(t *testing.T, ctx context.Context, st storage.Storage, data []byte) File {
	t.Helper()
	f := File{Content: data}
	var sizes []uint64
	if err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk []byte) error {
		f.ChunkHashes = append(f.ChunkHashes, xet.ComputeChunkHash(chunk))
		sizes = append(sizes, uint64(len(chunk)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := xet.ComputeFileHash(f.ChunkHashes, sizes)
	digest := sha256.Sum256(data)
	f.SHA256Hex = hex.EncodeToString(digest[:])

	const skipped = "bytes before the file"
	r := bytes.NewReader(append([]byte(skipped), data...))
	if _, err := r.Seek(int64(len(skipped)), io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got, err := storage.PutFile(ctx, st, "default", r)
	if err != nil || got != want {
		t.Fatalf("PutFile() = %s, %v; want %s", got.String(), err, want.String())
	}
	f.FileHash = got
	assertFilesCommitted(t, ctx, st, f)
	return f
}

// assertNewChunks checks f's shard packs exactly f's chunks missing from
// stored, once each and in file order, into xorbs that carry a footer.
func assertNewChunks(t *testing.T, ctx context.Context, st storage.Storage, f File, stored []xet.ChunkHash) {
	t.Helper()
	var want []xet.ChunkHash
	for _, h := range f.ChunkHashes {
		if !slices.Contains(stored, h) && !slices.Contains(want, h) {
			want = append(want, h)
		}
	}
	sh, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("GetShard(): %v", err)
	}
	var got []xet.ChunkHash
	for _, cb := range sh.CASInfos {
		for _, chunk := range cb.Chunks {
			got = append(got, chunk.ChunkHash)
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, "default", cb.CASHash)
		if err != nil {
			t.Fatal(err)
		}
		offsets, err := xorb.ReadChunkOffsets(rc)
		_ = rc.Close()
		if err != nil || len(offsets) != len(cb.Chunks) {
			t.Fatalf("xorb %s footer offsets = %d, %v; want %d", cb.CASHash.String(), len(offsets), err, len(cb.Chunks))
		}
	}
	// A fixture reusing no chunk could not tell reuse from packing everything.
	if len(want) == len(f.ChunkHashes) || !slices.Equal(got, want) {
		t.Fatalf("new CAS chunks = %d, want the %d of %d chunks not stored before", len(got), len(want), len(f.ChunkHashes))
	}
}

func countXorbs(t *testing.T, ctx context.Context, st storage.Storage) int {
	t.Helper()
	n := 0
	if err := st.WalkXorbs(ctx, "default", func(string, int64, time.Time) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// PutFile packs each chunk no stored shard holds once into one xorb; storing
// the file again adds no xorb, and a file sharing its leading half packs only
// the chunks it lacks.
func testPutFileReusesStoredChunks(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	random := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	// The first file repeats its leading 512 KiB, so chunks also repeat within it.
	first, fresh := slices.Concat(random[:1<<20], random[:1<<19]), random[1<<20:]

	f1 := storeFile(t, ctx, st, first)
	if n := countXorbs(t, ctx, st); n != 1 {
		t.Fatalf("xorbs after PutFile = %d, want 1", n)
	}
	assertNewChunks(t, ctx, st, f1, nil)
	storeFile(t, ctx, st, first)
	if n := countXorbs(t, ctx, st); n != 1 {
		t.Fatalf("xorbs after repeated PutFile = %d, want 1", n)
	}

	// Chunking restarts its hash at each boundary, so the second file repeats
	// the first's chunks up to the last boundary inside the shared half.
	f2 := storeFile(t, ctx, st, slices.Concat(first[:len(first)/2], fresh))
	if n := countXorbs(t, ctx, st); n != 2 {
		t.Fatalf("xorbs after PutFile of a half-shared file = %d, want 2", n)
	}
	assertNewChunks(t, ctx, st, f2, f1.ChunkHashes)
}

// An empty file has no chunks and commits under the zero file hash.
func testPutFileEmpty(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	fileHash, err := storage.PutFile(ctx, st, "default", bytes.NewReader(nil))
	if err != nil || fileHash != (xet.FileHash{}) {
		t.Fatalf("PutFile(empty) = %s, %v; want the zero hash", fileHash.String(), err)
	}
	if _, err := st.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("GetShard(zero hash): %v", err)
	}
}
