package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// addFile appends parts to sh as one file and returns its File.
func addFile(t *testing.T, ctx context.Context, st storage.Storage, sh *shard.Shard, parts [][]byte) File {
	t.Helper()
	f := File{Content: bytes.Join(parts, nil)}
	f.FileHash, f.XorbHashes, f.ChunkHashes = AddFileBlock(t, ctx, st, sh, parts)
	digest := sha256.Sum256(f.Content)
	f.SHA256Hex = hex.EncodeToString(digest[:])
	return f
}

// assertFilesCommitted checks each file resolves by file hash and SHA-256 and reconstructs.
func assertFilesCommitted(t *testing.T, ctx context.Context, st storage.Storage, files ...File) {
	t.Helper()
	for _, f := range files {
		AssertFileIntact(t, ctx, st, f)
		if got, err := st.GetFileHashBySHA256(ctx, "default", SHA256Digest(f.SHA256Hex)); err != nil || got != f.FileHash {
			t.Fatalf("GetFileHashBySHA256(%s) = %s, %v; want %s", f.SHA256Hex, got.String(), err, f.FileHash.String())
		}
	}
}

// One upload commits all its files in one shard; a file stored earlier must not hide the new ones.
func testPutShardCommitsNewFilesBesideKnownFile(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	partsA := [][]byte{[]byte("file A, uploaded earlier")}
	a := PutFile(t, ctx, st, partsA)

	sh := shard.NewShard()
	addFile(t, ctx, st, sh, partsA)
	fileB := addFile(t, ctx, st, sh, [][]byte{[]byte("file B chunk one"), []byte("file B chunk two")})
	if inserted, err := st.PutShard(ctx, sh); err != nil || !inserted {
		t.Fatalf("PutShard([A,B]) = %v, %v", inserted, err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)

	if inserted, err := st.PutShard(ctx, sh); err != nil || inserted {
		t.Fatalf("repeated PutShard([A,B]) = %v, %v; want no-op", inserted, err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)
}

// File entries commit last and in order, so a commit dying between A's and B's leaves B unindexed.
func testPutShardRetryRepairsMissingFileEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	a := addFile(t, ctx, st, sh, [][]byte{[]byte("file A of an interrupted upload")})
	fileB := addFile(t, ctx, st, sh, [][]byte{[]byte("file B of an interrupted upload")})
	if inserted, err := st.PutShard(ctx, sh); err != nil || !inserted {
		t.Fatalf("PutShard([A,B]) = %v, %v", inserted, err)
	}
	if _, err := storage.NewGC(st).Unlink(ctx, fileB.FileHash); err != nil {
		t.Fatal(err)
	}
	if got, err := st.GetFileIndexEntry(ctx, fileB.FileHash); err != nil || got != "" {
		t.Fatalf("GetFileIndexEntry(B) after unlink = %q, %v", got, err)
	}

	if _, err := st.PutShard(ctx, sh); err != nil {
		t.Fatalf("PutShard([A,B]) retry: %v", err)
	}
	assertFilesCommitted(t, ctx, st, a, fileB)
}

func testPutShardVerifiesFileHash(t *testing.T, b Backend) {
	for _, test := range []struct {
		name    string
		wantErr string
	}{
		{name: "correct"},
		{name: "wrong file hash", wantErr: "file hash mismatch"},
		{name: "wrong SHA-256", wantErr: "SHA-256 mismatch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			parts := [][]byte{[]byte("first chunk"), []byte("second chunk, different size")}
			sh := shard.NewShard()
			AddFileBlock(t, ctx, st, sh, parts)
			digest := sha256.Sum256(bytes.Join(parts, nil))
			sh.Files[0].MetadataExt = &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(digest)}
			sh.Files[0].Flags |= shard.FileWithMetadataExt
			switch test.name {
			case "wrong file hash":
				sh.Files[0].FileHash[0] ^= 1
			case "wrong SHA-256":
				digest[0] ^= 1
				sh.Files[0].MetadataExt.SHA256Hash = shard.NewSHA256Hash(digest)
			}
			inserted, err := st.PutShard(ctx, sh)
			if test.wantErr != "" {
				if inserted || !errors.Is(err, storage.ErrInvalidShard) || !strings.Contains(err.Error(), test.wantErr) {
					t.Errorf("PutShard() = %v, %v, want %q", inserted, err, test.wantErr)
				}
				if _, err := st.GetShard(ctx, sh.Files[0].FileHash); err == nil {
					t.Error("GetShard() succeeded for rejected shard")
				}
				return
			}
			if err != nil || !inserted {
				t.Fatalf("PutShard() = %v, %v", inserted, err)
			}
			if _, err := st.GetShard(ctx, sh.Files[0].FileHash); err != nil {
				t.Fatalf("GetShard(): %v", err)
			}
		})
	}
}
