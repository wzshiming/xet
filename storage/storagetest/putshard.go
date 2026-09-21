package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// addXorbFile stores parts as one multi-chunk xorb and appends the file and
// CAS blocks describing it to sh.
func addXorbFile(t *testing.T, ctx context.Context, st storage.Storage, sh *shard.Shard, parts [][]byte) File {
	t.Helper()
	encoded, xorbHash := EncodeXorb(t, true, parts...)
	if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	f := File{Content: bytes.Join(parts, nil), XorbHashes: []xet.XorbHash{xorbHash}}
	cb := shard.CASBlock{CASHash: xorbHash}
	var sizes []uint64
	for _, part := range parts {
		chunkHash := xet.ComputeChunkHash(part)
		f.ChunkHashes = append(f.ChunkHashes, chunkHash)
		sizes = append(sizes, uint64(len(part)))
		cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{
			ChunkHash: chunkHash, ByteRangeStart: cb.NumBytesInCAS, UnpackedSegBytes: uint32(len(part)),
		})
		cb.NumBytesInCAS += uint32(len(part))
	}
	f.FileHash = xet.ComputeFileHash(f.ChunkHashes, sizes)
	sh.AddCASBlock(cb)
	sh.AddFile(shard.FileBlock{FileHash: f.FileHash, Entries: []shard.FileDataSequenceEntry{
		{CASHash: xorbHash, UnpackedSegBytes: uint32(len(f.Content)), ChunkIndexEnd: uint32(len(parts))},
	}})
	digest := sha256.Sum256(f.Content)
	f.SHA256Hex = hex.EncodeToString(digest[:])
	return f
}

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

// Chunk hashes are indexed straight from the CAS blocks, so every declared
// chunk sequence must match the stored xorb even when no file references it.
func testPutShardVerifiesCASChunks(t *testing.T, b Backend) {
	parts := [][]byte{[]byte("first chunk"), []byte("second chunk, different size")}
	for _, test := range []struct {
		name    string
		mutate  func(sh *shard.Shard)
		wantErr string
	}{
		{name: "full CAS blocks"},
		{name: "dedup without CAS blocks", mutate: func(sh *shard.Shard) { sh.CASInfos = nil }},
		{name: "forged chunk hash", mutate: func(sh *shard.Shard) { sh.CASInfos[0].Chunks[1].ChunkHash[0] ^= 1 }, wantErr: "chunk 1 hash"},
		{name: "forged chunk hash in unreferenced block", mutate: func(sh *shard.Shard) { sh.CASInfos[1].Chunks[0].ChunkHash[0] ^= 1 }, wantErr: "chunk 0 hash"},
		{name: "forged chunk size", mutate: func(sh *shard.Shard) { sh.CASInfos[0].Chunks[1].UnpackedSegBytes++ }, wantErr: "chunk 1 has"},
		{name: "undeclared trailing chunk", mutate: func(sh *shard.Shard) { sh.CASInfos[1].Chunks = sh.CASInfos[1].Chunks[:1] }, wantErr: "1 declared chunks"},
		{name: "declared chunk beyond xorb", mutate: func(sh *shard.Shard) {
			cb := &sh.CASInfos[1]
			cb.Chunks = append(cb.Chunks, cb.Chunks[1])
		}, wantErr: "3 declared chunks"},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			sh := shard.NewShard()
			f := addXorbFile(t, ctx, st, sh, parts)
			unreferenced := shard.NewShard()
			addXorbFile(t, ctx, st, unreferenced, [][]byte{[]byte("chunk of an unreferenced xorb"), []byte("its second chunk")})
			sh.AddCASBlock(unreferenced.CASInfos[0])
			if test.mutate != nil {
				test.mutate(sh)
			}

			inserted, err := st.PutShard(ctx, sh)
			if test.wantErr == "" {
				if err != nil || !inserted {
					t.Fatalf("PutShard() = %v, %v", inserted, err)
				}
				assertFilesCommitted(t, ctx, st, f)
				return
			}
			if inserted || !errors.Is(err, storage.ErrInvalidShard) || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("PutShard() = %v, %v, want %q", inserted, err, test.wantErr)
			}
			if got, err := st.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != "" {
				t.Fatalf("GetFileIndexEntry() after rejection = %q, %v", got, err)
			}
			if got, err := st.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != "" {
				t.Fatalf("GetSHA256IndexEntry() after rejection = %q, %v", got, err)
			}
			for _, cb := range sh.CASInfos {
				for _, chunk := range cb.Chunks {
					if got, err := st.GetChunkIndexEntry(ctx, chunk.ChunkHash); err != nil || got != "" {
						t.Fatalf("GetChunkIndexEntry(%s) after rejection = %q, %v", chunk.ChunkHash.String(), got, err)
					}
				}
			}

			honest := shard.NewShard()
			addXorbFile(t, ctx, st, honest, parts)
			if inserted, err := st.PutShard(ctx, honest); err != nil || !inserted {
				t.Fatalf("honest PutShard() after rejection = %v, %v", inserted, err)
			}
			assertFilesCommitted(t, ctx, st, f)
			for _, chunkHash := range f.ChunkHashes {
				if _, err := st.GetShardByChunkHash(ctx, "default", chunkHash); err != nil {
					t.Fatalf("GetShardByChunkHash(%s) after honest retry: %v", chunkHash.String(), err)
				}
			}
		})
	}
}
