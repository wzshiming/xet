package storage

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

func TestChunkIndexPersistsAndShardReloadsAfterEviction(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath), WithFileCacheSize(1), WithShardCacheSize(1))
	if err != nil {
		t.Fatal(err)
	}

	fileHash := xet.FileHash{1, 2, 3}
	secondFileHash := xet.FileHash{4, 5, 6}
	chunkHash := xet.ChunkHash{7, 8, 9}
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{FileHash: fileHash})
	s.AddFile(shard.FileBlock{FileHash: secondFileHash})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xet.XorbHash{10},
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash}},
	})
	s.SetFooter()

	inserted, err := fs.PutShard(context.Background(), s)
	if err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
	shardEntries, err := os.ReadDir(filepath.Join(basePath, "shards"))
	if err != nil {
		t.Fatalf("read shards directory: %v", err)
	}
	if len(shardEntries) != 1 {
		t.Fatalf("shards directory contains %d entries, want 1", len(shardEntries))
	}
	shardHash := shardEntries[0].Name()
	if cached, ok := fs.shardIndex.Get(shardHash); !ok || cached.(*shard.Shard) != s {
		t.Fatal("shard was not added to the shard cache")
	}
	if cached, ok := fs.fileIndex.Get(secondFileHash); !ok || cached.(string) != shardHash {
		t.Fatal("file mapping was not added to the file cache")
	}
	if _, ok := fs.shardIndex.Get(secondFileHash); ok {
		t.Fatal("file hash was added to the shard cache")
	}

	for _, hash := range []xet.FileHash{fileHash, secondFileHash} {
		indexData, err := os.ReadFile(filepath.Join(basePath, "files", hash.String()))
		if err != nil {
			t.Fatalf("read file index %s: %v", hash, err)
		}
		if got := string(indexData); got != shardHash {
			t.Fatalf("file index %s = %q, want shard hash %q", hash, got, shardHash)
		}
	}
	if _, err := os.Stat(filepath.Join(basePath, "shard-index")); !os.IsNotExist(err) {
		t.Fatalf("legacy shard-index directory exists: %v", err)
	}

	indexData, err := os.ReadFile(filepath.Join(basePath, "chunks", chunkHash.String()))
	if err != nil {
		t.Fatalf("read chunk index: %v", err)
	}
	if got := string(indexData); got != shardHash {
		t.Fatalf("chunk index = %q, want shard hash %q", got, shardHash)
	}

	// Re-open storage to prove lookup does not depend on an in-memory index.
	fs, err = NewFileStorage(WithBasePath(basePath), WithFileCacheSize(1), WithShardCacheSize(1))
	if err != nil {
		t.Fatal(err)
	}
	// Resolve a non-primary file before any lookup has loaded the shard into
	// memory. Multi-file shards must be addressable after a process restart.
	if _, err := fs.GetShard(context.Background(), secondFileHash); err != nil {
		t.Fatalf("GetShard(second file) after reopen: %v", err)
	}
	got, err := fs.GetShardByChunkHash(context.Background(), "ignored", chunkHash)
	if err != nil {
		t.Fatalf("GetShardByChunkHash: %v", err)
	}
	if got.Files[0].FileHash != fileHash {
		t.Fatalf("loaded wrong shard: %s", got.Files[0].FileHash.String())
	}
	if cached, ok := fs.chunkIndex.Get(chunkHash); !ok || cached.(string) != shardHash {
		t.Fatal("chunk index was not added to the LRU cache")
	}

	// Evict the second file mapping. It must remain discoverable through the
	// persisted files index.
	fs.fileIndex.Add(fileHash, shardHash)
	if _, err := fs.GetShard(context.Background(), secondFileHash); err != nil {
		t.Fatalf("GetShard(second file): %v", err)
	}
}

func TestPutShardNormalizesXETCoreEmptyFileSHA256(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	fileHash := xet.ComputeFileHash(nil, nil)
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{
		FileHash:     fileHash,
		Flags:        shard.FileWithVerification | shard.FileWithMetadataExt,
		Entries:      []shard.FileDataSequenceEntry{},
		Verification: []xet.VerificationHash{},
		MetadataExt:  &shard.FileMetadataExt{},
	})

	inserted, err := fs.PutShard(context.Background(), s)
	if err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}

	wantSHA256 := sha256.Sum256(nil)
	if got := s.Files[0].MetadataExt.SHA256Hash; got != shard.NewSHA256Hash(wantSHA256) {
		t.Fatalf("normalized SHA-256 = %s, want %x", got.String(), wantSHA256)
	}
	if _, err := os.Stat(filepath.Join(basePath, "sha256", shard.NewSHA256Hash(wantSHA256).String())); err != nil {
		t.Fatalf("standard empty-file SHA-256 index is missing: %v", err)
	}

	r, err := fs.GetReconstructedFile(context.Background(), "default", wantSHA256)
	if err != nil {
		t.Fatalf("GetReconstructedFile: %v", err)
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read reconstructed empty file: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("reconstructed empty file contains %d bytes", len(data))
	}
}
