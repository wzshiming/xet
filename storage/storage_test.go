package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

func TestChunkIndexPersistsAndShardReloadsAfterEviction(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath), WithShardCacheSize(1))
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
	indexData, err := os.ReadFile(filepath.Join(basePath, "chunks", chunkHash.String()))
	if err != nil {
		t.Fatalf("read chunk index: %v", err)
	}
	if got := string(indexData); got != fileHash.String() {
		t.Fatalf("chunk index = %q, want %q", got, fileHash.String())
	}

	// Re-open storage to prove lookup does not depend on an in-memory index.
	fs, err = NewFileStorage(WithBasePath(basePath), WithShardCacheSize(1))
	if err != nil {
		t.Fatal(err)
	}
	got, err := fs.GetShardByChunkHash(context.Background(), "ignored", chunkHash)
	if err != nil {
		t.Fatalf("GetShardByChunkHash: %v", err)
	}
	if got.Files[0].FileHash != fileHash {
		t.Fatalf("loaded wrong shard: %s", got.Files[0].FileHash.String())
	}
	if cached, ok := fs.chunkIndex.Get(chunkHash); !ok || cached.(xet.FileHash) != fileHash {
		t.Fatal("chunk index was not added to the LRU cache")
	}

	// The second file is not the shard filename and must remain discoverable
	// after its LRU entry has been evicted.
	if _, err := fs.GetShard(context.Background(), secondFileHash); err != nil {
		t.Fatalf("GetShard(second file): %v", err)
	}
}
