package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
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
	shardHashes, err := fanoutEntries(filepath.Join(basePath, "shards"))
	if err != nil {
		t.Fatalf("read shards directory: %v", err)
	}
	if len(shardHashes) != 1 {
		t.Fatalf("shards directory contains %d entries, want 1", len(shardHashes))
	}
	shardHash := shardHashes[0]
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
		indexData, err := os.ReadFile(fs.objectPath("files", hash.String()))
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

	indexData, err := os.ReadFile(fs.objectPath("chunks", chunkHash.String()))
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

// fanoutEntries returns the hash names reassembled from a fanout directory.
func fanoutEntries(dir string) ([]string, error) {
	var names []string
	buckets, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			return nil, fmt.Errorf("unexpected flat entry %q", bucket.Name())
		}
		sub, err := os.ReadDir(filepath.Join(dir, bucket.Name()))
		if err != nil {
			return nil, err
		}
		for _, e := range sub {
			names = append(names, bucket.Name()+e.Name())
		}
	}
	return names, nil
}

// encodeTestXorb serializes chunks as an xorb, with or without footer.
func encodeTestXorb(t *testing.T, withFooter bool, chunks ...[]byte) ([]byte, xet.XorbHash) {
	t.Helper()
	var encoded bytes.Buffer
	enc := xorb.NewEncoder(&encoded, withFooter)
	for _, c := range chunks {
		if _, err := enc.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes(), enc.SummoryHash()
}

func TestGetXorbDataRangeFromOffsets(t *testing.T) {
	chunks := [][]byte{
		bytes.Repeat([]byte{1}, 1000),
		[]byte("second chunk"),
		bytes.Repeat([]byte("abc"), 700),
	}
	numChunks := uint32(len(chunks))

	// Reference ranges scanned from the chunk-data region, which is identical
	// for footer and chunk-only formats.
	reference, _ := encodeTestXorb(t, false, chunks...)

	for _, format := range []struct {
		name       string
		withFooter bool
	}{
		{"footer", true},
		{"chunk-only", false},
	} {
		t.Run(format.name, func(t *testing.T) {
			fs, err := NewFileStorage(WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			encoded, xorbHash := encodeTestXorb(t, format.withFooter, chunks...)
			if _, err := fs.PutXorb(context.Background(), "default", xorbHash, bytes.NewReader(encoded)); err != nil {
				t.Fatal(err)
			}

			for start := uint32(0); start < numChunks; start++ {
				for end := start + 1; end <= numChunks; end++ {
					wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(reference), start, end)
					if err != nil {
						t.Fatalf("ChunkDataRange(%d, %d): %v", start, end, err)
					}
					gotStart, gotEnd, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, start, end)
					if err != nil {
						t.Fatalf("GetXorbDataRange(%d, %d): %v", start, end, err)
					}
					if gotStart != wantStart || gotEnd != wantEnd {
						t.Fatalf("range [%d, %d) = [%d, %d], want [%d, %d]", start, end, gotStart, gotEnd, wantStart, wantEnd)
					}
				}
			}

			if _, _, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, 0, numChunks+1); err == nil {
				t.Fatal("GetXorbDataRange() accepted out-of-bounds chunk range")
			}

			// Cached offsets keep serving ranges without touching the xorb file.
			if err := os.Remove(fs.objectPath("xorbs", xorbHash.String())); err != nil {
				t.Fatal(err)
			}
			if _, _, err := fs.GetXorbDataRange(context.Background(), "default", xorbHash, 1, 2); err != nil {
				t.Fatalf("GetXorbDataRange() after xorb removal: %v", err)
			}
		})
	}
}
