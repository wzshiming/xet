package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// TestShardNameIsDeterministicContentHash proves the stored object name is the
// SHA-256 of the exact stored bytes and does not vary with the creation time
// embedded in the (unstored) footer.
func TestShardNameIsDeterministicContentHash(t *testing.T) {
	newIdenticalShard := func(creationTime uint64) *shard.Shard {
		s := shard.NewShard()
		s.AddFile(shard.FileBlock{FileHash: xet.FileHash{1}})
		s.AddCASBlock(shard.CASBlock{
			CASHash: xet.XorbHash{2},
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: xet.ChunkHash{3}}},
		})
		s.SetFooter(time.Unix(int64(creationTime), 0))
		return s
	}

	var names []string
	for _, creationTime := range []uint64{1, 1 << 30} {
		basePath := t.TempDir()
		st, err := NewFileStorage(WithBasePath(basePath))
		if err != nil {
			t.Fatal(err)
		}
		if inserted, err := st.PutShard(context.Background(), newIdenticalShard(creationTime)); err != nil || !inserted {
			t.Fatalf("PutShard() = %v, %v", inserted, err)
		}
		entries, err := fanoutEntries(filepath.Join(basePath, "shards"))
		if err != nil {
			t.Fatalf("read shards directory: %v", err)
		}
		if len(entries) != 1 {
			t.Fatalf("shards directory contains %d entries, want 1", len(entries))
		}
		name := entries[0]
		data, err := os.ReadFile(st.objectPath("shards", name))
		if err != nil {
			t.Fatalf("read stored shard: %v", err)
		}
		if footerSize := binary.LittleEndian.Uint64(data[40:48]); footerSize != 0 {
			t.Fatalf("stored FooterSize = %d, want 0", footerSize)
		}
		sum := sha256.Sum256(data)
		if want := hex.EncodeToString(sum[:]); name != want {
			t.Fatalf("shard name %q != sha256 of stored bytes %q", name, want)
		}
		names = append(names, name)
	}
	if names[0] != names[1] {
		t.Fatalf("identical content produced different names: %q vs %q", names[0], names[1])
	}
}

// legacyShardBytes serializes a shard the way it was stored before shard
// objects went footerless, and returns the bytes with their object name.
func legacyShardBytes(t *testing.T, s *shard.Shard, creationTime uint64) ([]byte, string) {
	t.Helper()
	s.SetFooter(time.Unix(int64(creationTime), 0))
	r, err := s.Encode(true)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	return data, hex.EncodeToString(sum[:])
}

// TestFooteredShardObjectStaysReadable covers objects written before shards
// went footerless: they must keep resolving, with their stored footer intact.
func TestFooteredShardObjectStaysReadable(t *testing.T) {
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	fileHash := xet.FileHash{7}
	s := shard.NewShard()
	s.AddFile(shard.FileBlock{FileHash: fileHash})
	s.AddCASBlock(shard.CASBlock{
		CASHash: xet.XorbHash{8},
		Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: xet.ChunkHash{9}}},
	})
	const creationTime = 1700000000
	data, name := legacyShardBytes(t, s, creationTime)

	shardPath := fs.objectPath("shards", name)
	if err := os.MkdirAll(filepath.Dir(shardPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shardPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeIndexFile(fs.objectPath("index/files", fileHash.String()), []byte(name)); err != nil {
		t.Fatal(err)
	}

	loaded, err := fs.GetShard(context.Background(), fileHash)
	if err != nil {
		t.Fatalf("GetShard on a footered object: %v", err)
	}
	if loaded.Files[0].FileHash != fileHash {
		t.Fatal("loaded the wrong shard")
	}
	if loaded.Footer == nil || loaded.Footer.ShardCreationTimestamp != creationTime {
		t.Fatalf("stored footer was not preserved: %+v", loaded.Footer)
	}
}

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
	s.SetFooter(time.Now())

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
	// Put warms no cache: the first read is what caches the shard, with the
	// footer rebuilt from the stored object.
	if _, ok := fs.shardIndex.Get(shardHash); ok {
		t.Fatal("shard was added to the shard cache on put")
	}
	if _, ok := fs.fileIndex.Get(secondFileHash); ok {
		t.Fatal("file mapping was added to the file cache on put")
	}

	for _, hash := range []xet.FileHash{fileHash, secondFileHash} {
		indexData, err := os.ReadFile(fs.objectPath("index/files", hash.String()))
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

	indexData, err := os.ReadFile(fs.objectPath("index/chunks", chunkHash.String()))
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
	info, err := os.Stat(fs.objectPath("shards", shardHash))
	if err != nil {
		t.Fatalf("stat stored shard: %v", err)
	}
	if got.Footer == nil || got.Footer.ShardCreationTimestamp != uint64(info.ModTime().Unix()) {
		t.Fatalf("reloaded footer creation time not pinned to mod time: %+v", got.Footer)
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
// putFileTestShard uploads one single-chunk xorb per part and returns a shard
// describing the concatenation of parts as one file.
func putFileTestShard(t *testing.T, ctx context.Context, fs *FileStorage, parts [][]byte) (*shard.Shard, xet.FileHash) {
	t.Helper()
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	for _, part := range parts {
		encoded, xorbHash := encodeTestXorb(t, true, part)
		if _, err := fs.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileHash := xet.ComputeFileHash(chunkHashes, chunkSizes)
	fileBlock.FileHash = fileHash
	shardObj.AddFile(fileBlock)
	return shardObj, fileHash
}

// TestPutShardRetryAfterPartialFailure kills PutShard midway through its index
// writes and verifies a retry fully repairs the shard instead of reporting
// "already exists" with indexes missing.
func TestPutShardRetryAfterPartialFailure(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	fs, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	content := []byte("retry me")
	shardObj, fileHash := putFileTestShard(t, ctx, fs, [][]byte{content})

	// First attempt dies while writing SHA-256 indexes: a regular file where
	// the index/sha256 directory belongs makes those writes fail.
	blocker := filepath.Join(basePath, "index", "sha256")
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, nil, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutShard(ctx, shardObj); err == nil {
		t.Fatal("PutShard() succeeded despite blocked sha256 index")
	}

	// The partial shard must not count as existing.
	if exists, err := fs.hasFile(fileHash); err != nil || exists {
		t.Fatalf("hasFile() after partial failure = %v, %v", exists, err)
	}

	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.PutShard(ctx, shardObj); err != nil {
		t.Fatalf("PutShard() retry: %v", err)
	}

	// A fresh storage must resolve every index written by the retry.
	fresh, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	gotFileHash, err := fresh.GetFileHashBySHA256(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetFileHashBySHA256 after retry: %v", err)
	}
	if gotFileHash != fileHash {
		t.Fatal("SHA-256 index resolved wrong file hash")
	}
	if _, err := fresh.GetShard(ctx, fileHash); err != nil {
		t.Fatalf("GetShard after retry: %v", err)
	}
	if _, err := fresh.GetShardByChunkHash(ctx, "default", xet.ComputeChunkHash(content)); err != nil {
		t.Fatalf("GetShardByChunkHash after retry: %v", err)
	}
}

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

			for start := range numChunks {
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

// TestOverwriteIndexFileConcurrentSameKey races writers over the same keys:
// each writer renames its own unique temp file, so no rename can steal
// another writer's temp (the old fixed "<path>.tmp" scheme failed with
// ENOENT) and every key ends up holding one of the written values.
func TestOverwriteIndexFileConcurrentSameKey(t *testing.T) {
	dir := t.TempDir()
	keys := []string{
		filepath.Join(dir, "aa", "key-one"),
		filepath.Join(dir, "ab", "key-two"),
	}
	const writers = 20
	const rounds = 50

	valid := map[string]bool{}
	for i := range writers {
		valid[fmt.Sprintf("shard-%02d", i)] = true
	}

	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value := []byte(fmt.Sprintf("shard-%02d", i))
			for r := range rounds {
				if err := overwriteIndexFile(keys[(i+r)%len(keys)], value); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("overwriteIndexFile: %v", err)
	}

	for _, key := range keys {
		data, err := os.ReadFile(key)
		if err != nil {
			t.Fatal(err)
		}
		if !valid[string(data)] {
			t.Fatalf("key %s holds %q, not one of the written values", key, data)
		}
	}
}
