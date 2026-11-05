package storage

import (
	"context"
	"slices"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

func TestPutShardIndexesWritesChunksThenSHA256ThenFiles(t *testing.T) {
	// A suffix of 1 keeps every hash off the sampling modulus.
	unsampled := func(id byte) xet.ChunkHash { return xet.ChunkHash{0: id, 24: 1} }
	xorbHash := xet.XorbHash{0: 1}
	sh := shard.NewShard()
	// Files start at indexes 0 and 3, index 2 is flagged: unsampled(2) alone gets no entry.
	sh.AddCASBlock(shard.CASBlock{CASHash: xorbHash, Chunks: []shard.CASChunkSequenceEntry{
		{ChunkHash: unsampled(1)},
		{ChunkHash: unsampled(2)},
		{ChunkHash: unsampled(3), Flags: shard.ChunkGlobalDedupEligible},
		{ChunkHash: unsampled(4)},
	}})
	file1, file2 := xet.FileHash{0: 1}, xet.FileHash{0: 2}
	digest1, digest2 := shard.SHA256Hash{0: 1}, shard.SHA256Hash{0: 2}
	for _, f := range []struct {
		hash   xet.FileHash
		digest shard.SHA256Hash
		start  uint32
	}{{file1, digest1, 0}, {file2, digest2, 3}} {
		sh.AddFile(shard.FileBlock{
			FileHash:    f.hash,
			Entries:     []shard.FileDataSequenceEntry{{CASHash: xorbHash, ChunkIndexStart: f.start, ChunkIndexEnd: f.start + 1}},
			MetadataExt: &shard.FileMetadataExt{SHA256Hash: f.digest},
		})
	}
	const shardHash = "shard-hash"

	var want []indexPut
	for _, e := range sh.GlobalDedupEligibleChunks() {
		want = append(want, indexPut{"index/chunks", e.Hash.String(), shardHash})
	}
	want = append(want,
		indexPut{"index/sha256", digest1.String(), shardHash},
		indexPut{"index/sha256", digest2.String(), shardHash},
		indexPut{"index/files", file1.String(), shardHash},
		indexPut{"index/files", file2.String(), shardHash},
	)

	rec := &indexRecorder{}
	if err := putShardIndexes(context.Background(), rec, sh, shardHash); err != nil {
		t.Fatalf("putShardIndexes() = %v", err)
	}
	if !slices.Equal(rec.puts, want) {
		t.Fatalf("putShardIndexes() wrote\n%v\nwant chunk entries, then sha256, then files entries last\n%v", rec.puts, want)
	}
}

type indexPut struct{ kind, name, shardHash string }

// indexRecorder records the index entry writes made through it.
type indexRecorder struct {
	Storage
	puts []indexPut
}

func (r *indexRecorder) PutFileIndexEntry(_ context.Context, fileHash xet.FileHash, shardHash string) error {
	r.puts = append(r.puts, indexPut{"index/files", fileHash.String(), shardHash})
	return nil
}

func (r *indexRecorder) PutChunkIndexEntry(_ context.Context, chunkHash xet.ChunkHash, shardHash string) error {
	r.puts = append(r.puts, indexPut{"index/chunks", chunkHash.String(), shardHash})
	return nil
}

func (r *indexRecorder) PutSHA256IndexEntry(_ context.Context, sha256Hex, shardHash string) error {
	r.puts = append(r.puts, indexPut{"index/sha256", sha256Hex, shardHash})
	return nil
}
