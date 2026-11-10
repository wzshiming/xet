package shard_test

import (
	"bytes"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/benchprof"
	"github.com/wzshiming/xet/shard"
)

const shardChunkSize = 64 << 10

// shardShape is the number of files a benchmarked shard describes and of
// 64 KiB chunks in each.
type shardShape struct{ files, chunksPerFile int }

func (s shardShape) String() string { return fmt.Sprintf("shape=%dx%d", s.files, s.chunksPerFile) }

var shardShapes = []shardShape{{1, 16}, {16, 256}, {64, 1024}}

// BenchmarkBuildShard builds a shard from file and chunk lists laid out as
// storage.PutFile lays them out, per shape.
func BenchmarkBuildShard(b *testing.B) {
	for _, s := range shardShapes {
		b.Run(s.String(), func(b *testing.B) {
			f := newShardFixture(b, s, false)
			b.SetBytes(f.shard.EncodedSize(false))
			b.ReportAllocs()
			stop := benchprof.Start(b)
			for b.Loop() {
				shard.BuildShard(f.files, f.chunks)
			}
			stop()
		})
	}
}

// BenchmarkShardEncode streams a built shard into io.Discard, per shape and
// footer mode.
func BenchmarkShardEncode(b *testing.B) {
	shardRows(b, func(b *testing.B, s shardShape, withFooter bool) {
		f := newShardFixture(b, s, withFooter)
		b.SetBytes(f.shard.EncodedSize(withFooter))
		b.ReportAllocs()
		stop := benchprof.Start(b)
		for b.Loop() {
			r, err := f.shard.Encode(withFooter)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				b.Fatal(err)
			}
		}
		stop()
	})
}

// BenchmarkShardDecode decodes BenchmarkShardEncode's output, per shape and
// footer mode.
func BenchmarkShardDecode(b *testing.B) {
	shardRows(b, func(b *testing.B, s shardShape, withFooter bool) {
		f := newShardFixture(b, s, withFooter)
		b.SetBytes(int64(len(f.encoded)))
		b.ReportAllocs()
		stop := benchprof.Start(b)
		for b.Loop() {
			var sh shard.Shard
			if err := sh.Decode(bytes.NewReader(f.encoded), withFooter); err != nil {
				b.Fatal(err)
			}
		}
		stop()
	})
}

// shardRows runs bench as row shape=<files>x<chunks>/footer=<with|without>
// for each shape and footer mode.
func shardRows(b *testing.B, bench func(b *testing.B, s shardShape, withFooter bool)) {
	for _, s := range shardShapes {
		for _, footer := range []string{"with", "without"} {
			b.Run(s.String()+"/footer="+footer, func(b *testing.B) {
				bench(b, s, footer == "with")
			})
		}
	}
}

// shardFixture is a shard of one shape, the BuildShard inputs it is built
// from and its encoding.
type shardFixture struct {
	files   []shard.FileInfo
	chunks  []shard.ChunkInfo
	shard   *shard.Shard
	encoded []byte
}

// newShardFixture builds a shard of shape s, stamps its footer when
// withFooter, and encodes it, failing b unless the encoding is as long as
// EncodedSize says and decodes to a valid shard of that size.
func newShardFixture(b *testing.B, s shardShape, withFooter bool) shardFixture {
	b.Helper()
	files, chunks := shardInputs(s)
	sh := shard.BuildShard(files, chunks)
	if withFooter {
		sh.SetFooter(time.Unix(0, 0))
	}
	r, err := sh.Encode(withFooter)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := io.ReadAll(r)
	if err != nil {
		b.Fatal(err)
	}
	if want := sh.EncodedSize(withFooter); int64(len(encoded)) != want {
		b.Fatalf("encoded %d bytes, want EncodedSize(%t) = %d", len(encoded), withFooter, want)
	}
	var dec shard.Shard
	if err := dec.Decode(bytes.NewReader(encoded), withFooter); err != nil {
		b.Fatal(err)
	}
	if err := dec.Validate(); err != nil {
		b.Fatalf("decoded shard: %v", err)
	}
	if got := dec.EncodedSize(withFooter); got != int64(len(encoded)) {
		b.Fatalf("decoded shard has EncodedSize(%t) = %d, want %d", withFooter, got, len(encoded))
	}
	return shardFixture{files: files, chunks: chunks, shard: sh, encoded: encoded}
}

// shardInputs lists s.files files of s.chunksPerFile random chunks as
// storage.PutFile does, each file packed into new xorbs. PutFile groups by
// xet.MaxXorbSize bytes, which equals xet.TargetChunksPerXorb (1024) chunks
// only at these 64 KiB chunks.
func shardInputs(s shardShape) ([]shard.FileInfo, []shard.ChunkInfo) {
	src := rand.NewChaCha8([32]byte{})
	files := make([]shard.FileInfo, s.files)
	var chunks []shard.ChunkInfo
	hashes := make([]xet.ChunkHash, s.chunksPerFile)
	sizes := make([]uint64, s.chunksPerFile)
	for f := range files {
		for i := range hashes {
			_, _ = src.Read(hashes[i][:])
			sizes[i] = shardChunkSize
			files[f].ChunkIndices = append(files[f].ChunkIndices, len(chunks)+i)
		}
		files[f].Hash = xet.ComputeFileHash(hashes, sizes)
		for start := 0; start < len(hashes); start += xet.TargetChunksPerXorb {
			end := min(start+xet.TargetChunksPerXorb, len(hashes))
			xorbHash := xet.ComputeXorbHash(hashes[start:end], sizes[start:end])
			for i := start; i < end; i++ {
				chunks = append(chunks, shard.ChunkInfo{
					Hash:       hashes[i],
					Size:       shardChunkSize,
					IsNew:      true,
					XorbHash:   xorbHash,
					ChunkIndex: uint32(i - start),
				})
			}
		}
	}
	return files, chunks
}
