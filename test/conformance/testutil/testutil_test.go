package testutil_test

import (
	"io"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/test/conformance/testutil"
)

func TestDecodeDedupShard(t *testing.T) {
	chunk, xorbHash := xet.ChunkHash{0: 2}, xet.XorbHash{0: 3}
	sh := shard.NewShard()
	sh.AddCASBlock(shard.CASBlock{CASHash: xorbHash, Chunks: []shard.CASChunkSequenceEntry{
		{ChunkHash: xet.ChunkHash{0: 1}},
		{ChunkHash: chunk},
	}})
	sh.AddFile(shard.FileBlock{
		FileHash: xet.FileHash{0: 4},
		Entries:  []shard.FileDataSequenceEntry{{CASHash: xorbHash, ChunkIndexEnd: 2}},
	})

	for _, test := range []struct {
		name       string
		withFooter bool
	}{
		{"footerless", false},
		{"with footer", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, err := sh.Encode(test.withFooter)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			got, err := testutil.DecodeDedupShard(body)
			if err != nil {
				t.Fatalf("DecodeDedupShard() = %v", err)
			}
			want := shard.ChunkLocation{XorbHash: xorbHash, ChunkIndex: 1}
			if loc, ok := got.LookupChunk(chunk); !ok || loc != want {
				t.Errorf("LookupChunk() = %+v, %t; want %+v, true", loc, ok, want)
			}
		})
	}

	t.Run("body shorter than header", func(t *testing.T) {
		if _, err := testutil.DecodeDedupShard(make([]byte, 10)); err == nil {
			t.Error("DecodeDedupShard(10 bytes) succeeded; want an error")
		}
	})
}
