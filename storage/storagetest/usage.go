package storagetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wzshiming/xet/storage"
)

func testUsageCountsObjectsByKind(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)

	got, err := st.Usage(ctx)
	if err != nil || got != (storage.Usage{}) {
		t.Fatalf("Usage(empty) = %+v, %v; want zero usage", got, err)
	}

	partA := []byte("part shared by both files")
	partB := []byte("part stored only by the first file, a little longer")
	partC := []byte("second file's own part")
	first := PutFile(t, ctx, st, [][]byte{partA, partB})
	second := PutFile(t, ctx, st, [][]byte{partA, partC})

	xorbBytes := func(parts ...[]byte) int64 {
		var total int64
		for _, part := range parts {
			encoded, _ := EncodeXorb(t, true, part)
			total += int64(len(encoded))
		}
		return total
	}
	shardBytes := map[string]int64{}
	if err := st.WalkShards(ctx, func(shardHash string, size int64, _ time.Time) error {
		shardBytes[shardHash] = size
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(shardBytes) != 2 {
		t.Fatalf("stored shards = %v, want the two files' shards", shardBytes)
	}

	want := storage.Usage{
		Xorbs:       storage.ObjectUsage{Count: 3, Bytes: xorbBytes(partA, partB, partC)},
		Shards:      storage.ObjectUsage{Count: 2, Bytes: shardBytes[first.ShardHash] + shardBytes[second.ShardHash]},
		FileIndex:   storage.ObjectUsage{Count: 2, Bytes: 128},
		ChunkIndex:  storage.ObjectUsage{Count: 3, Bytes: 192},
		SHA256Index: storage.ObjectUsage{Count: 2, Bytes: 128},
	}
	if got, err = st.Usage(ctx); err != nil || got != want {
		t.Fatalf("Usage(two files) = %+v, %v; want %+v", got, err, want)
	}

	UnlinkFile(t, ctx, st, second)
	res, err := storage.Sweep(ctx, st, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	// The dead shard owns chunk A's entry only where PutShard overwrites (S3).
	chunks := 3 - int64(res.DeletedChunkEntries)
	want = storage.Usage{
		Xorbs:       storage.ObjectUsage{Count: 2, Bytes: xorbBytes(partA, partB)},
		Shards:      storage.ObjectUsage{Count: 1, Bytes: shardBytes[first.ShardHash]},
		FileIndex:   storage.ObjectUsage{Count: 1, Bytes: 64},
		ChunkIndex:  storage.ObjectUsage{Count: chunks, Bytes: 64 * chunks},
		SHA256Index: storage.ObjectUsage{Count: 1, Bytes: 64},
	}
	if got, err = st.Usage(ctx); err != nil || got != want {
		t.Fatalf("Usage(after sweep) = %+v, %v; want %+v", got, err, want)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Usage(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Usage(canceled) = %v, want context.Canceled", err)
	}
	if _, err := b.New(t).Usage(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Usage(canceled, empty store) = %v, want context.Canceled", err)
	}
}
