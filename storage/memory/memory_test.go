package memory

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

func TestGetXorbURLUsesBaseURL(t *testing.T) {
	var xorbHash xet.XorbHash
	want := "/v1/xorbs/default/" + xorbHash.String()
	if got, err := NewStorage().GetXorbURL("default", xorbHash); err != nil || got != want {
		t.Fatalf("GetXorbURL() = %q, %v; want %q", got, err, want)
	}
	if got, err := NewStorage(WithBaseURL("http://cas.test")).GetXorbURL("default", xorbHash); err != nil || got != "http://cas.test"+want {
		t.Fatalf("GetXorbURL() = %q, %v; want %q", got, err, "http://cas.test"+want)
	}
}

func TestPutXorbOwnsDataAndServesIndependentReaders(t *testing.T) {
	ctx := context.Background()
	st := NewStorage()
	encoded, xorbHash := storagetest.EncodeXorb(t, true, []byte("owned chunk"))

	input := slices.Clone(encoded)
	if inserted, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(input)); err != nil || !inserted {
		t.Fatalf("PutXorb() = %v, %v", inserted, err)
	}
	// The caller reuses its buffer; the store must own its own copy.
	for i := range input {
		input[i] = 0
	}

	r1, err := st.GetXorbReadSeekCloser(ctx, "default", xorbHash)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := st.GetXorbReadSeekCloser(ctx, "default", xorbHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r1.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r2)
	_ = r1.Close()
	_ = r2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("second reader saw %d bytes %x, want the stored xorb %x", len(got), got, encoded)
	}
}

func TestXorbRangesMatchScanner(t *testing.T) {
	ctx := context.Background()
	st := NewStorage()
	chunks := [][]byte{[]byte("first"), []byte("second chunk"), []byte("3")}
	encoded, xorbHash := storagetest.EncodeXorb(t, true, chunks...)
	if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}

	wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(encoded), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	start, end, err := st.GetXorbDataRange(ctx, "default", xorbHash, 1, 3)
	if err != nil || start != wantStart || end != wantEnd {
		t.Fatalf("GetXorbDataRange() = [%d, %d], %v; want [%d, %d]", start, end, err, wantStart, wantEnd)
	}
	offsets, err := st.GetXorbChunkOffsets(ctx, xorbHash)
	if err != nil || len(offsets) != len(chunks) || int64(offsets[2]) != wantEnd+1 {
		t.Fatalf("GetXorbChunkOffsets() = %v, %v; want %d chunks ending at %d", offsets, err, len(chunks), wantEnd+1)
	}

	rc, err := st.ReadXorbRange(ctx, xorbHash, start, end)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(got, encoded[start:end+1]) {
		t.Fatalf("ReadXorbRange() = %x, %v; want inclusive range %x", got, err, encoded[start:end+1])
	}

	if _, _, err := st.GetXorbDataRange(ctx, "default", xorbHash, 0, 4); err == nil {
		t.Fatal("GetXorbDataRange() accepted an out-of-bounds chunk range")
	}
	var missing xet.XorbHash
	if _, _, err := st.GetXorbDataRange(ctx, "default", missing, 0, 1); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetXorbDataRange(missing) = %v, want fs.ErrNotExist", err)
	}
}

func TestWalksHonorCancellationAndCallbackErrors(t *testing.T) {
	ctx := context.Background()
	st := NewStorage()
	storagetest.PutFile(t, ctx, st, [][]byte{[]byte("walked")})

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	calls := 0
	err := st.WalkShards(canceled, func(string, int64, time.Time) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("WalkShards(canceled) = %v after %d callbacks, want context.Canceled and none", err, calls)
	}
	err = st.WalkFileIndex(canceled, func(string, string) error { calls++; return nil })
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("WalkFileIndex(canceled) = %v after %d callbacks, want context.Canceled and none", err, calls)
	}

	sentinel := errors.New("stop walking")
	if err := st.WalkXorbs(ctx, func(string, int64, time.Time) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("WalkXorbs() = %v, want the callback error", err)
	}
	if err := st.WalkSHA256Index(ctx, func(string, string) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("WalkSHA256Index() = %v, want the callback error", err)
	}
}

func TestGetShardReturnsIndependentValues(t *testing.T) {
	ctx := context.Background()
	st := NewStorage()
	before := time.Now().Add(-time.Second)
	f := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("isolated shard")})

	first, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatal(err)
	}
	// A caller corrupting its copy must not reach the store.
	first.Files[0].FileHash[0] ^= 1
	second, err := st.GetShard(ctx, f.FileHash)
	if err != nil {
		t.Fatal(err)
	}
	if second.Files[0].FileHash != f.FileHash {
		t.Fatal("GetShard() shares state between returned values")
	}
	if second.Footer == nil || int64(second.Footer.ShardCreationTimestamp) < before.Unix() {
		t.Fatalf("Footer = %+v, want the ingest time", second.Footer)
	}
	if _, err := st.GetShardByHash(ctx, strings.Repeat("0", 64)); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetShardByHash(missing) = %v, want fs.ErrNotExist", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	ctx := context.Background()
	st := NewStorage()
	encoded, xorbHash := storagetest.EncodeXorb(t, true, []byte("shared xorb"))

	const workers = 8
	shared := make([]*shard.Shard, workers) // identical content, so exactly one insert wins
	own := make([]*shard.Shard, workers)
	contents := make([][]byte, workers)
	for i := range own {
		shared[i] = shard.NewShard()
		storagetest.AddFileBlock(t, ctx, st, shared[i], [][]byte{[]byte("shared file")})
		contents[i] = fmt.Appendf(nil, "worker %d", i)
		own[i] = shard.NewShard()
		storagetest.AddFileBlock(t, ctx, st, own[i], [][]byte{contents[i]})
	}

	var xorbInserts, shardInserts atomic.Int32
	var wg sync.WaitGroup
	for i := range own {
		wg.Go(func() {
			if ok, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded)); err != nil {
				t.Error(err)
			} else if ok {
				xorbInserts.Add(1)
			}
			if ok, err := st.PutShard(ctx, shared[i]); err != nil {
				t.Error(err)
			} else if ok {
				shardInserts.Add(1)
			}
			if ok, err := st.PutShard(ctx, own[i]); err != nil || !ok {
				t.Errorf("PutShard(own %d) = %v, %v", i, ok, err)
			}
			rc, err := st.GetReconstructedFile(ctx, "default", sha256.Sum256(contents[i]))
			if err != nil {
				t.Error(err)
				return
			}
			got, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil || !bytes.Equal(got, contents[i]) {
				t.Errorf("reconstructed %q, %v; want %q", got, err, contents[i])
			}
			if _, err := storage.ListFiles(ctx, st); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()

	if xorbInserts.Load() != 1 || shardInserts.Load() != 1 {
		t.Fatalf("inserts = %d xorb, %d shard; want exactly one of each", xorbInserts.Load(), shardInserts.Load())
	}
	entries, err := storage.ListFiles(ctx, st)
	if err != nil || len(entries) != workers+1 {
		t.Fatalf("ListFiles() = %d entries, %v; want %d", len(entries), err, workers+1)
	}
}
