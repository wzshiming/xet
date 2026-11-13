package upload

import (
	"bytes"
	"context"
	"io"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/xorb"
)

// storageClient is the storage-backed CAS the mirror used to ingest with.
type storageClient struct{ st storage.Storage }

func (c storageClient) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return c.st.HasXorb(ctx, xorbHash)
}

func (c storageClient) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*XorbUploadResponse, error) {
	inserted, err := storage.PutXorb(ctx, c.st, xorbHash, r)
	if err != nil {
		return nil, err
	}
	return &XorbUploadResponse{WasInserted: inserted}, nil
}

func (c storageClient) UploadShard(ctx context.Context, sh *shard.Shard) (*ShardUploadResponse, error) {
	inserted, err := storage.PutShard(ctx, c.st, sh)
	if err != nil {
		return nil, err
	}
	resp := &ShardUploadResponse{}
	if inserted {
		resp.Result = 1
	}
	return resp, nil
}

func (c storageClient) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	located := map[xet.ChunkHash]shard.ChunkLocation{}
	for _, h := range chunkHashes {
		if _, ok := located[h]; ok {
			continue
		}
		sh, err := c.st.GetShardByChunkHash(ctx, h)
		if err != nil || sh == nil {
			continue
		}
		for h, loc := range sh.ChunkLocations() {
			if _, ok := located[h]; !ok {
				located[h] = loc
			}
		}
	}
	return located, nil
}

type storeState struct {
	xorbs     map[string][]byte
	shards    map[string]bool
	fileIndex map[string]string
}

func snapshot(t *testing.T, ctx context.Context, st storage.Storage) storeState {
	t.Helper()
	s := storeState{xorbs: map[string][]byte{}, shards: map[string]bool{}, fileIndex: map[string]string{}}
	err := st.WalkXorbs(ctx, func(hash string, _ int64, _ time.Time) error {
		xorbHash, err := xet.ParseXorbHash(hash)
		if err != nil {
			return err
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, xorbHash)
		if err != nil {
			return err
		}
		defer rc.Close()
		s.xorbs[hash], err = io.ReadAll(rc)
		return err
	})
	if err == nil {
		err = st.WalkShards(ctx, func(hash string, _ int64, _ time.Time) error {
			s.shards[hash] = true
			return nil
		})
	}
	if err == nil {
		err = st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
			s.fileIndex[fileHash] = shardHash
			return nil
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestUploadFileMatchesStoragePutFile(t *testing.T) {
	ctx := t.Context()
	native, uploaded := memory.NewStorage(), memory.NewStorage()
	random := make([]byte, 3<<19)
	_, _ = rand.NewChaCha8([32]byte{}).Read(random)
	first := slices.Concat(random[:1<<20], random[:1<<19])
	second := slices.Concat(first[:len(first)/2], random[1<<20:])

	for _, file := range []struct {
		name  string
		data  []byte
		xorbs int
	}{
		{"first", first, 1},
		// Only the chunks second does not share with first need a new xorb.
		{"second", second, 2},
	} {
		want, err := storage.PutFile(ctx, native, bytes.NewReader(file.data))
		if err != nil {
			t.Fatalf("%s: PutFile: %v", file.name, err)
		}
		got, err := UploadFile(ctx, storageClient{uploaded}, bytes.NewReader(file.data), WithEnableSHA256(true), WithCacheManager(NewCacheManager(t.TempDir(), 0)))
		if err != nil || got != want {
			t.Fatalf("%s: UploadFile() = %s, %v; PutFile() = %s", file.name, got.String(), err, want.String())
		}
		nativeState, uploadedState := snapshot(t, ctx, native), snapshot(t, ctx, uploaded)
		if !reflect.DeepEqual(uploadedState, nativeState) {
			t.Fatalf("%s: stores differ in xorb bytes or in UploadFile's xorbs %v, shards %v, file index %v vs PutFile's %v, %v, %v", file.name,
				slices.Sorted(maps.Keys(uploadedState.xorbs)), uploadedState.shards, uploadedState.fileIndex,
				slices.Sorted(maps.Keys(nativeState.xorbs)), nativeState.shards, nativeState.fileIndex)
		}
		if n := len(nativeState.xorbs); n != file.xorbs {
			t.Fatalf("%s: stores hold %d xorbs, want %d", file.name, n, file.xorbs)
		}
	}

	// Unlike UploadFile's zero-entry shard, PutFile stores nothing for an empty file.
	before := snapshot(t, ctx, native)
	if fileHash, err := storage.PutFile(ctx, native, bytes.NewReader(nil)); err != nil || fileHash != (xet.FileHash{}) {
		t.Fatalf("empty: PutFile() = %s, %v; want the zero hash", fileHash.String(), err)
	}
	if after := snapshot(t, ctx, native); !reflect.DeepEqual(after, before) {
		t.Fatalf("empty: PutFile changed the store to shards %v, file index %v from %v, %v", after.shards, after.fileIndex, before.shards, before.fileIndex)
	}
}

// Files sharing an xorb get a compression scheme each, which their first chunk predicts for a block of up to 32 chunks:
// the chunks of bf16-like weights that follow a small text file are stored as BG4, while those that follow text inside
// one file share its LZ4 block, which leaves them uncompressed.
func TestUploadFilesPicksCompressionPerFileInXorb(t *testing.T) {
	ctx := t.Context()
	st := memory.NewStorage()
	r := rand.New(rand.NewChaCha8([32]byte{'a', '3'}))
	words := strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")
	text := func(n int) []byte {
		var p []byte
		for len(p) < n {
			p = append(append(p, words[r.IntN(len(words))]...), ' ')
		}
		return p
	}
	// A rough stand-in for bf16 weights: a random low byte and a high byte in 0x3c-0x3e.
	weights := func(n int) []byte {
		p := make([]byte, n)
		for i := 0; i < n; i += 2 {
			p[i], p[i+1] = byte(r.IntN(256)), byte(0x3c+r.IntN(3))
		}
		return p
	}
	// The parts of the files in upload order, with the type of the chunks that start in each; the chunk where the third
	// file's weights begin holds enough of its text for LZ4 to shrink it.
	parts := []struct {
		data []byte
		want xorb.CompressionType
	}{
		{text(4 << 10), xorb.CompressionLZ4},
		{weights(256 << 10), xorb.CompressionByteGrouping4},
		{text(256 << 10), xorb.CompressionLZ4},
		{weights(256 << 10), xorb.CompressionNone},
	}
	files := []io.ReadSeeker{
		bytes.NewReader(parts[0].data),
		bytes.NewReader(parts[1].data),
		bytes.NewReader(slices.Concat(parts[2].data, parts[3].data)),
	}
	if _, err := UploadFiles(ctx, storageClient{st}, files, WithCacheManager(NewCacheManager(t.TempDir(), 0))); err != nil {
		t.Fatalf("UploadFiles: %v", err)
	}
	xorbs := snapshot(t, ctx, st).xorbs
	if len(xorbs) != 1 {
		t.Fatalf("store holds %d xorbs, want all files in one", len(xorbs))
	}
	for _, data := range xorbs {
		offsets, err := xorb.ReadChunkOffsets(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		var got, want []xorb.CompressionType
		var start uint64
		pos, part, partEnd := 0, 0, len(parts[0].data)
		for _, end := range offsets {
			h, err := xorb.ParseChunkHeader(data[start:])
			if err != nil {
				t.Fatal(err)
			}
			for pos >= partEnd {
				part++
				partEnd += len(parts[part].data)
			}
			got = append(got, h.Compression)
			want = append(want, parts[part].want)
			pos += int(h.UncompressedSize)
			start = end
		}
		if !slices.Equal(got, want) {
			t.Fatalf("chunks stored with compression types %v, want %v: LZ4 for the text, BG4 for the weights file, None for the weights after text in one file", got, want)
		}
	}
}
