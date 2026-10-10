package upload

import (
	"bytes"
	"context"
	"io"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/memory"
)

// storageClient is the storage-backed CAS the mirror used to ingest with.
type storageClient struct{ st storage.Storage }

func (c storageClient) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return c.st.HasXorb(ctx, "default", xorbHash)
}

func (c storageClient) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, r io.ReadSeeker) (*XorbUploadResponse, error) {
	inserted, err := c.st.PutXorb(ctx, "default", xorbHash, r)
	if err != nil {
		return nil, err
	}
	return &XorbUploadResponse{WasInserted: inserted}, nil
}

func (c storageClient) UploadShard(ctx context.Context, sh *shard.Shard) (*ShardUploadResponse, error) {
	inserted, err := c.st.PutShard(ctx, sh)
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
		sh, err := c.st.GetShardByChunkHash(ctx, "default", h)
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
	err := st.WalkXorbs(ctx, "default", func(hash string, _ int64, _ time.Time) error {
		xorbHash, err := xet.ParseXorbHash(hash)
		if err != nil {
			return err
		}
		rc, err := st.GetXorbReadSeekCloser(ctx, "default", xorbHash)
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
		{"empty", nil, 2},
	} {
		want, err := storage.PutFile(ctx, native, "default", bytes.NewReader(file.data))
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
}
