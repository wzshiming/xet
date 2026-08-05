package mirror

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

type storageUploadAdapter struct {
	storage storage.Storage
}

func (a storageUploadAdapter) HasXorb(ctx context.Context, hash xet.XorbHash) (bool, error) {
	return a.storage.HasXorb(ctx, "default", hash)
}

func (a storageUploadAdapter) UploadXorb(ctx context.Context, hash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	inserted, err := a.storage.PutXorb(ctx, "default", hash, reader)
	if err != nil {
		return nil, err
	}
	return &upload.XorbUploadResponse{WasInserted: inserted}, nil
}

func (a storageUploadAdapter) UploadShard(ctx context.Context, sh *shard.Shard) (*upload.ShardUploadResponse, error) {
	inserted, err := a.storage.PutShard(ctx, sh)
	if err != nil {
		return nil, err
	}
	result := 0
	if inserted {
		result = 1
	}
	return &upload.ShardUploadResponse{Result: result}, nil
}

func (a storageUploadAdapter) QueryDedupShards(ctx context.Context, hashes []xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(hashes))
	for _, hash := range hashes {
		result := &upload.DeduplicationResult{ChunkHash: hash, IsNew: true}
		sh, err := a.storage.GetShardByChunkHash(ctx, "default", hash)
		if err == nil {
			if xorbHash, index, ok := findChunk(sh, hash); ok {
				result.IsNew = false
				result.XorbHash = xorbHash
				result.ChunkIndex = index
			}
		}
		results[hash] = result
	}
	return results, nil
}

func findChunk(sh *shard.Shard, hash xet.ChunkHash) (xet.XorbHash, uint32, bool) {
	for _, cas := range sh.CASInfos {
		for index, chunk := range cas.Chunks {
			if chunk.ChunkHash == hash {
				return cas.CASHash, uint32(index), true
			}
		}
	}
	return xet.XorbHash{}, 0, false
}
