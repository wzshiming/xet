package mirror

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

// localCAS adapts storage.Storage to upload.ClientAdapter so the standard
// upload pipeline writes xorbs and shards straight into local storage without
// any HTTP hop.
type localCAS struct {
	storage   storage.Storage
	namespace string
}

var _ upload.ClientAdapter = (*localCAS)(nil)

func (l *localCAS) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return l.storage.HasXorb(ctx, l.namespace, xorbHash)
}

func (l *localCAS) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	wasInserted, err := l.storage.PutXorb(ctx, l.namespace, xorbHash, reader)
	if err != nil {
		return nil, err
	}
	return &upload.XorbUploadResponse{WasInserted: wasInserted}, nil
}

func (l *localCAS) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	wasInserted, err := l.storage.PutShard(ctx, shardObj)
	if err != nil {
		return nil, err
	}
	result := 0
	if wasInserted {
		result = 1
	}
	return &upload.ShardUploadResponse{Result: result}, nil
}

func (l *localCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	results := make(map[xet.ChunkHash]*upload.DeduplicationResult, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; ok {
			continue
		}
		shardObj, err := l.storage.GetShardByChunkHash(ctx, l.namespace, chunkHash)
		if err != nil || shardObj == nil {
			results[chunkHash] = &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
			continue
		}
		// Register every chunk of the found shard, matching the remote
		// global-dedup behavior where one probe yields the whole shard.
		for _, casBlock := range shardObj.CASInfos {
			for i, casChunk := range casBlock.Chunks {
				if _, ok := results[casChunk.ChunkHash]; ok {
					continue
				}
				results[casChunk.ChunkHash] = &upload.DeduplicationResult{
					ChunkHash:  casChunk.ChunkHash,
					IsNew:      false,
					XorbHash:   casBlock.CASHash,
					ChunkIndex: uint32(i),
				}
			}
		}
		if _, ok := results[chunkHash]; !ok {
			results[chunkHash] = &upload.DeduplicationResult{ChunkHash: chunkHash, IsNew: true}
		}
	}
	return results, nil
}
