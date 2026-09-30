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

// QueryDedupShards resolves chunkHashes against the shards stored locally;
// every chunk of a found shard is returned, matching the remote global-dedup
// behavior where one probe yields the whole shard. Local shards store raw
// chunk hashes, so keyed-shard candidates are unnecessary here.
func (l *localCAS) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, _ ...xet.ChunkHash) (map[xet.ChunkHash]shard.ChunkLocation, error) {
	results := make(map[xet.ChunkHash]shard.ChunkLocation, len(chunkHashes))
	for _, chunkHash := range chunkHashes {
		if _, ok := results[chunkHash]; ok {
			continue
		}
		shardObj, err := l.storage.GetShardByChunkHash(ctx, l.namespace, chunkHash)
		if err != nil || shardObj == nil {
			continue
		}
		for h, loc := range shardObj.ChunkLocations() {
			if _, ok := results[h]; !ok {
				results[h] = loc
			}
		}
	}
	return results, nil
}
