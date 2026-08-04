package upload

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// ClientAdapter provides access to client operations needed for uploading.
type ClientAdapter interface {
	HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error)
	UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*XorbUploadResponse, error)
	UploadShard(ctx context.Context, shardObj *shard.Shard) (*ShardUploadResponse, error)
	QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash) (map[xet.ChunkHash]*DeduplicationResult, error)
}
