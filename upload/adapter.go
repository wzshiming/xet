package upload

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// ClientAdapter provides access to client operations needed for uploading.
type ClientAdapter interface {
	HasXorb(ctx context.Context, xorbHash xet.Hash) (bool, error)
	UploadXorb(ctx context.Context, xorbHash xet.Hash, reader io.ReadSeeker) (*XorbUploadResponse, error)
	UploadShard(ctx context.Context, shardObj *shard.Shard) (*ShardUploadResponse, error)
	QueryDedupShards(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*DeduplicationResult, error)
}
