package upload

import (
	"context"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// ClientAdapter provides access to client operations needed for uploading.
type ClientAdapter interface {
	UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*XorbUploadResponse, error)
	UploadShard(ctx context.Context, shardObj *shard.Shard) (*ShardUploadResponse, error)
	QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*shard.Shard, error)
}
