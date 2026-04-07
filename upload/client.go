package upload

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// ClientAdapter provides access to client operations needed for uploading.
type ClientAdapter interface {
	UploadXorb(ctx context.Context, xorbHash xet.Hash, reader io.ReadSeeker) (*XorbUploadResponse, error)
	UploadShard(ctx context.Context, shardObj *shard.Shard) (*ShardUploadResponse, error)
	QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*DeduplicationResult, error)
	QueryChunksDeduplication(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*DeduplicationResult, error)
}
