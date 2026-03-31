package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

// uploadClientAdapter adapts the Client to the upload.ClientAdapter interface.
type uploadClientAdapter struct {
	client *Client
}

func (a *uploadClientAdapter) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	return a.client.UploadXorb(ctx, xorbObj)
}

func (a *uploadClientAdapter) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return a.client.UploadShard(ctx, shardObj)
}

func (a *uploadClientAdapter) QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*shard.Shard, error) {
	return a.client.QueryChunkDeduplication(ctx, chunkHash)
}

// UploadSession represents an upload session
type UploadSession struct {
	client *Client
}

// UploadSession creates a new upload session with optional global deduplication
func (c *Client) UploadSession() *UploadSession {
	return &UploadSession{
		client: c,
	}
}

// UploadFiles uploads multiple files and returns their hashes
func (s *UploadSession) UploadFiles(ctx context.Context, readers ...io.Reader) ([]xet.Hash, error) {
	adapter := &uploadClientAdapter{client: s.client}
	return upload.UploadFiles(ctx, adapter, readers...)
}
