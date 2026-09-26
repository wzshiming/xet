package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

type uploadAdapter struct {
	client          *Client
	shardAPIVersion shardAPIVersion
}

type shardAPIVersion uint8

const (
	shardAPIVersionV1 shardAPIVersion = 1
	shardAPIVersionV2 shardAPIVersion = 2
)

func (a uploadAdapter) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return a.client.HasXorb(ctx, xorbHash)
}

func (a uploadAdapter) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	return a.client.UploadXorb(ctx, xorbHash, reader)
}

func (a uploadAdapter) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	if a.shardAPIVersion == shardAPIVersionV2 {
		return a.client.UploadShardV2(ctx, shardObj)
	}
	return a.client.UploadShard(ctx, shardObj)
}

func (a uploadAdapter) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	return a.client.QueryDedupShards(ctx, chunkHashes, candidates...)
}

// UploadFile uploads a single file through the V1 shard API and returns its
// hash.
func (c *Client) UploadFile(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFile(ctx, readSeeker, shardAPIVersionV1)
}

// UploadFileV1 uploads a single file through the V1 shard API.
func (c *Client) UploadFileV1(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFile(ctx, readSeeker, shardAPIVersionV1)
}

// UploadFileV2 uploads a single file through the V2 shard API.
func (c *Client) UploadFileV2(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFile(ctx, readSeeker, shardAPIVersionV2)
}

func (c *Client) uploadFile(ctx context.Context, readSeeker io.ReadSeeker, shardAPIVersion shardAPIVersion) (xet.FileHash, error) {
	adapter := uploadAdapter{client: c, shardAPIVersion: shardAPIVersion}
	hash, err := upload.UploadFile(ctx, adapter, readSeeker,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithCacheDir(c.cacheDir),
		upload.WithEnableSHA256(true),
	)
	if err != nil {
		return xet.FileHash{}, err
	}
	return hash, nil
}

// UploadFiles uploads multiple files through the V1 shard API and returns
// their hashes.
func (c *Client) UploadFiles(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFiles(ctx, readSeekers, shardAPIVersionV1)
}

// UploadFilesV1 uploads multiple files through the V1 shard API.
func (c *Client) UploadFilesV1(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFiles(ctx, readSeekers, shardAPIVersionV1)
}

// UploadFilesV2 uploads multiple files through the V2 shard API.
func (c *Client) UploadFilesV2(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFiles(ctx, readSeekers, shardAPIVersionV2)
}

func (c *Client) uploadFiles(ctx context.Context, readSeekers []io.ReadSeeker, shardAPIVersion shardAPIVersion) ([]xet.FileHash, error) {
	adapter := uploadAdapter{client: c, shardAPIVersion: shardAPIVersion}
	return upload.UploadFiles(ctx, adapter, readSeekers,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithCacheDir(c.cacheDir),
		upload.WithEnableSHA256(true),
	)
}
