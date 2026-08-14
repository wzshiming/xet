package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

type authProviderUploadAdapter struct {
	client          *Client
	provider        AuthProvider
	shardAPIVersion shardAPIVersion
}

type shardAPIVersion uint8

const (
	shardAPIVersionV1 shardAPIVersion = 1
	shardAPIVersionV2 shardAPIVersion = 2
)

func (a authProviderUploadAdapter) HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error) {
	return a.client.HasXorbWithAuthProvider(ctx, a.provider, xorbHash)
}

func (a authProviderUploadAdapter) UploadXorb(ctx context.Context, xorbHash xet.XorbHash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	return a.client.UploadXorbWithAuthProvider(ctx, a.provider, xorbHash, reader)
}

func (a authProviderUploadAdapter) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	if a.shardAPIVersion == shardAPIVersionV2 {
		return a.client.UploadShardV2WithAuthProvider(ctx, a.provider, shardObj)
	}
	return a.client.UploadShardWithAuthProvider(ctx, a.provider, shardObj)
}

func (a authProviderUploadAdapter) QueryDedupShards(ctx context.Context, chunkHashes []xet.ChunkHash, candidates ...xet.ChunkHash) (map[xet.ChunkHash]*upload.DeduplicationResult, error) {
	return a.client.QueryDedupShardsWithAuthProvider(ctx, a.provider, chunkHashes, candidates...)
}

// UploadFile uploads a single file through the V1 shard API and returns its hash.
func (c *Client) UploadFile(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.UploadFileWithAuthProvider(ctx, nil, readSeeker)
}

// UploadFileV1 uploads a single file using the V1 shard API.
func (c *Client) UploadFileV1(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.UploadFileV1WithAuthProvider(ctx, nil, readSeeker)
}

// UploadFileV2 uploads a single file using the V2 shard API.
func (c *Client) UploadFileV2(ctx context.Context, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.UploadFileV2WithAuthProvider(ctx, nil, readSeeker)
}

// UploadFileWithAuthProvider uploads a single file through the V1 shard API
// using a per-call auth provider and returns its hash.
func (c *Client) UploadFileWithAuthProvider(ctx context.Context, provider AuthProvider, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFileWithAuthProvider(ctx, provider, readSeeker, shardAPIVersionV1)
}

// UploadFileV1WithAuthProvider uploads a single file through the V1 shard API.
func (c *Client) UploadFileV1WithAuthProvider(ctx context.Context, provider AuthProvider, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFileWithAuthProvider(ctx, provider, readSeeker, shardAPIVersionV1)
}

// UploadFileV2WithAuthProvider uploads a single file through the V2 shard API.
func (c *Client) UploadFileV2WithAuthProvider(ctx context.Context, provider AuthProvider, readSeeker io.ReadSeeker) (xet.FileHash, error) {
	return c.uploadFileWithAuthProvider(ctx, provider, readSeeker, shardAPIVersionV2)
}

func (c *Client) uploadFileWithAuthProvider(ctx context.Context, provider AuthProvider, readSeeker io.ReadSeeker, shardAPIVersion shardAPIVersion) (xet.FileHash, error) {
	adapter := authProviderUploadAdapter{client: c, provider: provider, shardAPIVersion: shardAPIVersion}
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

// UploadFiles uploads multiple files through the V1 shard API and returns their hashes.
func (c *Client) UploadFiles(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.UploadFilesWithAuthProvider(ctx, nil, readSeekers)
}

// UploadFilesV1 uploads multiple files using the V1 shard API.
func (c *Client) UploadFilesV1(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.UploadFilesV1WithAuthProvider(ctx, nil, readSeekers)
}

// UploadFilesV2 uploads multiple files using the V2 shard API.
func (c *Client) UploadFilesV2(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.UploadFilesV2WithAuthProvider(ctx, nil, readSeekers)
}

// UploadFilesWithAuthProvider uploads multiple files through the V1 shard API
// using a per-call auth provider and returns their hashes.
func (c *Client) UploadFilesWithAuthProvider(ctx context.Context, provider AuthProvider, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFilesWithAuthProvider(ctx, provider, readSeekers, shardAPIVersionV1)
}

// UploadFilesV1WithAuthProvider uploads multiple files through the V1 shard API.
func (c *Client) UploadFilesV1WithAuthProvider(ctx context.Context, provider AuthProvider, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFilesWithAuthProvider(ctx, provider, readSeekers, shardAPIVersionV1)
}

// UploadFilesV2WithAuthProvider uploads multiple files through the V2 shard API.
func (c *Client) UploadFilesV2WithAuthProvider(ctx context.Context, provider AuthProvider, readSeekers []io.ReadSeeker) ([]xet.FileHash, error) {
	return c.uploadFilesWithAuthProvider(ctx, provider, readSeekers, shardAPIVersionV2)
}

func (c *Client) uploadFilesWithAuthProvider(ctx context.Context, provider AuthProvider, readSeekers []io.ReadSeeker, shardAPIVersion shardAPIVersion) ([]xet.FileHash, error) {
	adapter := authProviderUploadAdapter{client: c, provider: provider, shardAPIVersion: shardAPIVersion}
	return upload.UploadFiles(ctx, adapter, readSeekers,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithCacheDir(c.cacheDir),
		upload.WithEnableSHA256(true),
	)
}
