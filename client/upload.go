package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
)

type authProviderUploadAdapter struct {
	client   *Client
	provider AuthProvider
}

func (a authProviderUploadAdapter) HasXorb(ctx context.Context, xorbHash xet.Hash) (bool, error) {
	return a.client.HasXorbWithAuthProvider(ctx, a.provider, xorbHash)
}

func (a authProviderUploadAdapter) UploadXorb(ctx context.Context, xorbHash xet.Hash, reader io.ReadSeeker) (*upload.XorbUploadResponse, error) {
	return a.client.UploadXorbWithAuthProvider(ctx, a.provider, xorbHash, reader)
}

func (a authProviderUploadAdapter) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	return a.client.UploadShardWithAuthProvider(ctx, a.provider, shardObj)
}

func (a authProviderUploadAdapter) QueryDedupShards(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	return a.client.QueryDedupShardsWithAuthProvider(ctx, a.provider, chunkHashes)
}

// UploadFile uploads a single file and returns its hash
func (c *Client) UploadFile(ctx context.Context, readSeeker io.ReadSeeker) (xet.Hash, error) {
	return c.UploadFileWithAuthProvider(ctx, nil, readSeeker)
}

// UploadFileWithAuthProvider uploads a single file using a per-call auth
// provider and returns its hash.
func (c *Client) UploadFileWithAuthProvider(ctx context.Context, provider AuthProvider, readSeeker io.ReadSeeker) (xet.Hash, error) {
	adapter := authProviderUploadAdapter{client: c, provider: provider}
	hash, err := upload.UploadFile(ctx, adapter, readSeeker,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithCacheDir(c.cacheDir),
		upload.WithEnableSHA256(true),
	)
	if err != nil {
		return xet.Hash{}, err
	}
	return hash, nil
}

// UploadFiles uploads multiple files and returns their hashes
func (c *Client) UploadFiles(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.Hash, error) {
	return c.UploadFilesWithAuthProvider(ctx, nil, readSeekers)
}

// UploadFilesWithAuthProvider uploads multiple files using a per-call auth
// provider and returns their hashes.
func (c *Client) UploadFilesWithAuthProvider(ctx context.Context, provider AuthProvider, readSeekers []io.ReadSeeker) ([]xet.Hash, error) {
	adapter := authProviderUploadAdapter{client: c, provider: provider}
	return upload.UploadFiles(ctx, adapter, readSeekers,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithCacheDir(c.cacheDir),
		upload.WithEnableSHA256(true),
	)
}
