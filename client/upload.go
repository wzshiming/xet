package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/upload"
	"github.com/wzshiming/xet/xorb"
)

const DefaultUploadConcurrency = 4

// uploadClientAdapter adapts the Client to the upload.ClientAdapter interface.
type uploadClientAdapter struct {
	client          *Client
	onUploadedBytes func(int64)
}

func (a *uploadClientAdapter) UploadXorb(ctx context.Context, xorbObj *xorb.Xorb) (*upload.XorbUploadResponse, error) {
	ctx = withUploadProgressContext(ctx, a.onUploadedBytes)
	return a.client.UploadXorb(ctx, xorbObj)
}

func (a *uploadClientAdapter) UploadShard(ctx context.Context, shardObj *shard.Shard) (*upload.ShardUploadResponse, error) {
	ctx = withUploadProgressContext(ctx, a.onUploadedBytes)
	return a.client.UploadShard(ctx, shardObj)
}

func (a *uploadClientAdapter) QueryChunkDeduplication(ctx context.Context, chunkHash xet.Hash) (*shard.Shard, error) {
	return a.client.QueryChunkDeduplication(ctx, chunkHash)
}

// UploadSession represents an upload session
type UploadSession struct {
	client      *Client
	concurrency int
	progress    ProgressFunc
}

// UploadSession creates a new upload session with optional global deduplication
func (c *Client) UploadSession() *UploadSession {
	return &UploadSession{
		client:      c,
		concurrency: DefaultUploadConcurrency,
	}
}

// WithConcurrency configures how many upload tasks run concurrently.
func (s *UploadSession) WithConcurrency(concurrency int) *UploadSession {
	if concurrency <= 0 {
		concurrency = 1
	}
	s.concurrency = concurrency
	return s
}

// WithProgress configures a callback invoked as upload bytes are processed.
func (s *UploadSession) WithProgress(progress ProgressFunc) *UploadSession {
	s.progress = progress
	return s
}

// UploadFiles uploads multiple files and returns their hashes
func (s *UploadSession) UploadFiles(ctx context.Context, readers ...io.Reader) ([]xet.Hash, error) {
	adapter := &uploadClientAdapter{client: s.client}
	tracker := newSessionProgressTracker(s.progress, func(readBytes, transferBytes int64) Progress {
		return newProgress(readBytes, 0, transferBytes)
	})
	if tracker != nil {
		adapter.onUploadedBytes = tracker.AddTransferBytes
	}
	return upload.UploadFiles(ctx, adapter, tracker.WrapReaders(readers), upload.WithConcurrency(s.concurrency))
}
