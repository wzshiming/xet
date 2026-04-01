package client

import (
	"context"
	"io"
	"sync/atomic"

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

func (a *uploadClientAdapter) QueryChunksDeduplication(ctx context.Context, chunkHashes []xet.Hash) (map[xet.Hash]*upload.DeduplicationResult, error) {
	return a.client.QueryChunksDeduplication(ctx, chunkHashes)
}

// UploadSession represents an upload session
type UploadSession struct {
	client       *Client
	concurrency  int
	progress     ProgressFunc
	enableSHA256 bool
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

// WithEnableSHA256 configures whether to compute SHA256 hashes for files.
func (s *UploadSession) WithEnableSHA256(enable bool) *UploadSession {
	s.enableSHA256 = enable
	return s
}

// UploadFile uploads a single file and returns its hash
func (s *UploadSession) UploadFile(ctx context.Context, reader io.Reader) (xet.Hash, error) {
	hashes, err := s.UploadFiles(ctx, []io.Reader{reader})
	if err != nil {
		return xet.Hash{}, err
	}
	return hashes[0], nil
}

// UploadFiles uploads multiple files and returns their hashes
func (s *UploadSession) UploadFiles(ctx context.Context, readers []io.Reader) ([]xet.Hash, error) {
	adapter := &uploadClientAdapter{client: s.client}
	var totalTransfer atomic.Int64

	if s.progress != nil {
		tracker := newSessionProgressTracker(s.progress, func() int64 { return totalTransfer.Load() })
		adapter.onUploadedBytes = tracker.AddTransferBytes
	}

	return upload.UploadFiles(ctx, adapter, readers,
		upload.WithConcurrency(s.concurrency),
		upload.WithEnableSHA256(s.enableSHA256),
		upload.WithOnTotalBytes(func(total int64) {
			totalTransfer.Store(total)
		}),
	)
}
