package client

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/reconstruction"
	"github.com/wzshiming/xet/pkg/xorb"
)

// clientAdapter adapts the Client to the reconstruction.ClientAdapter interface
type clientAdapter struct {
	client *Client
}

func (ca *clientAdapter) DownloadXorb(ctx context.Context, url string, reqOpts ...interface{}) (*xorb.Xorb, error) {
	// Convert interface{} reqOpts to ReqOpt
	var opts []ReqOpt
	for _, opt := range reqOpts {
		if byteRange, ok := opt.(*reconstruction.ByteRange); ok {
			opts = append(opts, WithRange(byteRange.Start, byteRange.End))
		}
	}
	return ca.client.DownloadXorb(ctx, url, opts...)
}

// DownloadSession represents a download session
type DownloadSession struct {
	client     *Client
	chunkCache map[xet.Hash][]byte
}

type downloadSessionOptions struct {
	EnableCaching bool
}

// WithDownloadCaching enables or disables chunk caching for a download session
func WithDownloadCaching(enabled bool) func(*downloadSessionOptions) {
	return func(opts *downloadSessionOptions) {
		opts.EnableCaching = enabled
	}
}

// DownloadSession creates a new download session with optional caching
func (c *Client) DownloadSession(opts ...func(*downloadSessionOptions)) *DownloadSession {
	options := &downloadSessionOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var cache map[xet.Hash][]byte
	if options.EnableCaching {
		cache = make(map[xet.Hash][]byte)
	}

	return &DownloadSession{
		client:     c,
		chunkCache: cache,
	}
}

// DownloadFile downloads and reconstructs a file from its hash, automatically falling back to V1 if V2 is not supported
func (s *DownloadSession) DownloadFile(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	r, size, err := s.DownloadFileV2(ctx, fileHash, opts...)
	if err != nil {
		if err == errNotFound {
			return s.DownloadFileV1(ctx, fileHash, opts...)
		}
		return nil, 0, err
	}
	return r, size, nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash
func (s *DownloadSession) DownloadFileV1(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	// Step 1: Query reconstruction
	reconstructionResp, err := s.client.GetReconstructionV1(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction: %w", err)
	}

	expectedLength := reconstruction.ExpectedLength(reconstructionResp)

	// Create adapter
	adapter := &clientAdapter{client: s.client}

	// Create a reader that reconstructs the file on-demand
	reader := reconstruction.NewReaderV1(ctx, adapter, reconstructionResp, s.chunkCache)

	return reader, expectedLength, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	reconstructionResp, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := reconstruction.ExpectedLengthV2(reconstructionResp)

	// Create adapter
	adapter := &clientAdapter{client: s.client}

	// Create a reader that reconstructs the file on-demand
	reader := reconstruction.NewReaderV2(ctx, adapter, reconstructionResp, s.chunkCache)

	return reader, expectedLength, nil
}
