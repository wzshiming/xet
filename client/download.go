package client

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/xorb"
)

const DefaultDownloadConcurrency = 4

// clientAdapter adapts the Client to the download.ClientAdapter interface
type clientAdapter struct {
	client            *Client
	onDownloadedBytes func(int64)
}

func (ca *clientAdapter) DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error) {
	// Convert interface{} reqOpts to ReqOpt
	var opts []ReqOpt
	if len(header) != 0 {
		opts = append(opts, func(req *http.Request) {
			req.Header = header
		})
	}
	if ca.onDownloadedBytes != nil {
		opts = append(opts, WithDownloadProgress(ca.onDownloadedBytes))
	}
	return ca.client.DownloadXorb(ctx, url, opts...)
}

// DownloadSession represents a download session
type DownloadSession struct {
	client      *Client
	concurrency int
	progress    ProgressFunc
}

// DownloadSession creates a new download session with optional caching
func (c *Client) DownloadSession() *DownloadSession {
	return &DownloadSession{
		client:      c,
		concurrency: DefaultDownloadConcurrency,
	}
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func (s *DownloadSession) WithConcurrency(concurrency int) *DownloadSession {
	if concurrency <= 0 {
		concurrency = 1
	}
	s.concurrency = concurrency
	return s
}

// WithProgress configures a callback invoked as download bytes are read.
func (s *DownloadSession) WithProgress(progress ProgressFunc) *DownloadSession {
	s.progress = progress
	return s
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

	expectedLength := download.ExpectedLengthV1(reconstructionResp)

	adapter := &clientAdapter{client: s.client}
	tracker := newSessionProgressTracker(s.progress, func(readBytes, transferBytes int64) Progress {
		return newProgress(readBytes, expectedLength, transferBytes)
	})
	if tracker != nil {
		adapter.onDownloadedBytes = tracker.AddTransferBytes
	}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV1(ctx, adapter, reconstructionResp, download.WithConcurrency(s.concurrency))
	if tracker != nil {
		tracker.Report()
		reader = tracker.WrapReader(reader)
	}

	return reader, expectedLength, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	reconstructionResp, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := download.ExpectedLengthV2(reconstructionResp)

	adapter := &clientAdapter{client: s.client}
	tracker := newSessionProgressTracker(s.progress, func(readBytes, transferBytes int64) Progress {
		return newProgress(readBytes, expectedLength, transferBytes)
	})
	if tracker != nil {
		adapter.onDownloadedBytes = tracker.AddTransferBytes
	}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV2(ctx, adapter, reconstructionResp, download.WithConcurrency(s.concurrency))
	if tracker != nil {
		tracker.Report()
		reader = tracker.WrapReader(reader)
	}

	return reader, expectedLength, nil
}
