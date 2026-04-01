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
	totalTransfer := download.ExpectedTransferBytesV1(reconstructionResp)

	adapter := &clientAdapter{client: s.client}
	if s.progress != nil {
		tracker := newSessionProgressTracker(s.progress, func() int64 { return totalTransfer })
		adapter.onDownloadedBytes = tracker.AddTransferBytes
		defer tracker.Report()
	}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV1(ctx, adapter, reconstructionResp, download.WithConcurrency(s.concurrency))

	return reader, expectedLength, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	reconstructionResp, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := download.ExpectedLengthV2(reconstructionResp)
	totalTransfer := download.ExpectedTransferBytesV2(reconstructionResp)

	adapter := &clientAdapter{client: s.client}
	if s.progress != nil {
		tracker := newSessionProgressTracker(s.progress, func() int64 { return totalTransfer })
		adapter.onDownloadedBytes = tracker.AddTransferBytes
		defer tracker.Report()
	}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV2(ctx, adapter, reconstructionResp, download.WithConcurrency(s.concurrency))

	return reader, expectedLength, nil
}

// DownloadFiles downloads multiple files using a single batch reconstruction request.
// All files share one fetch_info map, so each xorb is fetched only once across the batch.
// It returns a reader and size per file in the same order as fileHashes.
// Individual errors are embedded per-entry; a nil reader means that file was not found.
func (s *DownloadSession) DownloadFiles(ctx context.Context, fileHashes []xet.Hash) ([]io.Reader, []int64, error) {
	if len(fileHashes) == 0 {
		return nil, nil, nil
	}

	batchResp, err := s.client.BatchGetReconstruction(ctx, fileHashes)
	if err != nil {
		return nil, nil, fmt.Errorf("batch get reconstruction: %w", err)
	}

	adapter := &clientAdapter{client: s.client}

	readers := make([]io.Reader, len(fileHashes))
	sizes := make([]int64, len(fileHashes))
	for i, fileHash := range fileHashes {
		terms, ok := batchResp.Files[fileHash.String()]
		if !ok {
			// File not in response — return nil reader for this slot.
			continue
		}

		// Build a per-file ReconstructionResponse reusing the shared fetch_info.
		singleResp := &download.ReconstructionResponse{
			OffsetIntoFirstRange: 0,
			Terms:                terms,
			FetchInfo:            batchResp.FetchInfo,
		}

		sizes[i] = download.ExpectedLengthV1(singleResp)
		readers[i] = download.NewReaderV1(ctx, adapter, singleResp, download.WithConcurrency(s.concurrency))
	}

	return readers, sizes, nil
}
