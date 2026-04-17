package client

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// DownloadFile downloads and reconstructs a file from its hash, automatically falling back to V1 if V2 is not supported
func (c *Client) DownloadFile(ctx context.Context, fileHash xet.Hash, header http.Header) (io.Reader, int64, error) {
	r, size, err := c.DownloadFileV2(ctx, fileHash, header)
	if err != nil {
		if err == errNotFound {
			return c.DownloadFileV1(ctx, fileHash, header)
		}
		return nil, 0, err
	}
	return r, size, nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash
func (c *Client) DownloadFileV1(ctx context.Context, fileHash xet.Hash, header http.Header) (io.Reader, int64, error) {
	// Step 1: Query reconstruction
	reconstructionResp, err := c.GetReconstructionV1(ctx, fileHash, header)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction: %w", err)
	}

	// Create a reader that reconstructs the file on-demand
	opts := []download.Option{
		download.WithConcurrency(c.concurrency),
		download.WithRetries(c.retries),
	}
	if c.progressFunc != nil {
		opts = append(opts, download.WithProgressFunc(c.progressFunc))
	}
	if c.chunkCache != nil {
		opts = append(opts, download.WithChunkCache(c.chunkCache))
	}
	reader, err := download.NewReaderV1(ctx, c, reconstructionResp, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("initialize reader v1: %w", err)
	}

	expectedLength := download.ExpectedLengthV1(reconstructionResp)

	return reader, expectedLength, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (c *Client) DownloadFileV2(ctx context.Context, fileHash xet.Hash, header http.Header) (io.Reader, int64, error) {
	reconstructionResp, err := c.GetReconstructionV2(ctx, fileHash, header)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	// Create a reader that reconstructs the file on-demand
	opts := []download.Option{
		download.WithConcurrency(c.concurrency),
		download.WithRetries(c.retries),
	}
	if c.progressFunc != nil {
		opts = append(opts, download.WithProgressFunc(c.progressFunc))
	}
	if c.chunkCache != nil {
		opts = append(opts, download.WithChunkCache(c.chunkCache))
	}
	reader, err := download.NewReaderV2(ctx, c, reconstructionResp, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("initialize reader v2: %w", err)
	}

	expectedLength := download.ExpectedLengthV2(reconstructionResp)

	return reader, expectedLength, nil
}

// DownloadFiles downloads multiple files using a single batch reconstruction request.
// All files share one fetch_info map, so each xorb is fetched only once across the batch.
// It returns a reader and size per file in the same order as fileHashes.
// Individual errors are embedded per-entry; a nil reader means that file was not found.
func (c *Client) DownloadFiles(ctx context.Context, fileHashes []xet.Hash) ([]io.Reader, []int64, error) {
	if len(fileHashes) == 0 {
		return nil, nil, nil
	}

	batchResp, err := c.GetBatchReconstruction(ctx, fileHashes)
	if err != nil {
		return nil, nil, fmt.Errorf("get batch reconstruction: %w", err)
	}

	readers := make([]io.Reader, len(fileHashes))
	sizes := make([]int64, len(fileHashes))
	for i, fileHash := range fileHashes {
		terms, ok := batchResp.Files[fileHash.String()]
		if !ok {
			// File not in response — return nil reader for this slot.
			continue
		}

		// Build a per-file ReconstructionResponse reusing the shared fetch_info.
		singleResp := &download.ReconstructionResponseV1{
			OffsetIntoFirstRange: 0,
			Terms:                terms,
			FetchInfo:            batchResp.FetchInfo,
		}

		sizes[i] = download.ExpectedLengthV1(singleResp)
		opts := []download.Option{
			download.WithConcurrency(c.concurrency),
			download.WithRetries(c.retries),
		}
		if c.progressFunc != nil {
			opts = append(opts, download.WithProgressFunc(c.progressFunc))
		}
		if c.chunkCache != nil {
			opts = append(opts, download.WithChunkCache(c.chunkCache))
		}
		reader, err := download.NewReaderV1(ctx, c, singleResp, opts...)
		if err != nil {
			readers[i] = errReader{err: fmt.Errorf("initialize reader for file %s: %w", fileHash.String(), err)}
		} else {
			readers[i] = reader
		}
	}

	return readers, sizes, nil
}

type errReader struct {
	err error
}

func (e errReader) Read(p []byte) (n int, err error) {
	return 0, e.err
}
