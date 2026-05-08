package client

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// DownloadFile downloads and reconstructs a file from its hash into w, automatically falling back to V1 if V2 is not supported.
// It seeks w to determine the current size for resume support.
func (c *Client) DownloadFile(ctx context.Context, fileHash xet.Hash, w io.WriteSeeker) error {
	return c.DownloadFileWithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileWithAuthProvider downloads and reconstructs a file using a
// per-call auth provider.
func (c *Client) DownloadFileWithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.Hash, w io.WriteSeeker) error {
	err := c.DownloadFileV2WithAuthProvider(ctx, provider, fileHash, w)
	if err != nil {
		if err == errNotFound {
			return c.DownloadFileV1WithAuthProvider(ctx, provider, fileHash, w)
		}
		return err
	}
	return nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash into w.
// It seeks w to determine the current size for resume support.
func (c *Client) DownloadFileV1(ctx context.Context, fileHash xet.Hash, w io.WriteSeeker) error {
	return c.DownloadFileV1WithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileV1WithAuthProvider downloads and reconstructs a file from its
// hash into w using a per-call auth provider.
func (c *Client) DownloadFileV1WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.Hash, w io.WriteSeeker) error {
	resumeOffset, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		resumeOffset = 0
	}

	var header http.Header
	if resumeOffset > 0 {
		header = http.Header{
			"Range": []string{fmt.Sprintf("bytes=%d-", resumeOffset)},
		}
	}

	reconstructionResp, err := c.GetReconstructionV1WithAuthProvider(ctx, provider, fileHash, header)
	if err != nil {
		if resumeOffset > 0 {
			if _, seekErr := w.Seek(0, io.SeekStart); seekErr == nil {
				resumeOffset = 0
				reconstructionResp, err = c.GetReconstructionV1WithAuthProvider(ctx, provider, fileHash, nil)
			}
		}
		if err != nil {
			return fmt.Errorf("query reconstruction: %w", err)
		}
	}

	opts := []download.Option{
		download.WithConcurrency(c.concurrency),
	}
	if c.progressFunc != nil {
		opts = append(opts, download.WithProgressFunc(c.progressFunc))
	}
	reader, err := download.NewReaderV1(ctx, c, reconstructionResp, c.diskCache, opts...)
	if err != nil {
		return fmt.Errorf("initialize reader v1: %w", err)
	}

	expectedLength := download.ExpectedLengthV1(reconstructionResp)
	n, err := io.Copy(w, reader)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if n != expectedLength {
		return fmt.Errorf("downloaded file size mismatch: expected %d bytes, got %d bytes", expectedLength, n)
	}
	return nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash into w using the V2 API.
// It seeks w to determine the current size for resume support.
func (c *Client) DownloadFileV2(ctx context.Context, fileHash xet.Hash, w io.WriteSeeker) error {
	return c.DownloadFileV2WithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileV2WithAuthProvider downloads and reconstructs a file from its
// hash into w using the V2 API and a per-call auth provider.
func (c *Client) DownloadFileV2WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.Hash, w io.WriteSeeker) error {
	resumeOffset, err := w.Seek(0, io.SeekEnd)
	if err != nil {
		resumeOffset = 0
	}

	var header http.Header
	if resumeOffset > 0 {
		header = http.Header{
			"Range": []string{fmt.Sprintf("bytes=%d-", resumeOffset)},
		}
	}

	reconstructionResp, err := c.GetReconstructionV2WithAuthProvider(ctx, provider, fileHash, header)
	if err != nil {
		if resumeOffset > 0 {
			if _, seekErr := w.Seek(0, io.SeekStart); seekErr == nil {
				resumeOffset = 0
				reconstructionResp, err = c.GetReconstructionV2WithAuthProvider(ctx, provider, fileHash, nil)
			}
		}
		if err != nil {
			return fmt.Errorf("query reconstruction v2: %w", err)
		}
	}

	opts := []download.Option{
		download.WithConcurrency(c.concurrency),
	}
	if c.progressFunc != nil {
		opts = append(opts, download.WithProgressFunc(c.progressFunc))
	}
	reader, err := download.NewReaderV2(ctx, c, reconstructionResp, c.diskCache, opts...)
	if err != nil {
		return fmt.Errorf("initialize reader v2: %w", err)
	}

	expectedLength := download.ExpectedLengthV2(reconstructionResp)
	n, err := io.Copy(w, reader)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if n != expectedLength {
		return fmt.Errorf("downloaded file size mismatch: expected %d bytes, got %d bytes", expectedLength, n)
	}
	return nil
}

// DownloadFiles downloads multiple files using a single batch reconstruction request.
// All files share one fetch_info map, so each xorb is fetched only once across the batch.
// It returns a reader and size per file in the same order as fileHashes.
// Individual errors are embedded per-entry; a nil reader means that file was not found.
func (c *Client) DownloadFiles(ctx context.Context, fileHashes []xet.Hash) ([]io.Reader, []int64, error) {
	return c.DownloadFilesWithAuthProvider(ctx, nil, fileHashes)
}

// DownloadFilesWithAuthProvider downloads multiple files using a single batch
// reconstruction request and a per-call auth provider.
func (c *Client) DownloadFilesWithAuthProvider(ctx context.Context, provider AuthProvider, fileHashes []xet.Hash) ([]io.Reader, []int64, error) {
	if len(fileHashes) == 0 {
		return nil, nil, nil
	}

	batchResp, err := c.GetBatchReconstructionWithAuthProvider(ctx, provider, fileHashes)
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
		}
		if c.progressFunc != nil {
			opts = append(opts, download.WithProgressFunc(c.progressFunc))
		}
		reader, err := download.NewReaderV1(ctx, c, singleResp, c.diskCache, opts...)
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
