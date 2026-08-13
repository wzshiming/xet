package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/download"
)

// reconstructionAPIVersion selects the reconstruction API version, mirroring
// shardAPIVersion used by the upload path.
type reconstructionAPIVersion uint8

const (
	reconstructionAPIVersionV1 reconstructionAPIVersion = 1
	reconstructionAPIVersionV2 reconstructionAPIVersion = 2
)

// DownloadFile downloads and reconstructs a file from its hash into w,
// automatically falling back to V1 if V2 is not supported. It seeks w to
// determine the current size for resume support.
func (c *Client) DownloadFile(ctx context.Context, fileHash xet.FileHash, w io.WriteSeeker) error {
	return c.DownloadFileWithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileWithAuthProvider downloads and reconstructs a file using a
// per-call auth provider, falling back to V1 when the V2 API is unavailable.
func (c *Client) DownloadFileWithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, w io.WriteSeeker) error {
	err := c.DownloadFileV2WithAuthProvider(ctx, provider, fileHash, w)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return c.DownloadFileV1WithAuthProvider(ctx, provider, fileHash, w)
		}
		return err
	}
	return nil
}

// DownloadFileV1 downloads and reconstructs a file from its hash into w
// through the V1 API. It seeks w to determine the current size for resume support.
func (c *Client) DownloadFileV1(ctx context.Context, fileHash xet.FileHash, w io.WriteSeeker) error {
	return c.DownloadFileV1WithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileV1WithAuthProvider downloads and reconstructs a file from its
// hash into w through the V1 API using a per-call auth provider.
func (c *Client) DownloadFileV1WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, w io.WriteSeeker) error {
	return c.downloadFileWithAuthProvider(ctx, provider, fileHash, w, reconstructionAPIVersionV1)
}

// DownloadFileV2 downloads and reconstructs a file from its hash into w
// through the V2 API. It seeks w to determine the current size for resume support.
func (c *Client) DownloadFileV2(ctx context.Context, fileHash xet.FileHash, w io.WriteSeeker) error {
	return c.DownloadFileV2WithAuthProvider(ctx, nil, fileHash, w)
}

// DownloadFileV2WithAuthProvider downloads and reconstructs a file from its
// hash into w through the V2 API using a per-call auth provider.
func (c *Client) DownloadFileV2WithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, w io.WriteSeeker) error {
	return c.downloadFileWithAuthProvider(ctx, provider, fileHash, w, reconstructionAPIVersionV2)
}

// downloadFileWithAuthProvider downloads and reconstructs a file from its hash
// into w through the reconstruction API version selected by apiVersion.
func (c *Client) downloadFileWithAuthProvider(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, w io.WriteSeeker, apiVersion reconstructionAPIVersion) error {
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

	reader, expectedLength, err := c.newDownloadReader(ctx, provider, fileHash, header, resumeOffset, w, apiVersion)
	if err != nil {
		return err
	}
	// Close releases cache references even when the copy stops early on a
	// write error.
	defer reader.Close()

	n, err := io.Copy(w, reader)
	if err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if n != expectedLength {
		return fmt.Errorf("downloaded file size mismatch: expected %d bytes, got %d bytes", expectedLength, n)
	}
	return nil
}

// newDownloadReader queries reconstruction through the selected API version
// and returns a reader plus the expected reconstructed length. When a resume
// (Range) query is rejected, it retries once from the start without a Range
// header.
func (c *Client) newDownloadReader(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, header http.Header, resumeOffset int64, w io.WriteSeeker, apiVersion reconstructionAPIVersion) (io.ReadCloser, int64, error) {
	if apiVersion == reconstructionAPIVersionV1 {
		return c.newDownloadReaderV1(ctx, provider, fileHash, header, resumeOffset, w)
	}
	return c.newDownloadReaderV2(ctx, provider, fileHash, header, resumeOffset, w)
}

func (c *Client) newDownloadReaderV1(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, header http.Header, resumeOffset int64, w io.WriteSeeker) (io.ReadCloser, int64, error) {
	reconstructionResp, err := c.GetReconstructionV1WithAuthProvider(ctx, provider, fileHash, header)
	if err != nil {
		// Only a rejected Range query warrants restarting from scratch; a 404
		// means the file is absent and rewinding would just lose the resume
		// offset.
		if resumeOffset > 0 && !errors.Is(err, errNotFound) {
			if _, seekErr := w.Seek(0, io.SeekStart); seekErr == nil {
				resumeOffset = 0
				reconstructionResp, err = c.GetReconstructionV1WithAuthProvider(ctx, provider, fileHash, nil)
			}
		}
		if err != nil {
			return nil, 0, fmt.Errorf("query reconstruction: %w", err)
		}
	}

	reader, err := download.NewReaderV1(ctx, c, reconstructionResp, c.downloadOptions()...)
	if err != nil {
		return nil, 0, fmt.Errorf("initialize reader v1: %w", err)
	}
	return reader, download.ExpectedLengthV1(reconstructionResp), nil
}

func (c *Client) newDownloadReaderV2(ctx context.Context, provider AuthProvider, fileHash xet.FileHash, header http.Header, resumeOffset int64, w io.WriteSeeker) (io.ReadCloser, int64, error) {
	reconstructionResp, err := c.GetReconstructionV2WithAuthProvider(ctx, provider, fileHash, header)
	if err != nil {
		// A missing V2 endpoint must not rewind w: the caller falls back to
		// V1, which resumes from the same offset.
		if resumeOffset > 0 && !errors.Is(err, errNotFound) {
			if _, seekErr := w.Seek(0, io.SeekStart); seekErr == nil {
				resumeOffset = 0
				reconstructionResp, err = c.GetReconstructionV2WithAuthProvider(ctx, provider, fileHash, nil)
			}
		}
		if err != nil {
			return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
		}
	}

	reader, err := download.NewReaderV2(ctx, c, reconstructionResp, c.downloadOptions()...)
	if err != nil {
		return nil, 0, fmt.Errorf("initialize reader v2: %w", err)
	}
	return reader, download.ExpectedLengthV2(reconstructionResp), nil
}

// downloadOptions returns the shared download options derived from the client
// configuration.
func (c *Client) downloadOptions() []download.Option {
	opts := []download.Option{
		download.WithConcurrency(c.concurrency),
		download.WithCacheManager(c.cacheManager),
	}
	if c.progressFunc != nil {
		opts = append(opts, download.WithProgressFunc(c.progressFunc))
	}
	return opts
}

// DownloadFiles downloads multiple files using a single batch reconstruction request.
// All files share one fetch_info map, so each xorb is fetched only once across the batch.
// It returns a reader and size per file in the same order as fileHashes.
// Individual errors are embedded per-entry; a nil reader means that file was not found.
// Readers release their cache references when read to EOF; close any reader
// that is not fully consumed.
func (c *Client) DownloadFiles(ctx context.Context, fileHashes []xet.FileHash) ([]io.ReadCloser, []int64, error) {
	return c.DownloadFilesWithAuthProvider(ctx, nil, fileHashes)
}

// DownloadFilesWithAuthProvider downloads multiple files using a single batch
// reconstruction request and a per-call auth provider.
func (c *Client) DownloadFilesWithAuthProvider(ctx context.Context, provider AuthProvider, fileHashes []xet.FileHash) ([]io.ReadCloser, []int64, error) {
	if len(fileHashes) == 0 {
		return nil, nil, nil
	}

	batchResp, err := c.GetBatchReconstructionWithAuthProvider(ctx, provider, fileHashes)
	if err != nil {
		return nil, nil, fmt.Errorf("get batch reconstruction: %w", err)
	}

	readers := make([]io.ReadCloser, len(fileHashes))
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
		reader, err := download.NewReaderV1(ctx, c, singleResp, c.downloadOptions()...)
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

func (e errReader) Close() error {
	return nil
}
