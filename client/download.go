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

// clientAdapter adapts the Client to the download.ClientAdapter interface
type clientAdapter struct {
	client *Client
}

func (ca *clientAdapter) DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error) {
	// Convert interface{} reqOpts to ReqOpt
	var opts []ReqOpt
	if len(header) != 0 {
		opts = append(opts, func(req *http.Request) {
			// Merge headers instead of replacing them to preserve Authorization
			for key, values := range header {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		})
	}
	return ca.client.DownloadXorb(ctx, url, opts...)
}

// DownloadSession represents a download session
type DownloadSession struct {
	client *Client
}

// DownloadSession creates a new download session with optional caching
func (c *Client) DownloadSession() *DownloadSession {
	return &DownloadSession{
		client: c,
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

	expectedLength := download.ExpectedLengthV1(reconstructionResp)

	// Create adapter
	adapter := &clientAdapter{client: s.client}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV1(ctx, adapter, reconstructionResp)

	return reader, expectedLength, nil
}

// DownloadFileV2 downloads and reconstructs a file from its hash using the V2 API
func (s *DownloadSession) DownloadFileV2(ctx context.Context, fileHash xet.Hash, opts ...ReqOpt) (io.Reader, int64, error) {
	reconstructionResp, err := s.client.GetReconstructionV2(ctx, fileHash, opts...)
	if err != nil {
		return nil, 0, fmt.Errorf("query reconstruction v2: %w", err)
	}

	expectedLength := download.ExpectedLengthV2(reconstructionResp)

	// Create adapter
	adapter := &clientAdapter{client: s.client}

	// Create a reader that reconstructs the file on-demand
	reader := download.NewReaderV2(ctx, adapter, reconstructionResp)

	return reader, expectedLength, nil
}
