package client

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/upload"
)

// UploadFile uploads a single file and returns its hash
func (c *Client) UploadFile(ctx context.Context, readSeeker io.ReadSeeker) (xet.Hash, error) {
	hash, err := upload.UploadFile(ctx, c, readSeeker,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithEnableSHA256(true),
	)
	if err != nil {
		return xet.Hash{}, err
	}
	return hash, nil
}

// UploadFiles uploads multiple files and returns their hashes
func (c *Client) UploadFiles(ctx context.Context, readSeekers []io.ReadSeeker) ([]xet.Hash, error) {
	return upload.UploadFiles(ctx, c, readSeekers,
		upload.WithConcurrency(c.concurrency),
		upload.WithProgressFunc(c.progressFunc),
		upload.WithEnableSHA256(true),
	)
}
