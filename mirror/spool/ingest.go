package spool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/wzshiming/xet/upload"
)

// ErrCorrupt marks spooled bytes that failed verification; the spool must be
// discarded rather than kept for resume.
var ErrCorrupt = errors.New("spool corrupt")

// Result describes a spooled file that landed in storage; FileHash is empty for empty files, which are never uploaded.
type Result struct {
	SHA256   string
	FileHash string
	Size     int64
}

// ingest verifies the spooled bytes against wantSHA256 (when given) and runs
// the standard upload pipeline against cas; cache may be nil.
func ingest(ctx context.Context, cas upload.ClientAdapter, r io.ReadSeeker, wantSHA256 string) (Result, error) {
	hasher := sha256.New()
	size, err := io.Copy(hasher, r)
	if err != nil {
		return Result{}, fmt.Errorf("hash spool: %w", err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if wantSHA256 != "" && digest != wantSHA256 {
		return Result{}, fmt.Errorf("%w: sha256 mismatch: got %s, expected %s", ErrCorrupt, digest, wantSHA256)
	}

	res := Result{SHA256: digest, Size: size}
	if size > 0 {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return Result{}, fmt.Errorf("rewind spool: %w", err)
		}
		opts := []upload.Option{upload.WithEnableSHA256(true), upload.WithConcurrency(4)}
		fileHash, err := upload.UploadFile(ctx, cas, r, opts...)
		if err != nil {
			return Result{}, fmt.Errorf("ingest into storage: %w", err)
		}
		res.FileHash = fileHash.String()
	}
	return res, nil
}
