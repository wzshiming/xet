package download

import (
	"context"
	"fmt"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/progress"
)

// Option is a functional option for NewReaderV1 and NewReaderV2.
type Option func(*options)

type options struct {
	concurrency  int
	progressFunc progress.ProgressFunc
	cache        *CacheManager
	expectedHash *xet.FileHash
	retries      int
}

// reconstructionRefresher is a ClientAdapter that re-queries a reader's reconstruction after a fetch URL answers 403, up to RefreshRetries times per range.
type reconstructionRefresher[T any] interface {
	RefreshReconstruction(context.Context) (*T, error)
	RefreshRetries() int
}

// WithCacheManager shares one CacheManager across readers so the capacity
// bound applies to the whole cache directory. When unset, each reader uses a
// private manager for the cacheDir passed to its constructor.
func WithCacheManager(cache *CacheManager) Option {
	return func(o *options) {
		o.cache = cache
	}
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func WithConcurrency(concurrency int) Option {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithExpectedFileHash verifies full reconstructions before EOF and rejects nonzero offsets.
func WithExpectedFileHash(fileHash xet.FileHash) Option {
	return func(o *options) {
		o.expectedHash = &fileHash
	}
}

// WithProgressFunc sets a callback to receive download progress updates.
// name identifies this download in the callback (e.g. file hash string).
// total is computed upfront from the reconstruction plan so the caller always
// knows the expected transfer size before any xorb is fetched.
// progress is reported only when individual fetch entries complete successfully,
// so retries do not inflate the reported current value.
func WithProgressFunc(progressFunc progress.ProgressFunc) Option {
	return func(o *options) {
		o.progressFunc = progressFunc
	}
}

func newFileVerifier(o *options, offsetIntoFirstRange int64) (*fileVerifier, error) {
	if o.expectedHash == nil {
		return nil, nil
	}
	if offsetIntoFirstRange != 0 {
		return nil, fmt.Errorf("cannot verify file hash: reconstruction starts at offset %d", offsetIntoFirstRange)
	}
	return &fileVerifier{expected: *o.expectedHash}, nil
}

// Each original chunk occurrence contributes once, in file order.
type fileVerifier struct {
	expected xet.FileHash
	hashes   []xet.ChunkHash
	sizes    []uint64
}

func (v *fileVerifier) add(chunk []byte) {
	v.hashes = append(v.hashes, xet.ComputeChunkHash(chunk))
	v.sizes = append(v.sizes, uint64(len(chunk)))
}

func (v *fileVerifier) verify() error {
	if got := xet.ComputeFileHash(v.hashes, v.sizes); got != v.expected {
		return fmt.Errorf("file hash mismatch: expected %s, got %s", v.expected, got)
	}
	return nil
}
