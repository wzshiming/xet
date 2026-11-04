package download

import (
	"errors"
	"fmt"
	"io"

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
// name identifies one fetched xorb range in the callback, and total, its
// length in bytes, is reported before any range is fetched. current counts the
// response body bytes as they arrive, so it ends at total; a range served from
// the cache reports total at once.
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

// add records one chunk occurrence, hashing data only when no decoder did.
func (v *fileVerifier) add(m chunkRef, data []byte) {
	hash := m.hash
	if !m.hashed {
		hash = xet.ComputeChunkHash(data)
	}
	v.hashes = append(v.hashes, hash)
	v.sizes = append(v.sizes, uint64(len(data)))
}

func (v *fileVerifier) verify() error {
	if got := xet.ComputeFileHash(v.hashes, v.sizes); got != v.expected {
		return fmt.Errorf("file hash mismatch: expected %s, got %s", v.expected, got)
	}
	return nil
}

// writeChunk writes chunk to w with one Write and checks the result as io.Copy does.
func writeChunk(w io.Writer, chunk []byte) (int, error) {
	n, err := w.Write(chunk)
	if n < 0 || n > len(chunk) {
		n = 0
		if err == nil {
			err = errors.New("invalid write result")
		}
	}
	if n < len(chunk) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
