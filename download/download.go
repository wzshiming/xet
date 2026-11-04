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
	budget       *MemoryBudget
}

// WithCacheManager makes the reader look each xorb range up in the chunk cache
// before fetching it and add the ranges it fetches, off the delivery path.
// Share one CacheManager across readers so the capacity bound applies to the
// whole cache directory. Without it the reader uses no cache.
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

// WithMemoryBudget holds the reader's fetched terms within budget, which
// readers may share; without it, or with nil, the reader uses its own budget
// sized by DefaultMemorySizes.
func WithMemoryBudget(budget *MemoryBudget) Option {
	return func(o *options) {
		o.budget = budget
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

// computeChunkHash is xet.ComputeChunkHash; tests count its calls.
var computeChunkHash = xet.ComputeChunkHash

// addChunks records the chunks of one term occurrence: hashes[i] is the hash
// of a chunk of offsets[i+1]-offsets[i] bytes.
func (v *fileVerifier) addChunks(hashes []xet.ChunkHash, offsets []int64) {
	for i, hash := range hashes {
		v.hashes = append(v.hashes, hash)
		v.sizes = append(v.sizes, uint64(offsets[i+1]-offsets[i]))
	}
}

func (v *fileVerifier) verify() error {
	if got := xet.ComputeFileHash(v.hashes, v.sizes); got != v.expected {
		return fmt.Errorf("file hash mismatch: expected %s, got %s", v.expected, got)
	}
	return nil
}

// writeTerm writes term to w with one Write and checks the result as io.Copy does.
func writeTerm(w io.Writer, term []byte) (int, error) {
	n, err := w.Write(term)
	if n < 0 || n > len(term) {
		n = 0
		if err == nil {
			err = errors.New("invalid write result")
		}
	}
	if n < len(term) && err == nil {
		err = io.ErrShortWrite
	}
	return n, err
}
