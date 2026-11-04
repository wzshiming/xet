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
	memoryLimit  int64
	expectedHash *xet.FileHash
}

// WithCacheManager stores fetched xorb ranges in cache's directory, where
// later readers reuse them; share one CacheManager across readers so the
// capacity bound applies to the whole cache directory. Without it, or with
// nil, a reader starts fetching on its first Read, fetches at most
// WithConcurrency ranges ahead of the one being read, and skips decoding
// chunks no term reads. Either way a reader keeps each decoded chunk only
// until the last term reading it has read it, within WithMemoryLimit.
func WithCacheManager(cache *CacheManager) Option {
	return func(o *options) {
		o.cache = cache
	}
}

// DefaultMemoryLimit is how many decoded bytes a reader holds in memory
// unless WithMemoryLimit sets another bound.
const DefaultMemoryLimit int64 = 64 << 20

// WithMemoryLimit bounds the decoded bytes a reader holds in memory. Past it,
// chunks leave memory, first those a file also holds, the cache entry or the
// *os.File WriteTo wrote them to, then those read again furthest ahead, which
// wait in a temporary file. Zero or negative keeps every chunk in memory.
func WithMemoryLimit(limit int64) Option {
	return func(o *options) {
		o.memoryLimit = limit
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
