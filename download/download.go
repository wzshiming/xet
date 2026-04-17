package download

import (
	"github.com/wzshiming/xet/progress"
)

// Option is a functional option for NewReaderV1 and NewReaderV2.
type Option func(*options)

type options struct {
	concurrency     int
	retries         int
	progressFunc    progress.ProgressFunc
	persistentCache *ChunkCache
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func WithConcurrency(concurrency int) Option {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithRetries configures how many times xorb range prefetch should retry when
// stream reads fail with transient truncation errors (for example unexpected EOF).
func WithRetries(retries int) Option {
	return func(o *options) {
		if retries < 0 {
			retries = 0
		}
		o.retries = retries
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

// WithChunkCache sets a persistent, cross-file chunk cache that is reused
// across multiple downloads. Decoded chunks are stored on disk keyed by
// (xorbHash, absoluteChunkIndex). On a cache hit the network download and
// decoding are skipped entirely.
func WithChunkCache(cache *ChunkCache) Option {
	return func(o *options) {
		o.persistentCache = cache
	}
}
