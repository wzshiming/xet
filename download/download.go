package download

import (
	"github.com/wzshiming/xet/progress"
)

// Option is a functional option for NewReaderV1 and NewReaderV2.
type Option func(*options)

type options struct {
	concurrency  int
	progressFunc progress.ProgressFunc
	cache        *CacheManager
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
