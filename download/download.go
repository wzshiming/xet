package download

type options struct {
	concurrency int
	retries     int
}

// WithConcurrency configures how many xorb ranges are prefetched concurrently.
func WithConcurrency(concurrency int) func(*options) {
	return func(o *options) {
		o.concurrency = concurrency
	}
}

// WithRetries configures how many times xorb range prefetch should retry when
// stream reads fail with transient truncation errors (for example unexpected EOF).
func WithRetries(retries int) func(*options) {
	return func(o *options) {
		if retries < 0 {
			retries = 0
		}
		o.retries = retries
	}
}

func (o *options) concurrencyValue() int {
	if o == nil || o.concurrency <= 0 {
		return 1
	}
	return o.concurrency
}
