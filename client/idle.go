package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultIdleTimeout is the default read-idle timeout for GET and HEAD requests.
const DefaultIdleTimeout = 60 * time.Second

// NewIdleTimeoutTransport fails GET/HEAD requests that receive no headers or body bytes for timeout; <= 0 returns base.
func NewIdleTimeoutTransport(base http.RoundTripper, timeout time.Duration) http.RoundTripper {
	if timeout <= 0 {
		return base
	}
	if base == nil {
		base = http.DefaultTransport
	}
	return &idleTimeoutTransport{base: base, timeout: timeout}
}

type idleTimeoutTransport struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *idleTimeoutTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		return t.base.RoundTrip(req)
	}
	g := &idleGuard{timeout: t.timeout, parent: req.Context()}
	ctx, cancel := context.WithCancel(g.parent)
	g.cancel = cancel

	timer := g.arm()
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err = g.wrap(g.disarm(timer), err); err != nil {
		cancel()
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, err
	}
	if req.Method == http.MethodHead || resp.Body == nil || resp.Body == http.NoBody {
		cancel()
		return resp, nil
	}
	resp.Body = &idleBody{ReadCloser: resp.Body, guard: g}
	return resp, nil
}

// idleGuard cancels one request's child context when a blocking header wait or body read stays idle.
type idleGuard struct {
	timeout time.Duration
	parent  context.Context
	cancel  context.CancelFunc
}

type armedTimer struct {
	*time.Timer
	done chan struct{}
}

func (g *idleGuard) arm() armedTimer {
	done := make(chan struct{})
	return armedTimer{time.AfterFunc(g.timeout, func() { g.cancel(); close(done) }), done}
}

// disarm reports whether the deadline won, first waiting for that callback's cancel to finish.
func (g *idleGuard) disarm(t armedTimer) bool {
	if t.Stop() {
		return false
	}
	<-t.done
	return true
}

// wrap tags a deadline-won outcome as an idle timeout; parent cancellation keeps its own cause.
func (g *idleGuard) wrap(fired bool, err error) error {
	if !fired || g.parent.Err() != nil {
		return err
	}
	return &idleTimeoutError{timeout: g.timeout, err: err}
}

type idleBody struct {
	io.ReadCloser
	guard *idleGuard
	err   error // sticky once the deadline won
}

func (b *idleBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if len(p) == 0 {
		return b.ReadCloser.Read(p)
	}
	timer := b.guard.arm()
	n, err := b.ReadCloser.Read(p)
	if fired := b.guard.disarm(timer); fired || err != nil {
		b.guard.cancel()
		if err != io.EOF {
			err = b.guard.wrap(fired, err)
		}
		if fired {
			b.err = err
		}
	}
	return n, err
}

func (b *idleBody) Close() error {
	b.guard.cancel()
	return b.ReadCloser.Close()
}

type idleTimeoutError struct {
	timeout time.Duration
	err     error
}

func (e *idleTimeoutError) Error() string {
	if e.err == nil {
		return fmt.Sprintf("no data received for %v", e.timeout)
	}
	return fmt.Sprintf("no data received for %v: %v", e.timeout, e.err)
}

func (e *idleTimeoutError) Unwrap() error { return e.err }

func (e *idleTimeoutError) Is(target error) bool { return target == context.DeadlineExceeded }

func (e *idleTimeoutError) Timeout() bool { return true }
