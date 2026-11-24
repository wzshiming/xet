package client

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"time"
)

// xet-core's retry_base_delay and retry_max_duration.
const (
	defaultRetryBaseDelay = 3 * time.Second
	maxRetryDelay         = 6 * time.Minute
)

// Tests replace these to observe the waits without sleeping.
var (
	retryJitter = rand.N[time.Duration]
	retrySleep  = sleepContext
)

// errRetriesExhausted heads the error of a request whose attempts all failed on the network or with a transient status.
var errRetriesExhausted = errors.New("network error")

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNetworkError(err error) bool {
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	if _, ok := errors.AsType[*url.Error](err); ok {
		return true
	}
	return false
}

// isRetryableStatus is xet-core's transient status: 408, 429 or any 5xx but 501, which is permanent.
func isRetryableStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusTooManyRequests ||
		statusCode >= 500 && statusCode < 600 && statusCode != http.StatusNotImplemented
}

func (c *Client) retryAttempts() int {
	return max(c.retries+1, 1)
}

// waitToRetry sleeps a random part of retry n's nominal delay: the base tripled n-1 times, at most maxRetryDelay.
func (c *Client) waitToRetry(ctx context.Context, n int) error {
	if c.retryDelay <= 0 {
		return nil
	}
	d := c.retryDelay
	for ; n > 1 && d < maxRetryDelay; n-- {
		d *= 3
	}
	return retrySleep(ctx, retryJitter(min(d, maxRetryDelay)))
}

func resetRequestBody(req *http.Request) error {
	if req.Body == nil {
		return nil
	}

	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return err
		}
		req.Body = body
		return nil
	}

	return fmt.Errorf("request body is not retryable")
}

func (c *Client) doWithNetworkRetry(req *http.Request) (*http.Response, error) {
	var resp *http.Response
	err := c.doWithRetry(req, func(r *http.Response) error {
		resp = r
		return nil
	})
	return resp, err
}

// doWithRetry sends req until a response is not a transient failure and hands it to handle, which owns its body and may return a *retryableShardUploadError to try again.
func (c *Client) doWithRetry(req *http.Request, handle func(*http.Response) error) error {
	attempts := c.retryAttempts()

	var lastErr error
	for i := range attempts {
		if i > 0 {
			if err := c.waitToRetry(req.Context(), i); err != nil {
				return fmt.Errorf("%w while waiting to retry: %v", err, lastErr)
			}
			ctx, err := c.renewCASContext(req.Context())
			if err != nil {
				return fmt.Errorf("%w while resolving to retry: %v", err, lastErr)
			}
			req = req.WithContext(ctx)
			if err := resetRequestBody(req); err != nil {
				return fmt.Errorf("reset request body: %w", err)
			}
		}

		resp, err := c.do(req)
		switch {
		case err != nil:
			if !isNetworkError(err) {
				return fmt.Errorf("do request: %w", err)
			}
		case isRetryableStatus(resp.StatusCode):
			resp.Body.Close()
			err = fmt.Errorf("server error status %s", resp.Status)
		default:
			err = handle(resp)
			if _, ok := errors.AsType[*retryableShardUploadError](err); !ok {
				return err
			}
		}
		lastErr = err
		if req.Context().Err() != nil {
			break
		}
	}

	if _, ok := errors.AsType[*retryableShardUploadError](lastErr); ok {
		return fmt.Errorf("v2 shard upload failed after %d attempts: %w", attempts, lastErr)
	}
	return fmt.Errorf("%w after %d attempts: %w", errRetriesExhausted, attempts, lastErr)
}
