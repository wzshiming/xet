package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

// statusTransport answers every request with the statuses in turn, then 200.
func statusTransport(attempts *atomic.Int32, statuses ...int) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		status := http.StatusOK
		if n := int(attempts.Add(1)); n <= len(statuses) {
			status = statuses[n-1]
		}
		return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: http.Header{}, Body: http.NoBody, Request: r}, nil
	})
}

func TestRetryableStatusesRetried(t *testing.T) {
	for _, status := range []int{408, 429, 500, 502, 503, 504} {
		var attempts atomic.Int32
		c, err := NewClient(WithHTTPClient(&http.Client{Transport: statusTransport(&attempts, status)}), WithBaseURL("http://cas"), WithRetries(1), WithRetryBackoff(0))
		if err != nil {
			t.Fatal(err)
		}
		ok, err := c.HasXorb(t.Context(), xet.XorbHash{})
		if err != nil || !ok {
			t.Fatalf("%d: HasXorb = %v, %v; want true after one retry", status, ok, err)
		}
		if n := attempts.Load(); n != 2 {
			t.Fatalf("%d: attempts = %d, want 2", status, n)
		}
	}
}

func TestTerminalStatusesNotRetried(t *testing.T) {
	for _, status := range []int{401, 403, 404} {
		var attempts atomic.Int32
		c, err := NewClient(WithHTTPClient(&http.Client{Transport: statusTransport(&attempts, status, status)}), WithBaseURL("http://cas"), WithRetries(1))
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.GetReconstructionV1(context.Background(), xet.FileHash{}, nil)
		if err == nil || (status == 404) != errors.Is(err, errNotFound) {
			t.Fatalf("%d: err = %v", status, err)
		}
		if n := attempts.Load(); n != 1 {
			t.Fatalf("%d: attempts = %d, want 1", status, n)
		}
	}
}

// TestRetryDelayBounds checks the jittered exponential schedule: [d/2, d] with
// d doubling from the base up to the 5s cap, and no wait when disabled.
func TestRetryDelayBounds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     []Options
		retry    int
		min, max time.Duration
	}{
		{"default first retry", nil, 0, 250 * time.Millisecond, 500 * time.Millisecond},
		{"default second retry", nil, 1, 500 * time.Millisecond, time.Second},
		{"default reaches cap", nil, 4, 2500 * time.Millisecond, 5 * time.Second},
		{"huge retry index", nil, 1 << 30, 2500 * time.Millisecond, 5 * time.Second},
		{"base above cap", []Options{WithRetryBackoff(time.Duration(math.MaxInt64))}, 3, 2500 * time.Millisecond, 5 * time.Second},
		{"custom base", []Options{WithRetryBackoff(40 * time.Millisecond)}, 2, 80 * time.Millisecond, 160 * time.Millisecond},
		{"disabled", []Options{WithRetryBackoff(0)}, 3, 0, 0},
		{"negative disables", []Options{WithRetryBackoff(-time.Second)}, 0, 0, 0},
	} {
		c, err := NewClient(tc.opts...)
		if err != nil {
			t.Fatal(err)
		}
		for range 64 {
			if d := c.retryDelay(tc.retry); d < tc.min || d > tc.max {
				t.Fatalf("%s: retryDelay(%d) = %v, want within [%v, %v]", tc.name, tc.retry, d, tc.min, tc.max)
			}
		}
	}
}

// TestRetryWaitStopsWithContext ends the request context during an hour-long
// backoff on each retry path: the call returns promptly, reports the context
// error together with the failure being retried, and makes no further attempt.
func TestRetryWaitStopsWithContext(t *testing.T) {
	failing := roundTripFunc(func(r *http.Request) (*http.Response, error) { return nil, io.ErrUnexpectedEOF })
	for _, tc := range []struct {
		name      string
		transport func(*atomic.Int32) http.RoundTripper
		call      func(context.Context, *Client) error
		original  error
	}{
		{"request loop status", func(n *atomic.Int32) http.RoundTripper { return statusTransport(n, 503, 503, 503) },
			func(ctx context.Context, c *Client) error { _, err := c.HasXorb(ctx, xet.XorbHash{}); return err }, nil},
		{"request loop network", func(n *atomic.Int32) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) { n.Add(1); return failing.RoundTrip(r) })
		}, func(ctx context.Context, c *Client) error { _, err := c.HasXorb(ctx, xet.XorbHash{}); return err }, io.ErrUnexpectedEOF},
		{"xorb status", func(n *atomic.Int32) http.RoundTripper { return statusTransport(n, 503, 503, 503) },
			func(ctx context.Context, c *Client) error {
				_, err := c.DownloadXorbWithURL(ctx, "http://cas/xorb", nil)
				return err
			}, nil},
		{"xorb network", func(n *atomic.Int32) http.RoundTripper {
			return roundTripFunc(func(r *http.Request) (*http.Response, error) { n.Add(1); return failing.RoundTrip(r) })
		}, func(ctx context.Context, c *Client) error {
			_, err := c.DownloadXorbWithURL(ctx, "http://cas/xorb", nil)
			return err
		}, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32
			c, err := NewClient(WithHTTPClient(&http.Client{Transport: tc.transport(&attempts)}), WithBaseURL("http://cas"), WithRetryBackoff(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			began := time.Now()
			err = tc.call(ctx, c)
			if took := time.Since(began); took > 2*time.Second {
				t.Fatalf("returned after %v, want the wait cut short by the context", took)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want the context deadline reported", err)
			}
			if tc.original != nil && !errors.Is(err, tc.original) {
				t.Fatalf("err = %v, want the retried failure %v kept", err, tc.original)
			}
			if tc.original == nil && !strings.Contains(err.Error(), "503") {
				t.Fatalf("err = %v, want the retried status kept", err)
			}
			if n := attempts.Load(); n != 1 {
				t.Fatalf("attempts = %d, want 1", n)
			}
		})
	}
}
