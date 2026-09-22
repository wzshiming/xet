package client

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
)

// statusTransport answers every request with the statuses in turn, then 200.
func statusTransport(attempts *atomic.Int32, statuses ...int) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		status := http.StatusOK
		if n := int(attempts.Add(1)); n <= len(statuses) {
			status = statuses[n-1]
		}
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: http.Header{}, Body: http.NoBody, Request: r}, nil
	})
}

func TestRetryableStatusesRetried(t *testing.T) {
	for _, status := range []int{408, 429, 500, 502, 503, 504} {
		var attempts atomic.Int32
		c, err := NewClient(WithHTTPClient(&http.Client{Transport: statusTransport(&attempts, status)}), WithBaseURL("http://cas"), WithRetries(1))
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
