package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestComputeUsageReturnsWalkerError(t *testing.T) {
	boom := errors.New("boom")
	got, err := ComputeUsage(context.Background(), func(_ context.Context, kind string, fn func(string, int64, time.Time) error) error {
		if kind == "shards" {
			return boom
		}
		return fn("h", 7, time.Time{})
	})
	if !errors.Is(err, boom) || got != (Usage{}) {
		t.Fatalf("ComputeUsage = %+v, %v; want zero usage and %v", got, err, boom)
	}
}
