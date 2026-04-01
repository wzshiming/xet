package client

import (
	"testing"
)

func TestDownloadSessionWithConcurrency(t *testing.T) {
	session := NewClient().DownloadSession()
	if session.concurrency != DefaultDownloadConcurrency {
		t.Fatalf("unexpected default concurrency: got %d want %d", session.concurrency, DefaultDownloadConcurrency)
	}

	if got := session.WithConcurrency(8); got != session {
		t.Fatalf("WithConcurrency should return the same session instance")
	}
	if session.concurrency != 8 {
		t.Fatalf("unexpected configured concurrency: got %d want 8", session.concurrency)
	}

	session.WithConcurrency(0)
	if session.concurrency != 1 {
		t.Fatalf("non-positive concurrency should clamp to 1, got %d", session.concurrency)
	}
}

func TestProgressTrackerTransferBytes(t *testing.T) {
	type update struct{ current, total int64 }
	var updates []update
	total := int64(100)
	tracker := newSessionProgressTracker(func(current, tot int64) {
		updates = append(updates, update{current, tot})
	}, func() int64 { return total })

	tracker.Report()
	tracker.AddTransferBytes(7)
	tracker.AddTransferBytes(3)

	if len(updates) < 3 {
		t.Fatalf("expected at least 3 progress updates, got %d", len(updates))
	}
	if updates[0].current != 0 || updates[0].total != 100 {
		t.Fatalf("unexpected initial progress: %+v", updates[0])
	}
	last := updates[len(updates)-1]
	if last.current != 10 || last.total != 100 {
		t.Fatalf("unexpected final progress: %+v", last)
	}
}
