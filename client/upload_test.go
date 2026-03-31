package client

import "testing"

func TestUploadSessionWithConcurrency(t *testing.T) {
	session := NewClient().UploadSession()
	if session.concurrency != DefaultUploadConcurrency {
		t.Fatalf("unexpected default concurrency: got %d want %d", session.concurrency, DefaultUploadConcurrency)
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

func TestUploadSessionWithProgress(t *testing.T) {
	session := NewClient().UploadSession()
	called := false
	progress := func(progress Progress) {
		called = true
	}

	if got := session.WithProgress(progress); got != session {
		t.Fatalf("WithProgress should return the same session instance")
	}
	if session.progress == nil {
		t.Fatal("expected progress callback to be configured")
	}

	session.progress(Progress{})
	if !called {
		t.Fatal("expected configured progress callback to be invoked")
	}
}

func TestUploadProgressTrackerAggregatesReadAndUploadedBytes(t *testing.T) {
	var updates []Progress
	tracker := newByteProgressTracker(func(readBytes, transferBytes int64) {
		updates = append(updates, newProgress(readBytes, 0, transferBytes))
	})

	tracker.AddReadBytes(11)
	tracker.AddTransferBytes(7)

	if len(updates) < 2 {
		t.Fatalf("expected at least 2 progress updates, got %d", len(updates))
	}
	last := updates[len(updates)-1]
	if last.BytesRead != 11 || last.TransferredBytes != 7 {
		t.Fatalf("unexpected tracker progress: %+v", last)
	}
}
