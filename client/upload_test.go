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
	progress := func(current, total int64) {
		called = true
	}

	if got := session.WithProgress(progress); got != session {
		t.Fatalf("WithProgress should return the same session instance")
	}
	if session.progress == nil {
		t.Fatal("expected progress callback to be configured")
	}

	session.progress(0, 0)
	if !called {
		t.Fatal("expected configured progress callback to be invoked")
	}
}

func TestUploadProgressTrackerTransferBytes(t *testing.T) {
	type update struct{ current, total int64 }
	var updates []update
	tracker := newSessionProgressTracker(func(current, total int64) {
		updates = append(updates, update{current, total})
	}, func() int64 { return 50 })

	tracker.AddTransferBytes(11)
	tracker.AddTransferBytes(7)

	if len(updates) < 2 {
		t.Fatalf("expected at least 2 progress updates, got %d", len(updates))
	}
	last := updates[len(updates)-1]
	if last.current != 18 || last.total != 50 {
		t.Fatalf("unexpected tracker progress: %+v", last)
	}
}
