package client

import (
	"io"
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

func TestDownloadSessionWithProgress(t *testing.T) {
	session := NewClient().DownloadSession()
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

func TestNewSessionProgressTrackerNilProgress(t *testing.T) {
	tracker := newSessionProgressTracker(nil, func() int64 { return 0 })
	if tracker != nil {
		t.Fatal("expected nil tracker when progress is nil")
	}
}

type chunkedStringReader struct {
	remaining string
	chunkSize int
}

func newChunkedStringReader(value string, chunkSize int) *chunkedStringReader {
	return &chunkedStringReader{remaining: value, chunkSize: chunkSize}
}

func (r *chunkedStringReader) Read(p []byte) (int, error) {
	if len(r.remaining) == 0 {
		return 0, io.EOF
	}
	limit := len(r.remaining)
	if r.chunkSize > 0 && limit > r.chunkSize {
		limit = r.chunkSize
	}
	n := copy(p, r.remaining[:limit])
	r.remaining = r.remaining[n:]
	if len(r.remaining) == 0 {
		return n, io.EOF
	}
	return n, nil
}
