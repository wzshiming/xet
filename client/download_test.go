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

func TestWrapReaderWithProgress(t *testing.T) {
	var updates []Progress
	tracker := newByteProgressTracker(func(readBytes, transferBytes int64) {
		updates = append(updates, newProgress(readBytes, 5, transferBytes))
	})
	tracker.Report()
	reader := wrapReaderWithReadProgress(newChunkedStringReader("hello", 2), tracker.AddReadBytes)

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("unexpected content: %q", string(data))
	}
	if len(updates) < 2 {
		t.Fatalf("expected at least 2 progress updates, got %d", len(updates))
	}
	if updates[0].BytesRead != 0 || updates[0].TotalBytes != 5 {
		t.Fatalf("unexpected initial progress: %+v", updates[0])
	}
	last := updates[len(updates)-1]
	if last.BytesRead != 5 || last.TotalBytes != 5 {
		t.Fatalf("unexpected final progress: %+v", last)
	}
	if last.TransferredBytes != 0 {
		t.Fatalf("unexpected downloaded bytes in reader-only test: %+v", last)
	}
}

func TestProgressTrackerAggregatesDownloadedAndReadBytes(t *testing.T) {
	var updates []Progress
	tracker := newByteProgressTracker(func(readBytes, transferBytes int64) {
		updates = append(updates, newProgress(readBytes, 100, transferBytes))
	})

	tracker.Report()
	tracker.AddTransferBytes(7)
	tracker.AddReadBytes(3)

	if len(updates) < 3 {
		t.Fatalf("expected at least 3 progress updates, got %d", len(updates))
	}
	last := updates[len(updates)-1]
	if last.TotalBytes != 100 || last.TransferredBytes != 7 || last.BytesRead != 3 {
		t.Fatalf("unexpected aggregated progress: %+v", last)
	}
}

func TestNewSessionProgressTracker(t *testing.T) {
	var updates []Progress
	tracker := newSessionProgressTracker(func(progress Progress) {
		updates = append(updates, progress)
	}, func(readBytes, transferBytes int64) Progress {
		return newProgress(readBytes, 100, transferBytes)
	})

	tracker.AddTransferBytes(7)
	tracker.AddReadBytes(3)

	if len(updates) < 2 {
		t.Fatalf("expected at least 2 updates, got %d", len(updates))
	}
	last := updates[len(updates)-1]
	if last.TotalBytes != 100 || last.TransferredBytes != 7 || last.BytesRead != 3 {
		t.Fatalf("unexpected aggregated progress: %+v", last)
	}
}

func TestNilProgressTrackerWrapReader(t *testing.T) {
	reader := newChunkedStringReader("hello", 2)
	if wrapped := (*byteProgressTracker)(nil).WrapReader(reader); wrapped != reader {
		t.Fatal("expected nil tracker to leave reader unchanged")
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
