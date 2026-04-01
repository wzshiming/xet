package client

import (
	"io"
	"sync/atomic"
)

// ProgressFunc is called periodically with the number of bytes transferred so
// far (current) and the total expected bytes (total, 0 if unknown).
type ProgressFunc func(current, total int64)

type byteProgressTracker struct {
	transferBytes atomic.Int64
	report        func(transferBytes int64)
}

func newSessionProgressTracker(progress ProgressFunc, getTotal func() int64) *byteProgressTracker {
	return &byteProgressTracker{report: func(transferBytes int64) {
		progress(transferBytes, getTotal())
	}}
}

func (t *byteProgressTracker) AddTransferBytes(n int64) {
	if t == nil || n <= 0 || t.report == nil {
		return
	}
	t.transferBytes.Add(n)
	t.Report()
}

func (t *byteProgressTracker) Report() {
	if t == nil || t.report == nil {
		return
	}
	t.report(t.transferBytes.Load())
}

type readProgressReader struct {
	reader io.Reader
	onRead func(int64)
}

func (r *readProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.onRead != nil {
		r.onRead(int64(n))
	}
	return n, err
}

func wrapReaderWithReadProgress(reader io.Reader, onRead func(int64)) io.Reader {
	if reader == nil || onRead == nil {
		return reader
	}
	return &readProgressReader{reader: reader, onRead: onRead}
}
