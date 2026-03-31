package client

import (
	"io"
	"sync/atomic"
)

type Progress struct {
	BytesRead        int64
	TotalBytes       int64
	TransferredBytes int64
}

type ProgressFunc func(Progress)

func newProgress(readBytes, totalBytes, transferredBytes int64) Progress {
	return Progress{
		BytesRead:        readBytes,
		TotalBytes:       totalBytes,
		TransferredBytes: transferredBytes,
	}
}

type byteProgressTracker struct {
	readBytes     atomic.Int64
	transferBytes atomic.Int64
	report        func(readBytes, transferBytes int64)
}

func newByteProgressTracker(report func(readBytes, transferBytes int64)) *byteProgressTracker {
	if report == nil {
		return nil
	}
	return &byteProgressTracker{report: report}
}

func newSessionProgressTracker[T any](progress func(T), build func(readBytes, transferBytes int64) T) *byteProgressTracker {
	if progress == nil {
		return nil
	}
	return newByteProgressTracker(func(readBytes, transferBytes int64) {
		progress(build(readBytes, transferBytes))
	})
}

func (t *byteProgressTracker) AddReadBytes(n int64) {
	if t == nil || n <= 0 || t.report == nil {
		return
	}
	t.readBytes.Add(n)
	t.Report()
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
	t.report(t.readBytes.Load(), t.transferBytes.Load())
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

func (t *byteProgressTracker) WrapReader(reader io.Reader) io.Reader {
	if t == nil {
		return reader
	}
	return wrapReaderWithReadProgress(reader, t.AddReadBytes)
}

func wrapReadersWithReadProgress(readers []io.Reader, onRead func(int64)) []io.Reader {
	if onRead == nil || len(readers) == 0 {
		return readers
	}

	wrapped := make([]io.Reader, len(readers))
	for i, reader := range readers {
		wrapped[i] = wrapReaderWithReadProgress(reader, onRead)
	}

	return wrapped
}

func (t *byteProgressTracker) WrapReaders(readers []io.Reader) []io.Reader {
	if t == nil {
		return readers
	}
	return wrapReadersWithReadProgress(readers, t.AddReadBytes)
}
