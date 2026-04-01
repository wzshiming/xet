package progress

import (
	"io"
)

// ProgressFunc is a callback function type for tracking progress of uploads or downloads.
type ProgressFunc func(name string, current, total int64)

type progressWrapper struct {
	r            io.Reader
	name         string
	progressFunc ProgressFunc
	current      int64
	total        int64
}

// NewProgressReader wraps an io.Reader with a ProgressFunc callback to track read progress.
func NewProgressReader(r io.Reader, name string, total int64, progressFunc ProgressFunc) io.Reader {
	return &progressWrapper{
		r:            r,
		name:         name,
		total:        total,
		progressFunc: progressFunc,
	}
}

func (pw *progressWrapper) Read(p []byte) (n int, err error) {
	n, err = pw.r.Read(p)
	if n > 0 {
		pw.current += int64(n)
		if pw.progressFunc != nil {
			pw.progressFunc(pw.name, pw.current, pw.total)
		}
	}
	return n, err
}
