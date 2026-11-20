// Package httpserve provides ServeContent, a drop-in for http.ServeContent
// that hands the frames of an io.WriterTo content straight to the response,
// leaving sendfile-capable contents (syscall.Conn, such as *os.File) and all
// others to net/http.
package httpserve

import (
	"errors"
	"io"
	"net/http"
	"syscall"
	"time"
)

// ServeContent is http.ServeContent, with the body of a content implementing
// io.WriterTo written by its WriteTo.
func ServeContent(w http.ResponseWriter, r *http.Request, name string, modtime time.Time, content io.ReadSeeker) {
	http.ServeContent(responseWriter{w}, r, name, modtime, content)
}

// responseWriter hands http.ServeContent's body copy to the content's WriteTo.
type responseWriter struct {
	http.ResponseWriter
}

// Unwrap lets http.ResponseController reach the wrapped writer.
func (w responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

// Flush keeps the wrapped writer's http.Flusher, which embedding would hide.
func (w responseWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ReadFrom leaves syscall.Conn sources, such as *os.File, for net/http to sendfile.
func (w responseWriter) ReadFrom(src io.Reader) (int64, error) {
	lr, _ := src.(*io.LimitedReader)
	r := src
	if lr != nil {
		r = lr.R
	}
	wt, ok := r.(io.WriterTo)
	if _, isConn := r.(syscall.Conn); !ok || isConn {
		return io.Copy(w.ResponseWriter, src)
	}
	if lr == nil {
		return wt.WriteTo(w.ResponseWriter)
	}
	if lr.N <= 0 {
		return 0, nil
	}
	n, err := wt.WriteTo(&limitWriter{w: w.ResponseWriter, n: lr.N})
	lr.N -= n
	if errors.Is(err, errLimit) {
		err = nil
	}
	return n, err
}

// errLimit stops a WriteTo once its limit is written.
var errLimit = errors.New("httpserve: write limit reached")

// limitWriter writes at most n more bytes to w.
type limitWriter struct {
	w io.Writer
	n int64
}

func (l *limitWriter) Write(p []byte) (int, error) {
	if l.n <= 0 {
		return 0, errLimit
	}
	p = p[:min(int64(len(p)), l.n)]
	m, err := l.w.Write(p)
	l.n -= int64(m)
	if err == nil && l.n == 0 {
		return m, errLimit
	}
	return m, err
}
