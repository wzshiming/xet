package httpserve

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// testFrames lays the test content out as frames [0, 5), [5, 14), [14, 30) and [30, 40).
var testFrames = []int{5, 9, 16, 10}

const testETag = `"v1"`

// frameContent serves the bytes of frames of the given sizes. Its WriteTo
// writes the rest of the frame holding the position, then each later frame,
// one Write each, recording their lengths and stopping at an error.
type frameContent struct {
	data     []byte
	sizes    []int
	pos      int64
	writeTos int
	writes   []int
}

func newFrameContent(sizes []int) *frameContent {
	c := &frameContent{sizes: sizes}
	for _, n := range sizes {
		for range n {
			c.data = append(c.data, byte('a'+len(c.data)%26))
		}
	}
	return c
}

func (c *frameContent) Read(p []byte) (int, error) {
	if c.pos >= int64(len(c.data)) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.pos:])
	c.pos += int64(n)
	return n, nil
}

func (c *frameContent) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		offset += c.pos
	case io.SeekEnd:
		offset += int64(len(c.data))
	}
	if offset < 0 {
		return 0, errors.New("seek to a negative position")
	}
	c.pos = offset
	return offset, nil
}

func (c *frameContent) WriteTo(w io.Writer) (int64, error) {
	c.writeTos++
	var n, end int64
	for _, size := range c.sizes {
		end += int64(size)
		if c.pos >= end {
			continue
		}
		p := c.data[c.pos:end]
		c.writes = append(c.writes, len(p))
		m, err := w.Write(p)
		c.pos += int64(m)
		n += int64(m)
		if err != nil {
			return n, err
		}
		if m < len(p) {
			return n, io.ErrShortWrite
		}
	}
	return n, nil
}

// hideWriteTo leaves the content it wraps only Read and Seek.
type hideWriteTo struct{ io.ReadSeeker }

// bodyKind is a content the parity table serves.
type bodyKind struct {
	name     string
	sizes    []int
	writerTo bool
}

var bodyKinds = []bodyKind{
	{"frames", testFrames, true},
	{"frames without WriteTo", testFrames, false},
	{"empty", nil, true},
	{"empty without WriteTo", nil, false},
}

// open returns a fresh content of k and the frameContent behind it.
func (k bodyKind) open() (io.ReadSeeker, *frameContent) {
	c := newFrameContent(k.sizes)
	if k.writerTo {
		return c, c
	}
	return hideWriteTo{c}, c
}

// requestShapes are the requests the parity table sends, with the Writes
// WriteTo makes into the response for the frames content; nil where it must
// not run.
var requestShapes = []struct {
	name   string
	method string
	header http.Header
	writes []int
}{
	{"GET", http.MethodGet, nil, testFrames},
	{"bytes=10-19", http.MethodGet, http.Header{"Range": {"bytes=10-19"}}, []int{4, 16}},
	{"bytes=10-", http.MethodGet, http.Header{"Range": {"bytes=10-"}}, []int{4, 16, 10}},
	{"bytes=-5", http.MethodGet, http.Header{"Range": {"bytes=-5"}}, []int{5}},
	{"HEAD", http.MethodHead, nil, nil},
	{"If-None-Match", http.MethodGet, http.Header{"If-None-Match": {testETag}}, nil},
	{"If-Range hit", http.MethodGet, http.Header{"Range": {"bytes=10-19"}, "If-Range": {testETag}}, []int{4, 16}},
	{"If-Range miss", http.MethodGet, http.Header{"Range": {"bytes=10-19"}, "If-Range": {`"v0"`}}, testFrames},
	{"unsatisfiable", http.MethodGet, http.Header{"Range": {"bytes=40-50"}}, nil},
}

// serveWith serves content through serve under the headers the server's handlers set.
func serveWith(serve func(http.ResponseWriter, *http.Request, string, time.Time, io.ReadSeeker), content io.ReadSeeker) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("ETag", testETag)
		serve(w, r, "content", time.Time{}, content)
	})
}

// observed is what the parity table compares of a response.
type observed struct {
	status int
	header http.Header
	body   []byte
}

// observe reads res, dropping Date and renaming the multipart boundary, which
// ServeContent draws at random, BOUNDARY.
func observe(t *testing.T, res *http.Response) observed {
	t.Helper()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	h := res.Header.Clone()
	h.Del("Date")
	ctype := h.Get("Content-Type")
	if mt, params, err := mime.ParseMediaType(ctype); err == nil && mt == "multipart/byteranges" && params["boundary"] != "" {
		boundary := params["boundary"]
		h.Set("Content-Type", strings.ReplaceAll(ctype, boundary, "BOUNDARY"))
		body = bytes.ReplaceAll(body, []byte(boundary), []byte("BOUNDARY"))
	}
	return observed{res.StatusCode, h, body}
}

// matchNetHTTP fails t unless got, ServeContent's answer, is want,
// http.ServeContent's.
func matchNetHTTP(t *testing.T, got, want observed) {
	t.Helper()
	if got.status != want.status || !maps.EqualFunc(got.header, want.header, slices.Equal) || !bytes.Equal(got.body, want.body) {
		t.Errorf("ServeContent answered %d %v with %d bytes %.256q; want http.ServeContent's %d %v with %d bytes %.256q",
			got.status, got.header, len(got.body), got.body, want.status, want.header, len(want.body), want.body)
	}
}

// carriers take a request to a handler through a recorder or over TCP.
var carriers = []struct {
	name string
	do   func(t *testing.T, h http.Handler, method string, header http.Header) observed
}{
	{"recorder", viaRecorder},
	{"server", viaServer},
}

func viaRecorder(t *testing.T, h http.Handler, method string, header http.Header) observed {
	t.Helper()
	req := httptest.NewRequest(method, "/content", nil)
	maps.Copy(req.Header, header)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return observe(t, rec.Result())
}

// viaServer returns once its server has closed, so the handler is done.
func viaServer(t *testing.T, h http.Handler, method string, header http.Header) observed {
	t.Helper()
	srv := httptest.NewServer(h)
	defer srv.Close()
	req, err := http.NewRequest(method, srv.URL+"/content", nil)
	if err != nil {
		t.Fatal(err)
	}
	maps.Copy(req.Header, header)
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	return observe(t, res)
}

// ServeContent answers each request on each content as http.ServeContent
// does, through a recorder and over TCP: same status, headers but Date, and
// body. Its copy runs a WriterTo content's WriteTo once, whose Writes are the
// frames from the range's start; never for a body-less answer.
func TestServeContentMatchesNetHTTP(t *testing.T) {
	for _, via := range carriers {
		for _, k := range bodyKinds {
			for _, rq := range requestShapes {
				t.Run(via.name+"/"+k.name+"/"+rq.name, func(t *testing.T) {
					content, c := k.open()
					std, _ := k.open()
					got := via.do(t, serveWith(ServeContent, content), rq.method, rq.header)
					want := via.do(t, serveWith(http.ServeContent, std), rq.method, rq.header)
					matchNetHTTP(t, got, want)
					wantWrites, wantCalls := rq.writes, 1
					if !k.writerTo || len(k.sizes) == 0 || wantWrites == nil {
						wantWrites, wantCalls = nil, 0
					}
					if c.writeTos != wantCalls || !slices.Equal(c.writes, wantWrites) {
						t.Errorf("ServeContent ran WriteTo %d times, making Writes of %v bytes; want %d with %v", c.writeTos, c.writes, wantCalls, wantWrites)
					}
				})
			}
		}
	}
}

// twoRanges asks for two ranges. http.ServeContent answers them with
// multipart/byteranges, or with the whole content if GODEBUG
// httpservecontentmaxranges allows fewer, as its default of 1 under a go.mod
// before go 1.26 does; httpservecontentmaxranges=0 lifts the limit.
var twoRanges = http.Header{"Range": {"bytes=0-4,10-14"}}

// ServeContent answers two ranges as http.ServeContent does, through a
// recorder and over TCP, under the default GODEBUG and with no limit on
// ranges; then a content's answer is multipart/byteranges, whose parts
// net/http copies through a pipe, so WriteTo never runs.
func TestServeContentMultipleRangesMatchesNetHTTP(t *testing.T) {
	for _, via := range carriers {
		for _, k := range bodyKinds {
			t.Run(via.name+"/"+k.name+"/default GODEBUG", func(t *testing.T) {
				content, _ := k.open()
				std, _ := k.open()
				got := via.do(t, serveWith(ServeContent, content), http.MethodGet, twoRanges)
				want := via.do(t, serveWith(http.ServeContent, std), http.MethodGet, twoRanges)
				matchNetHTTP(t, got, want)
			})
			if len(k.sizes) == 0 {
				continue // an empty content has no parts to answer with
			}
			t.Run(via.name+"/"+k.name+"/httpservecontentmaxranges=0", func(t *testing.T) {
				t.Setenv("GODEBUG", "httpservecontentmaxranges=0")
				content, c := k.open()
				std, _ := k.open()
				got := via.do(t, serveWith(ServeContent, content), http.MethodGet, twoRanges)
				want := via.do(t, serveWith(http.ServeContent, std), http.MethodGet, twoRanges)
				if mt, _, _ := mime.ParseMediaType(want.header.Get("Content-Type")); want.status != http.StatusPartialContent || mt != "multipart/byteranges" {
					t.Fatalf("http.ServeContent answered two ranges with %d %q; want 206 multipart/byteranges", want.status, want.header.Get("Content-Type"))
				}
				matchNetHTTP(t, got, want)
				if c.writeTos != 0 {
					t.Errorf("ServeContent ran WriteTo %d times for a multipart answer; want never", c.writeTos)
				}
			})
		}
	}
}

// readFromSpy is an http.ResponseWriter that records each source of its
// ReadFrom and hands it on to the ReadFrom of the net/http writer it wraps.
type readFromSpy struct {
	http.ResponseWriter
	srcs []io.Reader
}

func (w *readFromSpy) ReadFrom(src io.Reader) (int64, error) {
	w.srcs = append(w.srcs, src)
	return w.ResponseWriter.(io.ReaderFrom).ReadFrom(src)
}

// ServeContent leaves an *os.File, though an io.WriterTo, to net/http, which
// can sendfile it: over TCP it answers as http.ServeContent does, its copy
// handing the file under its limit to the inner writer's ReadFrom.
func TestServeContentLeavesFilesToNetHTTP(t *testing.T) {
	data := make([]byte, 300<<10)
	for i := range data {
		data[i] = byte(i % 251)
	}
	name := filepath.Join(t.TempDir(), "content")
	if err := os.WriteFile(name, data, 0o600); err != nil {
		t.Fatal(err)
	}
	open := func() *os.File {
		f, err := os.Open(name)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return f
	}

	f := open()
	spy := &readFromSpy{}
	got := viaServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.ResponseWriter = w
		serveWith(ServeContent, f).ServeHTTP(spy, r)
	}), http.MethodGet, nil)
	matchNetHTTP(t, got, viaServer(t, serveWith(http.ServeContent, open()), http.MethodGet, nil))
	if len(spy.srcs) != 1 {
		t.Fatalf("ServeContent called the inner ReadFrom %d times; want once, with the file for net/http to sendfile", len(spy.srcs))
	}
	if lr, ok := spy.srcs[0].(*io.LimitedReader); !ok || lr.R != f {
		t.Errorf("the inner ReadFrom got %#v; want an *io.LimitedReader over the file", spy.srcs[0])
	}
}

var errInnerWrite = errors.New("injected response write failure")

// sizeRecorder is an http.ResponseWriter without ReadFrom that records the
// length of each Write; its Write number failOn fails with errInnerWrite.
type sizeRecorder struct {
	*httptest.ResponseRecorder
	writes []int
	failOn int
}

func newSizeRecorder() *sizeRecorder {
	return &sizeRecorder{ResponseRecorder: httptest.NewRecorder()}
}

func (w *sizeRecorder) Write(p []byte) (int, error) {
	w.writes = append(w.writes, len(p))
	if len(w.writes) == w.failOn {
		return 0, errInnerWrite
	}
	return w.ResponseRecorder.Write(p)
}

// readFromRecorder is a sizeRecorder whose ReadFrom records each source.
type readFromRecorder struct {
	*sizeRecorder
	srcs []io.Reader
}

func (w *readFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	w.srcs = append(w.srcs, src)
	return io.Copy(w.sizeRecorder, src)
}

// ReadFrom hands a source without WriteTo, limited or not, as it is to the
// inner writer's ReadFrom, where net/http looks for an *os.File to sendfile.
func TestReadFromPassesSourceWithoutWriteTo(t *testing.T) {
	data := newFrameContent(testFrames).data
	for _, c := range []struct {
		name string
		src  io.Reader
		want []byte
	}{
		{"limited", &io.LimitedReader{R: &hideWriteTo{newFrameContent(testFrames)}, N: 12}, data[:12]},
		{"unlimited", &hideWriteTo{newFrameContent(testFrames)}, data},
	} {
		t.Run(c.name, func(t *testing.T) {
			inner := &readFromRecorder{sizeRecorder: newSizeRecorder()}
			var rf io.ReaderFrom = responseWriter{inner}
			n, err := rf.ReadFrom(c.src)
			if n != int64(len(c.want)) || err != nil || !bytes.Equal(inner.Body.Bytes(), c.want) {
				t.Fatalf("ReadFrom = %d, %v with %q written; want %d, nil and %q", n, err, inner.Body.Bytes(), len(c.want), c.want)
			}
			if len(inner.srcs) != 1 || inner.srcs[0] != c.src {
				t.Errorf("the inner ReadFrom got %d sources, %v; want only the one given, %T %v", len(inner.srcs), inner.srcs, c.src, c.src)
			}
		})
	}
}

// connContent is a frameContent that is also a syscall.Conn, as an *os.File
// is, which net/http can sendfile.
type connContent struct{ *frameContent }

func (connContent) SyscallConn() (syscall.RawConn, error) { return nil, nil }

// ReadFrom hands a limited WriterTo that is also a syscall.Conn as it is to
// the inner writer's ReadFrom, where net/http looks for a file to sendfile,
// and never runs its WriteTo.
func TestReadFromPassesSyscallConnSource(t *testing.T) {
	c := newFrameContent(testFrames)
	src := &io.LimitedReader{R: connContent{c}, N: 12}
	inner := &readFromRecorder{sizeRecorder: newSizeRecorder()}
	var rf io.ReaderFrom = responseWriter{inner}
	n, err := rf.ReadFrom(src)
	if n != 12 || err != nil || !bytes.Equal(inner.Body.Bytes(), c.data[:12]) {
		t.Fatalf("ReadFrom = %d, %v with %q written; want 12, nil and %q", n, err, inner.Body.Bytes(), c.data[:12])
	}
	if len(inner.srcs) != 1 || inner.srcs[0] != src {
		t.Errorf("the inner ReadFrom got %d sources, %v; want only the *io.LimitedReader given", len(inner.srcs), inner.srcs)
	}
	if c.writeTos != 0 {
		t.Errorf("ReadFrom ran the WriteTo of a syscall.Conn %d times; want never", c.writeTos)
	}
}

// ReadFrom of a limit of zero or less returns 0, nil without running WriteTo
// or writing.
func TestReadFromEmptyLimitWritesNothing(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		c := newFrameContent(testFrames)
		inner := newSizeRecorder()
		var rf io.ReaderFrom = responseWriter{inner}
		if n, err := rf.ReadFrom(&io.LimitedReader{R: c, N: limit}); n != 0 || err != nil || c.writeTos != 0 || len(inner.writes) != 0 {
			t.Errorf("ReadFrom of a limit of %d = %d, %v after %d WriteTo calls and %d Writes; want 0, nil and neither", limit, n, err, c.writeTos, len(inner.writes))
		}
	}
}

// ReadFrom of a limited WriterTo writes its frames up to the limit, cut
// there, and returns what it wrote with no error and the limit used up; it
// asks WriteTo for no Write past the limit.
func TestReadFromLimitsWriteTo(t *testing.T) {
	for _, c := range []struct {
		limit, n, left int64
		attempts       []int // Writes WriteTo makes
		writes         []int // Writes the inner writer gets
	}{
		{12, 12, 0, []int{5, 9}, []int{5, 7}}, // inside frame 1
		{14, 14, 0, []int{5, 9}, []int{5, 9}}, // at frame 1's end
		{40, 40, 0, testFrames, testFrames},   // at the content's end
		{100, 40, 60, testFrames, testFrames}, // past it
	} {
		t.Run(fmt.Sprintf("N=%d", c.limit), func(t *testing.T) {
			content := newFrameContent(testFrames)
			inner := newSizeRecorder()
			lr := &io.LimitedReader{R: content, N: c.limit}
			var rf io.ReaderFrom = responseWriter{inner}
			n, err := rf.ReadFrom(lr)
			if n != c.n || err != nil || lr.N != c.left {
				t.Fatalf("ReadFrom of a limit of %d = %d, %v leaving %d; want %d, nil leaving %d", c.limit, n, err, lr.N, c.n, c.left)
			}
			if want := content.data[:c.n]; !bytes.Equal(inner.Body.Bytes(), want) || !slices.Equal(inner.writes, c.writes) {
				t.Errorf("the inner writer got Writes of %v bytes, %q; want %v, %q", inner.writes, inner.Body.Bytes(), c.writes, want)
			}
			if content.writeTos != 1 || !slices.Equal(content.writes, c.attempts) {
				t.Errorf("WriteTo ran %d times, making Writes of %v bytes; want once with %v, none past the limit", content.writeTos, content.writes, c.attempts)
			}
		})
	}
}

// ReadFrom passes on the inner writer's error with the bytes written before
// it, limited or not.
func TestReadFromPassesWriteErrors(t *testing.T) {
	for _, limited := range []bool{true, false} {
		t.Run(fmt.Sprintf("limited=%t", limited), func(t *testing.T) {
			content := newFrameContent(testFrames)
			inner := newSizeRecorder()
			inner.failOn = 2
			lr := &io.LimitedReader{R: content, N: 40}
			var src io.Reader = content
			if limited {
				src = lr
			}
			var rf io.ReaderFrom = responseWriter{inner}
			if n, err := rf.ReadFrom(src); n != 5 || !errors.Is(err, errInnerWrite) {
				t.Fatalf("ReadFrom into a writer failing its second Write = %d, %v; want 5, %v", n, err, errInnerWrite)
			}
			if limited && lr.N != 35 {
				t.Errorf("ReadFrom left a limit of %d; want 35, the 5 bytes written taken off", lr.N)
			}
		})
	}
}

// controlWriter records the Flush and SetWriteDeadline calls reaching it.
type controlWriter struct {
	*httptest.ResponseRecorder
	flushes  int
	deadline time.Time
}

func (w *controlWriter) Flush() { w.flushes++ }

func (w *controlWriter) SetWriteDeadline(d time.Time) error {
	w.deadline = d
	return nil
}

// http.ResponseController's Flush reaches the inner writer through the
// wrapper's own Flush, its SetWriteDeadline through Unwrap, which returns the
// inner writer; Flush over an inner writer that cannot flush does nothing.
func TestResponseControllerReachesInnerWriter(t *testing.T) {
	inner := &controlWriter{ResponseRecorder: httptest.NewRecorder()}
	rc := http.NewResponseController(responseWriter{inner})
	if err := rc.Flush(); err != nil || inner.flushes != 1 {
		t.Errorf("ResponseController.Flush = %v with %d inner Flushes; want nil and 1", err, inner.flushes)
	}
	deadline := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := rc.SetWriteDeadline(deadline); err != nil || !inner.deadline.Equal(deadline) {
		t.Errorf("ResponseController.SetWriteDeadline = %v with the inner deadline %v; want nil and %v", err, inner.deadline, deadline)
	}
	if u, ok := any(responseWriter{inner}).(interface{ Unwrap() http.ResponseWriter }); !ok || u.Unwrap() != http.ResponseWriter(inner) {
		t.Error("responseWriter{inner} does not Unwrap to inner")
	}

	noFlush := struct{ http.ResponseWriter }{httptest.NewRecorder()}
	f, ok := any(responseWriter{noFlush}).(http.Flusher)
	if !ok {
		t.Fatal("responseWriter is no http.Flusher")
	}
	defer func() {
		if p := recover(); p != nil {
			t.Errorf("Flush over an inner writer that cannot flush panicked: %v; want it to do nothing", p)
		}
	}()
	f.Flush()
}
