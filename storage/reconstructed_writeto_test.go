package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

var errWrite = errors.New("injected write failure")

// writerTo fails tb unless the reconstructed file rc is an io.WriterTo.
func writerTo(tb testing.TB, rc io.Reader) io.WriterTo {
	tb.Helper()
	wt, ok := rc.(io.WriterTo)
	if !ok {
		tb.Fatalf("reconstructed file %T is no io.WriterTo; want a WriteTo writing its decoded chunks", rc)
	}
	return wt
}

// writeRecorder keeps what it is written and each Write's length; accept,
// when set, says how much of a Write it keeps and with what error.
type writeRecorder struct {
	data   []byte
	writes []int
	accept func(p []byte) (int, error)
}

func (w *writeRecorder) Write(p []byte) (int, error) {
	n := len(p)
	var err error
	if w.accept != nil {
		n, err = w.accept(p)
	}
	w.writes = append(w.writes, len(p))
	w.data = append(w.data, p[:n]...)
	return n, err
}

// notifyAt returns a channel w closes once it holds k bytes.
func (w *writeRecorder) notifyAt(k int64) <-chan struct{} {
	reached := make(chan struct{})
	w.accept = func(p []byte) (int, error) {
		if held := int64(len(w.data)); held < k && held+int64(len(p)) >= k {
			close(reached)
		}
		return len(p), nil
	}
	return reached
}

type writeToResult struct {
	n   int64
	err error
}

// startWriteTo runs wt.WriteTo(w) in a goroutine.
func startWriteTo(wt io.WriterTo, w io.Writer) <-chan writeToResult {
	done := make(chan writeToResult, 1)
	go func() {
		n, err := wt.WriteTo(w)
		done <- writeToResult{n, err}
	}()
	return done
}

// awaitSignal fails t unless ch delivers within 5s.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("waited 5s for %s", what)
	}
}

// assertWriteToWaits fails t if the WriteTo behind done returns within 50ms.
func assertWriteToWaits(t *testing.T, done <-chan writeToResult, on string) {
	t.Helper()
	select {
	case r := <-done:
		t.Fatalf("WriteTo returned %d, %v while %s; want it waiting", r.n, r.err, on)
	case <-time.After(50 * time.Millisecond):
	}
}

// WriteTo writes every class's file, on either backend, as stored; a second
// WriteTo then writes nothing and a Read is at EOF.
func TestReconstructedFileWriteToMatchesContent(t *testing.T) {
	for _, b := range backends {
		for _, c := range storagetest.ChunkClasses {
			t.Run(b.name+"/"+c.Name, func(t *testing.T) {
				st := b.open(t)
				f := putXorbs(t, st, xorbsOf(c, 3, mixedChunkSizes))
				rc := openFile(t, t.Context(), st, f)
				wt := writerTo(t, rc)
				w := &writeRecorder{}
				if n, err := wt.WriteTo(w); err != nil || n != int64(len(f.Content)) || !bytes.Equal(w.data, f.Content) {
					t.Fatalf("WriteTo = %d, %v with %d bytes written; want the %d stored", n, err, len(w.data), len(f.Content))
				}
				again := &writeRecorder{}
				if n, err := wt.WriteTo(again); n != 0 || err != nil || len(again.writes) != 0 {
					t.Fatalf("second WriteTo = %d, %v after %d Writes; want 0, nil and no Write", n, err, len(again.writes))
				}
				if n, err := rc.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Fatalf("Read after WriteTo = %d, %v; want 0, io.EOF", n, err)
				}
			})
		}
	}
}

// WriteTo makes one Write per non-empty decoded chunk, the first from the
// position on, also after a Seek or a Read inside a chunk.
func TestReconstructedFileWriteToWritesWholeChunks(t *testing.T) {
	st := newLocal(t)
	xorbs := xorbsOf(lz4Class, 2, mixedChunkSizes)
	mixed := putXorbs(t, st, xorbs)
	var sizes []int
	for _, chunks := range xorbs {
		for _, c := range chunks {
			sizes = append(sizes, len(c))
		}
	}
	// Chunk 1, 128 KiB, holds file bytes [1, 131073).
	const pos = 5001
	rest := xet.MaxChunkSize + 1 - pos
	for _, c := range []struct {
		name       string
		f          storedFile
		seek, read int64
		want       []int
	}{
		{"from the start", mixed, 0, 0, sizes},
		{"after Seek(5001)", mixed, pos, 0, slices.Concat([]int{rest}, sizes[2:])},
		{"after Seek(5001) and a 1000-byte Read", mixed, pos, 1000, slices.Concat([]int{rest - 1000}, sizes[2:])},
		{"empty chunks between and after", putXorbs(t, st, xorbsOf(lz4Class, 1, []int{4 << 10, 0, 0, 4 << 10, 0})), 0, 0, []int{4 << 10, 4 << 10}},
	} {
		t.Run(c.name, func(t *testing.T) {
			rc := openFile(t, t.Context(), st, c.f)
			wt := writerTo(t, rc)
			if _, err := rc.Seek(c.seek, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(rc, make([]byte, c.read)); err != nil {
				t.Fatal(err)
			}
			from := c.seek + c.read
			w := &writeRecorder{}
			if n, err := wt.WriteTo(w); err != nil || !bytes.Equal(w.data, c.f.Content[from:]) {
				t.Fatalf("WriteTo from %d = %d, %v; want the %d bytes after it", from, n, err, int64(len(c.f.Content))-from)
			}
			if !slices.Equal(w.writes, c.want) {
				t.Errorf("WriteTo from %d made Writes of %v bytes; want one per non-empty decoded chunk, %v", from, w.writes, c.want)
			}
		})
	}
}

// WriteTo reads each non-empty term through one GetXorbReadSeekCloser, read
// from its chunks' first byte through their last and closed once, and through
// no other seeker.
func TestReconstructedFileWriteToOneRangeReadPerTerm(t *testing.T) {
	files := []struct {
		name string
		put  func(t *testing.T, st storage.Storage) storedFile
	}{
		{"whole xorbs", func(t *testing.T, st storage.Storage) storedFile {
			return putXorbs(t, st, xorbsOf(lz4Class, 3, mixedChunkSizes))
		}},
		{"sub-range terms", func(t *testing.T, st storage.Storage) storedFile {
			return putSubRangeFile(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
		}},
		{"zero-length terms", func(t *testing.T, st storage.Storage) storedFile {
			return withZeroLengthTerms(putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes)))
		}},
	}
	for _, b := range backends {
		for _, file := range files {
			t.Run(b.name+"/"+file.name, func(t *testing.T) {
				st := b.open(t)
				f := file.put(t, st)
				dec := newCountingStore(st)
				rc := openFile(t, t.Context(), dec, f)
				w := &writeRecorder{}
				if n, err := writerTo(t, rc).WriteTo(w); err != nil || !bytes.Equal(w.data, f.Content) {
					t.Fatalf("WriteTo = %d, %v; want the %d stored", n, err, len(f.Content))
				}
				if err := rc.Close(); err != nil {
					t.Fatalf("Close = %v", err)
				}
				var want []seekRead
				terms := map[xet.XorbHash]int{}
				for _, e := range f.entries() {
					if e.UnpackedSegBytes == 0 {
						continue
					}
					terms[e.CASHash]++
					start, end := termRange(framesOf(t, st, e.CASHash), e)
					want = append(want, seekRead{hash: e.CASHash, nth: terms[e.CASHash], start: start, read: end - start + 1, closes: 1})
				}
				if got := dec.seekReads(); !slices.Equal(got, want) {
					t.Errorf("seekers = %v; want only one per non-empty term, read from its first byte through its last and closed once: %v", got, want)
				}
			})
		}
	}
}

// A Write failing once k bytes are written ends WriteTo with k and that
// error at position k, from where a Read goes on.
func TestReconstructedFileWriteToFailedWriteKeepsPosition(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	// The start, a chunk's end, inside the 128 KiB chunk 1, inside the second term.
	for _, k := range []int64{0, 1, 5001, f.termStart(1) + 7} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			rc := openFile(t, t.Context(), st, f)
			w := &writeRecorder{}
			w.accept = func(p []byte) (int, error) {
				if n := k - int64(len(w.data)); n < int64(len(p)) {
					return int(n), errWrite
				}
				return len(p), nil
			}
			if n, err := writerTo(t, rc).WriteTo(w); n != k || !errors.Is(err, errWrite) || !bytes.Equal(w.data, f.Content[:k]) {
				t.Fatalf("WriteTo into a writer failing after %d bytes = %d, %v with %d bytes written; want %d, %v", k, n, err, len(w.data), k, errWrite)
			}
			if pos, err := rc.Seek(0, io.SeekCurrent); err != nil || pos != k {
				t.Fatalf("Seek(0, SeekCurrent) after the failed WriteTo = %d, %v; want %d", pos, err, k)
			}
			if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content[k:]) {
				t.Fatalf("read after the failed WriteTo = %d bytes, %v; want the %d from %d on", len(got), err, int64(len(f.Content))-k, k)
			}
		})
	}
}

// A Write failing on the second chunk ends WriteTo with the first chunk's
// bytes and that error, the pipeline stopped before WriteTo returns; the
// error is not the file's, so the next WriteTo writes the rest.
func TestReconstructedFileWriteToFailedWriteStopsPipeline(t *testing.T) {
	const size = 16 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 2, slices.Repeat([]int{size}, 16)))
	dec := newCountingStore(st)
	baseline := runtime.NumGoroutine()
	rc := openFile(t, t.Context(), dec, f)
	wt := writerTo(t, rc)
	w := &writeRecorder{}
	w.accept = func(p []byte) (int, error) {
		if len(w.writes) > 0 {
			return 0, errWrite
		}
		return len(p), nil
	}
	if n, err := wt.WriteTo(w); n != size || !errors.Is(err, errWrite) {
		t.Fatalf("WriteTo into a writer failing its second Write = %d, %v; want %d, %v", n, err, size, errWrite)
	}
	dec.mu.Lock()
	if len(dec.seekers) == 0 {
		t.Error("WriteTo opened no GetXorbReadSeekCloser")
	}
	for _, r := range dec.seekers {
		if r.closes != 1 {
			t.Errorf("seeker %v closed %d times when WriteTo returned, want once", r, r.closes)
		}
	}
	for i, ctx := range dec.ctxs {
		if ctx.Err() == nil {
			t.Errorf("xorb read %d was given a context still live when WriteTo returned, want the stopped pipeline's", i)
		}
	}
	dec.mu.Unlock()
	settleGoroutines(t, baseline)
	rest := &writeRecorder{}
	if n, err := wt.WriteTo(rest); err != nil || !bytes.Equal(rest.data, f.Content[size:]) {
		t.Fatalf("WriteTo after the failed one = %d, %v; want the %d bytes from %d on", n, err, len(f.Content)-size, size)
	}
}

// A Write keeping all but the last byte of the second chunk without an error
// ends WriteTo with the bytes kept and io.ErrShortWrite, at the position after
// them, the pipeline stopped before WriteTo returns.
func TestReconstructedFileWriteToShortWriteStopsPipeline(t *testing.T) {
	const size = 16 << 10
	const k = 2*size - 1
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 2, slices.Repeat([]int{size}, 16)))
	dec := newCountingStore(st)
	baseline := runtime.NumGoroutine()
	rc := openFile(t, t.Context(), dec, f)
	w := &writeRecorder{}
	w.accept = func(p []byte) (int, error) {
		if len(w.writes) == 1 {
			return len(p) - 1, nil
		}
		return len(p), nil
	}
	if n, err := writerTo(t, rc).WriteTo(w); n != k || !errors.Is(err, io.ErrShortWrite) || !bytes.Equal(w.data, f.Content[:k]) {
		t.Fatalf("WriteTo into a writer keeping all but a byte of its second Write = %d, %v with %d bytes written; want %d, %v", n, err, len(w.data), k, io.ErrShortWrite)
	}
	dec.mu.Lock()
	if len(dec.seekers) == 0 {
		t.Error("WriteTo opened no GetXorbReadSeekCloser")
	}
	for _, r := range dec.seekers {
		if r.closes != 1 {
			t.Errorf("seeker %v closed %d times when WriteTo returned, want once", r, r.closes)
		}
	}
	for i, ctx := range dec.ctxs {
		if ctx.Err() == nil {
			t.Errorf("xorb read %d was given a context still live when WriteTo returned, want the stopped pipeline's", i)
		}
	}
	dec.mu.Unlock()
	settleGoroutines(t, baseline)
	if pos, err := rc.Seek(0, io.SeekCurrent); err != nil || pos != k {
		t.Fatalf("Seek(0, SeekCurrent) after the short Write = %d, %v; want %d", pos, err, k)
	}
}

// On a last term declared short by its last chunk, WriteTo writes up to the
// final declared chunk, then fails with the term's size error, which the next
// WriteTo and Read repeat until a Seek away clears it.
func TestReconstructedFileWriteToShortLastTermFails(t *testing.T) {
	st := newLocal(t)
	f, final := shortLastTerm(t, st)
	entries := f.entries()
	size := f.termStart(len(entries))
	rc := openFile(t, t.Context(), st, f)
	wt := writerTo(t, rc)
	w := &writeRecorder{}
	var n int64
	var err error
	within(t, 5*time.Second, "WriteTo of a short last term", func() { n, err = wt.WriteTo(w) })
	if want := fmt.Sprintf("chunks exceed the term's %d bytes", entries[len(entries)-1].UnpackedSegBytes); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("WriteTo = %d, %v; want the term's size error, containing %q", n, err, want)
	}
	if want := size - final; n != want || !bytes.Equal(w.data, f.Content[:want]) {
		t.Errorf("WriteTo wrote %d of %d bytes; want the %d before the final declared chunk", n, size, want)
	}
	if m, again := wt.WriteTo(&writeRecorder{}); m != 0 || again == nil || again.Error() != err.Error() {
		t.Fatalf("WriteTo after %v = %d, %v; want 0 and the same error", err, m, again)
	}
	if m, again := rc.Read(make([]byte, 1)); m != 0 || again == nil || again.Error() != err.Error() {
		t.Fatalf("Read after %v = %d, %v; want 0 and the same error", err, m, again)
	}
	if _, err := rc.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 100)
	if _, err := io.ReadFull(rc, got); err != nil || !bytes.Equal(got, f.Content[:100]) {
		t.Fatalf("read after a Seek away = %v; want the file's first 100 bytes, the error cleared", err)
	}
}

// After Close, WriteTo fails with fs.ErrClosed and makes no Write.
func TestReconstructedFileWriteToAfterClose(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	rc := openFile(t, t.Context(), st, f)
	wt := writerTo(t, rc)
	if _, err := io.ReadFull(rc, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	w := &writeRecorder{}
	if n, err := wt.WriteTo(w); n != 0 || !errors.Is(err, fs.ErrClosed) || len(w.writes) != 0 {
		t.Fatalf("WriteTo after Close = %d, %v after %d Writes; want 0, fs.ErrClosed and no Write", n, err, len(w.writes))
	}
}

// WriteTo of an empty file, or at or past a file's end, returns 0, nil
// without a Write or storage, leaving no goroutine behind.
func TestReconstructedFileWriteToAtEndDoesNoIO(t *testing.T) {
	st := newLocal(t)
	empty := loadFile(t, st, storagetest.PutFile(t, t.Context(), st, nil), [32]byte{})
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	size := int64(len(f.Content))
	for _, c := range []struct {
		name string
		f    storedFile
		pos  int64
	}{
		{"empty file", empty, 0},
		{"at the end", f, size},
		{"past the end", f, size + 10},
	} {
		t.Run(c.name, func(t *testing.T) {
			dec := newCountingStore(st)
			baseline := runtime.NumGoroutine()
			rc := openFile(t, t.Context(), dec, c.f)
			wt := writerTo(t, rc)
			if _, err := rc.Seek(c.pos, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			w := &writeRecorder{}
			if n, err := wt.WriteTo(w); n != 0 || err != nil || len(w.writes) != 0 {
				t.Fatalf("WriteTo at %d = %d, %v after %d Writes; want 0, nil and no Write", c.pos, n, err, len(w.writes))
			}
			if n := dec.total(); n != 0 {
				t.Errorf("WriteTo at %d made %d xorb reads, want none", c.pos, n)
			}
			settleGoroutines(t, baseline)
		})
	}
}

// Close while WriteTo waits on a seeker read deaf to cancellation returns
// within 1s once that read gives up, and ends WriteTo in fs.ErrClosed after
// the bytes before the stuck term.
func TestReconstructedFileWriteToCloseInterruptsWait(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	before := f.termStart(1)
	blocked, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	release := make(chan struct{})
	unstick := sync.OnceFunc(func() { close(release) })
	defer unstick()
	dec := newCountingStore(st)
	dec.fault = func(ctx context.Context, r seekRead, rs io.ReadSeeker) io.ReadSeeker {
		if r.hash != f.XorbHashes[1] || r.nth > 1 {
			return rs
		}
		return &stuckSeeker{ReadSeeker: rs, ctx: ctx, err: errInjected, blocked: blocked, cancelled: cancelled, release: release}
	}
	rc := openFile(t, t.Context(), dec, f)
	w := &writeRecorder{}
	wrote := w.notifyAt(before)
	done := startWriteTo(writerTo(t, rc), w)
	awaitSignal(t, wrote, "WriteTo to write the first term")
	awaitSignal(t, blocked, "a read of the second term's stuck seeker")
	assertWriteToWaits(t, done, "the second term's seeker was stuck")

	closed := make(chan error, 1)
	deadline := time.After(time.Second)
	go func() { closed <- rc.Close() }()
	select {
	case <-cancelled:
	case <-deadline:
		t.Fatal("Close left the context it gave storage live for 1s; want it cancelled before Close waits on WriteTo")
	}
	unstick()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close during a waiting WriteTo = %v, want nil", err)
		}
	case <-deadline:
		t.Fatal("Close during a waiting WriteTo still blocked after 1s")
	}
	select {
	case r := <-done:
		if r.n != before || !errors.Is(r.err, fs.ErrClosed) || !bytes.Equal(w.data, f.Content[:before]) {
			t.Fatalf("WriteTo racing Close = %d, %v with %d bytes written; want the %d before the stuck term, fs.ErrClosed", r.n, r.err, len(w.data), before)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("WriteTo racing Close still blocked 5s after Close returned")
	}
}

// Cancelling the reader's context while WriteTo waits on storage ends it
// within 1s in the context's error after the bytes before the wait; the next
// WriteTo fails the same way.
func TestReconstructedFileWriteToParentCancelStops(t *testing.T) {
	const size = 4 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 1, slices.Repeat([]int{size}, 16)))
	baseline := runtime.NumGoroutine()
	dec, blocked := blockAfterFirstChunk(t, st, f)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rc := openFile(t, ctx, dec, f)
	wt := writerTo(t, rc)
	w := &writeRecorder{}
	wrote := w.notifyAt(size)
	done := startWriteTo(wt, w)
	awaitSignal(t, wrote, "WriteTo to write the first chunk")
	awaitSignal(t, blocked, "a read of the blocked seeker")
	assertWriteToWaits(t, done, "its seeker read was blocked")
	cancel()
	select {
	case r := <-done:
		if r.n != size || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("WriteTo waiting when its context was cancelled = %d, %v; want %d, context.Canceled", r.n, r.err, size)
		}
	case <-time.After(time.Second):
		t.Fatal("WriteTo still blocked 1s after its context was cancelled")
	}
	if n, err := wt.WriteTo(&writeRecorder{}); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("WriteTo after the cancel = %d, %v; want 0, context.Canceled", n, err)
	}
	closeWithin(t, rc, time.Second)
	settleGoroutines(t, baseline)
}

// discardWrites lacks io.Discard's ReadFrom: io.Copy into it takes the file's WriteTo.
type discardWrites struct{}

func (discardWrites) Write(p []byte) (int, error) { return len(p), nil }

// copyFile copies the fixture's file by io.Copy into discardWrites.
func copyFile(ctx context.Context, fx fixture) error {
	rc, err := fx.st.GetReconstructedFile(ctx, fx.digest)
	if err != nil {
		return err
	}
	defer rc.Close()
	if _, ok := rc.(io.WriterTo); !ok {
		return fmt.Errorf("reconstructed file %T is no io.WriterTo", rc)
	}
	n, err := io.Copy(discardWrites{}, rc)
	if err == nil && n != fileSize {
		err = fmt.Errorf("copied %d bytes, want %d", n, fileSize)
	}
	return err
}

// BenchmarkReconstructedFileWriteTo times what BenchmarkReconstructedFile's
// readers=1 rows read, written out by the file's WriteTo instead.
func BenchmarkReconstructedFileWriteTo(b *testing.B) {
	for _, c := range storagetest.ChunkClasses {
		b.Run("class="+c.Name, func(b *testing.B) {
			fx := buildFixture(b, c)
			ctx := b.Context()
			b.SetBytes(fileSize)
			b.ReportAllocs()
			for b.Loop() {
				if err := copyFile(ctx, fx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
