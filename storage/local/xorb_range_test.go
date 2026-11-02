package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math/rand/v2"
	"os"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// newRangeStore returns a store over a fresh temp dir.
func newRangeStore(t *testing.T, opts ...Option) *Storage {
	t.Helper()
	fs, err := NewStorage(append([]Option{WithBasePath(t.TempDir())}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

// putRandomXorb stores four incompressible 4 KiB chunks drawn from seed and returns the stored bytes.
func putRandomXorb(t *testing.T, fs *Storage, seed byte) ([]byte, xet.XorbHash) {
	t.Helper()
	rng := rand.NewChaCha8([32]byte{seed})
	chunks := make([][]byte, 4)
	for i := range chunks {
		chunks[i] = make([]byte, 4096)
		rng.Read(chunks[i])
	}
	encoded, xorbHash := storagetest.EncodeXorb(t, false, chunks...)
	if _, err := fs.PutXorb(context.Background(), xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	stored, err := os.ReadFile(fs.objectPath("xorbs", xorbHash.String()))
	if err != nil {
		t.Fatal(err)
	}
	return stored, xorbHash
}

func mustOpenRange(t *testing.T, fs *Storage, xorbHash xet.XorbHash, start, end int64) io.ReadCloser {
	t.Helper()
	rc, err := fs.GetXorbRangeReadCloser(context.Background(), xorbHash, start, end)
	if err != nil {
		t.Fatalf("GetXorbRangeReadCloser(%d, %d): %v", start, end, err)
	}
	return rc
}

// drained is what a reader returned up to EOF, or its first error.
type drained struct {
	data []byte
	err  error
}

// drainRange opens, reads and closes [start, end] without a *testing.T, so it can run in the background.
func drainRange(fs *Storage, xorbHash xet.XorbHash, start, end int64) drained {
	rc, err := fs.GetXorbRangeReadCloser(context.Background(), xorbHash, start, end)
	if err != nil {
		return drained{err: err}
	}
	data, err := io.ReadAll(rc)
	if closeErr := rc.Close(); err == nil {
		err = closeErr
	}
	return drained{data, err}
}

// readInSteps reads r to EOF in 1- to 7-byte steps, phase-shifted by i.
func readInSteps(r io.Reader, i int) drained {
	var got []byte
	buf := make([]byte, 7)
	for step := i; ; step++ {
		n, err := r.Read(buf[:1+step%7])
		got = append(got, buf[:n]...)
		if err == io.EOF {
			return drained{data: got}
		}
		if err != nil {
			return drained{got, err}
		}
	}
}

// inBackground runs call on its own goroutine so await bounds a call that blocks.
func inBackground[T any](call func() T) <-chan T {
	done := make(chan T, 1)
	go func() { done <- call() }()
	return done
}

func assertDrained(t *testing.T, what string, got drained, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("%s: %v", what, got.err)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("%s read %d bytes that differ from the %d stored bytes", what, len(got.data), len(want))
	}
}

// assertNext reads the next len(want) bytes of r.
func assertNext(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	n, err := io.ReadFull(r, got)
	assertDrained(t, "open reader", drained{got[:n], err}, want)
}

// assertRest reads r to EOF.
func assertRest(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got, err := io.ReadAll(r)
	assertDrained(t, "open reader", drained{got, err}, want)
}

func TestXorbRangeReaderDoesNotBlockSecondReader(t *testing.T) {
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	a := mustOpenRange(t, fs, x, 10, 900)
	defer a.Close()
	assertNext(t, a, encoded[10:17])

	b := await(t, inBackground(func() drained { return drainRange(fs, x, 100, 2000) }))
	assertDrained(t, "second reader of X", b, encoded[100:2001])
	assertRest(t, a, encoded[17:901])
}

func TestXorbRangeReaderSurvivesEviction(t *testing.T) {
	fs := newRangeStore(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, fs, 1)
	encodedY, y := putRandomXorb(t, fs, 2)
	a := mustOpenRange(t, fs, x, 10, 900)
	defer a.Close()
	assertNext(t, a, encodedX[10:17])

	other := await(t, inBackground(func() drained { return drainRange(fs, y, 0, 99) }))
	assertDrained(t, "reader of Y", other, encodedY[:100])
	if _, ok := fs.xorbIndex.Get(x); ok {
		t.Fatal("X is still cached after Y took the only xorbIndex slot")
	}
	assertRest(t, a, encodedX[17:901])
}

func TestXorbRangeReadersInterleaveOnOneXorb(t *testing.T) {
	const readers = 8
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	span := int64(len(encoded)) / readers

	type opened struct {
		rc  io.ReadCloser
		err error
	}
	opens := make([]<-chan opened, readers)
	for i := range readers {
		opens[i] = inBackground(func() opened {
			rc, err := fs.GetXorbRangeReadCloser(context.Background(), x, int64(i)*span, int64(i+1)*span-1)
			return opened{rc, err}
		})
	}
	rcs := make([]io.ReadCloser, readers)
	for i, pending := range opens {
		o := await(t, pending)
		if o.err != nil {
			t.Fatalf("GetXorbRangeReadCloser for reader %d: %v", i, o.err)
		}
		defer o.rc.Close()
		rcs[i] = o.rc
	}

	// Park every reader before releasing them together so their reads overlap.
	var ready sync.WaitGroup
	ready.Add(readers)
	begin := make(chan struct{})
	reads := make([]<-chan drained, readers)
	for i, rc := range rcs {
		reads[i] = inBackground(func() drained {
			ready.Done()
			<-begin
			return readInSteps(rc, i)
		})
	}
	ready.Wait()
	close(begin)
	for i, pending := range reads {
		start := int64(i) * span
		if got := await(t, pending); got.err != nil || !bytes.Equal(got.data, encoded[start:start+span]) {
			t.Errorf("reader %d of [%d, %d] read %d bytes (err %v) that differ from the stored slice", i, start, start+span-1, len(got.data), got.err)
		}
	}
}

func TestXorbRangeReaderReopensAfterEviction(t *testing.T) {
	fs := newRangeStore(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, fs, 1)
	_, y := putRandomXorb(t, fs, 2)
	a := mustOpenRange(t, fs, x, 10, 900)
	defer a.Close()
	assertNext(t, a, encodedX[10:17])
	if got := await(t, inBackground(func() drained { return drainRange(fs, y, 0, 99) })); got.err != nil {
		t.Fatalf("reader of Y evicting X: %v", got.err)
	}

	again := await(t, inBackground(func() drained { return drainRange(fs, x, 500, 1500) }))
	assertDrained(t, "new reader of evicted X", again, encodedX[500:1501])
	if _, ok := fs.xorbIndex.Get(x); !ok {
		t.Fatal("re-opened X is not cached again")
	}
	assertRest(t, a, encodedX[17:901])
}

func TestXorbHandlesMatchCacheWhenNoReaderOpen(t *testing.T) {
	ctx := context.Background()
	fs := newRangeStore(t, WithXorbCacheSize(1))
	_, x := putRandomXorb(t, fs, 1)
	_, y := putRandomXorb(t, fs, 2)
	assertIdle := func(after string) {
		t.Helper()
		if handles, cached := fs.xorbHandles.Load(), fs.xorbIndex.Len(); handles != int64(cached) {
			t.Fatalf("after %s: xorbHandles = %d, want xorbIndex.Len() = %d", after, handles, cached)
		}
	}

	if got := drainRange(fs, x, 0, 99); got.err != nil {
		t.Fatalf("reader of X: %v", got.err)
	}
	assertIdle("a reader of X closed")
	if _, _, err := fs.GetXorbDataRange(ctx, x, 0, 1); err != nil {
		t.Fatal(err)
	}
	assertIdle("loading X's chunk offsets")

	a := mustOpenRange(t, fs, x, 0, 99)
	defer a.Close()
	if got := await(t, inBackground(func() drained { return drainRange(fs, y, 0, 99) })); got.err != nil {
		t.Fatalf("reader of Y evicting X: %v", got.err)
	}
	if _, err := io.ReadAll(a); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	assertIdle("the last reader of evicted X closed")

	for _, xorbHash := range []xet.XorbHash{x, y} {
		if err := fs.DeleteXorb(ctx, xorbHash); err != nil {
			t.Fatal(err)
		}
	}
	if handles, cached := fs.xorbHandles.Load(), fs.xorbIndex.Len(); handles != 0 || cached != 0 {
		t.Fatalf("after closing every reader and deleting every xorb: xorbHandles = %d, xorbIndex.Len() = %d; want 0, 0", handles, cached)
	}
}

func TestXorbHandleClosedByEvictionIsNotReacquired(t *testing.T) {
	fs := newRangeStore(t, WithXorbCacheSize(1))
	_, x := putRandomXorb(t, fs, 1)
	_, y := putRandomXorb(t, fs, 2)
	if got := drainRange(fs, x, 0, 99); got.err != nil {
		t.Fatalf("reader of X: %v", got.err)
	}
	xf, ok := fs.xorbIndex.Get(x)
	if !ok {
		t.Fatal("X is not cached after its reader closed")
	}
	if got := drainRange(fs, y, 0, 99); got.err != nil {
		t.Fatalf("reader of Y evicting X: %v", got.err)
	}
	if handles := fs.xorbHandles.Load(); handles != 1 {
		t.Fatalf("xorbHandles = %d after evicting X dropped its last reference, want 1 for cached Y", handles)
	}
	// An openXorb cache hit racing this eviction must retry, not reuse the closed file.
	if xf.acquire() {
		t.Fatal("acquire() on the X handle eviction closed = true, want false")
	}
}

func TestOpenXorbRetriesWhenEvictionRacesAcquire(t *testing.T) {
	const goroutines, iterations = 8, 500
	fs := newRangeStore(t, WithXorbCacheSize(1))
	encodedX, x := putRandomXorb(t, fs, 1)
	encodedY, y := putRandomXorb(t, fs, 2)
	hashes, stored := [2]xet.XorbHash{x, y}, [2][]byte{encodedX, encodedY}

	// Odd goroutines start with Y, so X and Y keep evicting each other from the only slot.
	runs := make([]<-chan error, goroutines)
	for g := range goroutines {
		runs[g] = inBackground(func() error {
			for i := range iterations {
				k := (g + i) % 2
				got := drainRange(fs, hashes[k], 0, int64(len(stored[k]))-1)
				if got.err != nil {
					return fmt.Errorf("iteration %d: %w", i, got.err)
				}
				if !bytes.Equal(got.data, stored[k]) {
					return fmt.Errorf("iteration %d read %d bytes that differ from the %d stored bytes", i, len(got.data), len(stored[k]))
				}
			}
			return nil
		})
	}
	for g, pending := range runs {
		if err := await(t, pending); err != nil {
			t.Errorf("goroutine %d: %v", g, err)
		}
	}
	if handles, cached := fs.xorbHandles.Load(), fs.xorbIndex.Len(); handles != int64(cached) {
		t.Fatalf("after every reader closed: xorbHandles = %d, want xorbIndex.Len() = %d", handles, cached)
	}
}

func TestGetXorbDataRangeDoesNotWaitForOpenReader(t *testing.T) {
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	wantStart, wantEnd, err := xorb.ChunkDataRange(bytes.NewReader(encoded), 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	a := mustOpenRange(t, fs, x, 10, 900)
	defer a.Close()
	assertNext(t, a, encoded[10:17])

	type byteRange struct {
		start, end int64
		err        error
	}
	got := await(t, inBackground(func() byteRange {
		start, end, err := fs.GetXorbDataRange(context.Background(), x, 1, 3)
		return byteRange{start, end, err}
	}))
	if got.err != nil || got.start != wantStart || got.end != wantEnd {
		t.Fatalf("GetXorbDataRange(1, 3) with a reader open = [%d, %d], %v; want [%d, %d]", got.start, got.end, got.err, wantStart, wantEnd)
	}
	assertRest(t, a, encoded[17:901])
}

func TestDeleteXorbDoesNotWaitForOpenReader(t *testing.T) {
	ctx := context.Background()
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	a := mustOpenRange(t, fs, x, 10, 900)
	defer a.Close()
	assertNext(t, a, encoded[10:17])

	if err := await(t, inBackground(func() error { return fs.DeleteXorb(ctx, x) })); err != nil {
		t.Fatalf("DeleteXorb with a reader open: %v", err)
	}
	if ok, err := fs.HasXorb(ctx, x); err != nil || ok {
		t.Fatalf("HasXorb after DeleteXorb = %v, %v; want false, nil", ok, err)
	}
	assertRest(t, a, encoded[17:901])
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if handles := fs.xorbHandles.Load(); handles != 0 {
		t.Fatalf("xorbHandles = %d after the deleted xorb's last reader closed, want 0", handles)
	}
}

func TestXorbRangeReaderDoubleCloseReleasesOnce(t *testing.T) {
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	a := mustOpenRange(t, fs, x, 10, 900)
	for i := range 2 {
		if err := a.Close(); err != nil {
			t.Fatalf("Close #%d = %v, want nil", i+1, err)
		}
	}
	// A second release would close the handle the cache still hands out.
	if handles, cached := fs.xorbHandles.Load(), fs.xorbIndex.Len(); handles != int64(cached) {
		t.Fatalf("after a double Close: xorbHandles = %d, want xorbIndex.Len() = %d", handles, cached)
	}
	assertDrained(t, "reader of X after a double Close", drainRange(fs, x, 10, 900), encoded[10:901])
}

func TestXorbRangeReaderReadAfterCloseFails(t *testing.T) {
	fs := newRangeStore(t)
	_, x := putRandomXorb(t, fs, 1)
	a := mustOpenRange(t, fs, x, 10, 900)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := a.Read(make([]byte, 16)); n != 0 || err == nil || err == io.EOF {
		t.Fatalf("Read after Close = %d, %v; want 0 and an error other than io.EOF", n, err)
	}
}

func TestXorbRangeReaderMissingXorbIsNotExist(t *testing.T) {
	fs := newRangeStore(t)
	if _, err := fs.GetXorbRangeReadCloser(context.Background(), xet.XorbHash{1}, 0, 99); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetXorbRangeReadCloser(missing xorb) error = %v, want fs.ErrNotExist", err)
	}
}

func TestXorbRangeReaderMissingXorbLeavesHandlesUnchanged(t *testing.T) {
	fs := newRangeStore(t)
	before := fs.xorbHandles.Load()
	if _, err := fs.GetXorbRangeReadCloser(context.Background(), xet.XorbHash{1}, 0, 99); err == nil {
		t.Fatal("GetXorbRangeReadCloser(missing xorb) error = nil, want an error")
	}
	if after := fs.xorbHandles.Load(); after != before {
		t.Fatalf("xorbHandles = %d after a failed open, want %d as before it", after, before)
	}
}

func TestXorbRangeReaderEmptyRangeReadsEOF(t *testing.T) {
	fs := newRangeStore(t)
	_, x := putRandomXorb(t, fs, 1)
	a := mustOpenRange(t, fs, x, 100, 99)
	if n, err := a.Read(make([]byte, 16)); n != 0 || err != io.EOF {
		t.Fatalf("first Read of [100, 99] = %d, %v; want 0, io.EOF", n, err)
	}
	if err := a.Close(); err != nil {
		t.Fatalf("Close of [100, 99] = %v, want nil", err)
	}
}

func TestXorbRangeReaderEndPastSizeReadsToEOF(t *testing.T) {
	fs := newRangeStore(t)
	encoded, x := putRandomXorb(t, fs, 1)
	size := int64(len(encoded))
	a := mustOpenRange(t, fs, x, size-100, size+1000)
	defer a.Close()
	assertRest(t, a, encoded[size-100:])
	if n, err := a.Read(make([]byte, 16)); n != 0 || err != io.EOF {
		t.Fatalf("Read past the stored end = %d, %v; want 0, io.EOF", n, err)
	}
}
