package xorbmap_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/xorbmap"
)

const fileSize = 16 << 10

// store holds xorb files in a temp dir and records the cache's open and release calls.
type store struct {
	dir  string
	flag int

	mu       sync.Mutex
	opens    []xet.XorbHash
	files    []*os.File
	releases []xet.XorbHash
}

func newStore(t *testing.T) *store {
	return &store{dir: t.TempDir(), flag: os.O_RDONLY}
}

// put writes size random bytes seeded by h as h's file and returns them.
func (s *store) put(t *testing.T, h xet.XorbHash, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	rand.NewChaCha8([32]byte(h)).Read(data)
	if err := os.WriteFile(filepath.Join(s.dir, h.String()), data, 0o644); err != nil {
		t.Fatal(err)
	}
	return data
}

// open records the call; a hash with no file fails with an error wrapping fs.ErrNotExist.
func (s *store) open(h xet.XorbHash) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(s.dir, h.String()), s.flag, 0)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opens = append(s.opens, h)
	if err != nil {
		return nil, err
	}
	s.files = append(s.files, f)
	return f, nil
}

func (s *store) release(h xet.XorbHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases = append(s.releases, h)
}

func (s *store) opened(h xet.XorbHash) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, o := range s.opens {
		if o == h {
			n++
		}
	}
	return n
}

func (s *store) released() []xet.XorbHash {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.releases)
}

func (s *store) onlyFile(t *testing.T) *os.File {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.files) != 1 {
		t.Fatalf("open returned %d files, want 1", len(s.files))
	}
	return s.files[0]
}

func mustAcquire(t *testing.T, c *xorbmap.Cache, h xet.XorbHash) *xorbmap.Mapping {
	t.Helper()
	m, err := c.Acquire(h)
	if err != nil {
		t.Fatalf("Acquire(%s): %v", h, err)
	}
	return m
}

// drained is what a reader returned up to EOF, or its first error.
type drained struct {
	data []byte
	err  error
}

// drain reads h whole through a Reader without a *testing.T, so it can run in the background.
func drain(c *xorbmap.Cache, h xet.XorbHash) drained {
	m, err := c.Acquire(h)
	if err != nil {
		return drained{err: err}
	}
	r := m.Reader()
	data, err := io.ReadAll(r)
	if closeErr := r.Close(); err == nil {
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

// await receives from ch, failing instead of hanging if what never happens.
func await[T any](t *testing.T, what string, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
	var zero T
	return zero
}

func assertDrained(t *testing.T, what string, got drained, want []byte) {
	t.Helper()
	if got.err != nil {
		t.Fatalf("%s: %v", what, got.err)
	}
	if !bytes.Equal(got.data, want) {
		t.Fatalf("%s read %d bytes that differ from the %d in the file", what, len(got.data), len(want))
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

func assertCache(t *testing.T, c *xorbmap.Cache, after string, wantLen int, wantMapped int64) {
	t.Helper()
	if n, mapped := c.Len(), c.Mapped(); n != wantLen || mapped != wantMapped {
		t.Fatalf("after %s: Len() = %d, Mapped() = %d; want %d, %d", after, n, mapped, wantLen, wantMapped)
	}
}

func assertReleased(t *testing.T, s *store, after string, want ...xet.XorbHash) {
	t.Helper()
	if got := s.released(); !slices.Equal(got, want) {
		t.Fatalf("after %s: release calls = %v, want %v", after, got, want)
	}
}

func assertClosed(t *testing.T, f *os.File) {
	t.Helper()
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Stat of the file open returned = %v, want os.ErrClosed", err)
	}
}

// waitInsideAcquire polls until n goroutines have a Cache.Acquire frame on their stack.
func waitInsideAcquire(n int) error {
	buf := make([]byte, 1<<20)
	deadline := time.Now().Add(10 * time.Second)
	for {
		inside := 0
		for g := range strings.SplitSeq(string(buf[:runtime.Stack(buf, true)]), "\n\n") {
			if strings.Contains(g, "xorbmap.(*Cache).Acquire(") {
				inside++
			}
		}
		if inside >= n {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("only %d of %d callers inside Acquire after 10s", inside, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReaderSurvivesEviction(t *testing.T) {
	s := newStore(t)
	a, b := xet.XorbHash{1}, xet.XorbHash{2}
	dataA, dataB := s.put(t, a, fileSize), s.put(t, b, fileSize)
	c := xorbmap.New(1, s.open, s.release)
	ra := mustAcquire(t, c, a).Reader()
	defer ra.Close()
	assertNext(t, ra, dataA[:7])

	assertDrained(t, "reader of B", await(t, "B to evict A while A's reader is open", inBackground(func() drained { return drain(c, b) })), dataB)
	if c.Has(a) {
		t.Fatal("Has(A) = true after B took the only slot, want false")
	}
	assertReleased(t, s, "B evicted A while A's reader is open")
	assertCache(t, c, "B evicted A while A's reader is open", 1, 2)
	assertRest(t, ra, dataA[7:])
	if err := ra.Close(); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, s, "A's last reader closed", a)
	if mapped := c.Mapped(); mapped != 1 {
		t.Fatalf("Mapped() = %d after A's last reader closed, want 1 (only B)", mapped)
	}
}

func TestReaderClosedErrors(t *testing.T) {
	s := newStore(t)
	a := xet.XorbHash{1}
	data := s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	r := mustAcquire(t, c, a).Reader()
	if err := r.Close(); err != nil {
		t.Fatalf("first Close = %v, want nil", err)
	}
	if n, err := r.Read(make([]byte, 16)); n != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Read after Close = %d, %v; want 0, os.ErrClosed", n, err)
	}
	if off, err := r.Seek(0, io.SeekStart); off != 0 || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Seek after Close = %d, %v; want 0, os.ErrClosed", off, err)
	}
	if err := r.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second Close = %v, want os.ErrClosed", err)
	}
	// A second release would drop the cache's own hold.
	assertCache(t, c, "a double Close", 1, 1)
	assertReleased(t, s, "a double Close")
	assertDrained(t, "reader of A after a double Close", drain(c, a), data)
}

func TestConcurrentReadersOneHash(t *testing.T) {
	const readers, rounds = 8, 4
	s := newStore(t)
	a := xet.XorbHash{1}
	data := s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	span := int64(len(data)) / readers

	type openResult struct {
		rc  io.ReadSeekCloser
		err error
	}
	openReader := func() openResult {
		m, err := c.Acquire(a)
		if err != nil {
			return openResult{err: err}
		}
		return openResult{rc: m.Reader()}
	}
	// Each first reader opens while the earlier ones are still open, so no open waits for a reader.
	firsts := make([]io.ReadSeekCloser, readers)
	for i := range firsts {
		o := await(t, fmt.Sprintf("reader %d to open", i), inBackground(openReader))
		if o.err != nil {
			t.Fatalf("Acquire for reader %d: %v", i, o.err)
		}
		firsts[i] = o.rc
	}

	// Park every reader before releasing them together so their reads overlap.
	var ready sync.WaitGroup
	ready.Add(readers)
	begin := make(chan struct{})
	runs := make([]<-chan error, readers)
	for i, rc := range firsts {
		runs[i] = inBackground(func() error {
			ready.Done()
			<-begin
			for round := range rounds {
				if round > 0 {
					o := openReader()
					if o.err != nil {
						return fmt.Errorf("round %d: %w", round, o.err)
					}
					rc = o.rc
				}
				for k := range readers {
					seg := int64((i + k) % readers)
					if _, err := rc.Seek(seg*span, io.SeekStart); err != nil {
						return fmt.Errorf("round %d: Seek(%d): %w", round, seg*span, err)
					}
					got := readInSteps(io.LimitReader(rc, span), i+k)
					if want := data[seg*span : (seg+1)*span]; got.err != nil || !bytes.Equal(got.data, want) {
						return fmt.Errorf("round %d: [%d, %d) read %d bytes (err %v) that differ from the file", round, seg*span, (seg+1)*span, len(got.data), got.err)
					}
				}
				if err := rc.Close(); err != nil {
					return fmt.Errorf("round %d: Close: %w", round, err)
				}
			}
			return nil
		})
	}
	ready.Wait()
	close(begin)
	for i, pending := range runs {
		if err := await(t, fmt.Sprintf("reader %d", i), pending); err != nil {
			t.Fatalf("reader %d: %v", i, err)
		}
	}
	assertCache(t, c, "every reader closed", 1, 1)
	if n := s.opened(a); n != 1 {
		t.Fatalf("open(A) ran %d times, want 1", n)
	}
}

func TestReaderWholeBytes(t *testing.T) {
	s := newStore(t)
	a := xet.XorbHash{1}
	data := s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	m := mustAcquire(t, c, a)
	if got := m.Bytes(); !bytes.Equal(got, data) {
		t.Fatalf("Bytes() holds %d bytes that differ from the %d in the file", len(got), len(data))
	}
	r := m.Reader()
	defer r.Close()
	if end, err := r.Seek(0, io.SeekEnd); end != int64(len(data)) || err != nil {
		t.Fatalf("Seek(0, SeekEnd) = %d, %v; want the file's length %d", end, err, len(data))
	}
	if start, err := r.Seek(0, io.SeekStart); start != 0 || err != nil {
		t.Fatalf("Seek(0, SeekStart) = %d, %v; want 0", start, err)
	}
	assertRest(t, r, data)
}

func TestAcquireRefillsDeadEntry(t *testing.T) {
	s := newStore(t)
	a, b := xet.XorbHash{1}, xet.XorbHash{2}
	dataA, dataB := s.put(t, a, fileSize), s.put(t, b, fileSize)
	c := xorbmap.New(1, s.open, s.release)
	assertDrained(t, "reader of A", drain(c, a), dataA)
	assertDrained(t, "reader of B evicting A", drain(c, b), dataB)
	assertReleased(t, s, "B evicted idle A", a)

	assertDrained(t, "reader of evicted A", drain(c, a), dataA)
	if n := s.opened(a); n != 2 {
		t.Fatalf("open(A) ran %d times, want 2 (a refill after the eviction)", n)
	}
	if mapped := c.Mapped(); mapped != 1 {
		t.Fatalf("Mapped() = %d after refilled A evicted B, want 1", mapped)
	}
}

func TestAcquireRacesEviction(t *testing.T) {
	const goroutines, iterations = 8, 500
	s := newStore(t)
	hashes := [2]xet.XorbHash{{1}, {2}}
	stored := [2][]byte{s.put(t, hashes[0], fileSize), s.put(t, hashes[1], fileSize)}
	c := xorbmap.New(1, s.open, s.release)

	// Odd goroutines start with B, so A and B keep evicting each other from the only slot.
	runs := make([]<-chan error, goroutines)
	for g := range goroutines {
		runs[g] = inBackground(func() error {
			for i := range iterations {
				k := (g + i) % 2
				got := drain(c, hashes[k])
				if got.err != nil {
					return fmt.Errorf("iteration %d: %w", i, got.err)
				}
				if !bytes.Equal(got.data, stored[k]) {
					return fmt.Errorf("iteration %d read %d bytes that differ from the %d in the file", i, len(got.data), len(stored[k]))
				}
			}
			return nil
		})
	}
	for g, pending := range runs {
		if err := await(t, fmt.Sprintf("goroutine %d", g), pending); err != nil {
			t.Errorf("goroutine %d: %v", g, err)
		}
	}
	assertCache(t, c, "every reader closed", 1, 1)
	opens, releases := s.opened(hashes[0])+s.opened(hashes[1]), len(s.released())
	if releases != opens-1 {
		t.Fatalf("%d release calls for %d opens, want %d (every mapping but the cached one torn down once)", releases, opens, opens-1)
	}
}

func TestFillOutsideLock(t *testing.T) {
	s := newStore(t)
	a, b := xet.XorbHash{1}, xet.XorbHash{2}
	dataA, dataB := s.put(t, a, fileSize), s.put(t, b, fileSize)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblockA := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(unblockA) // a failed test must not leave A's fill parked
	c := xorbmap.New(0, func(h xet.XorbHash) (*os.File, error) {
		if h == a {
			close(entered)
			<-unblock
		}
		return s.open(h)
	}, s.release)

	fillA := inBackground(func() drained { return drain(c, a) })
	await(t, "open(A) to be called", entered)
	assertDrained(t, "reader of B", await(t, "Acquire(B) while A's fill is parked in open", inBackground(func() drained { return drain(c, b) })), dataB)
	if await(t, "Has(A) while A's fill is parked in open", inBackground(func() bool { return c.Has(a) })) {
		t.Fatal("Has(A) = true while A's fill is still in open, want false")
	}
	if !await(t, "Has(B) while A's fill is parked in open", inBackground(func() bool { return c.Has(b) })) {
		t.Fatal("Has(B) = false after B was read, want true")
	}
	unblockA()
	assertDrained(t, "reader of A", await(t, "Acquire(A) once its open was unblocked", fillA), dataA)
}

func TestSingleFlightSharesFill(t *testing.T) {
	const callers = 8
	s := newStore(t)
	a := xet.XorbHash{1}
	data := s.put(t, a, fileSize)
	// The fill parks until every caller is inside Acquire, so all of them miss together.
	c := xorbmap.New(0, func(h xet.XorbHash) (*os.File, error) {
		if err := waitInsideAcquire(callers); err != nil {
			return nil, err
		}
		return s.open(h)
	}, s.release)

	runs := make([]<-chan drained, callers)
	for i := range runs {
		runs[i] = inBackground(func() drained { return drain(c, a) })
	}
	for i, pending := range runs {
		assertDrained(t, fmt.Sprintf("caller %d", i), await(t, fmt.Sprintf("caller %d", i), pending), data)
	}
	if n := s.opened(a); n != 1 {
		t.Fatalf("open(A) ran %d times for %d concurrent Acquires, want 1 shared fill", n, callers)
	}
	assertCache(t, c, "every caller closed", 1, 1)
}

func TestReleaseHookOnceAfterLastHold(t *testing.T) {
	s := newStore(t)
	a := xet.XorbHash{1}
	s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	r1 := mustAcquire(t, c, a).Reader()
	defer r1.Close()
	r2 := mustAcquire(t, c, a).Reader()
	defer r2.Close()

	c.Remove(a)
	assertReleased(t, s, "Remove with two readers open")
	if err := r1.Close(); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, s, "the first reader closed")
	if err := r2.Close(); err != nil {
		t.Fatal(err)
	}
	assertReleased(t, s, "the last reader closed", a)
	c.Remove(a)
	assertReleased(t, s, "a second Remove", a)
}

func TestAcquireOpenErrorWrapsNotExist(t *testing.T) {
	s := newStore(t)
	c := xorbmap.New(0, s.open, s.release)
	if _, err := c.Acquire(xet.XorbHash{1}); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("Acquire of a hash whose open fails with fs.ErrNotExist: error = %v, want one wrapping fs.ErrNotExist", err)
	}
	assertCache(t, c, "a failed open", 0, 0)
	assertReleased(t, s, "a failed open")
}

func TestRemoveThenAcquireReopens(t *testing.T) {
	s := newStore(t)
	a := xet.XorbHash{1}
	data := s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	assertDrained(t, "reader of A", drain(c, a), data)
	c.Remove(a)
	assertReleased(t, s, "removing idle A", a)

	assertDrained(t, "reader of removed A", drain(c, a), data)
	if n := s.opened(a); n != 2 {
		t.Fatalf("open(A) ran %d times, want 2 (a refill after Remove)", n)
	}
	if !c.Has(a) {
		t.Fatal("Has(A) = false after removed A was acquired again, want true")
	}
}

func TestZeroByteFile(t *testing.T) {
	s := newStore(t)
	z := xet.XorbHash{1}
	s.put(t, z, 0)
	c := xorbmap.New(0, s.open, s.release)
	m := mustAcquire(t, c, z)
	if got := m.Bytes(); got != nil {
		t.Fatalf("Bytes() of a 0-byte file = non-nil slice of %d bytes, want nil", len(got))
	}
	assertCache(t, c, "acquiring a 0-byte file", 1, 0)
	r := m.Reader()
	defer r.Close()
	if n, err := r.Read(make([]byte, 16)); n != 0 || err != io.EOF {
		t.Fatalf("Read of a 0-byte file = %d, %v; want 0, io.EOF", n, err)
	}
}

func TestCacheBounded(t *testing.T) {
	s := newStore(t)
	c := xorbmap.New(2, s.open, s.release)
	var hashes []xet.XorbHash
	for i := range byte(5) {
		h := xet.XorbHash{i + 1}
		data := s.put(t, h, fileSize)
		assertDrained(t, fmt.Sprintf("reader of hash %d", i+1), drain(c, h), data)
		hashes = append(hashes, h)
	}
	assertCache(t, c, "reading 5 hashes through a 2-entry cache", 2, 2)
	assertReleased(t, s, "reading 5 hashes through a 2-entry cache", hashes[:3]...)
}

func TestFillClosesFile(t *testing.T) {
	a := xet.XorbHash{1}
	s := newStore(t)
	s.put(t, a, fileSize)
	c := xorbmap.New(0, s.open, s.release)
	m := mustAcquire(t, c, a)
	defer m.Release()
	assertClosed(t, s.onlyFile(t))

	t.Run("map error", func(t *testing.T) {
		s := newStore(t)
		s.put(t, a, fileSize)
		s.flag = os.O_WRONLY
		// A release hook may remove the file, which Windows refuses while it is open.
		var statErr error
		c := xorbmap.New(0, s.open, func(h xet.XorbHash) {
			_, statErr = s.onlyFile(t).Stat()
			s.release(h)
		})
		m, err := c.Acquire(a)
		if err == nil {
			m.Release()
			t.Skip("this OS maps a write-only file read-only")
		}
		if errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("Acquire of a write-only file: error = %v wraps fs.ErrNotExist, want a map error", err)
		}
		assertCache(t, c, "a failed map", 0, 0)
		assertReleased(t, s, "a failed map", a)
		if !errors.Is(statErr, os.ErrClosed) {
			t.Fatalf("Stat of the file open returned, as release ran = %v, want os.ErrClosed", statErr)
		}
	})
}
