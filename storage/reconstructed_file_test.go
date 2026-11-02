package storage_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/memory"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/xorb"
)

// mixedChunkSizes runs from one byte to a full chunk, so frames of very
// different packed sizes sit side by side.
var mixedChunkSizes = []int{1, xet.MaxChunkSize, 3, 4 << 10, 2, 64<<10 + 1, 1000}

var noneClass, lz4Class = storagetest.ChunkClasses[0], storagetest.ChunkClasses[1]

var (
	errInjected = errors.New("injected range failure")
	errReleased = errors.New("blocked range released at test end")
	missingXorb = xet.XorbHash{0xde, 0xad} // held by no store
)

func newLocal(t *testing.T) storage.Storage {
	t.Helper()
	st, err := local.NewStorage(local.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

var backends = []struct {
	name string
	open func(t *testing.T) storage.Storage
}{
	{"local", newLocal},
	{"memory", func(*testing.T) storage.Storage { return memory.NewStorage() }},
}

// xorbsOf draws xorbs of chunks sized by sizes from class c; xorb x starts x
// places further along sizes.
func xorbsOf(c storagetest.ChunkClass, xorbs int, sizes []int) [][][]byte {
	out := make([][][]byte, xorbs)
	for x := range out {
		out[x] = make([][]byte, len(sizes))
		for i := range sizes {
			out[x][i] = make([]byte, sizes[(i+x)%len(sizes)])
			c.Chunk(x, i, out[x][i])
		}
	}
	return out
}

// storedFile is a stored file with the shard and digest that open it.
type storedFile struct {
	storagetest.File
	digest [32]byte
	sh     *shard.Shard
}

// putXorbs stores a file of one whole-xorb term per element of xorbs.
func putXorbs(t *testing.T, st storage.Storage, xorbs [][][]byte) storedFile {
	t.Helper()
	f := storagetest.PutXorbFile(t, t.Context(), st, xorbs)
	return loadFile(t, st, f, storagetest.SHA256Digest(f.SHA256Hex))
}

func loadFile(t *testing.T, st storage.Storage, f storagetest.File, digest [32]byte) storedFile {
	t.Helper()
	sh, err := st.GetShard(t.Context(), f.FileHash)
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	return storedFile{File: f, digest: digest, sh: sh}
}

func (f storedFile) entries() []shard.FileDataSequenceEntry {
	return storage.FindFileBySHA256(f.sh, f.digest).Entries
}

// termStart is the file offset where term i begins.
func (f storedFile) termStart(i int) int64 {
	var n int64
	for _, e := range f.entries()[:i] {
		n += int64(e.UnpackedSegBytes)
	}
	return n
}

// withEntries returns f described, in a shard never stored, by entries.
func (f storedFile) withEntries(entries []shard.FileDataSequenceEntry) storedFile {
	fb := *storage.FindFileBySHA256(f.sh, f.digest)
	fb.Entries = entries
	f.sh = &shard.Shard{Files: []shard.FileBlock{fb}}
	return f
}

// subRanges are the terms of the sub-range file: chunks [start, end) of xorb x.
var subRanges = []struct {
	x          int
	start, end uint32
}{{0, 1, 3}, {1, 0, 2}, {0, 3, 7}, {1, 5, 7}, {0, 0, 1}}

// putSubRangeFile stores xorbs, then commits through PutShard a file of
// subRanges over them.
func putSubRangeFile(t *testing.T, st storage.Storage, xorbs [][][]byte) storedFile {
	t.Helper()
	whole := putXorbs(t, st, xorbs)
	f := storagetest.File{XorbHashes: whole.XorbHashes}
	fb := shard.FileBlock{}
	var sizes []uint64
	for _, r := range subRanges {
		e := shard.FileDataSequenceEntry{CASHash: whole.XorbHashes[r.x], ChunkIndexStart: r.start, ChunkIndexEnd: r.end}
		for _, c := range xorbs[r.x][r.start:r.end] {
			f.Content = append(f.Content, c...)
			f.ChunkHashes = append(f.ChunkHashes, xet.ComputeChunkHash(c))
			sizes = append(sizes, uint64(len(c)))
			e.UnpackedSegBytes += uint32(len(c))
		}
		fb.Entries = append(fb.Entries, e)
	}
	f.FileHash = xet.ComputeFileHash(f.ChunkHashes, sizes)
	fb.FileHash = f.FileHash
	sh := shard.NewShard()
	sh.AddFile(fb)
	if _, err := st.PutShard(t.Context(), sh); err != nil {
		t.Fatalf("PutShard(sub-range file): %v", err)
	}
	return loadFile(t, st, f, sha256.Sum256(f.Content))
}

// withZeroLengthTerms returns f with a zero-length term of missingXorb
// before, between and after its terms.
func withZeroLengthTerms(f storedFile) storedFile {
	empty := shard.FileDataSequenceEntry{CASHash: missingXorb}
	var entries []shard.FileDataSequenceEntry
	for _, e := range f.entries() {
		entries = append(entries, empty, e)
	}
	return f.withEntries(append(entries, empty))
}

// frame is one stored chunk: its header and packed bytes [start, end) in its xorb.
type frame struct {
	xorb.ChunkHeader
	start, end int64
}

// framesOf parses the chunk headers of the stored xorb h.
func framesOf(t *testing.T, st storage.Storage, h xet.XorbHash) []frame {
	t.Helper()
	offsets, err := st.GetXorbChunkOffsets(t.Context(), h)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := st.GetXorbRangeReadCloser(t.Context(), h, 0, int64(offsets[len(offsets)-1])-1)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	frames := make([]frame, len(offsets))
	var start int64
	for i, end := range offsets {
		hdr, err := xorb.ParseChunkHeader(data[start:])
		if err != nil {
			t.Fatalf("xorb %s chunk %d: %v", h.String(), i, err)
		}
		frames[i] = frame{ChunkHeader: hdr, start: start, end: int64(end)}
		start = int64(end)
	}
	return frames
}

// termRange is the inclusive packed byte range of term e's chunks.
func termRange(frames []frame, e shard.FileDataSequenceEntry) (start, end int64) {
	return frames[e.ChunkIndexStart].start, frames[e.ChunkIndexEnd-1].end - 1
}

// countingStore records the xorb reads a reconstructed file makes; fault,
// when set before the file opens, may wrap each range it serves.
type countingStore struct {
	storage.Storage
	fault func(ctx context.Context, r rangeRead, rd io.Reader) io.Reader

	mu      sync.Mutex
	calls   map[xet.XorbHash]int // xorb reads of any kind
	seekers int                  // GetXorbReadSeekCloser calls
	offsets map[xet.XorbHash]int // GetXorbChunkOffsets calls
	ranges  []*rangeRead
	open    int // range readers not yet closed
	maxOpen int
	ctxs    []context.Context // what each xorb read was given
}

// rangeRead is one GetXorbRangeReadCloser call and what its reader served.
type rangeRead struct {
	hash       xet.XorbHash
	nth        int // 1 for the first range of hash
	start, end int64
	read       int64
	closes     int
}

func (r rangeRead) String() string {
	return fmt.Sprintf("%.8s#%d[%d,%d] read %d closed %d", r.hash.String(), r.nth, r.start, r.end, r.read, r.closes)
}

func newCountingStore(st storage.Storage) *countingStore {
	return &countingStore{Storage: st, calls: map[xet.XorbHash]int{}, offsets: map[xet.XorbHash]int{}}
}

func (s *countingStore) note(ctx context.Context, h xet.XorbHash) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[h]++
	s.ctxs = append(s.ctxs, ctx)
}

func (s *countingStore) GetXorbReadSeekCloser(ctx context.Context, h xet.XorbHash) (io.ReadSeekCloser, error) {
	s.note(ctx, h)
	s.mu.Lock()
	s.seekers++
	s.mu.Unlock()
	return s.Storage.GetXorbReadSeekCloser(ctx, h)
}

func (s *countingStore) GetXorbDataRange(ctx context.Context, h xet.XorbHash, chunkStart, chunkEnd uint32) (int64, int64, error) {
	s.note(ctx, h)
	return s.Storage.GetXorbDataRange(ctx, h, chunkStart, chunkEnd)
}

func (s *countingStore) GetXorbChunkOffsets(ctx context.Context, h xet.XorbHash) ([]uint64, error) {
	s.note(ctx, h)
	s.mu.Lock()
	s.offsets[h]++
	s.mu.Unlock()
	return s.Storage.GetXorbChunkOffsets(ctx, h)
}

func (s *countingStore) GetXorbRangeReadCloser(ctx context.Context, h xet.XorbHash, start, end int64) (io.ReadCloser, error) {
	s.note(ctx, h)
	rc, err := s.Storage.GetXorbRangeReadCloser(ctx, h, start, end)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	r := &rangeRead{hash: h, nth: 1, start: start, end: end}
	for _, prev := range s.ranges {
		if prev.hash == h {
			r.nth++
		}
	}
	s.ranges = append(s.ranges, r)
	s.open++
	s.maxOpen = max(s.maxOpen, s.open)
	s.mu.Unlock()
	var rd io.Reader = rc
	if s.fault != nil {
		rd = s.fault(ctx, *r, rc)
	}
	return &countedRange{s: s, rec: r, r: rd, c: rc}, nil
}

func (s *countingStore) total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		n += c
	}
	return n
}

func (s *countingStore) readsOf(h xet.XorbHash) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[h]
}

func (s *countingStore) rangeReads() []rangeRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]rangeRead, len(s.ranges))
	for i, r := range s.ranges {
		out[i] = *r
	}
	return out
}

func (s *countingStore) rangesOf(h xet.XorbHash) int {
	n := 0
	for _, r := range s.rangeReads() {
		if r.hash == h {
			n++
		}
	}
	return n
}

// rangeBytes is how many bytes the range readers have served.
func (s *countingStore) rangeBytes() int64 {
	var n int64
	for _, r := range s.rangeReads() {
		n += r.read
	}
	return n
}

// countedRange is a range reader whose reads and closes its store records.
type countedRange struct {
	s   *countingStore
	rec *rangeRead
	r   io.Reader
	c   io.Closer
}

func (r *countedRange) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.s.mu.Lock()
	r.rec.read += int64(n)
	r.s.mu.Unlock()
	return n, err
}

func (r *countedRange) Close() error {
	r.s.mu.Lock()
	r.rec.closes++
	if r.rec.closes == 1 {
		r.s.open--
	}
	r.s.mu.Unlock()
	return r.c.Close()
}

// faultyRange serves r, at xorb offset off, with the byte at a non-zero flip
// inverted; at a non-zero stop it fails with err or, without one, signals
// blocked and waits for ctx or release.
type faultyRange struct {
	r          io.Reader
	off        int64
	flip, stop int64
	err        error
	ctx        context.Context
	release    <-chan struct{}
	blocked    chan struct{}
}

func (f *faultyRange) Read(p []byte) (int, error) {
	if f.stop != 0 {
		if f.off >= f.stop {
			if f.err != nil {
				return 0, f.err
			}
			select {
			case f.blocked <- struct{}{}:
			default:
			}
			select {
			case <-f.ctx.Done():
				return 0, f.ctx.Err()
			case <-f.release:
				return 0, errReleased
			}
		}
		p = p[:min(int64(len(p)), f.stop-f.off)]
	}
	n, err := f.r.Read(p)
	if f.flip != 0 && f.flip >= f.off && f.flip < f.off+int64(n) {
		p[f.flip-f.off] ^= 0xff
	}
	f.off += int64(n)
	return n, err
}

// inject has dec serve ranges of xorb h through f, or only the first with first set.
func inject(dec *countingStore, h xet.XorbHash, first bool, f faultyRange) {
	dec.fault = func(ctx context.Context, r rangeRead, rd io.Reader) io.Reader {
		if r.hash != h || (first && r.nth > 1) {
			return rd
		}
		g := f
		g.r, g.ctx, g.off = rd, ctx, r.start
		return &g
	}
}

// stuckRange serves r only once release closes, as a backend read deaf to
// cancellation would: its first Read signals blocked, then waits, signalling
// cancelled should ctx end first; released, it fails with a non-nil err or
// serves r.
type stuckRange struct {
	r                  io.Reader
	ctx                context.Context
	err                error
	blocked, cancelled chan<- struct{}
	release            <-chan struct{}
	waited             bool
}

func (s *stuckRange) Read(p []byte) (int, error) {
	if !s.waited {
		s.waited = true
		s.blocked <- struct{}{}
		select {
		case <-s.release:
		case <-s.ctx.Done():
			s.cancelled <- struct{}{}
			<-s.release
		}
	}
	if s.err != nil {
		return 0, s.err
	}
	return s.r.Read(p)
}

// openFile opens f over st and closes it when the test ends.
func openFile(t *testing.T, ctx context.Context, st storage.Storage, f storedFile) io.ReadSeekCloser {
	t.Helper()
	rc, err := storage.NewReconstructedFile(ctx, st, f.sh, f.digest)
	if err != nil {
		t.Fatalf("NewReconstructedFile: %v", err)
	}
	t.Cleanup(func() { rc.Close() })
	return rc
}

// readAllBy reads r to EOF in Reads of at most size bytes.
func readAllBy(r io.Reader, size int) ([]byte, error) {
	var out []byte
	buf := make([]byte, size)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

// checkFailedRead fails unless err ended a read of got that matches the start
// of content and the next Read repeats err.
func checkFailedRead(t *testing.T, rc io.Reader, content, got []byte, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("read %d of %d bytes without error; bad chunk data must fail the read", len(got), len(content))
	}
	if len(got) > len(content) || !bytes.Equal(got, content[:len(got)]) {
		t.Fatalf("the %d bytes before %v differ from the file's", len(got), err)
	}
	if n, again := rc.Read(make([]byte, 1)); n != 0 || again == nil || again.Error() != err.Error() {
		t.Fatalf("Read after %v = %d, %v; want the same error again", err, n, again)
	}
}

// eventually reports whether cond holds within 5 s.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(time.Millisecond)
	}
	return true
}

// settleGoroutines fails unless the goroutine count falls back to baseline.
func settleGoroutines(t *testing.T, baseline int) {
	t.Helper()
	if !eventually(func() bool { return runtime.NumGoroutine() <= baseline }) {
		buf := make([]byte, 1<<20)
		t.Fatalf("%d goroutines outlive the reader:\n%s", runtime.NumGoroutine()-baseline, buf[:runtime.Stack(buf, true)])
	}
}

// within fails unless op returns within d.
func within(t *testing.T, d time.Duration, what string, op func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		op()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s still blocked after %v", what, d)
	}
}

// closeWithin fails unless rc.Close returns within d.
func closeWithin(t *testing.T, rc io.Closer, d time.Duration) {
	t.Helper()
	within(t, d, "Close", func() { rc.Close() })
}

// blockAfterFirstChunk returns a store whose first range of f's first xorb
// serves chunk 0, then blocks until the context it was given ends; blocked
// receives once a Read waits there.
func blockAfterFirstChunk(t *testing.T, st storage.Storage, f storedFile) (*countingStore, <-chan struct{}) {
	t.Helper()
	h := f.XorbHashes[0]
	blocked := make(chan struct{}, 1)
	dec := newCountingStore(st)
	inject(dec, h, true, faultyRange{stop: framesOf(t, st, h)[0].end, release: t.Context().Done(), blocked: blocked})
	return dec, blocked
}

// readFirstChunk reads f's first size bytes, then waits for the blocked range.
func readFirstChunk(t *testing.T, rc io.Reader, f storedFile, size int, blocked <-chan struct{}) {
	t.Helper()
	got := make([]byte, size)
	var err error
	within(t, 5*time.Second, "Read of the first chunk", func() { _, err = io.ReadFull(rc, got) })
	if err != nil || !bytes.Equal(got, f.Content[:size]) {
		t.Fatalf("reading the first chunk: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("no Read ever waited on the term's GetXorbRangeReadCloser")
	}
}

// stalledReader reads one byte of a one-term file of 128 1 KiB chunks.
func stalledReader(t *testing.T) (*countingStore, io.ReadSeekCloser, []frame) {
	t.Helper()
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 1, slices.Repeat([]int{1 << 10}, 128)))
	dec := newCountingStore(st)
	rc := openFile(t, t.Context(), dec, f)
	if _, err := io.ReadFull(rc, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	return dec, rc, framesOf(t, st, f.XorbHashes[0])
}

// Every class, as one or three xorbs of mixed chunk sizes, reads back as
// stored a byte, 32 KiB or everything at a time.
func TestReconstructedFileMatchesContent(t *testing.T) {
	reads := []struct {
		name string
		read func(io.Reader) ([]byte, error)
	}{
		{"1 B", func(r io.Reader) ([]byte, error) { return readAllBy(r, 1) }},
		{"32 KiB", func(r io.Reader) ([]byte, error) { return readAllBy(r, 32<<10) }},
		{"io.ReadAll", io.ReadAll},
	}
	for _, b := range backends {
		for _, c := range storagetest.ChunkClasses {
			for _, n := range []int{1, 3} {
				t.Run(fmt.Sprintf("%s/%s/xorbs=%d", b.name, c.Name, n), func(t *testing.T) {
					st := b.open(t)
					f := putXorbs(t, st, xorbsOf(c, n, mixedChunkSizes))
					seen := map[xorb.CompressionType]bool{}
					for _, h := range f.XorbHashes {
						for _, fr := range framesOf(t, st, h) {
							seen[fr.Compression] = true
						}
					}
					if !seen[c.Compression] {
						t.Fatalf("no chunk of the fixture is stored as %s", c.Name)
					}
					for _, read := range reads {
						rc, err := st.GetReconstructedFile(t.Context(), f.digest)
						if err != nil {
							t.Fatal(err)
						}
						got, err := read.read(rc)
						rc.Close()
						if err != nil || !bytes.Equal(got, f.Content) {
							t.Fatalf("read by %s = %d bytes, %v; want the %d stored", read.name, len(got), err, len(f.Content))
						}
					}
				})
			}
		}
	}
}

// Seeks into terms and chunks, back and forth, then reads to EOF.
func TestReconstructedFileSeekMidTermReadsSuffix(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			f := putXorbs(t, st, xorbsOf(lz4Class, 3, mixedChunkSizes))
			rc := openFile(t, t.Context(), st, f)
			size := int64(len(f.Content))
			for _, pos := range []int64{f.termStart(1) + 70000, 5, f.termStart(2) + 9, f.termStart(1), size - 1, 1, size} {
				if got, err := rc.Seek(pos, io.SeekStart); err != nil || got != pos {
					t.Fatalf("Seek(%d) = %d, %v", pos, got, err)
				}
				if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content[pos:]) {
					t.Fatalf("read after Seek(%d) = %d bytes, %v; want the %d after it", pos, len(got), err, size-pos)
				}
			}
		})
	}
}

// A Seek back inside the decoded chunk rereads it without touching storage.
func TestReconstructedFileSeekBackInsideChunkRereadsWithoutIO(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			f := putXorbs(t, st, xorbsOf(lz4Class, 1, mixedChunkSizes))
			dec := newCountingStore(st)
			rc := openFile(t, t.Context(), dec, f)
			// Chunk 1, 128 KiB, holds file bytes [1, 131073).
			const from, pos, back = 5001, 9001, 2000
			if _, err := rc.Seek(from, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadFull(rc, make([]byte, pos-from)); err != nil {
				t.Fatal(err)
			}
			reads := dec.readsOf(f.XorbHashes[0])
			if got, err := rc.Seek(-back, io.SeekCurrent); err != nil || got != pos-back {
				t.Fatalf("Seek(-%d, SeekCurrent) = %d, %v; want %d", back, got, err, pos-back)
			}
			got := make([]byte, back)
			if _, err := io.ReadFull(rc, got); err != nil || !bytes.Equal(got, f.Content[pos-back:pos]) {
				t.Fatalf("reread after Seek back = %v; want the file's bytes [%d, %d)", err, pos-back, pos)
			}
			if n := dec.readsOf(f.XorbHashes[0]) - reads; n != 0 {
				t.Fatalf("Seek back inside the decoded chunk made %d xorb reads, want none", n)
			}
		})
	}
}

// A Seek one byte past the decoded chunk's end is a Seek away: the next Read
// serves the bytes there through one new range read.
func TestReconstructedFileSeekPastChunkEndOpensNewRange(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 1, mixedChunkSizes))
	dec := newCountingStore(st)
	rc := openFile(t, t.Context(), dec, f)
	// Chunk 1, 128 KiB, holds file bytes [1, 131073).
	const in, past = 5001, 131073 + 1
	if _, err := rc.Seek(in, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(rc, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	opened := len(dec.rangeReads())
	if got, err := rc.Seek(past, io.SeekStart); err != nil || got != past {
		t.Fatalf("Seek(%d) = %d, %v", past, got, err)
	}
	got := make([]byte, 100)
	if _, err := io.ReadFull(rc, got); err != nil || !bytes.Equal(got, f.Content[past:past+100]) {
		t.Fatalf("read after Seek(%d) = %v; want the file's bytes [%d, %d)", past, err, past, past+100)
	}
	if n := len(dec.rangeReads()) - opened; n != 1 {
		t.Fatalf("Read after a Seek one past the decoded chunk opened %d ranges, want 1", n)
	}
}

// A Seek to a term's first byte resumes from that term's range alone: the
// term ending there is not opened again.
func TestReconstructedFileSeekToTermStartOpensOnlyThatTerm(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	dec := newCountingStore(st)
	rc := openFile(t, t.Context(), dec, f)
	if _, err := io.ReadFull(rc, make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	pos := f.termStart(1)
	if got, err := rc.Seek(pos, io.SeekStart); err != nil || got != pos {
		t.Fatalf("Seek(%d) = %d, %v", pos, got, err)
	}
	opened := len(dec.rangeReads())
	if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content[pos:]) {
		t.Fatalf("read after Seek(%d) = %d bytes, %v; want the %d after it", pos, len(got), err, int64(len(f.Content))-pos)
	}
	if next := dec.rangeReads()[opened:]; len(next) != 1 || next[0].hash != f.XorbHashes[1] {
		t.Fatalf("ranges after Seek(%d) = %v; want only the second term's", pos, next)
	}
}

// Opening, a HEAD's Seek(0, End) and Seek(0, Start), other seeks and Close
// before any Read neither read storage nor start goroutines.
func TestReconstructedFileHeadSeeksDoNoIO(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 3, mixedChunkSizes))
	dec := newCountingStore(st)
	baseline := runtime.NumGoroutine()
	rc := openFile(t, t.Context(), dec, f)
	size := int64(len(f.Content))
	for _, s := range []struct {
		offset int64
		whence int
		want   int64
	}{
		{0, io.SeekEnd, size},
		{0, io.SeekStart, 0},
		{0, io.SeekCurrent, 0},
		{size / 2, io.SeekStart, size / 2},
		{-1, io.SeekCurrent, size/2 - 1},
	} {
		if got, err := rc.Seek(s.offset, s.whence); err != nil || got != s.want {
			t.Fatalf("Seek(%d, %d) = %d, %v; want %d", s.offset, s.whence, got, err, s.want)
		}
	}
	if n := dec.total(); n != 0 {
		t.Fatalf("open and seeks made %d xorb reads, want none before a Read", n)
	}
	if n := runtime.NumGoroutine() - baseline; n > 0 {
		t.Fatalf("open and seeks started %d goroutines, want none before a Read", n)
	}
	if err := rc.Close(); err != nil {
		t.Fatalf("Close = %v", err)
	}
	if n := dec.total(); n != 0 {
		t.Fatalf("Close before any Read made %d xorb reads, want none", n)
	}
}

// Terms over part of an xorb, one xorb several times, read back as their
// chunks, also from inside a term starting past its xorb's first chunk.
func TestReconstructedFileSubRangeTerms(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			xorbs := xorbsOf(lz4Class, 2, mixedChunkSizes)
			f := putSubRangeFile(t, st, xorbs)
			rc, err := st.GetReconstructedFile(t.Context(), f.digest)
			if err != nil {
				t.Fatal(err)
			}
			defer rc.Close()
			if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content) {
				t.Fatalf("read = %d bytes, %v; want the %d of the terms' chunks", len(got), err, len(f.Content))
			}
			// Into the second chunk of the term over chunks [3, 7) of xorb 0.
			pos := f.termStart(2) + int64(len(xorbs[0][3])) + 1
			if _, err := rc.Seek(pos, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content[pos:]) {
				t.Fatalf("read after Seek(%d) = %d bytes, %v; want the %d after it", pos, len(got), err, len(f.Content)-int(pos))
			}
		})
	}
}

// A file of no terms, stored under the zero digest, is at EOF at once.
func TestReconstructedFileEmptyFileIsImmediateEOF(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			f := loadFile(t, st, storagetest.PutFile(t, t.Context(), st, nil), [32]byte{})
			dec := newCountingStore(st)
			baseline := runtime.NumGoroutine()
			rc := openFile(t, t.Context(), dec, f)
			if n, err := rc.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("Read = %d, %v; want 0, io.EOF", n, err)
			}
			if n := dec.total(); n != 0 {
				t.Fatalf("empty file made %d xorb reads, want none", n)
			}
			if n := runtime.NumGoroutine() - baseline; n > 0 {
				t.Fatalf("empty file started %d goroutines, want none", n)
			}
		})
	}
}

// Zero-length terms read as nothing, though they name a missing xorb.
func TestReconstructedFileSkipsZeroLengthTerms(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			f := withZeroLengthTerms(putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes)))
			dec := newCountingStore(st)
			if got, err := io.ReadAll(openFile(t, t.Context(), dec, f)); err != nil || !bytes.Equal(got, f.Content) {
				t.Fatalf("read = %d bytes, %v; want the %d of the non-empty terms", len(got), err, len(f.Content))
			}
			if n := dec.readsOf(missingXorb); n != 0 {
				t.Fatalf("zero-length terms made %d reads of their xorb, want none", n)
			}
		})
	}
}

// Each non-empty term is one GetXorbRangeReadCloser of exactly its chunks,
// read through and closed once before the next opens, beside at most one
// GetXorbChunkOffsets and no GetXorbReadSeekCloser, whether the file is read
// to EOF or, as http.ServeContent sends it, copied to exactly its size.
func TestReconstructedFileOneRangeReadPerTerm(t *testing.T) {
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
	reads := []struct {
		name string
		read func(t *testing.T, rc io.Reader, f storedFile)
	}{
		{"io.ReadAll", func(t *testing.T, rc io.Reader, f storedFile) {
			if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content) {
				t.Fatalf("read = %d bytes, %v; want %d", len(got), err, len(f.Content))
			}
		}},
		{"io.CopyN of the size", func(t *testing.T, rc io.Reader, f storedFile) {
			size := int64(len(f.Content))
			if n, err := io.CopyN(io.Discard, rc, size); err != nil || n != size {
				t.Fatalf("io.CopyN(%d) = %d, %v; want all of it", size, n, err)
			}
		}},
	}
	for _, b := range backends {
		for _, file := range files {
			for _, read := range reads {
				t.Run(b.name+"/"+file.name+"/"+read.name, func(t *testing.T) {
					st := b.open(t)
					f := file.put(t, st)
					dec := newCountingStore(st)
					rc := openFile(t, t.Context(), dec, f)
					read.read(t, rc, f)
					if err := rc.Close(); err != nil {
						t.Fatalf("Close = %v", err)
					}
					var want []rangeRead
					terms := map[xet.XorbHash]int{}
					for _, e := range f.entries() {
						if e.UnpackedSegBytes == 0 {
							continue
						}
						terms[e.CASHash]++
						start, end := termRange(framesOf(t, st, e.CASHash), e)
						want = append(want, rangeRead{hash: e.CASHash, nth: terms[e.CASHash], start: start, end: end, read: end - start + 1, closes: 1})
					}
					if got := dec.rangeReads(); !slices.Equal(got, want) {
						t.Errorf("range reads = %v; want one per non-empty term, read through and closed once: %v", got, want)
					}
					dec.mu.Lock()
					defer dec.mu.Unlock()
					for h, n := range dec.offsets {
						if n > terms[h] {
							t.Errorf("GetXorbChunkOffsets(%.8s) called %d times for %d terms, want at most one per term", h.String(), n, terms[h])
						}
					}
					if dec.seekers != 0 {
						t.Errorf("GetXorbReadSeekCloser called %d times, want never", dec.seekers)
					}
					if dec.maxOpen > 1 {
						t.Errorf("%d range readers open at once, want one at a time", dec.maxOpen)
					}
				})
			}
		}
	}
}

// A fault in chunk k of the second term yields every byte before chunk k,
// then an error every later Read repeats.
func TestReconstructedFileFaultFailsAtChunk(t *testing.T) {
	const k, size = 3, 16 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, slices.Repeat([]int{size}, 8)))
	h := f.XorbHashes[1]
	fk := framesOf(t, st, h)[k]
	if fk.Compression == xorb.CompressionNone {
		t.Fatalf("chunk %d is stored uncompressed, so a flipped byte would pass unnoticed", k)
	}
	before := f.termStart(1) + k*size
	for _, fault := range []struct {
		name string
		f    faultyRange
	}{
		{"flipped body byte", faultyRange{flip: (fk.start + xorb.ChunkHeaderSize + fk.end) / 2}},
		{"header size off its frame", faultyRange{flip: fk.start + 1}},
		{"range fails mid-frame", faultyRange{stop: fk.start + xorb.ChunkHeaderSize + 5, err: errInjected}},
		{"range truncated at chunk boundary", faultyRange{stop: fk.start, err: io.EOF}},
	} {
		t.Run(fault.name, func(t *testing.T) {
			dec := newCountingStore(st)
			inject(dec, h, false, fault.f)
			rc := openFile(t, t.Context(), dec, f)
			got, err := io.ReadAll(rc)
			if dec.rangesOf(h) == 0 {
				t.Fatal("no GetXorbRangeReadCloser of the second term, so the fault was never injected")
			}
			checkFailedRead(t, rc, f.Content, got, err)
			if int64(len(got)) != before {
				t.Errorf("%d bytes before the error, want the %d before chunk %d", len(got), before, k)
			}
		})
	}
}

// A term whose UnpackedSegBytes disagree with its chunks fails rather than
// serving a cut or padded file: declared short, at the chunk overshooting
// it; declared long, once the term's chunks run out, the last term with its
// size mismatch rather than a bare unexpected EOF.
func TestReconstructedFileTermSizeMismatchFails(t *testing.T) {
	st := newLocal(t)
	xorbs := xorbsOf(lz4Class, 3, mixedChunkSizes)
	f := putXorbs(t, st, xorbs)
	for _, term := range []int{0, len(xorbs) - 1} {
		chunks := xorbs[term]
		end := f.termStart(term + 1)
		for _, c := range []struct {
			delta int
			want  int64
			upTo  string
		}{
			{-1, end - int64(len(chunks[len(chunks)-1])), "the term's last chunk, which overshoots its declared size"},
			{1, end, "the end of the term's chunks"},
		} {
			t.Run(fmt.Sprintf("term %d declared %+d", term, c.delta), func(t *testing.T) {
				entries := slices.Clone(f.entries())
				entries[term].UnpackedSegBytes = uint32(int(entries[term].UnpackedSegBytes) + c.delta)
				rc := openFile(t, t.Context(), st, f.withEntries(entries))
				got, err := io.ReadAll(rc)
				checkFailedRead(t, rc, f.Content, got, err)
				if int64(len(got)) != c.want {
					t.Errorf("%d bytes before the error, want the %d up to %s", len(got), c.want, c.upTo)
				}
				if term == len(xorbs)-1 && c.delta > 0 {
					sum := end - f.termStart(term)
					declared := sum + int64(c.delta)
					if want := fmt.Sprintf("has %d bytes, expected %d", sum, declared); !strings.Contains(err.Error(), want) {
						t.Errorf("error %q, want the term's size mismatch, containing %q", err, want)
					}
				}
			})
		}
	}
}

// A term declared short by whole chunks, last or not, fails at its declared
// end rather than reading as a clean shorter file: the bytes up to that end,
// less the file's final declared chunk when the term is last, then the term's
// size error, which every later Read repeats, promptly even when the left-out
// chunks outnumber the window.
func TestReconstructedFileTermShortByWholeChunksFails(t *testing.T) {
	st := newLocal(t)
	// Terms long enough to leave out more chunks than the window holds.
	xorbs := xorbsOf(lz4Class, 3, slices.Repeat([]int{4 << 10}, 8+2))
	f := putXorbs(t, st, xorbs)
	last := len(xorbs) - 1
	for _, c := range []struct {
		name        string
		term, short int
	}{
		{"last term declared one chunk short", last, 1},
		{"last term declared more chunks short than the window", last, 8 + 1},
		{"middle term declared one chunk short", 1, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			declared := 0
			for _, chunk := range xorbs[c.term][:len(xorbs[c.term])-c.short] {
				declared += len(chunk)
			}
			entries := slices.Clone(f.entries())
			entries[c.term].UnpackedSegBytes = uint32(declared)
			rc := openFile(t, t.Context(), st, f.withEntries(entries))
			var got []byte
			var err error
			within(t, 5*time.Second, "Read past the term's declared end", func() { got, err = io.ReadAll(rc) })
			checkFailedRead(t, rc, f.Content, got, err)
			want := f.termStart(c.term) + int64(declared)
			upTo := "the term's declared end"
			if c.term == last {
				want -= int64(len(xorbs[c.term][len(xorbs[c.term])-c.short-1]))
				upTo = "its final declared chunk, which waits on the term's accounting"
			}
			if int64(len(got)) != want {
				t.Errorf("%d bytes before the error, want the %d up to %s", len(got), want, upTo)
			}
			if want := fmt.Sprintf("chunks exceed the term's %d bytes", declared); !strings.Contains(err.Error(), want) {
				t.Errorf("error %q, want the term's size error, containing %q", err, want)
			}
		})
	}
}

// With the last term declared short by its last chunk, a Read after only
// Seek(0, SeekEnd) is still io.EOF without storage: no pipeline has started
// to check the term.
func TestReconstructedFileSeekEndOfShortLastTermReadsEOFWithoutIO(t *testing.T) {
	st := newLocal(t)
	xorbs := xorbsOf(lz4Class, 2, mixedChunkSizes)
	f := putXorbs(t, st, xorbs)
	lastTerm := xorbs[len(xorbs)-1]
	entries := slices.Clone(f.entries())
	entries[len(entries)-1].UnpackedSegBytes -= uint32(len(lastTerm[len(lastTerm)-1]))
	dec := newCountingStore(st)
	rc := openFile(t, t.Context(), dec, f.withEntries(entries))
	if _, err := rc.Seek(0, io.SeekEnd); err != nil {
		t.Fatal(err)
	}
	if n, err := rc.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("Read after Seek(0, SeekEnd) = %d, %v; want 0, io.EOF", n, err)
	}
	if n := dec.total(); n != 0 {
		t.Fatalf("Read at the end with no pipeline started made %d xorb reads, want none", n)
	}
}

// shortLastTerm stores two xorbs of mixed chunk sizes as a file whose last
// term is declared short by exactly its last chunk, so the file ends at a
// chunk boundary, and returns it with the length of its final declared chunk.
func shortLastTerm(t *testing.T, st storage.Storage) (storedFile, int64) {
	t.Helper()
	xorbs := xorbsOf(lz4Class, 2, mixedChunkSizes)
	f := putXorbs(t, st, xorbs)
	last := xorbs[len(xorbs)-1]
	entries := slices.Clone(f.entries())
	entries[len(entries)-1].UnpackedSegBytes -= uint32(len(last[len(last)-1]))
	return f.withEntries(entries), int64(len(last[len(last)-2]))
}

// io.CopyN of the declared size, as http.ServeContent sends a body, never
// Reads past the end; on a last term declared short by exactly its last chunk
// it still fails with the term's size error, the final declared chunk withheld.
func TestReconstructedFileCopyNOfShortLastTermFails(t *testing.T) {
	st := newLocal(t)
	f, final := shortLastTerm(t, st)
	entries := f.entries()
	size := f.termStart(len(entries))
	rc := openFile(t, t.Context(), st, f)
	var n int64
	var err error
	within(t, 5*time.Second, "io.CopyN of the declared size", func() { n, err = io.CopyN(io.Discard, rc, size) })
	if want := fmt.Sprintf("chunks exceed the term's %d bytes", entries[len(entries)-1].UnpackedSegBytes); err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("io.CopyN(%d) = %d, %v; want the term's size error, containing %q", size, n, err, want)
	}
	if want := size - final; n != want {
		t.Errorf("io.CopyN copied %d of %d bytes, want the %d before the final declared chunk", n, size, want)
	}
}

// Over HTTP the same file fails closed: http.ServeContent promises the
// declared size in Content-Length, then sends a body short of it.
func TestReconstructedFileServeContentOfShortLastTermFallsShort(t *testing.T) {
	st := newLocal(t)
	f, _ := shortLastTerm(t, st)
	size := f.termStart(len(f.entries()))
	rc := openFile(t, t.Context(), st, f)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	within(t, 5*time.Second, "http.ServeContent", func() { http.ServeContent(rec, req, "", time.Time{}, rc) })
	if got := rec.Header().Get("Content-Length"); got != fmt.Sprint(size) {
		t.Fatalf("Content-Length = %q, want the declared %d", got, size)
	}
	if n := int64(rec.Body.Len()); n >= size {
		t.Errorf("GET answered %d with %d of the %d bytes its Content-Length promised; want the body cut short", rec.Code, n, size)
	}
}

// More empty chunks than the window holds past a last term's final declared
// chunk never stall the reader awaiting the term's accounting: it releases
// each, then serves the file or, when one more chunk overshoots the term,
// fails without that final chunk.
func TestReconstructedFileDrainsEmptyChunksPastWindow(t *testing.T) {
	const size = 4 << 10
	chunks := xorbsOf(lz4Class, 1, []int{size, size, size})[0]
	empty := make([][]byte, 3*8)
	for _, c := range []struct {
		name    string
		chunks  [][]byte
		want    int64  // bytes read
		errText string // in the read's error; none for a clean EOF
	}{
		// Stored as declared: PutShard accepts the empty chunks.
		{"declared over every chunk", slices.Concat(chunks[:2], empty), 2 * size, ""},
		{"then one more chunk", slices.Concat(chunks[:2], empty, chunks[2:]), size, fmt.Sprintf("chunks exceed the term's %d bytes", 2*size)},
	} {
		t.Run(c.name, func(t *testing.T) {
			st := newLocal(t)
			f := putXorbs(t, st, [][][]byte{c.chunks})
			entries := slices.Clone(f.entries())
			entries[0].UnpackedSegBytes = 2 * size
			rc := openFile(t, t.Context(), st, f.withEntries(entries))
			var got []byte
			var err error
			within(t, 5*time.Second, "Read past the empty chunks", func() { got, err = io.ReadAll(rc) })
			if c.errText == "" && err != nil {
				t.Fatalf("read = %d bytes, %v; want the %d declared", len(got), err, c.want)
			}
			if c.errText != "" {
				checkFailedRead(t, rc, f.Content, got, err)
				if !strings.Contains(err.Error(), c.errText) {
					t.Errorf("error %q, want the term's size error, containing %q", err, c.errText)
				}
			}
			if int64(len(got)) != c.want || !bytes.Equal(got, f.Content[:len(got)]) {
				t.Errorf("read %d bytes, want the file's first %d", len(got), c.want)
			}
			closeWithin(t, rc, time.Second)
		})
	}
}

// An error survives a Seek to the current position and clears on a Seek
// elsewhere, from where the file reads again.
func TestReconstructedFileSeekAwayClearsError(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 1, slices.Repeat([]int{16 << 10}, 8)))
	h := f.XorbHashes[0]
	dec := newCountingStore(st)
	inject(dec, h, true, faultyRange{stop: framesOf(t, st, h)[2].start + 5, err: errInjected})
	rc := openFile(t, t.Context(), dec, f)
	got, err := io.ReadAll(rc)
	if dec.rangesOf(h) == 0 {
		t.Fatal("no GetXorbRangeReadCloser, so the fault was never injected")
	}
	checkFailedRead(t, rc, f.Content, got, err)
	if pos, serr := rc.Seek(0, io.SeekCurrent); serr != nil || pos != int64(len(got)) {
		t.Fatalf("Seek(0, SeekCurrent) = %d, %v; want %d", pos, serr, len(got))
	}
	if _, again := rc.Read(make([]byte, 1)); again == nil || again.Error() != err.Error() {
		t.Fatalf("Read after a Seek to the same position = %v; want the sticky %v", again, err)
	}
	if _, err := rc.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if all, err := io.ReadAll(rc); err != nil || !bytes.Equal(all, f.Content) {
		t.Fatalf("read after a Seek away = %d bytes, %v; want all %d", len(all), err, len(f.Content))
	}
}

// After Close, Read and Seek fail with fs.ErrClosed and Close again is nil.
func TestReconstructedFileUseAfterClose(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 2, mixedChunkSizes))
	for _, use := range []struct {
		name string
		do   func(rc io.ReadSeekCloser) error
		want error
	}{
		{"Read", func(rc io.ReadSeekCloser) error { _, err := rc.Read(make([]byte, 1)); return err }, fs.ErrClosed},
		{"Seek", func(rc io.ReadSeekCloser) error { _, err := rc.Seek(0, io.SeekStart); return err }, fs.ErrClosed},
		{"Close", func(rc io.ReadSeekCloser) error { return rc.Close() }, nil},
	} {
		t.Run(use.name, func(t *testing.T) {
			rc := openFile(t, t.Context(), st, f)
			if _, err := io.ReadFull(rc, make([]byte, 100)); err != nil {
				t.Fatal(err)
			}
			if err := rc.Close(); err != nil {
				t.Fatalf("Close = %v", err)
			}
			if err := use.do(rc); !errors.Is(err, use.want) {
				t.Fatalf("%s after Close = %v, want %v", use.name, err, use.want)
			}
		})
	}
}

// Cancelling the reader's context while it waits on storage fails the
// waiting Read and every later one with the context's error; Close is then
// prompt and leaves no goroutine behind.
func TestReconstructedFileParentCancelStopsPipeline(t *testing.T) {
	const size = 4 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 1, slices.Repeat([]int{size}, 16)))
	baseline := runtime.NumGoroutine()
	dec, blocked := blockAfterFirstChunk(t, st, f)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rc := openFile(t, ctx, dec, f)
	readFirstChunk(t, rc, f, size, blocked)

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := rc.Read(make([]byte, 1))
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Read of the blocked chunk returned %d, %v before the cancel", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case r := <-done:
		if r.n != 0 || !errors.Is(r.err, context.Canceled) {
			t.Fatalf("waiting Read after the cancel = %d, %v; want 0, context.Canceled", r.n, r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting Read still blocked 1s after its context was cancelled")
	}
	if n, err := rc.Read(make([]byte, 1)); n != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("Read after the cancel = %d, %v; want 0, context.Canceled", n, err)
	}
	closeWithin(t, rc, time.Second)
	settleGoroutines(t, baseline)
}

// Close cancels the context it gave storage: it returns promptly with a range
// read in flight, closes that range once, and leaves nothing running.
func TestReconstructedFileCloseCancelsInFlightRangeRead(t *testing.T) {
	const size = 4 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 1, slices.Repeat([]int{size}, 16)))
	baseline := runtime.NumGoroutine()
	dec, blocked := blockAfterFirstChunk(t, st, f)
	rc := openFile(t, t.Context(), dec, f)
	readFirstChunk(t, rc, f, size, blocked)
	closeWithin(t, rc, time.Second)
	dec.mu.Lock()
	for i, ctx := range dec.ctxs {
		if ctx.Err() == nil {
			t.Errorf("xorb read %d was given a context Close left live, want a child of the reader's that Close cancels", i)
		}
	}
	for _, r := range dec.ranges {
		if r.closes != 1 {
			t.Errorf("range %v closed %d times, want once", r, r.closes)
		}
	}
	dec.mu.Unlock()
	settleGoroutines(t, baseline)
}

// closeRacesBlockedRead Reads f over st until its first range, a stuckRange
// failing with fail, blocks; Closes; releases the range once Close has
// cancelled it; then checks the Read, Close and a fresh reader's bytes.
func closeRacesBlockedRead(t *testing.T, st storage.Storage, f storedFile, fail error) {
	t.Helper()
	blocked, cancelled := make(chan struct{}, 1), make(chan struct{}, 1)
	release := make(chan struct{})
	unstick := sync.OnceFunc(func() { close(release) })
	defer unstick()
	dec := newCountingStore(st)
	dec.fault = func(ctx context.Context, r rangeRead, rd io.Reader) io.Reader {
		if r.nth > 1 {
			return rd
		}
		return &stuckRange{r: rd, ctx: ctx, err: fail, blocked: blocked, cancelled: cancelled, release: release}
	}
	rc, err := storage.NewReconstructedFile(t.Context(), dec, f.sh, f.digest)
	if err != nil {
		t.Fatalf("NewReconstructedFile: %v", err)
	}
	type result struct {
		got []byte
		err error
	}
	read := make(chan result, 1)
	go func() {
		got, err := readAllBy(rc, 32<<10)
		read <- result{got, err}
	}()
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("no Read ever waited on the stuck GetXorbRangeReadCloser")
	}

	closed := make(chan error, 1)
	deadline := time.After(time.Second)
	go func() { closed <- rc.Close() }()
	select {
	case <-cancelled:
	case <-deadline:
		t.Fatal("Close left the context it gave storage live for 1s; want it cancelled before Close waits on the Read")
	}
	unstick()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close racing a blocked Read = %v, want nil", err)
		}
	case <-deadline:
		t.Fatal("Close racing a blocked Read still blocked after 1s")
	}
	var r result
	select {
	case r = <-read:
	case <-time.After(5 * time.Second):
		t.Fatal("Read racing Close still blocked 5s after Close returned")
	}
	if !errors.Is(r.err, fs.ErrClosed) {
		t.Fatalf("Read racing Close ended in %v after %d bytes; want fs.ErrClosed", r.err, len(r.got))
	}
	if len(r.got) > len(f.Content) || !bytes.Equal(r.got, f.Content[:len(r.got)]) {
		t.Fatalf("Read racing Close returned %d bytes before %v that differ from the file's", len(r.got), r.err)
	}

	fresh, err := storage.NewReconstructedFile(t.Context(), st, f.sh, f.digest)
	if err != nil {
		t.Fatalf("NewReconstructedFile: %v", err)
	}
	defer fresh.Close()
	if got, err := io.ReadAll(fresh); err != nil || !bytes.Equal(got, f.Content) {
		t.Fatalf("a fresh reader then read %d bytes, %v; want the %d stored, from a pool no buffer reentered twice or nil", len(got), err, len(f.Content))
	}
}

// Close racing a Read that waits on a range read deaf to cancellation, which
// then fails or serves its chunk, returns nil within 1s, ends the Read in
// fs.ErrClosed after only the file's bytes, and reenters no pool buffer twice;
// under -race it also pins Read and Close as serialized.
func TestReconstructedFileCloseRacesBlockedRead(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 1, slices.Repeat([]int{4 << 10}, 16)))
	for _, c := range []struct {
		name string
		fail error
	}{
		{"range fails once released", errInjected},
		{"range serves its chunk once released", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			for range 200 {
				closeRacesBlockedRead(t, st, f, c.fail)
			}
		})
	}
}

// Close while a Read waits in the final chunk's drain, on a last term declared
// short by its last chunk whose range read blocks there, returns nil within
// 1s and ends that Read in fs.ErrClosed without the withheld chunk; under
// -race it also pins Read and Close as serialized.
func TestReconstructedFileCloseInterruptsFinalChunkDrain(t *testing.T) {
	st := newLocal(t)
	f, final := shortLastTerm(t, st)
	h := f.XorbHashes[len(f.XorbHashes)-1]
	frames := framesOf(t, st, h)
	blocked := make(chan struct{}, 1)
	dec := newCountingStore(st)
	inject(dec, h, true, faultyRange{stop: frames[len(frames)-1].start, release: t.Context().Done(), blocked: blocked})
	rc := openFile(t, t.Context(), dec, f)
	size := f.termStart(len(f.entries()))
	if _, err := io.ReadFull(rc, make([]byte, size-final)); err != nil {
		t.Fatalf("reading up to the final declared chunk: %v", err)
	}
	select {
	case <-blocked:
	case <-time.After(5 * time.Second):
		t.Fatal("no range read ever waited on the chunk past the last term's declared size")
	}

	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n, err := rc.Read(make([]byte, 32<<10))
		done <- result{n, err}
	}()
	select {
	case r := <-done:
		t.Fatalf("Read of the final declared chunk returned %d, %v before Close; want it waiting on the last term's accounting", r.n, r.err)
	case <-time.After(50 * time.Millisecond):
	}
	var err error
	within(t, time.Second, "Close during the final chunk's drain", func() { err = rc.Close() })
	if err != nil {
		t.Fatalf("Close during the final chunk's drain = %v, want nil", err)
	}
	select {
	case r := <-done:
		if r.n != 0 || !errors.Is(r.err, fs.ErrClosed) {
			t.Fatalf("Read waiting in the final chunk's drain = %d, %v after Close; want 0, fs.ErrClosed", r.n, r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Read waiting in the final chunk's drain still blocked 1s after Close returned")
	}
}

// A Seek out of the decoded chunk stops the pipeline before it returns: every
// range closed once, no goroutine left; the next Read resumes from a range of
// the target's term.
func TestReconstructedFileSeekAwayReleasesPipeline(t *testing.T) {
	const size = 4 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 2, slices.Repeat([]int{size}, 16)))
	dec := newCountingStore(st)
	baseline := runtime.NumGoroutine()
	rc := openFile(t, t.Context(), dec, f)
	if _, err := io.ReadFull(rc, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if dec.rangesOf(f.XorbHashes[0]) == 0 {
		t.Fatal("the first Read opened no GetXorbRangeReadCloser of the first term")
	}
	pos := f.termStart(1) + size + 7 // in chunk 1 of the second term
	var at int64
	var err error
	within(t, time.Second, "Seek out of the decoded chunk", func() { at, err = rc.Seek(pos, io.SeekStart) })
	if err != nil || at != pos {
		t.Fatalf("Seek(%d) = %d, %v", pos, at, err)
	}
	opened := dec.rangeReads()
	for _, r := range opened {
		if r.closes != 1 {
			t.Errorf("range %v closed %d times when Seek returned, want once", r, r.closes)
		}
	}
	settleGoroutines(t, baseline)
	got := make([]byte, 100)
	if _, err := io.ReadFull(rc, got); err != nil || !bytes.Equal(got, f.Content[pos:pos+100]) {
		t.Fatalf("read after Seek(%d) = %v; want the file's bytes there", pos, err)
	}
	frames := framesOf(t, st, f.XorbHashes[1])
	_, termEnd := termRange(frames, f.entries()[1])
	next := dec.rangeReads()[len(opened):]
	if len(next) == 0 || next[0].hash != f.XorbHashes[1] || next[0].start > frames[1].start || next[0].end != termEnd {
		t.Fatalf("ranges after the Seek = %v; want first the second term's from at most chunk 1 to its end %d", next, termEnd)
	}
}

// A reader stalled after its first byte reads ahead until its window is full.
func TestReconstructedFileReadAheadFillsWindow(t *testing.T) {
	dec, rc, frames := stalledReader(t)
	want := frames[8-1].end
	if !eventually(func() bool { return dec.rangeBytes() >= want }) {
		t.Fatalf("stalled reader fetched %d bytes, want read-ahead through the window's %d frames, %d bytes", dec.rangeBytes(), 8, want)
	}
	closeWithin(t, rc, time.Second)
}

// However long it stalls, a reader holds a window of frames, not the file:
// at most 8+2 frames fetched and a fetcher beside min(GOMAXPROCS, 4) workers.
func TestReconstructedFileStalledReaderStaysBounded(t *testing.T) {
	baseline := runtime.NumGoroutine()
	dec, rc, frames := stalledReader(t)
	if full := frames[8-1].end; !eventually(func() bool { return dec.rangeBytes() >= full }) {
		t.Fatalf("stalled reader fetched %d bytes, never filling its window of %d frames, %d bytes", dec.rangeBytes(), 8, full)
	}
	if got, limit := dec.rangeBytes(), frames[8+1].end; got > limit {
		t.Errorf("stalled reader fetched %d of %d bytes, want at most 8+2 frames, %d bytes", got, frames[len(frames)-1].end, limit)
	}
	workers := min(runtime.GOMAXPROCS(0), 4)
	if got, limit := runtime.NumGoroutine()-baseline, 1+workers; got > limit {
		t.Errorf("%d goroutines for one reader, want at most a fetcher and %d workers", got, workers)
	}
	closeWithin(t, rc, time.Second)
}

// Readers sharing a store are independent: concurrent ones each read the
// file from their own offset.
func TestReconstructedFileConcurrentReadersAreIndependent(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 3, mixedChunkSizes))
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			rc, err := st.GetReconstructedFile(t.Context(), f.digest)
			if err != nil {
				errs <- err
				return
			}
			defer rc.Close()
			pos := int64(i * len(f.Content) / 4)
			if _, err := rc.Seek(pos, io.SeekStart); err != nil {
				errs <- err
				return
			}
			got, err := readAllBy(rc, 32<<10)
			if err == nil && !bytes.Equal(got, f.Content[pos:]) {
				err = fmt.Errorf("reader %d read %d wrong bytes after offset %d", i, len(got), pos)
			}
			errs <- err
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}

// Two goroutines Reading one reader at once, each through its own 32 KiB
// buffer, take turns: between them they read each 32 KiB block of the file
// once, and a Seek back then reads it whole; under -race it pins Reads as
// serialized.
func TestReconstructedFileConcurrentReadsAreSerialized(t *testing.T) {
	const bufSize = 32 << 10
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 1, mixedChunkSizes))
	blocks := map[string]int{} // file offset of each bufSize-aligned block
	for off := 0; off < len(f.Content); off += bufSize {
		blocks[string(f.Content[off:min(off+bufSize, len(f.Content))])] = off
	}
	rc := openFile(t, t.Context(), st, f)
	for range 100 {
		if _, err := rc.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		var offsets [2][]int
		var errs [2]error
		var wg sync.WaitGroup
		for g := range 2 {
			wg.Go(func() {
				buf := make([]byte, bufSize)
				for {
					n, err := rc.Read(buf)
					if n > 0 {
						off, ok := blocks[string(buf[:n])]
						if !ok {
							errs[g] = fmt.Errorf("a concurrent Read returned %d bytes found at no 32 KiB boundary of the file", n)
							return
						}
						offsets[g] = append(offsets[g], off)
					}
					if err != nil {
						if err != io.EOF {
							errs[g] = err
						}
						return
					}
				}
			})
		}
		within(t, 5*time.Second, "two concurrent Read loops to EOF", wg.Wait)
		seen := map[int]int{}
		for g := range 2 {
			if errs[g] != nil {
				t.Fatal(errs[g])
			}
			for _, off := range offsets[g] {
				seen[off]++
			}
		}
		for _, off := range blocks {
			if seen[off] != 1 {
				t.Fatalf("concurrent Reads served the 32 KiB at offset %d %d times, want once", off, seen[off])
			}
		}
		if _, err := rc.Seek(0, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, f.Content) {
			t.Fatalf("read after concurrent Reads and Seek(0, SeekStart) = %d bytes, %v; want all %d", len(got), err, len(f.Content))
		}
	}
}

// One goroutine Seeking a reader while another Reads it through a 32 KiB
// buffer: each Read returns the file's bytes from where the last one ended or
// from a Seek's target, and a Seek back then reads the file whole; under -race
// it pins Seek and Read as serialized.
func TestReconstructedFileSeekRacesRead(t *testing.T) {
	const bufSize, rounds = 32 << 10, 4
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(noneClass, 3, mixedChunkSizes))
	size := int64(len(f.Content))
	// Twice into the 128 KiB chunk of terms 0 and 1 each, so a Seek may land
	// inside the chunk the last Read decoded; into term 2; the start, the last
	// byte and the end.
	targets := []int64{5001, 70001, f.termStart(1) + 1000, f.termStart(1) + 70000, f.termStart(2) + 9, 0, size - 1, size}
	for range 100 {
		rc := openFile(t, t.Context(), st, f)
		var readErr, seekErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			buf := make([]byte, bufSize)
			var end int64 // where the last Read ended
			for range rounds * len(targets) {
				n, err := rc.Read(buf)
				if err != nil && err != io.EOF {
					readErr = fmt.Errorf("a Read racing Seeks: %w", err)
					return
				}
				starts := append([]int64{end}, targets...)
				i := slices.IndexFunc(starts, func(pos int64) bool {
					return int64(n) == min(bufSize, size-pos) && bytes.Equal(buf[:n], f.Content[pos:pos+int64(n)])
				})
				if i < 0 {
					readErr = fmt.Errorf("a Read racing Seeks returned %d bytes, %v; want the file's from %d, where the last Read ended, or from a Seek target", n, err, end)
					return
				}
				end = starts[i] + int64(n)
			}
		})
		wg.Go(func() {
			for range rounds {
				for _, pos := range targets {
					if got, err := rc.Seek(pos, io.SeekStart); err != nil || got != pos {
						seekErr = fmt.Errorf("a Seek(%d) racing Reads = %d, %v; want %d, nil", pos, got, err, pos)
						return
					}
				}
			}
		})
		within(t, 5*time.Second, "a Read loop racing a Seek loop", wg.Wait)
		if err := errors.Join(readErr, seekErr); err != nil {
			t.Fatal(err)
		}
		var got []byte
		var err error
		within(t, 5*time.Second, "Seek(0, SeekStart) and io.ReadAll after the race", func() {
			if _, err = rc.Seek(0, io.SeekStart); err == nil {
				got, err = io.ReadAll(rc)
			}
		})
		if err != nil || !bytes.Equal(got, f.Content) {
			t.Fatalf("read after the race and Seek(0, SeekStart) = %d bytes, %v; want all %d", len(got), err, size)
		}
		closeWithin(t, rc, time.Second)
	}
}

// An empty Read, and a Read at or past the end, return at once without
// storage; one Read fills its buffer across every chunk and term.
func TestReconstructedFileReadEdges(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 3, mixedChunkSizes))
	size := int64(len(f.Content))
	t.Run("empty buffer", func(t *testing.T) {
		dec := newCountingStore(st)
		rc := openFile(t, t.Context(), dec, f)
		if n, err := rc.Read(nil); n != 0 || err != nil {
			t.Fatalf("Read(nil) = %d, %v; want 0, nil", n, err)
		}
		if n := dec.total(); n != 0 {
			t.Fatalf("Read(nil) made %d xorb reads, want none", n)
		}
	})
	for _, pos := range []int64{size, size + 10} {
		t.Run(fmt.Sprintf("at %d of %d", pos, size), func(t *testing.T) {
			dec := newCountingStore(st)
			rc := openFile(t, t.Context(), dec, f)
			if _, err := rc.Seek(pos, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			if n, err := rc.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("Read = %d, %v; want 0, io.EOF", n, err)
			}
			if n := dec.total(); n != 0 {
				t.Fatalf("Read at the end made %d xorb reads, want none", n)
			}
		})
	}
	t.Run("fills buffer across terms", func(t *testing.T) {
		rc := openFile(t, t.Context(), st, f)
		got := make([]byte, size)
		if n, err := rc.Read(got); n != len(got) || (err != nil && err != io.EOF) || !bytes.Equal(got, f.Content) {
			t.Fatalf("one Read of the whole file = %d, %v; want all %d bytes", n, err, size)
		}
	})
}

// Seek rejects an unknown whence and a negative position without storage reads.
func TestReconstructedFileSeekRejectsInvalid(t *testing.T) {
	st := newLocal(t)
	f := putXorbs(t, st, xorbsOf(lz4Class, 1, mixedChunkSizes))
	dec := newCountingStore(st)
	rc := openFile(t, t.Context(), dec, f)
	for _, s := range []struct {
		offset int64
		whence int
	}{{0, 3}, {-1, io.SeekStart}, {-1, io.SeekCurrent}, {-int64(len(f.Content)) - 1, io.SeekEnd}} {
		if _, err := rc.Seek(s.offset, s.whence); err == nil {
			t.Errorf("Seek(%d, %d) succeeded, want an error", s.offset, s.whence)
		}
	}
	if n := dec.total(); n != 0 {
		t.Fatalf("rejected seeks made %d xorb reads, want none", n)
	}
}
