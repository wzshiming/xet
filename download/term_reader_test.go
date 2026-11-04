package download

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/internal/flock"
	"github.com/wzshiming/xet/xorb"
)

// fixtureXorb is one encoded xorb whose terms the tests reconstruct.
type fixtureXorb struct {
	hash    string
	chunks  [][]byte
	encoded []byte
	ends    []uint64 // packed end of each chunk in encoded
}

func newFixtureXorb(t *testing.T, id byte, chunks [][]byte) *fixtureXorb {
	t.Helper()
	encoded := buildTestXorb(t, chunks)
	ends, err := xorb.ScanChunkOffsets(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return &fixtureXorb{hash: testXorbHash(id).String(), chunks: chunks, encoded: encoded, ends: ends}
}

// fixtureTerm reads chunks through the fetch descriptor fetch, which covers them.
type fixtureTerm struct{ chunks, fetch ChunkRange }

// span is the inclusive packed byte range of chunks c.
func (x *fixtureXorb) span(c ChunkRange) ByteRange {
	start, end, err := xorb.ChunkDataRangeFromOffsets(x.ends, c.Start, c.End)
	if err != nil {
		panic(err)
	}
	return ByteRange{Start: start, End: end}
}

// file is the reconstruction of terms.
func (x *fixtureXorb) file(terms []fixtureTerm) []byte {
	var file []byte
	for _, term := range terms {
		file = append(file, bytes.Join(x.chunks[term.chunks.Start:term.chunks.End], nil)...)
	}
	return file
}

// terms returns terms as reconstruction terms and their distinct fetch descriptors.
func (x *fixtureXorb) terms(terms []fixtureTerm) ([]Term, []XorbRangeDescriptor) {
	var out []Term
	var ranges []XorbRangeDescriptor
	for _, term := range terms {
		out = append(out, Term{Hash: x.hash, UnpackedLength: uint64(len(x.file([]fixtureTerm{term}))), Range: term.chunks})
		rg := XorbRangeDescriptor{Chunks: term.fetch, Bytes: x.span(term.fetch)}
		if !slices.Contains(ranges, rg) {
			ranges = append(ranges, rg)
		}
	}
	return out, ranges
}

func (x *fixtureXorb) v1(offset int64, terms []fixtureTerm) *ReconstructionResponseV1 {
	out, ranges := x.terms(terms)
	resp := &ReconstructionResponseV1{OffsetIntoFirstRange: offset, Terms: out, FetchInfo: map[string][]FetchInfoEntry{}}
	for _, rg := range ranges {
		resp.FetchInfo[x.hash] = append(resp.FetchInfo[x.hash], FetchInfoEntry{Range: rg.Chunks, URL: "test://xorb", URLRange: rg.Bytes})
	}
	return resp
}

func (x *fixtureXorb) v2(offset int64, terms []fixtureTerm) *ReconstructionResponseV2 {
	out, ranges := x.terms(terms)
	return &ReconstructionResponseV2{
		OffsetIntoFirstRange: offset,
		Terms:                out,
		Xorbs:                map[string][]XorbMultiRangeFetch{x.hash: {{URL: "test://xorb", Ranges: ranges}}},
	}
}

var readerVersions = []string{"v1", "v2"}

// newVersionReader opens version's reader of the reconstruction of terms over x.
func newVersionReader(version string, ctx context.Context, client ClientAdapter, x *fixtureXorb, offset int64, terms []fixtureTerm, opts ...Option) (*termReader, error) {
	switch version {
	case "v1":
		r, err := NewReaderV1(ctx, client, x.v1(offset, terms), opts...)
		if err != nil {
			return nil, err
		}
		return r.(*ReaderV1).termReader, nil
	case "v2":
		r, err := NewReaderV2(ctx, client, x.v2(offset, terms), opts...)
		if err != nil {
			return nil, err
		}
		return r.(*ReaderV2).termReader, nil
	}
	panic(version)
}

// openTermReader is newVersionReader failing the test on error and closing the reader at its end.
func openTermReader(t *testing.T, version string, ctx context.Context, client ClientAdapter, x *fixtureXorb, offset int64, terms []fixtureTerm, opts ...Option) *termReader {
	t.Helper()
	r, err := newVersionReader(version, ctx, client, x, offset, terms, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// countingClient serves data like fakeClientAdapter and counts the GETs.
type countingClient struct {
	fakeClientAdapter
	gets atomic.Int32
}

func (c *countingClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	c.gets.Add(1)
	return c.fakeClientAdapter.DownloadXorbWithURL(ctx, url, header)
}

// heldClient serves data like fakeClientAdapter once release is closed.
type heldClient struct {
	fakeClientAdapter
	release chan struct{}
}

func (c *heldClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	select {
	case <-c.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return c.fakeClientAdapter.DownloadXorbWithURL(ctx, url, header)
}

type gatedClient struct {
	data      []byte
	stallAt   int
	ignoreCtx bool // a stalled Read returns only on release or body Close, like a body not bound to ctx
	release   chan struct{}
	stalled   chan struct{}
	closed    chan struct{}
	calls     atomic.Int32
	once      sync.Once
	closeOnce sync.Once
}

func (c *gatedClient) DownloadXorbWithURL(ctx context.Context, _ string, _ http.Header) (io.ReadCloser, error) {
	c.calls.Add(1)
	return &gatedBody{client: c, ctx: ctx}, nil
}

func (*gatedClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

type gatedBody struct {
	client   *gatedClient
	ctx      context.Context
	pos      int
	released bool
}

func (b *gatedBody) Read(p []byte) (int, error) {
	c := b.client
	if !b.released && b.pos >= c.stallAt {
		c.once.Do(func() { close(c.stalled) })
		ctxDone := b.ctx.Done()
		if c.ignoreCtx {
			ctxDone = nil
		}
		select {
		case <-c.release:
			b.released = true
		case <-ctxDone:
			return 0, b.ctx.Err()
		case <-c.closed:
			return 0, errors.New("body closed")
		}
	}
	limit := c.stallAt
	if b.released {
		limit = len(c.data)
	}
	if b.pos >= limit {
		return 0, io.EOF
	}
	n := copy(p, c.data[b.pos:limit])
	b.pos += n
	return n, nil
}

func (b *gatedBody) Close() error {
	b.client.closeOnce.Do(func() { close(b.client.closed) })
	return nil
}

// within fails the test unless done closes promptly, so a stuck Close or
// leaked worker fails the run instead of hanging it.
func within(t *testing.T, what string, done <-chan struct{}) bool {
	t.Helper()
	select {
	case <-done:
		return true
	case <-time.After(3 * time.Second):
		t.Errorf("%s did not finish within 3s", what)
		return false
	}
}

// bodyClient serves body(start, end) for the inclusive Range [start, end].
type bodyClient func(start, end int64) []byte

func (c bodyClient) DownloadXorbWithURL(_ context.Context, _ string, header http.Header) (io.ReadCloser, error) {
	var start, end int64
	if _, err := fmt.Sscanf(header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(c(start, end))), nil
}

func (bodyClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

func awaitGets(t *testing.T, c *countingClient, n int32) {
	t.Helper()
	for deadline := time.Now().Add(hangGuard); c.gets.Load() != n; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d GETs after %v, want %d", c.gets.Load(), hangGuard, n)
		}
	}
}

// compressibleChunks returns n chunks that the encoder compresses.
func compressibleChunks(n int) [][]byte {
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = bytes.Repeat(fmt.Appendf(nil, "chunk %d of %d; ", i, n), 100+50*i)
	}
	return chunks
}

// mixedChunks returns chunks of several sizes that the encoder stores
// uncompressed, as LZ4 and as ByteGrouping4.
func mixedChunks() [][]byte {
	rng := rand.New(rand.NewSource(7))
	random := func(n int) []byte {
		b := make([]byte, n)
		rng.Read(b)
		return b
	}
	floats := func(n int) []byte {
		b := make([]byte, 4*n)
		for i := range n {
			binary.LittleEndian.PutUint32(b[4*i:], math.Float32bits(float32(math.Sin(float64(i)/50))))
		}
		return b
	}
	return [][]byte{random(1000), compressibleChunks(1)[0], floats(3000), random(64 << 10), bytes.Repeat([]byte("ab"), 4000), floats(700)}
}

// compressionOf returns how x stores chunk i.
func (x *fixtureXorb) compressionOf(t *testing.T, i int) xorb.CompressionType {
	t.Helper()
	h, err := xorb.ParseChunkHeader(x.encoded[x.span(ChunkRange{Start: uint32(i), End: uint32(i + 1)}).Start:])
	if err != nil {
		t.Fatal(err)
	}
	return h.Compression
}

// TestTermReaderWithoutCache reconstructs terms of several ranges of one xorb
// stored with every compression, two terms reading one range and one term a
// part of a range, without a cache: each range is fetched once.
func TestTermReaderWithoutCache(t *testing.T) {
	x := newFixtureXorb(t, 0x31, mixedChunks())
	used := map[xorb.CompressionType]bool{}
	for i := range x.chunks {
		used[x.compressionOf(t, i)] = true
	}
	if len(used) != 3 {
		t.Fatalf("fixture stores chunks with %d compressions, want all 3", len(used))
	}
	terms := []fixtureTerm{
		{ChunkRange{Start: 0, End: 2}, ChunkRange{Start: 0, End: 2}},
		{ChunkRange{Start: 2, End: 5}, ChunkRange{Start: 2, End: 5}},
		{ChunkRange{Start: 5, End: 6}, ChunkRange{Start: 5, End: 6}},
		{ChunkRange{Start: 0, End: 2}, ChunkRange{Start: 0, End: 2}},
		{ChunkRange{Start: 3, End: 4}, ChunkRange{Start: 2, End: 5}},
	}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			client := &countingClient{fakeClientAdapter: fakeClientAdapter{data: x.encoded}}
			r := openTermReader(t, version, t.Context(), client, x, 0, terms)
			got, err := io.ReadAll(r)
			if want := x.file(terms); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, %v; want %d bytes", len(got), err, len(want))
			}
			if n := client.gets.Load(); n != 3 {
				t.Fatalf("%d GETs, want one per distinct range: 3", n)
			}
		})
	}
}

// TestTermReaderWaitsForBudget pins the back-pressure: with a budget for two
// terms and nothing read, exactly two range GETs start, and the third starts
// only once the first term is read completely.
func TestTermReaderWaitsForBudget(t *testing.T) {
	x := newFixtureXorb(t, 0x32, randomChunks(32, 3, 1000))
	terms := []fixtureTerm{
		{ChunkRange{Start: 0, End: 1}, ChunkRange{Start: 0, End: 1}},
		{ChunkRange{Start: 1, End: 2}, ChunkRange{Start: 1, End: 2}},
		{ChunkRange{Start: 2, End: 3}, ChunkRange{Start: 2, End: 3}},
	}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			budget := budgetWithTarget(2000)
			client := &countingClient{fakeClientAdapter: fakeClientAdapter{data: x.encoded}}
			r := openTermReader(t, version, t.Context(), client, x, 0, terms, WithConcurrency(8), WithMemoryBudget(budget))
			awaitQueued(t, budget, 1)
			awaitGets(t, client, 2)

			head := make([]byte, 999)
			if _, err := io.ReadFull(r, head); err != nil {
				t.Fatal(err)
			}
			awaitQueued(t, budget, 1)
			if n, used := client.gets.Load(), usedOf(budget); n != 2 || used != 2000 {
				t.Fatalf("after reading all but a byte of the first term: %d GETs, %d bytes used; want 2 GETs, 2000 bytes", n, used)
			}
			last := make([]byte, 1)
			if _, err := io.ReadFull(r, last); err != nil {
				t.Fatal(err)
			}
			awaitGets(t, client, 3)

			rest, err := io.ReadAll(r)
			if got, want := slices.Concat(head, last, rest), x.file(terms); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, %v; want %d", len(got), err, len(want))
			}
		})
	}
}

// TestTermReaderCreatesNoFiles pins that terms stay in memory: a download
// larger than its budget creates no file in the temporary directory, and with
// a cache files only in the cache directory.
func TestTermReaderCreatesNoFiles(t *testing.T) {
	x := newFixtureXorb(t, 0x33, randomChunks(33, 6, 1000))
	var terms []fixtureTerm
	for i := range uint32(6) {
		terms = append(terms, fixtureTerm{ChunkRange{Start: i, End: i + 1}, ChunkRange{Start: i, End: i + 1}})
	}
	cacheDir := t.TempDir()
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	for _, version := range readerVersions {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cached=%v", version, cached), func(t *testing.T) {
				opts := []Option{WithMemoryBudget(budgetWithTarget(2000)), WithConcurrency(2)}
				if cached {
					opts = append(opts, WithCacheManager(NewCacheManager(filepath.Join(cacheDir, version), 0)))
				}
				r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: x.encoded}, x, 0, terms, opts...)
				got, err := io.ReadAll(r)
				if want := x.file(terms); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("read %d bytes, %v; want %d bytes", len(got), err, len(want))
				}
				r.Close()
				var created []string
				err = filepath.WalkDir(tmp, func(path string, _ fs.DirEntry, err error) error {
					if path != tmp {
						created = append(created, path)
					}
					return err
				})
				if err != nil || len(created) != 0 {
					t.Fatalf("download created %v in the temporary directory, %v", created, err)
				}
			})
		}
	}
}

// TestTermReaderWritesWholeTerms pins io.WriterTo: io.Copy writes each term
// with one Write, the first taking what is left of the term the copy starts in.
func TestTermReaderWritesWholeTerms(t *testing.T) {
	rng := rand.New(rand.NewSource(34))
	var chunks [][]byte
	for _, size := range []int{1000, 1500, 700, 1200} {
		chunk := make([]byte, size)
		rng.Read(chunk)
		chunks = append(chunks, chunk)
	}
	x := newFixtureXorb(t, 0x34, chunks)
	terms := []fixtureTerm{
		{ChunkRange{Start: 0, End: 2}, ChunkRange{Start: 0, End: 2}},
		{ChunkRange{Start: 2, End: 4}, ChunkRange{Start: 2, End: 4}},
		{ChunkRange{Start: 1, End: 3}, ChunkRange{Start: 1, End: 3}},
	}
	file := x.file(terms)
	for _, version := range readerVersions {
		for _, test := range []struct {
			name         string
			offset, read int // OffsetIntoFirstRange; bytes taken with Read before io.Copy
			writes       []int
		}{
			{"whole file", 0, 0, []int{2500, 1900, 2200}},
			{"after a partial Read", 0, 500, []int{2000, 1900, 2200}},
			{"after the first term", 0, 2500, []int{1900, 2200}},
			{"offset into first range", 1300, 0, []int{1200, 1900, 2200}},
			{"offset and a partial Read", 1300, 200, []int{1000, 1900, 2200}},
		} {
			t.Run(version+"/"+test.name, func(t *testing.T) {
				r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: x.encoded}, x, int64(test.offset), terms)
				head := make([]byte, test.read)
				if _, err := io.ReadFull(r, head); err != nil {
					t.Fatal(err)
				}
				w := &recordingWriter{limit: math.MaxInt}
				n, err := io.Copy(w, r)
				if got := append(head, w.data...); err != nil || n != int64(len(w.data)) || !bytes.Equal(got, file[test.offset:]) {
					t.Fatalf("io.Copy = %d, %v; got %d bytes in all, want %d", n, err, len(got), len(file)-test.offset)
				}
				if !slices.Equal(w.sizes, test.writes) {
					t.Fatalf("Write sizes %v, want one Write per term: %v", w.sizes, test.writes)
				}
			})
		}
	}
}

// TestTermReaderHashesOnlyToVerify pins that no chunk is hashed without
// WithExpectedFileHash: substituted content is delivered unverified with no
// chunk hash computed, and fails verification with the file hash mismatch.
func TestTermReaderHashesOnlyToVerify(t *testing.T) {
	chunks, wrong := randomChunks(35, 3, 1000), randomChunks(36, 3, 1000)
	x, y := newFixtureXorb(t, 0x35, chunks), newFixtureXorb(t, 0x36, wrong)
	if len(x.encoded) != len(y.encoded) {
		t.Fatalf("substituted xorb is %d bytes, want %d", len(y.encoded), len(x.encoded))
	}
	fileHash := xet.ComputeFileHash(chunkHashes(chunks), []uint64{1000, 1000, 1000})
	var hashed atomic.Int32
	hash := computeChunkHash
	computeChunkHash = func(data []byte) xet.ChunkHash {
		hashed.Add(1)
		return hash(data)
	}
	t.Cleanup(func() { computeChunkHash = hash })
	terms := []fixtureTerm{{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}}}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			hashed.Store(0)
			r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: y.encoded}, x, 0, terms)
			got, err := io.ReadAll(r)
			if want := bytes.Join(wrong, nil); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("unverified read of substituted content: %d bytes, %v; want its %d bytes", len(got), err, len(want))
			}
			if n := hashed.Load(); n != 0 {
				t.Fatalf("unverified read hashed %d chunks, want none", n)
			}

			r = openTermReader(t, version, context.Background(), &fakeClientAdapter{data: y.encoded}, x, 0, terms, WithExpectedFileHash(fileHash))
			if _, err := io.ReadAll(r); err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
				t.Fatalf("verified read of substituted content: %v, want a file hash mismatch", err)
			}
			if n := hashed.Load(); n != 3 {
				t.Fatalf("verified read hashed %d chunks, want 3", n)
			}
		})
	}
}

// corruptFrame returns x's encoded bytes with the first byte of chunk i's
// compressed frame flipped.
func (x *fixtureXorb) corruptFrame(i int) []byte {
	data := slices.Clone(x.encoded)
	data[x.span(ChunkRange{Start: uint32(i), End: uint32(i + 1)}).Start+xorb.ChunkHeaderSize] ^= 0xff
	return data
}

// TestTermReaderSkipsChunksNoTermNeeds pins how a range is decoded: without a
// cache, chunks no term reads are skipped undecoded, so a corrupt frame there
// is harmless while one inside a term fails the read; with a cache the whole
// range is decoded and cached as one entry.
func TestTermReaderSkipsChunksNoTermNeeds(t *testing.T) {
	x := newFixtureXorb(t, 0x37, compressibleChunks(4))
	for i := range x.chunks {
		if c := x.compressionOf(t, i); c == xorb.CompressionNone {
			t.Fatalf("fixture stores chunk %d uncompressed", i)
		}
	}
	terms := []fixtureTerm{{ChunkRange{Start: 1, End: 3}, ChunkRange{Start: 0, End: 4}}}
	for _, version := range readerVersions {
		for _, corrupt := range []int{0, 1, 2, 3} {
			t.Run(fmt.Sprintf("%s/corrupt=%d", version, corrupt), func(t *testing.T) {
				r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: x.corruptFrame(corrupt)}, x, 0, terms)
				got, err := io.ReadAll(r)
				if needed := corrupt == 1 || corrupt == 2; needed {
					if err == nil {
						t.Fatalf("read %d bytes with term chunk %d corrupt, want an error", len(got), corrupt)
					}
					return
				}
				if want := x.file(terms); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("read %d bytes, %v with unread chunk %d corrupt; want %d bytes", len(got), err, corrupt, len(want))
				}
			})
		}
		t.Run(version+"/cached", func(t *testing.T) {
			dir := t.TempDir()
			r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: x.encoded}, x, 0, terms, WithCacheManager(NewCacheManager(dir, 0)))
			if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, x.file(terms)) {
				t.Fatalf("read %d bytes, %v", len(got), err)
			}
			r.Close()
			c := mustLookup(t, NewCacheManager(dir, 0), testXorbHash(0x37), 0, 4)
			defer c.release()
			if got, want := readSpan(t, c, 0, 4), string(bytes.Join(x.chunks, nil)); got != want {
				t.Fatalf("cached range holds %d bytes, want the %d bytes of all four chunks", len(got), len(want))
			}
		})
	}
}

// TestTermReaderCachesRanges pins the cache writes: once the first reader's
// Close returned, however slow its cache writes, a second reader of the same
// ranges with a fresh manager on the directory issues no GET.
func TestTermReaderCachesRanges(t *testing.T) {
	x := newFixtureXorb(t, 0x38, mixedChunks())
	terms := []fixtureTerm{
		{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}},
		{ChunkRange{Start: 3, End: 6}, ChunkRange{Start: 3, End: 6}},
		{ChunkRange{Start: 1, End: 2}, ChunkRange{Start: 0, End: 3}},
	}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			dir := t.TempDir()
			client := &heldClient{fakeClientAdapter: fakeClientAdapter{data: x.encoded}, release: make(chan struct{})}
			r := openTermReader(t, version, context.Background(), client, x, 0, terms, WithCacheManager(NewCacheManager(dir, 0)))
			r.writer.beforeWrite = func() { time.Sleep(100 * time.Millisecond) }
			close(client.release)
			if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, x.file(terms)) {
				t.Fatalf("first read: %d bytes, %v", len(got), err)
			}
			r.Close()

			refusing := &refusingClient{}
			r = openTermReader(t, version, context.Background(), refusing, x, 0, terms, WithCacheManager(NewCacheManager(dir, 0)))
			if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, x.file(terms)) {
				t.Fatalf("second read: %d bytes, %v", len(got), err)
			}
			if n := refusing.calls.Load(); n != 0 {
				t.Fatalf("second reader issued %d GETs, want every range from the cache", n)
			}
		})
	}
}

// stallFixture is one xorb of three compressible chunks read as terms [0,2)
// and [2,3), each with its own range, and the packed end of its first chunk.
func stallFixture(t *testing.T) (*fixtureXorb, []fixtureTerm, int) {
	t.Helper()
	x := newFixtureXorb(t, 0x39, [][]byte{bytes.Repeat([]byte{'a'}, 1000), bytes.Repeat([]byte{'b'}, 1000), bytes.Repeat([]byte{'c'}, 1000)})
	terms := []fixtureTerm{
		{ChunkRange{Start: 0, End: 2}, ChunkRange{Start: 0, End: 2}},
		{ChunkRange{Start: 2, End: 3}, ChunkRange{Start: 2, End: 3}},
	}
	return x, terms, int(x.ends[0])
}

// TestTermReaderCloseAbortsStalledDownload closes a reader whose fetch is
// stalled inside a body read that never completes, the parent context staying
// live: Close must cancel the fetch instead of waiting for it, start no queued
// fetch, cache nothing and return every byte to the budget, which then serves
// a whole download.
func TestTermReaderCloseAbortsStalledDownload(t *testing.T) {
	x, terms, firstChunk := stallFixture(t)
	for _, version := range readerVersions {
		for _, ignoreCtx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignoreCtx=%v", version, ignoreCtx), func(t *testing.T) {
				dir := t.TempDir()
				m := NewCacheManager(dir, 0)
				budget := budgetWithTarget(1 << 20)
				ctx := t.Context()
				client := &gatedClient{
					data:      x.encoded,
					stallAt:   firstChunk,
					ignoreCtx: ignoreCtx,
					release:   make(chan struct{}),
					stalled:   make(chan struct{}),
					closed:    make(chan struct{}),
				}
				r := openTermReader(t, version, ctx, client, x, 0, terms, WithConcurrency(1), WithCacheManager(m), WithMemoryBudget(budget))
				if !within(t, "the stall", client.stalled) {
					t.FailNow()
				}

				closed := make(chan struct{})
				go func() {
					defer close(closed)
					r.Close()
				}()
				if !within(t, "Close during a stalled download", closed) {
					close(client.release) // let the stalled fetch finish so the failed run can exit
					t.FailNow()
				}
				if ctx.Err() != nil {
					t.Fatal("Close canceled the caller's context")
				}
				within(t, "response body Close", client.closed)
				if got := client.calls.Load(); got != 1 {
					t.Fatalf("queued range was requested after Close: %d requests, want 1", got)
				}
				if used := usedOf(budget); used != 0 {
					t.Fatalf("closed reader holds %d bytes of the budget, want 0", used)
				}
				if _, err := os.Stat(cacheFilePath(dir, x.hash, 0, 2, 0, x.span(terms[0].fetch).End)); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("aborted range left a cache entry: %v", err)
				}

				r = openTermReader(t, version, ctx, &fakeClientAdapter{data: x.encoded}, x, 0, terms, WithCacheManager(m), WithMemoryBudget(budget))
				if got, err := io.ReadAll(r); err != nil || !bytes.Equal(got, x.file(terms)) {
					t.Fatalf("download after Close: %d bytes, %v", len(got), err)
				}
			})
		}
	}
}

// TestTermReaderCanceledContextFailsRead cancels the reader's context during a
// fetch: Read fails with the cancellation, which sticks, and Close returns the
// budget.
func TestTermReaderCanceledContextFailsRead(t *testing.T) {
	x, terms, firstChunk := stallFixture(t)
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			budget := budgetWithTarget(1 << 20)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			client := &gatedClient{data: x.encoded, stallAt: firstChunk, release: make(chan struct{}), stalled: make(chan struct{}), closed: make(chan struct{})}
			r := openTermReader(t, version, ctx, client, x, 0, terms, WithConcurrency(1), WithMemoryBudget(budget))
			go func() {
				<-client.stalled
				cancel()
			}()
			if _, err := io.ReadAll(r); !errors.Is(err, context.Canceled) {
				t.Fatalf("read with a canceled context: %v, want context.Canceled", err)
			}
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, context.Canceled) {
				t.Fatalf("Read after the failure: %v, want it to stick", err)
			}
			r.Close()
			if used := usedOf(budget); used != 0 {
				t.Fatalf("closed reader holds %d bytes of the budget, want 0", used)
			}
		})
	}
}

// TestTermReaderReportsFirstFailure fails one range while another one is
// stalled: the read fails with that range's error, not with the cancellation
// it causes in the other one, and the error sticks for Read and WriteTo.
func TestTermReaderReportsFirstFailure(t *testing.T) {
	x, terms, _ := stallFixture(t)
	errRange := errors.New("range failed")
	second := x.span(terms[1].fetch).Start
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			stalling := &stallingClient{fail: second, err: errRange}
			r := openTermReader(t, version, context.Background(), stalling, x, 0, terms, WithConcurrency(2))
			if _, err := io.ReadAll(r); !errors.Is(err, errRange) {
				t.Fatalf("read: %v, want the failed range's error", err)
			}
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, errRange) {
				t.Fatalf("Read after the failure: %v, want it to stick", err)
			}
			if _, err := r.WriteTo(io.Discard); !errors.Is(err, errRange) {
				t.Fatalf("WriteTo after the failure: %v, want it to stick", err)
			}
		})
	}
}

// stallingClient fails the GET of the range starting at fail with err and
// holds every other GET until its context ends.
type stallingClient struct {
	fail int64
	err  error
}

func (c *stallingClient) DownloadXorbWithURL(ctx context.Context, _ string, header http.Header) (io.ReadCloser, error) {
	var start, end int64
	if _, err := fmt.Sscanf(header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil {
		return nil, err
	}
	if start == c.fail {
		return nil, c.err
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*stallingClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

// TestTermReaderVerifiesWithFetchHashes corrupts the hash a fetch kept for a
// chunk: verification must fail although every byte is intact, so the chunks
// are hashed by the fetches, not again by the reader.
func TestTermReaderVerifiesWithFetchHashes(t *testing.T) {
	chunks := randomChunks(5, 3, 1000)
	x := newFixtureXorb(t, 0x3a, chunks)
	fileHash := xet.ComputeFileHash(chunkHashes(chunks), []uint64{1000, 1000, 1000})
	terms := []fixtureTerm{{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}}}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			client := &heldClient{fakeClientAdapter: fakeClientAdapter{data: x.encoded}, release: make(chan struct{})}
			r := openTermReader(t, version, context.Background(), client, x, 0, terms, WithExpectedFileHash(fileHash))
			f := r.fetches[0]
			close(client.release)
			if !within(t, "the fetch", f.done) {
				t.FailNow()
			}
			f.hashes[1][0] ^= 0xff
			got, err := io.ReadAll(r)
			if err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
				t.Fatalf("err = %v, want the corrupted kept hash to fail verification", err)
			}
			if want := bytes.Join(chunks, nil); !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, want the %d intact bytes", len(got), len(want))
			}
		})
	}
}

// TestTermReaderDoesNotWaitForLockedEntry holds the flock of a range's cache
// entry for the whole test, as another process writing it would: the reader
// downloads the range without waiting for the lock and leaves the entry to its
// holder.
func TestTermReaderDoesNotWaitForLockedEntry(t *testing.T) {
	x := newFixtureXorb(t, 0x3b, randomChunks(37, 3, 1000))
	terms := []fixtureTerm{{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}}}
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			dir := t.TempDir()
			path := cacheFilePath(dir, x.hash, 0, 3, 0, x.span(terms[0].fetch).End)
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			holder, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			if err := flock.TryLock(holder); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				flock.Unlock(holder) //nolint:errcheck
				holder.Close()
			})

			r := openTermReader(t, version, context.Background(), &fakeClientAdapter{data: x.encoded}, x, 0, terms, WithCacheManager(NewCacheManager(dir, 0)))
			var got []byte
			read := make(chan struct{})
			go func() {
				defer close(read)
				got, err = io.ReadAll(r)
				r.Close()
			}()
			if !within(t, "a download of a range locked elsewhere", read) {
				t.FailNow() // Cleanup releases the lock so the reader can exit
			}
			if want := x.file(terms); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, %v; want %d bytes", len(got), err, len(want))
			}
			if info, err := holder.Stat(); err != nil || info.Size() != 0 {
				t.Fatalf("the reader wrote the entry locked elsewhere: %v, %v", info, err)
			}
		})
	}
}

// TestTermReaderDeliversAfterTrailingRead stalls a fetch's body after the
// last chunk of its range: the term is delivered only once the fetch has read
// the body to its end, and a Close during that read returns at once and keeps
// the decoded range cached.
func TestTermReaderDeliversAfterTrailingRead(t *testing.T) {
	x, _, _ := stallFixture(t)
	// gatedClient ignores Range, so the range is the whole xorb.
	first := []fixtureTerm{{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}}}
	end := len(x.encoded)
	for _, version := range readerVersions {
		t.Run(version, func(t *testing.T) {
			client := &gatedClient{data: x.encoded, stallAt: end, release: make(chan struct{}), stalled: make(chan struct{}), closed: make(chan struct{})}
			r := openTermReader(t, version, context.Background(), client, x, 0, first)
			var got []byte
			var err error
			read := make(chan struct{})
			go func() {
				defer close(read)
				got, err = io.ReadAll(r)
			}()
			if !within(t, "the stall", client.stalled) {
				t.FailNow()
			}
			select {
			case <-read:
				t.Fatalf("the reader finished (%d bytes, %v) while its fetch was still reading the response body", len(got), err)
			case <-time.After(50 * time.Millisecond):
			}
			close(client.release)
			if !within(t, "the reader", read) {
				t.FailNow()
			}
			if want := x.file(first); err != nil || !bytes.Equal(got, want) {
				t.Fatalf("read %d bytes, %v; want %d bytes", len(got), err, len(want))
			}
		})
		t.Run(version+"/Close", func(t *testing.T) {
			dir := t.TempDir()
			client := &gatedClient{data: x.encoded, stallAt: end, release: make(chan struct{}), stalled: make(chan struct{}), closed: make(chan struct{})}
			r := openTermReader(t, version, context.Background(), client, x, 0, first, WithCacheManager(NewCacheManager(dir, 0)))
			if !within(t, "the stall", client.stalled) {
				t.FailNow()
			}
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				r.Close()
			}()
			if !within(t, "Close during the trailing read", closed) {
				close(client.release)
				t.FailNow()
			}
			c := mustLookup(t, NewCacheManager(dir, 0), testXorbHash(0x39), 0, 3)
			defer c.release()
			if got, want := readSpan(t, c, 0, 3), string(x.file(first)); got != want {
				t.Fatalf("cached range holds %d bytes, want %d", len(got), len(want))
			}
		})
	}
}

// TestTermReaderRejectsMalformedRange serves a range's body one byte short,
// and one byte long: both reads fail.
func TestTermReaderRejectsMalformedRange(t *testing.T) {
	x := newFixtureXorb(t, 0x3c, randomChunks(38, 3, 1000))
	terms := []fixtureTerm{{ChunkRange{Start: 0, End: 3}, ChunkRange{Start: 0, End: 3}}}
	for _, version := range readerVersions {
		for name, body := range map[string]bodyClient{
			"short": func(start, end int64) []byte { return x.encoded[start:end] },
			"long":  func(start, end int64) []byte { return append(slices.Clone(x.encoded[start:end+1]), 0) },
		} {
			t.Run(version+"/"+name, func(t *testing.T) {
				r := openTermReader(t, version, context.Background(), body, x, 0, terms)
				if got, err := io.ReadAll(r); err == nil {
					t.Fatalf("read %d bytes of a %s range, want an error", len(got), name)
				}
			})
		}
	}
}
