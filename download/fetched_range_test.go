package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/xorb"
)

// revisitReaders builds V1 and V2 constructors for one six-chunk xorb fetched
// as ranges A=[0,2), B=[2,4) and C=[4,6), read by terms that come back to A
// and B after C: A is last read by term 5, B by term 4 and C by term 3. want
// is the reconstructed file and keys holds the fetch keys of A, B and C.
func revisitReaders(t *testing.T, seed int64) (encoded, want []byte, fileHash xet.FileHash, keys []fetchKey, readers map[string]func(context.Context, ClientAdapter, int64, ...Option) (io.ReadCloser, error)) {
	t.Helper()
	const chunkSize = 1000
	chunks := randomChunks(seed, 6, chunkSize)
	encoded = buildTestXorb(t, chunks)
	var ranges []XorbRangeDescriptor
	var fetchInfo []FetchInfoEntry
	for start := 0; start < len(chunks); start += 2 {
		rg := XorbRangeDescriptor{
			Chunks: ChunkRange{Start: uint32(start), End: uint32(start + 2)},
			Bytes:  ByteRange{Start: int64(len(buildTestXorb(t, chunks[:start]))), End: int64(len(buildTestXorb(t, chunks[:start+2]))) - 1},
		}
		ranges = append(ranges, rg)
		fetchInfo = append(fetchInfo, FetchInfoEntry{Range: rg.Chunks, URL: "test://xorb", URLRange: rg.Bytes})
		keys = append(keys, fetchKey{Hash: testCacheHash, Start: rg.Bytes.Start, End: rg.Bytes.End})
	}

	var terms []Term
	var hashes []xet.ChunkHash
	var sizes []uint64
	for _, rg := range []ChunkRange{{0, 2}, {2, 3}, {1, 2}, {4, 6}, {3, 4}, {0, 1}} {
		terms = append(terms, Term{Hash: testCacheHash, UnpackedLength: uint64(rg.End-rg.Start) * chunkSize, Range: rg})
		for i := rg.Start; i < rg.End; i++ {
			want = append(want, chunks[i]...)
			hashes = append(hashes, xet.ComputeChunkHash(chunks[i]))
			sizes = append(sizes, chunkSize)
		}
	}
	fileHash = xet.ComputeFileHash(hashes, sizes)

	readers = map[string]func(context.Context, ClientAdapter, int64, ...Option) (io.ReadCloser, error){
		"v1": func(ctx context.Context, client ClientAdapter, offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV1(ctx, client, &ReconstructionResponseV1{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				FetchInfo:            map[string][]FetchInfoEntry{testCacheHash: fetchInfo},
			}, opts...)
		},
		"v2": func(ctx context.Context, client ClientAdapter, offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV2(ctx, client, &ReconstructionResponseV2{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				Xorbs:                map[string][]XorbMultiRangeFetch{testCacheHash: {{URL: "test://xorb", Ranges: ranges}}},
			}, opts...)
		},
	}
	return encoded, want, fileHash, keys, readers
}

// TestReadersWithoutCache reads through readers given no cache manager: they
// reconstruct and verify the file at any concurrency, honor
// OffsetIntoFirstRange, and leave the default cache directory alone.
func TestReadersWithoutCache(t *testing.T) {
	encoded, want, fileHash, _, readers := revisitReaders(t, 1)
	wrongEncoded, _, _, _, _ := revisitReaders(t, 2)
	if len(wrongEncoded) != len(encoded) {
		t.Fatalf("substituted xorb is %d bytes, want %d", len(wrongEncoded), len(encoded))
	}
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)

	for name, newReader := range readers {
		for _, concurrency := range []int{0, 1, 2, 8} {
			for _, oneByte := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/concurrency=%d/oneByte=%v", name, concurrency, oneByte), func(t *testing.T) {
					r, err := newReader(t.Context(), &fakeClientAdapter{data: encoded}, 0, WithConcurrency(concurrency), WithExpectedFileHash(fileHash))
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					var src io.Reader = r
					if oneByte {
						src = iotest.OneByteReader(r)
					}
					if got, err := io.ReadAll(src); err != nil || !bytes.Equal(got, want) {
						t.Fatalf("got %d bytes, %v; want %d bytes", len(got), err, len(want))
					}
				})
			}
		}

		t.Run(name+"/offset", func(t *testing.T) {
			for _, offset := range []int64{1, 1500} {
				r, err := newReader(t.Context(), &fakeClientAdapter{data: encoded}, offset)
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(r)
				r.Close()
				if err != nil || !bytes.Equal(got, want[offset:]) {
					t.Fatalf("offset %d: got %d bytes, %v; want %d bytes", offset, len(got), err, len(want)-int(offset))
				}
			}
		})

		t.Run(name+"/substituted content", func(t *testing.T) {
			r, err := newReader(t.Context(), &fakeClientAdapter{data: wrongEncoded}, 0, WithExpectedFileHash(fileHash))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, err := io.ReadAll(r); err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
				t.Fatalf("err = %v, want file hash mismatch", err)
			}
		})
	}

	if _, err := os.Stat(filepath.Join(tmp, "xet-cache")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("readers without a cache manager touched the default cache directory: %v", err)
	}
}

// TestReaderReleasesRangeAfterItsLastTerm reads the revisiting terms one at a
// time, with and without a cache manager: a chunk is held while a later term
// still reads it, a range is released once the reader moves past its last
// term, and the cache keeps every entry sealed for later readers.
func TestReaderReleasesRangeAfterItsLastTerm(t *testing.T) {
	encoded, want, _, keys, readers := revisitReaders(t, 1)
	chunks := randomChunks(1, 6, 1000)
	for name, newReader := range readers {
		for _, cached := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cached=%v", name, cached), func(t *testing.T) {
				dir := t.TempDir()
				var m *CacheManager
				if cached {
					m = NewCacheManager(dir, 0)
				}
				r, err := newReader(t.Context(), &fakeClientAdapter{data: encoded}, 0, WithCacheManager(m))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				p := readerPrefetcher(t, r)
				released := func(key fetchKey) bool {
					fr := p.entries[key].cache.(*fetchedRange)
					fr.store.mu.Lock()
					defer fr.store.mu.Unlock()
					return fr.closed
				}
				held := func(key fetchKey) []bool {
					fr := p.entries[key].cache.(*fetchedRange)
					fr.store.mu.Lock()
					defer fr.store.mu.Unlock()
					held := make([]bool, len(fr.need))
					for i, c := range fr.chunks {
						held[i] = c.data != nil || c.file != nil
					}
					return held
				}

				// A term is finished by the Read after its last byte, so each step
				// reads the next term and checks the ranges of the terms before it.
				steps := []struct {
					end      int
					released []bool // A, B, C; nil for ranges no term has read yet
					aHeld    []bool // A's chunks 0 and 1, read again by terms 5 and 2
				}{
					{3000, []bool{false, false}, []bool{true, true}},         // terms 0 (A) and 1 (B)
					{4000, []bool{false, false}, []bool{true, false}},        // term 2 (A)
					{6000, []bool{false, false, false}, []bool{true, false}}, // term 3 (C)
					{7000, []bool{false, false, true}, []bool{true, false}},  // term 4 (B); C read last by term 3
					{8000, []bool{false, true, true}, []bool{false, false}},  // term 5 (A); B read last by term 4
				}
				read := 0
				for _, step := range steps {
					got := make([]byte, step.end-read)
					if _, err := io.ReadFull(r, got); err != nil || !bytes.Equal(got, want[read:step.end]) {
						t.Fatalf("reading bytes [%d, %d): %v", read, step.end, err)
					}
					read = step.end
					for i, wantReleased := range step.released {
						if got := released(keys[i]); got != wantReleased {
							t.Fatalf("after %d bytes: range %c released = %v, want %v", read, 'A'+i, got, wantReleased)
						}
					}
					if got := held(keys[0]); !slices.Equal(got, step.aHeld) {
						t.Fatalf("after %d bytes: A's chunks held = %v, want %v", read, got, step.aHeld)
					}
				}
				if n, err := r.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Fatalf("Read at the end = %d, %v; want EOF", n, err)
				}
				for i, key := range keys {
					if !released(key) {
						t.Fatalf("range %c still held at EOF", 'A'+i)
					}
				}
				if !cached {
					return
				}
				// A fresh manager verifies each entry's checksum on its first open.
				fresh := NewCacheManager(dir, 0)
				for i := range keys {
					start := uint32(2 * i)
					c, err := openCachedRange(fresh, testCacheHash, start, start+2)
					if err != nil || c == nil {
						t.Fatalf("range %c is not cached: %v", 'A'+i, err)
					}
					buf := make([]byte, 1000)
					for j := range uint32(2) {
						if n, err := c.Chunk(j, buf); err != nil || !bytes.Equal(buf[:n], chunks[start+j]) {
							t.Fatalf("cached chunk %d: %v", start+j, err)
						}
					}
					c.Done()
				}
			})
		}
	}
}

// TestReaderMemoryLimit reads the revisiting terms under memory limits that
// evict none, some or all chunks, with and without a cache manager, through
// Read and through WriteTo into a buffer and into a file: the output
// verifies, no chunk is left in memory at EOF, and no spill file outlives
// the reader.
func TestReaderMemoryLimit(t *testing.T) {
	encoded, want, fileHash, _, readers := revisitReaders(t, 1)
	outDir := t.TempDir()
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	sinks := map[string]func(t *testing.T, r io.Reader) []byte{
		"Read": func(t *testing.T, r io.Reader) []byte {
			got, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			return got
		},
		"WriteTo buffer": func(t *testing.T, r io.Reader) []byte {
			var buf bytes.Buffer
			if _, err := r.(io.WriterTo).WriteTo(&buf); err != nil {
				t.Fatal(err)
			}
			return buf.Bytes()
		},
		"WriteTo file": func(t *testing.T, r io.Reader) []byte {
			f, err := os.CreateTemp(outDir, "out")
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if _, err := r.(io.WriterTo).WriteTo(f); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(f.Name())
			if err != nil {
				t.Fatal(err)
			}
			return got
		},
	}
	for name, newReader := range readers {
		for sinkName, sink := range sinks {
			for _, limit := range []int64{0, 1, 2500} {
				for _, cached := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/limit=%d/cached=%v", name, sinkName, limit, cached), func(t *testing.T) {
						var m *CacheManager
						if cached {
							// Not t.TempDir, which would land in TMPDIR.
							dir, err := os.MkdirTemp(outDir, "cache")
							if err != nil {
								t.Fatal(err)
							}
							m = NewCacheManager(dir, 0)
						}
						r, err := newReader(t.Context(), &fakeClientAdapter{data: encoded}, 0, WithCacheManager(m), WithMemoryLimit(limit), WithConcurrency(2), WithExpectedFileHash(fileHash))
						if err != nil {
							t.Fatal(err)
						}
						defer r.Close()
						s := readerPrefetcher(t, r).store
						if got := sink(t, r); !bytes.Equal(got, want) {
							t.Fatalf("got %d bytes, want %d", len(got), len(want))
						}
						s.mu.Lock()
						used, held := s.used, len(s.heap)
						s.mu.Unlock()
						if used != 0 || held != 0 {
							t.Fatalf("%d bytes in %d chunks left in memory at EOF", used, held)
						}
						if entries, err := os.ReadDir(tmp); err != nil || len(entries) != 0 {
							t.Fatalf("spill files left behind: %v, %v", entries, err)
						}
					})
				}
			}
		}
	}
}

// holdClient serves like fakeClientAdapter but holds the request for one
// byte range until release is closed.
type holdClient struct {
	fakeClientAdapter
	hold    string // Range header to hold
	release chan struct{}
}

func (c *holdClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	if header.Get("Range") == c.hold {
		select {
		case <-c.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.fakeClientAdapter.DownloadXorbWithURL(ctx, url, header)
}

// TestReaderWithoutCacheRereadsFromOutputFile has WriteTo write the revisiting
// terms into a file while range C is held back: by then the first terms are
// in the file, and A's chunk 0, which term 5 reads again, is noted there
// unless the file cannot be read. Once C's chunks squeeze it out of memory,
// the chunk is read back from the file, and a changed file is rejected
// instead of copied; an unreadable file leaves it to the spill file.
func TestReaderWithoutCacheRereadsFromOutputFile(t *testing.T) {
	encoded, want, fileHash, keys, readers := revisitReaders(t, 1)
	cases := []struct {
		name    string
		flag    int
		change  bool
		noted   bool // A's chunk 0 is noted in the output file
		wantErr string
	}{
		{"read back", os.O_RDWR, false, true, ""},
		{"changed", os.O_RDWR, true, true, "no longer holds it"},
		{"write-only", os.O_WRONLY, false, false, ""},
	}
	for name, newReader := range readers {
		for _, tc := range cases {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "out")
				out, err := os.OpenFile(path, tc.flag|os.O_CREATE, 0o644)
				if err != nil {
					t.Fatal(err)
				}
				defer out.Close()
				client := &holdClient{
					fakeClientAdapter: fakeClientAdapter{data: encoded},
					hold:              fmt.Sprintf("bytes=%d-%d", keys[2].Start, keys[2].End),
					release:           make(chan struct{}),
				}
				r, err := newReader(t.Context(), client, 0, WithConcurrency(2), WithMemoryLimit(0), WithExpectedFileHash(fileHash))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				entry := readerPrefetcher(t, r).entries[keys[0]]

				done := make(chan error, 1)
				go func() {
					_, err := r.(io.WriterTo).WriteTo(out)
					done <- err
				}()
				// Terms 0 to 2 fill 4000 bytes; term 3 waits for C.
				deadline := time.Now().Add(3 * time.Second)
				for {
					if info, err := os.Stat(path); err == nil && info.Size() == 4000 {
						break
					}
					if time.Now().After(deadline) {
						close(client.release)
						t.Fatal("WriteTo did not reach range C")
					}
					time.Sleep(time.Millisecond)
				}
				<-entry.ready
				fr := entry.cache.(*fetchedRange)
				fr.store.mu.Lock()
				c := fr.chunks[0]
				fr.store.limit = 1 // C's chunks will squeeze A's chunk 0 out of memory
				fr.store.mu.Unlock()
				if c.data == nil {
					t.Error("A's chunk 0 left memory without a limit")
				}
				if noted := c.file == out && c.off == 0 && c.output; noted != tc.noted {
					t.Errorf("A's chunk 0 noted in the output file = %v (at %v:%d), want %v", noted, c.file, c.off, tc.noted)
				}
				if tc.change {
					if _, err := out.WriteAt([]byte{^want[0]}, 0); err != nil {
						t.Fatal(err)
					}
				}
				close(client.release)

				err = <-done
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("WriteTo err = %v, want %q", err, tc.wantErr)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, want) {
					t.Fatalf("output has %d bytes, %v; want %d", len(got), err, len(want))
				}
			})
		}
	}
}

// countingClient counts the ranges requested through it.
type countingClient struct {
	ClientAdapter
	calls atomic.Int32
}

func (c *countingClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	c.calls.Add(1)
	return c.ClientAdapter.DownloadXorbWithURL(ctx, url, header)
}

// TestReaderWithoutCacheFetchesWithinWindow pins the memory bound of a reader
// without a cache: nothing is fetched before the first Read, and never more
// than WithConcurrency ranges beyond the newest one the reader asked for.
func TestReaderWithoutCacheFetchesWithinWindow(t *testing.T) {
	const n, concurrency = 6, 2
	chunks := randomChunks(3, n, 1000)
	encoded := buildTestXorb(t, chunks)
	recon := &ReconstructionResponseV2{Xorbs: map[string][]XorbMultiRangeFetch{testCacheHash: {{URL: "test://xorb"}}}}
	for i := range n {
		rg := XorbRangeDescriptor{
			Chunks: ChunkRange{Start: uint32(i), End: uint32(i + 1)},
			Bytes:  ByteRange{Start: int64(len(buildTestXorb(t, chunks[:i]))), End: int64(len(buildTestXorb(t, chunks[:i+1]))) - 1},
		}
		recon.Terms = append(recon.Terms, Term{Hash: testCacheHash, UnpackedLength: 1000, Range: rg.Chunks})
		recon.Xorbs[testCacheHash][0].Ranges = append(recon.Xorbs[testCacheHash][0].Ranges, rg)
	}
	client := &countingClient{ClientAdapter: &fakeClientAdapter{data: encoded}}
	r, err := NewReaderV2(t.Context(), client, recon, WithConcurrency(concurrency))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Fetches beyond the window would happen right away; give them the chance.
	settle := func(t *testing.T, want int32) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for client.calls.Load() < want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
		if got := client.calls.Load(); got != want {
			t.Fatalf("%d ranges requested, want %d", got, want)
		}
	}
	settle(t, 0)
	buf := make([]byte, 1000)
	for i := range n {
		if _, err := io.ReadFull(r, buf); err != nil || !bytes.Equal(buf, chunks[i]) {
			t.Fatalf("term %d: %v", i, err)
		}
		settle(t, int32(min(i+1+concurrency, n)))
	}
	if _, err := r.Read(buf); err != io.EOF {
		t.Fatalf("Read at the end = %v, want EOF", err)
	}
}

// TestReaderWithoutCacheCloseAbortsStalledDownload closes a reader without a
// cache while its worker is stuck in a network read: Close must cancel the
// fetch instead of waiting for it and leave the queued range unrequested.
func TestReaderWithoutCacheCloseAbortsStalledDownload(t *testing.T) {
	chunks, encoded, _, readers := splitRangeReaders(t)
	for name, newReader := range readers {
		for _, ignoreCtx := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignoreCtx=%v", name, ignoreCtx), func(t *testing.T) {
				ctx := t.Context()
				client := &gatedClient{
					data:      encoded,
					stallAt:   len(buildTestXorb(t, chunks[:1])),
					ignoreCtx: ignoreCtx,
					release:   make(chan struct{}),
					stalled:   make(chan struct{}),
					closed:    make(chan struct{}),
				}
				r, err := newReader(ctx, client, nil)
				if err != nil {
					t.Fatal(err)
				}
				p := readerPrefetcher(t, r)
				// The worker decodes the first chunk, then stalls inside the first range.
				got := make([]byte, len(chunks[0]))
				if _, err := io.ReadFull(r, got); err != nil || !bytes.Equal(got, chunks[0]) {
					t.Fatalf("first chunk: %v", err)
				}
				<-client.stalled

				closed := make(chan struct{})
				go func() {
					defer close(closed)
					r.Close()
				}()
				if !within(t, "Close during a stalled download", closed) || !awaitWorkers(t, p) {
					close(client.release)
					awaitWorkers(t, p)
					t.FailNow()
				}
				if ctx.Err() != nil {
					t.Fatal("Close canceled the caller's context")
				}
				within(t, "response body Close", client.closed)
				if got := client.calls.Load(); got != 1 {
					t.Fatalf("queued range was requested after Close: %d requests, want 1", got)
				}
				if _, err := r.Read(got); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("Read after Close = %v, want fs.ErrClosed", err)
				}
			})
		}
	}
}

// TestReaderWithoutCacheCancelWhileWaiting cancels the reader's context while
// Read waits for a chunk its worker cannot deliver.
func TestReaderWithoutCacheCancelWhileWaiting(t *testing.T) {
	chunks, encoded, recon := readerV2Fixture(t, 3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client := &gatedClient{
		data:    encoded,
		stallAt: len(buildTestXorb(t, chunks[:1])),
		stalled: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	r, err := NewReaderV2(ctx, client, recon)
	if err != nil {
		t.Fatal(err)
	}
	p := readerPrefetcher(t, r)
	go func() {
		<-client.stalled
		cancel()
	}()
	got, err := io.ReadAll(r)
	if err == nil || !bytes.Equal(got, chunks[0]) {
		t.Fatalf("canceled download read %d bytes, %v; want the first chunk and an error", len(got), err)
	}
	r.Close()
	within(t, "response body Close", client.closed)
	awaitWorkers(t, p)
}

// endClient serves data like fakeClientAdapter and records once a body has
// been read to its end.
type endClient struct {
	data  []byte
	ended atomic.Bool
}

func (c *endClient) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	rc, err := (&fakeClientAdapter{data: c.data}).DownloadXorbWithURL(ctx, url, header)
	return &endBody{ReadCloser: rc, ended: &c.ended}, err
}

func (*endClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

type endBody struct {
	io.ReadCloser
	ended *atomic.Bool
}

func (b *endBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err == io.EOF {
		time.Sleep(50 * time.Millisecond) // a reader not waiting for the end gets ahead
		b.ended.Store(true)
	}
	return n, err
}

// TestReaderWithoutCacheFinishesBodyBeforeLastChunk pins that the worker reads
// a range's body to its end before the reader can have the range's last
// chunk: a reader with every chunk may hit EOF and cancel the fetch, which
// closes bodies, and a body must not be closed while it is being read.
func TestReaderWithoutCacheFinishesBodyBeforeLastChunk(t *testing.T) {
	chunks, encoded, recon := readerV2Fixture(t, 2)
	client := &endClient{data: encoded}
	r, err := NewReaderV2(t.Context(), client, recon)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	want := bytes.Join(chunks, nil)
	got := make([]byte, len(want))
	if _, err := io.ReadFull(r, got); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("read %v", err)
	}
	if !client.ended.Load() {
		t.Fatal("the reader had the last chunk before its body was read to the end")
	}
}

// chunkWhere reports where chunk i of r is held, memory first.
func chunkWhere(r *fetchedRange, i int) string {
	s := r.store
	s.mu.Lock()
	defer s.mu.Unlock()
	c := r.chunks[i]
	switch {
	case c.data != nil:
		return "memory"
	case c.file == nil:
		return "nowhere"
	case r.spill != nil && c.file == r.spill.f:
		return "spill"
	case c.output:
		return "output"
	}
	return "cache"
}

// TestSpillFileReusesSpace pins the spill file allocator: chunks pack end to
// end, freed space is reused first fit, adjacent free space merges, and the
// end moves back over free space before it.
func TestSpillFileReusesSpace(t *testing.T) {
	var sf spillFile
	check := func(step string, end int64, free ...extent) {
		t.Helper()
		if sf.end != end || !slices.Equal(sf.free, free) {
			t.Fatalf("after %s: end %d, free %v; want end %d, free %v", step, sf.end, sf.free, end, free)
		}
	}
	for i, n := range []int64{10, 20, 30} {
		if off, want := sf.alloc(n), []int64{0, 10, 30}[i]; off != want {
			t.Fatalf("alloc(%d) = %d, want %d", n, off, want)
		}
	}
	check("packing", 60)
	sf.release(10, 20)
	check("a hole", 60, extent{10, 20})
	if off := sf.alloc(5); off != 10 {
		t.Fatalf("alloc(5) = %d, want the hole at 10", off)
	}
	check("filling part of the hole", 60, extent{15, 15})
	if off := sf.alloc(16); off != 60 {
		t.Fatalf("alloc(16) = %d, want the end, as no hole fits", off)
	}
	sf.release(60, 16)
	check("freeing the tail", 60, extent{15, 15})
	sf.release(0, 10)
	sf.release(10, 5)
	check("merging", 60, extent{0, 30})
	sf.release(30, 30)
	check("freeing all", 0)
}

func TestFetchedRange(t *testing.T) {
	chunks := [][]byte{[]byte("first chunk"), []byte("second chunk"), []byte("third chunk!")}
	encoded := buildTestXorb(t, chunks)
	two := buildTestXorb(t, chunks[:2])
	// once has each of n chunks read once, in order.
	once := func(n int) [][]int64 {
		uses := make([][]int64, n)
		for i := range uses {
			uses[i] = []int64{int64(i)}
		}
		return uses
	}
	fill := func(r *fetchedRange, data []byte) *xorb.Decoder {
		dec := xorb.NewDecoder(bytes.NewReader(data), false)
		r.fill(dec)
		return dec
	}
	// read checks chunk i is held where wantIn says, then reads it.
	read := func(t *testing.T, r *fetchedRange, i int, wantIn string, again bool) {
		t.Helper()
		if in := chunkWhere(r, i); in != wantIn {
			t.Fatalf("chunk %d is in %s, want %s", i, in, wantIn)
		}
		buf := make([]byte, 64)
		ref, err := r.readChunk(uint32(i), buf)
		if err != nil || !bytes.Equal(buf[:ref.size], chunks[i]) || ref.again != again {
			t.Fatalf("chunk %d = %q, again %v, %v; want %q, again %v", i, buf[:ref.size], ref.again, err, chunks[i], again)
		}
		if !ref.hashed || ref.hash != xet.ComputeChunkHash(chunks[i]) {
			t.Fatalf("chunk %d kept hash %v (hashed %v), want the decoder's", i, ref.hash, ref.hashed)
		}
	}
	noSpillFiles := func(t *testing.T, dir string) {
		t.Helper()
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Fatalf("spill files left behind: %v, %v", entries, err)
		}
	}

	cases := []struct {
		name     string
		data     []byte
		expected int
		served   int    // leading chunks that must be served
		wantErr  string // error for chunk index served
	}{
		{"complete", two, 2, 2, "out of range"},
		{"short", buildTestXorb(t, chunks[:1]), 2, 1, "decoded 1 chunks, expected 2"},
		{"extra chunk", two, 1, 1, "decoded 2 chunks, expected 1"},
		{"truncated", two[:len(two)-1], 2, 1, "failed to read chunk data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newFetchedRange(newChunkStore(0), once(tc.expected), nil)
			fill(r, tc.data)
			for i := range tc.served {
				read(t, r, i, "memory", false)
			}
			if _, err := r.readChunk(uint32(tc.served), make([]byte, 64)); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("chunk %d: err = %v, want %q", tc.served, err, tc.wantErr)
			}
			r.Done()
			if _, err := r.readChunk(0, make([]byte, 64)); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("chunk 0 after Done: err = %v, want fs.ErrClosed", err)
			}
		})
	}

	t.Run("each chunk until its last read", func(t *testing.T) {
		s := newChunkStore(100)
		r := newFetchedRange(s, [][]int64{nil, {0, 1}}, nil)
		if hashes, _ := fill(r, two).Chunks(); len(hashes) != 1 {
			t.Fatalf("decoded %d chunks, want only the one a term reads", len(hashes))
		}
		if _, err := r.readChunk(0, make([]byte, 64)); err == nil || !strings.Contains(err.Error(), "no read left") {
			t.Fatalf("chunk no term reads: err = %v, want no read left", err)
		}
		read(t, r, 1, "memory", true)
		read(t, r, 1, "memory", false)
		if s.used != 0 || len(s.heap) != 0 {
			t.Fatalf("%d bytes in %d chunks held after the last read", s.used, len(s.heap))
		}
		if _, err := r.readChunk(1, make([]byte, 64)); err == nil || !strings.Contains(err.Error(), "no read left") {
			t.Fatalf("read past the last: err = %v, want no read left", err)
		}
	})

	t.Run("shows the last chunk read once the body ends", func(t *testing.T) {
		pr, pw := io.Pipe()
		r := newFetchedRange(newChunkStore(0), [][]int64{{0}, nil, nil}, nil)
		filled := make(chan struct{})
		go func() {
			defer close(filled)
			r.fill(xorb.NewDecoder(pr, false))
		}()
		got := make(chan error, 1)
		go func() {
			_, err := r.readChunk(0, make([]byte, 64))
			got <- err
		}()
		// Write returns once the worker has taken every chunk; it then waits
		// for the end of the body.
		if _, err := pw.Write(encoded); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-got:
			t.Fatalf("chunk 0 served (%v) before the body ended", err)
		case <-time.After(20 * time.Millisecond):
		}
		pw.Close()
		select {
		case err := <-got:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("chunk 0 not served after the body ended")
		}
		within(t, "fill", filled)
	})

	t.Run("evicts the chunk read again furthest ahead", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("TMPDIR", tmp)
		s := newChunkStore(int64(len(chunks[1]) + len(chunks[2])))
		r := newFetchedRange(s, [][]int64{{2}, {0}, {1}}, nil)
		fill(r, encoded)
		read(t, r, 1, "memory", false)
		read(t, r, 2, "memory", false)
		read(t, r, 0, "spill", false)
		r.Done()
		noSpillFiles(t, tmp)
	})

	t.Run("evicts a chunk a file holds first", func(t *testing.T) {
		out, err := os.Create(filepath.Join(t.TempDir(), "out"))
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		s := newChunkStore(0)
		r := newFetchedRange(s, [][]int64{{0, 1, 4}, {2}, {3}}, nil)
		fill(r, encoded)
		read(t, r, 0, "memory", true)
		if _, err := out.Write(append([]byte("prefix"), chunks[0]...)); err != nil {
			t.Fatal(err)
		}
		noteWritten(r, 0, out, int64(len(chunks[0])))
		// Chunk 0 is read again next, but the output file holds it.
		s.mu.Lock()
		s.limit = s.used - 1
		s.evict()
		s.mu.Unlock()
		read(t, r, 0, "output", true)
		read(t, r, 1, "memory", false)
		read(t, r, 2, "memory", false)

		if _, err := out.WriteAt([]byte("X"), int64(len("prefix"))); err != nil {
			t.Fatal(err)
		}
		if _, err := r.readChunk(0, make([]byte, 64)); err == nil || !strings.Contains(err.Error(), "no longer holds it") {
			t.Fatalf("read of a changed output file: err = %v, want it rejected", err)
		}
	})

	t.Run("notes a written chunk only in a file it can read", func(t *testing.T) {
		out, err := os.OpenFile(filepath.Join(t.TempDir(), "out"), os.O_WRONLY|os.O_CREATE, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		r := newFetchedRange(newChunkStore(0), [][]int64{{0, 1}}, nil)
		fill(r, buildTestXorb(t, chunks[:1]))
		read(t, r, 0, "memory", true)
		if _, err := out.Write(chunks[0]); err != nil {
			t.Fatal(err)
		}
		noteWritten(r, 0, out, int64(len(chunks[0])))
		r.store.mu.Lock()
		noted := r.chunks[0].file != nil
		r.store.mu.Unlock()
		if noted {
			t.Fatal("chunk noted in a write-only file")
		}
		read(t, r, 0, "memory", false)
	})

	t.Run("reuses the space of its spill file", func(t *testing.T) {
		tmp := t.TempDir()
		t.Setenv("TMPDIR", tmp)
		pr, pw := io.Pipe()
		r := newFetchedRange(newChunkStore(1), once(3), nil)
		filled := make(chan struct{})
		go func() {
			defer close(filled)
			r.fill(xorb.NewDecoder(pr, false))
		}()
		// Each chunk spills and is read before the next arrives.
		for i := range chunks {
			start, end := len(buildTestXorb(t, chunks[:i])), len(buildTestXorb(t, chunks[:i+1]))
			if _, err := pw.Write(encoded[start:end]); err != nil {
				t.Fatal(err)
			}
			if i == len(chunks)-1 {
				pw.Close()
			}
			buf := make([]byte, 64)
			if ref, err := r.readChunk(uint32(i), buf); err != nil || !bytes.Equal(buf[:ref.size], chunks[i]) {
				t.Fatalf("chunk %d = %q, %v; want %q", i, buf[:ref.size], err, chunks[i])
			}
		}
		within(t, "fill", filled)
		r.store.mu.Lock()
		info, err := r.spill.f.Stat()
		r.store.mu.Unlock()
		if err != nil || info.Size() > int64(len(chunks[1])) {
			t.Fatalf("spill file grew to %v bytes (%v), want the largest chunk's %d", info.Size(), err, len(chunks[1]))
		}
		r.Done()
		noSpillFiles(t, tmp)
	})

	t.Run("holds chunks the spill file cannot take", func(t *testing.T) {
		t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
		s := newChunkStore(1)
		r := newFetchedRange(s, once(2), nil)
		fill(r, two)
		read(t, r, 0, "memory", false)
		read(t, r, 1, "memory", false)
		if s.spillErr == nil || s.used != 0 {
			t.Fatalf("spill error %v, %d bytes held; want an error and nothing held", s.spillErr, s.used)
		}
	})

	t.Run("writes every chunk to the cache entry", func(t *testing.T) {
		dir := t.TempDir()
		cache, err := newChunkCache(nil, NewCacheManager(dir, 0), testCacheHash, 0, 3, 0, int64(len(encoded)-1))
		if err != nil {
			t.Fatal(err)
		}
		// Room for chunk 0 alone; chunk 1 is read by no term.
		s := newChunkStore(int64(len(chunks[0])))
		r := newFetchedRange(s, [][]int64{{0}, nil, {1}}, cache)
		if hashes, _ := fill(r, encoded).Chunks(); len(hashes) != 3 {
			t.Fatalf("decoded %d chunks, want all 3 for the cache", len(hashes))
		}
		read(t, r, 0, "memory", false)
		read(t, r, 2, "cache", false)
		r.Done()

		cached, err := openCachedRange(NewCacheManager(dir, 0), testCacheHash, 0, 3)
		if err != nil || cached == nil {
			t.Fatalf("entry not sealed: %v", err)
		}
		defer cached.Done()
		buf := make([]byte, 64)
		for i, want := range chunks {
			if n, err := cached.Chunk(uint32(i), buf); err != nil || !bytes.Equal(buf[:n], want) {
				t.Fatalf("cached chunk %d = %q, %v; want %q", i, buf[:n], err, want)
			}
		}
	})

	t.Run("Done wakes a waiting reader and stops the worker", func(t *testing.T) {
		pr, pw := io.Pipe()
		r := newFetchedRange(newChunkStore(0), once(1), nil)
		filled := make(chan struct{})
		go func() {
			defer close(filled)
			r.fill(xorb.NewDecoder(pr, false))
		}()
		waited := make(chan error, 1)
		go func() {
			_, err := r.readChunk(0, make([]byte, 64))
			waited <- err
		}()
		time.Sleep(20 * time.Millisecond) // let readChunk start waiting
		r.Done()
		select {
		case err := <-waited:
			if !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("waiting readChunk returned %v, want fs.ErrClosed", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Done did not wake the waiting readChunk")
		}

		// The worker drops the chunk it was reading and stops.
		encodedChunk := buildTestXorb(t, chunks[:1])
		go func() {
			pw.Write(encodedChunk) //nolint:errcheck
			pw.Close()
		}()
		within(t, "fill after Done", filled)
		r.store.mu.Lock()
		defer r.store.mu.Unlock()
		if r.store.used != 0 || len(r.store.heap) != 0 {
			t.Fatalf("released range stored %d bytes", r.store.used)
		}
	})
}
