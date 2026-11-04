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

// TestReaderWithoutCacheReleasesRangeAfterItsLastTerm reads the revisiting
// terms one at a time: a range stays in memory while a later term still
// reads it and is dropped once the reader moves past its last term.
func TestReaderWithoutCacheReleasesRangeAfterItsLastTerm(t *testing.T) {
	encoded, want, _, keys, readers := revisitReaders(t, 1)
	for name, newReader := range readers {
		t.Run(name, func(t *testing.T) {
			r, err := newReader(t.Context(), &fakeClientAdapter{data: encoded}, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			p := readerPrefetcher(t, r)
			released := func(key fetchKey) bool {
				m := p.entries[key].cache.(*memoryRange)
				m.mu.Lock()
				defer m.mu.Unlock()
				return errors.Is(m.err, fs.ErrClosed)
			}

			// A term is finished by the Read after its last byte, so each step
			// reads the next term and checks the ranges of the terms before it.
			steps := []struct {
				end      int
				released []bool // A, B, C; nil for ranges no term has read yet
			}{
				{3000, []bool{false, false}},        // terms 0 (A) and 1 (B)
				{4000, []bool{false, false}},        // term 2 (A)
				{6000, []bool{false, false, false}}, // term 3 (C)
				{7000, []bool{false, false, true}},  // term 4 (B); C read last by term 3
				{8000, []bool{false, true, true}},   // term 5 (A); B read last by term 4
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
			}
			if n, err := r.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("Read at the end = %d, %v; want EOF", n, err)
			}
			for i, key := range keys {
				if !released(key) {
					t.Fatalf("range %c still held at EOF", 'A'+i)
				}
			}
		})
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

func TestMemoryRange(t *testing.T) {
	chunks := [][]byte{[]byte("first chunk"), []byte("second chunk")}
	encoded := buildTestXorb(t, chunks)
	cases := []struct {
		name     string
		data     []byte
		expected uint32
		served   int    // leading chunks that must be served
		wantErr  string // error for chunk index served
	}{
		{"complete", encoded, 2, 2, "out of range"},
		{"short", buildTestXorb(t, chunks[:1]), 2, 1, "decoded 1 chunks, expected 2"},
		{"extra chunk", encoded, 1, 1, "decoded 2 chunks, expected 1"},
		{"truncated", encoded[:len(encoded)-1], 2, 1, "failed to read chunk data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newMemoryRange(tc.expected)
			r.fill(xorb.NewDecoder(bytes.NewReader(tc.data), false))
			buf := make([]byte, 64)
			for i := range tc.served {
				ref, err := r.readChunk(uint32(i), buf)
				if err != nil || !bytes.Equal(buf[:ref.size], chunks[i]) {
					t.Fatalf("chunk %d = %q, %v; want %q", i, buf[:ref.size], err, chunks[i])
				}
				if !ref.hashed || ref.hash != xet.ComputeChunkHash(chunks[i]) {
					t.Fatalf("chunk %d kept hash %v (hashed %v), want the decoder's", i, ref.hash, ref.hashed)
				}
			}
			if _, err := r.readChunk(uint32(tc.served), buf); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("chunk %d: err = %v, want %q", tc.served, err, tc.wantErr)
			}
			r.Done()
			if _, err := r.readChunk(0, buf); !errors.Is(err, fs.ErrClosed) {
				t.Fatalf("chunk 0 after Done: err = %v, want fs.ErrClosed", err)
			}
		})
	}

	t.Run("Done wakes a waiting reader and stops the worker", func(t *testing.T) {
		pr, pw := io.Pipe()
		r := newMemoryRange(1)
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
		r.mu.Lock()
		defer r.mu.Unlock()
		if len(r.chunks) != 0 {
			t.Fatalf("released range stored %d chunks", len(r.chunks))
		}
	})
}
