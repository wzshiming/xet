package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// fakeClientAdapter serves a pre-encoded xorb honoring the Range header.
type fakeClientAdapter struct {
	data []byte
}

func (f *fakeClientAdapter) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	start, end := int64(0), int64(len(f.data)-1)
	if rangeHeader := header.Get("Range"); rangeHeader != "" {
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			return nil, fmt.Errorf("parse range %q: %w", rangeHeader, err)
		}
	}
	if start < 0 || start > end || end >= int64(len(f.data)) {
		return nil, fmt.Errorf("invalid range %d-%d", start, end)
	}
	return io.NopCloser(bytes.NewReader(f.data[start : end+1])), nil
}

func (f *fakeClientAdapter) DownloadXorbsMultipartWithURL(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error) {
	return nil, nil, fmt.Errorf("not implemented")
}

func buildTestXorb(t *testing.T, chunks [][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, false)
	for _, c := range chunks {
		if _, err := enc.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestReaderOffsetIntoFirstRange verifies OffsetIntoFirstRange skipping,
// including offsets that span multiple chunks and partial reads that split
// the first emitted chunk across Read calls.
func TestReaderOffsetIntoFirstRange(t *testing.T) {
	const chunkSize = 1000
	chunks := make([][]byte, 3)
	var full []byte
	for i := range chunks {
		chunk := make([]byte, chunkSize)
		for j := range chunk {
			chunk[j] = byte(i*131 + j*7)
		}
		chunks[i] = chunk
		full = append(full, chunk...)
	}
	encoded := buildTestXorb(t, chunks)
	adapter := &fakeClientAdapter{data: encoded}

	terms := []Term{{
		Hash:           testCacheHash,
		UnpackedLength: uint64(len(full)),
		Range:          ChunkRange{Start: 0, End: uint32(len(chunks))},
	}}

	readers := map[string]func(ctx context.Context, offset int64, cache *CacheManager) (io.ReadCloser, error){
		"v1": func(ctx context.Context, offset int64, cache *CacheManager) (io.ReadCloser, error) {
			return NewReaderV1(ctx, adapter, &ReconstructionResponseV1{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				FetchInfo: map[string][]FetchInfoEntry{
					testCacheHash: {{
						Range:    ChunkRange{Start: 0, End: uint32(len(chunks))},
						URL:      "test://xorb",
						URLRange: ByteRange{Start: 0, End: int64(len(encoded) - 1)},
					}},
				},
			}, WithCacheManager(cache))
		},
		"v2": func(ctx context.Context, offset int64, cache *CacheManager) (io.ReadCloser, error) {
			return NewReaderV2(ctx, adapter, &ReconstructionResponseV2{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				Xorbs: map[string][]XorbMultiRangeFetch{
					testCacheHash: {{
						URL: "test://xorb",
						Ranges: []XorbRangeDescriptor{{
							Chunks: ChunkRange{Start: 0, End: uint32(len(chunks))},
							Bytes:  ByteRange{Start: 0, End: int64(len(encoded) - 1)},
						}},
					}},
				},
			}, WithCacheManager(cache))
		},
	}

	offsets := []int64{0, 1, 500, chunkSize, chunkSize + 500, 2*chunkSize + chunkSize - 1}

	for name, newReader := range readers {
		for _, offset := range offsets {
			for _, oneByte := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/offset=%d/oneByte=%v", name, offset, oneByte), func(t *testing.T) {
					cache := NewCacheManager(t.TempDir(), 0)
					r, err := newReader(context.Background(), offset, cache)
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()

					var src io.Reader = r
					if oneByte {
						src = iotest.OneByteReader(r)
					}
					got, err := io.ReadAll(src)
					if err != nil {
						t.Fatal(err)
					}
					want := full[offset:]
					if !bytes.Equal(got, want) {
						t.Fatalf("offset %d: got %d bytes, want %d bytes; output mismatch", offset, len(got), len(want))
					}
				})
			}
		}
	}
}

// refusingClient counts download attempts and fails them so nothing is ever cached.
type refusingClient struct {
	calls atomic.Int32
}

func (c *refusingClient) DownloadXorbWithURL(context.Context, string, http.Header) (io.ReadCloser, error) {
	c.calls.Add(1)
	return nil, errors.New("unexpected download")
}

func (*refusingClient) DownloadXorbsMultipartWithURL(context.Context, string, http.Header) (*multipart.Reader, io.Closer, error) {
	panic("not used")
}

// rangeReaders build V1 and V2 readers for one term served from a fetch range spanning bytes [0, bytesEnd].
var rangeReaders = map[string]func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager, opts ...Option) (io.ReadCloser, error){
	"v1": func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager, opts ...Option) (io.ReadCloser, error) {
		return NewReaderV1(context.Background(), client, &ReconstructionResponseV1{
			Terms: []Term{{Hash: testCacheHash, Range: term}},
			FetchInfo: map[string][]FetchInfoEntry{
				testCacheHash: {{Range: fetch, URL: "test://xorb", URLRange: ByteRange{Start: 0, End: bytesEnd}}},
			},
		}, append(opts, WithCacheManager(cache))...)
	},
	"v2": func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager, opts ...Option) (io.ReadCloser, error) {
		return NewReaderV2(context.Background(), client, &ReconstructionResponseV2{
			Terms: []Term{{Hash: testCacheHash, Range: term}},
			Xorbs: map[string][]XorbMultiRangeFetch{
				testCacheHash: {{URL: "test://xorb", Ranges: []XorbRangeDescriptor{{Chunks: fetch, Bytes: ByteRange{Start: 0, End: bytesEnd}}}}},
			},
		}, append(opts, WithCacheManager(cache))...)
	},
}

func TestReaderRejectsInvalidChunkRanges(t *testing.T) {
	cases := map[string]struct{ term, fetch ChunkRange }{
		"fetchPastMax":   {ChunkRange{Start: 0, End: 1}, ChunkRange{Start: 0, End: xet.MaxChunksPerXorb + 1}},
		"fetchMaxUint32": {ChunkRange{Start: 0, End: 1}, ChunkRange{Start: 0, End: math.MaxUint32}},
		"emptyTerm":      {ChunkRange{Start: 1, End: 1}, ChunkRange{Start: 0, End: 8}},
		"reversedTerm":   {ChunkRange{Start: 2, End: 1}, ChunkRange{Start: 0, End: 8}},
	}
	for name, newReader := range rangeReaders {
		for caseName, tc := range cases {
			t.Run(name+"/"+caseName, func(t *testing.T) {
				dir := t.TempDir()
				client := &refusingClient{}
				r, err := newReader(client, tc.term, tc.fetch, 1023, NewCacheManager(dir, 0))
				if err == nil {
					r.Close()
					t.Fatalf("accepted term %v served from fetch range %v", tc.term, tc.fetch)
				}
				if calls := client.calls.Load(); calls != 0 {
					t.Fatalf("rejected plan made %d downloads", calls)
				}
				if entries, _ := os.ReadDir(dir); len(entries) != 0 {
					t.Fatalf("rejected plan created cache entries: %v", entries)
				}
			})
		}
	}
}

func TestReaderAcceptsMaxChunkRange(t *testing.T) {
	chunks := make([][]byte, xet.MaxChunksPerXorb)
	for i := range chunks {
		chunks[i] = []byte{byte(i)}
	}
	encoded := buildTestXorb(t, chunks)
	full := bytes.Join(chunks, nil)
	fetch := ChunkRange{Start: 0, End: xet.MaxChunksPerXorb}
	terms := []ChunkRange{fetch, {Start: xet.MaxChunksPerXorb - 1, End: xet.MaxChunksPerXorb}}
	for name, newReader := range rangeReaders {
		for _, term := range terms {
			t.Run(fmt.Sprintf("%s/term=%d-%d", name, term.Start, term.End), func(t *testing.T) {
				r, err := newReader(&fakeClientAdapter{data: encoded}, term, fetch, int64(len(encoded)-1), NewCacheManager(t.TempDir(), 0))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				if want := full[term.Start:term.End]; err != nil || !bytes.Equal(got, want) {
					t.Fatalf("read %d bytes, %v; want %d bytes", len(got), err, len(want))
				}
			})
		}
	}
}

// randomChunks returns n incompressible chunks so the encoded xorb length does
// not depend on the content.
func randomChunks(seed int64, n, size int) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	chunks := make([][]byte, n)
	for i := range chunks {
		chunks[i] = make([]byte, size)
		rng.Read(chunks[i])
	}
	return chunks
}

func chunkHashes(chunks [][]byte) []xet.ChunkHash {
	hashes := make([]xet.ChunkHash, len(chunks))
	for i, c := range chunks {
		hashes[i] = xet.ComputeChunkHash(c)
	}
	return hashes
}

// TestReaderVerifiesFileHash pins WithExpectedFileHash: the output is hashed
// once per term chunk occurrence along the original chunk boundaries and
// compared with the expected file hash at EOF, on cold and cached reads.
func TestReaderVerifiesFileHash(t *testing.T) {
	const chunkSize = 1000
	chunks := randomChunks(1, 3, chunkSize)
	wrongChunks := randomChunks(2, 3, chunkSize)
	encoded := buildTestXorb(t, chunks)
	wrongEncoded := buildTestXorb(t, wrongChunks)
	if len(wrongEncoded) != len(encoded) {
		t.Fatalf("substituted xorb is %d bytes, want %d", len(wrongEncoded), len(encoded))
	}
	hashes := chunkHashes(chunks)

	// Terms reuse chunks and select subsets of the single wide fetch entry.
	terms := []Term{
		{Hash: testCacheHash, UnpackedLength: 2 * chunkSize, Range: ChunkRange{Start: 0, End: 2}},
		{Hash: testCacheHash, UnpackedLength: 2 * chunkSize, Range: ChunkRange{Start: 1, End: 3}},
		{Hash: testCacheHash, UnpackedLength: chunkSize, Range: ChunkRange{Start: 0, End: 1}},
	}
	var full []byte
	var fileHashes []xet.ChunkHash
	var fileSizes []uint64
	for _, term := range terms {
		for i := term.Range.Start; i < term.Range.End; i++ {
			full = append(full, chunks[i]...)
			fileHashes = append(fileHashes, hashes[i])
			fileSizes = append(fileSizes, chunkSize)
		}
	}
	fileHash := xet.ComputeFileHash(fileHashes, fileSizes)

	readers := map[string]func(ctx context.Context, data []byte, terms []Term, offset int64, opts ...Option) (io.ReadCloser, error){
		"v1": func(ctx context.Context, data []byte, terms []Term, offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV1(ctx, &fakeClientAdapter{data: data}, &ReconstructionResponseV1{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				FetchInfo: map[string][]FetchInfoEntry{
					testCacheHash: {{
						Range:    ChunkRange{Start: 0, End: 3},
						URL:      "test://xorb",
						URLRange: ByteRange{Start: 0, End: int64(len(encoded) - 1)},
					}},
				},
			}, opts...)
		},
		"v2": func(ctx context.Context, data []byte, terms []Term, offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV2(ctx, &fakeClientAdapter{data: data}, &ReconstructionResponseV2{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				Xorbs: map[string][]XorbMultiRangeFetch{
					testCacheHash: {{
						URL: "test://xorb",
						Ranges: []XorbRangeDescriptor{{
							Chunks: ChunkRange{Start: 0, End: 3},
							Bytes:  ByteRange{Start: 0, End: int64(len(encoded) - 1)},
						}},
					}},
				},
			}, opts...)
		},
	}

	readAll := func(t *testing.T, r io.ReadCloser, oneByte bool) ([]byte, error) {
		t.Helper()
		defer r.Close()
		var src io.Reader = r
		if oneByte {
			src = iotest.OneByteReader(r)
		}
		return io.ReadAll(src)
	}
	wantMismatch := func(t *testing.T, err error) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
			t.Fatalf("err = %v, want file hash mismatch", err)
		}
	}

	ctx := context.Background()
	for name, newReader := range readers {
		t.Run(name, func(t *testing.T) {
			for _, oneByte := range []bool{false, true} {
				cache := NewCacheManager(t.TempDir(), 0)
				r, err := newReader(ctx, encoded, terms, 0, WithCacheManager(cache), WithExpectedFileHash(fileHash))
				if err != nil {
					t.Fatal(err)
				}
				got, err := readAll(t, r, oneByte)
				if err != nil || !bytes.Equal(got, full) {
					t.Fatalf("oneByte=%v: got %d bytes, err %v; want %d bytes", oneByte, len(got), err, len(full))
				}
			}

			t.Run("substituted content", func(t *testing.T) {
				cache := NewCacheManager(t.TempDir(), 0)
				r, err := newReader(ctx, wrongEncoded, terms, 0, WithCacheManager(cache), WithExpectedFileHash(fileHash))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got, err := io.ReadAll(r)
				wantMismatch(t, err)
				if len(got) != len(full) {
					t.Fatalf("got %d bytes before the mismatch, want %d", len(got), len(full))
				}
				if _, err := r.Read(make([]byte, 1)); err == nil || !strings.Contains(err.Error(), "file hash mismatch") {
					t.Fatalf("Read after mismatch = %v, want the mismatch to persist", err)
				}
			})

			// Cached ranges bypass the network entirely; the reader gets no
			// fetchable source.
			for _, test := range []struct {
				name   string
				chunks [][]byte
				ok     bool
			}{
				{"cached content", chunks, true},
				{"substituted cached content", wrongChunks, false},
			} {
				t.Run(test.name, func(t *testing.T) {
					cache := NewCacheManager(t.TempDir(), 0)
					cached := make([]string, len(test.chunks))
					for i, c := range test.chunks {
						cached[i] = string(c)
					}
					writeRangeEntry(t, cache, testCacheHash, 0, 3, 0, int64(len(encoded)-1), cached)
					r, err := newReader(ctx, nil, terms, 0, WithCacheManager(cache), WithExpectedFileHash(fileHash))
					if err != nil {
						t.Fatal(err)
					}
					got, err := readAll(t, r, false)
					if test.ok {
						if err != nil || !bytes.Equal(got, full) {
							t.Fatalf("got %d bytes, err %v; want %d bytes", len(got), err, len(full))
						}
						return
					}
					wantMismatch(t, err)
				})
			}

			t.Run("empty file", func(t *testing.T) {
				cache := NewCacheManager(t.TempDir(), 0)
				r, err := newReader(ctx, nil, nil, 0, WithCacheManager(cache), WithExpectedFileHash(xet.FileHash{}))
				if err != nil {
					t.Fatal(err)
				}
				if got, err := readAll(t, r, false); err != nil || len(got) != 0 {
					t.Fatalf("got %d bytes, err %v; want empty", len(got), err)
				}
				r, err = newReader(ctx, nil, nil, 0, WithCacheManager(cache), WithExpectedFileHash(fileHash))
				if err != nil {
					t.Fatal(err)
				}
				_, err = readAll(t, r, false)
				wantMismatch(t, err)
			})

			t.Run("offset into first range", func(t *testing.T) {
				cache := NewCacheManager(t.TempDir(), 0)
				r, err := newReader(ctx, encoded, terms, 1, WithCacheManager(cache), WithExpectedFileHash(fileHash))
				if err == nil {
					r.Close()
					t.Fatal("expected an error: a partial reconstruction cannot be verified")
				}
			})
		})
	}
}

// TestReaderVerifiesWithDecoderChunkHashes corrupts the hash a writer kept for
// a chunk it decoded: verification must fail although every byte is intact,
// so the verifier uses the decoder's hash instead of hashing the chunk again,
// with or without a cache manager.
func TestReaderVerifiesWithDecoderChunkHashes(t *testing.T) {
	chunks := randomChunks(5, 3, 1000)
	encoded := buildTestXorb(t, chunks)
	fileHash := xet.ComputeFileHash(chunkHashes(chunks), []uint64{1000, 1000, 1000})
	key := fetchKey{Hash: testCacheHash, Start: 0, End: int64(len(encoded) - 1)}
	whole := ChunkRange{Start: 0, End: 3}
	for name, newReader := range rangeReaders {
		for _, cached := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/cached=%v", name, cached), func(t *testing.T) {
				var m *CacheManager
				if cached {
					m = NewCacheManager(t.TempDir(), 0)
				}
				r, err := newReader(&fakeClientAdapter{data: encoded}, whole, whole, key.End, m, WithExpectedFileHash(fileHash))
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				src, err := readerPrefetcher(t, r).Get(key)
				if err != nil {
					t.Fatal(err)
				}
				// Tamper only once the whole range is decoded.
				var kept bool
				switch src := src.(type) {
				case *chunkCache:
					if _, err := src.readChunk(2, make([]byte, 1000)); err != nil {
						t.Fatal(err)
					}
					src.mut.Lock()
					kept = src.metas[1].hashed
					src.metas[1].hash[0] ^= 0xff
					src.mut.Unlock()
				case *memoryRange:
					src.mu.Lock()
					for len(src.chunks) < 3 {
						src.more.Wait()
					}
					kept = src.chunks[1].ref.hashed
					src.chunks[1].ref.hash[0] ^= 0xff
					src.mu.Unlock()
				}
				if !kept {
					t.Fatal("decoded chunk 1 kept no decoder hash")
				}

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
}

// recordingWriter records the size of every Write. Past limit bytes it takes
// nothing more and fails with err, or writes short when err is nil.
type recordingWriter struct {
	data  []byte
	sizes []int
	limit int
	err   error
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.sizes = append(w.sizes, len(p))
	n := min(len(p), w.limit-len(w.data))
	w.data = append(w.data, p[:n]...)
	if n < len(p) {
		return n, w.err
	}
	return n, nil
}

// TestReaderWriteTo pins io.WriterTo on both readers: io.Copy writes each
// chunk occurrence with one Write, the first taking the rest of the chunk the
// copy starts in, and returns file hash mismatches and the writer's errors;
// after a failed Write, reading resumes where the writer stopped.
func TestReaderWriteTo(t *testing.T) {
	sizes := []int{1000, 1500, 700}
	rng := rand.New(rand.NewSource(1))
	chunks := make([][]byte, len(sizes))
	for i, size := range sizes {
		chunks[i] = make([]byte, size)
		rng.Read(chunks[i])
	}
	encoded := buildTestXorb(t, chunks)
	hashes := chunkHashes(chunks)

	terms := []Term{
		{Hash: testCacheHash, Range: ChunkRange{Start: 0, End: 2}},
		{Hash: testCacheHash, Range: ChunkRange{Start: 1, End: 3}},
		{Hash: testCacheHash, Range: ChunkRange{Start: 0, End: 1}},
	}
	var full []byte
	var occurrences []int
	var fileHashes []xet.ChunkHash
	var fileSizes []uint64
	for _, term := range terms {
		for i := term.Range.Start; i < term.Range.End; i++ {
			full = append(full, chunks[i]...)
			occurrences = append(occurrences, sizes[i])
			fileHashes = append(fileHashes, hashes[i])
			fileSizes = append(fileSizes, uint64(sizes[i]))
		}
	}
	fileHash := xet.ComputeFileHash(fileHashes, fileSizes)

	fetch := ChunkRange{Start: 0, End: uint32(len(chunks))}
	span := ByteRange{Start: 0, End: int64(len(encoded) - 1)}
	readers := map[string]func(offset int64, opts ...Option) (io.ReadCloser, error){
		"v1": func(offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV1(context.Background(), &fakeClientAdapter{data: encoded}, &ReconstructionResponseV1{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				FetchInfo:            map[string][]FetchInfoEntry{testCacheHash: {{Range: fetch, URL: "test://xorb", URLRange: span}}},
			}, opts...)
		},
		"v2": func(offset int64, opts ...Option) (io.ReadCloser, error) {
			return NewReaderV2(context.Background(), &fakeClientAdapter{data: encoded}, &ReconstructionResponseV2{
				OffsetIntoFirstRange: offset,
				Terms:                terms,
				Xorbs:                map[string][]XorbMultiRangeFetch{testCacheHash: {{URL: "test://xorb", Ranges: []XorbRangeDescriptor{{Chunks: fetch, Bytes: span}}}}},
			}, opts...)
		},
	}

	for name, newReader := range readers {
		open := func(t *testing.T, offset int, opts ...Option) io.ReadCloser {
			t.Helper()
			r, err := newReader(int64(offset), append(opts, WithCacheManager(NewCacheManager(t.TempDir(), 0)))...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { r.Close() })
			return r
		}

		for _, test := range []struct {
			name         string
			offset, read int // OffsetIntoFirstRange; bytes taken with Read before io.Copy
		}{
			{"whole file", 0, 0},
			{"after a partial Read", 0, 500},
			{"offset into first range", 1300, 0},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				var opts []Option
				if test.offset == 0 {
					opts = append(opts, WithExpectedFileHash(fileHash))
				}
				r := open(t, test.offset, opts...)
				head := make([]byte, test.read)
				if _, err := io.ReadFull(r, head); err != nil {
					t.Fatal(err)
				}
				w := &recordingWriter{limit: math.MaxInt}
				n, err := io.Copy(w, r)
				if got := append(head, w.data...); err != nil || n != int64(len(w.data)) || !bytes.Equal(got, full[test.offset:]) {
					t.Fatalf("io.Copy = %d, %v; got %d bytes in all, want %d", n, err, len(got), len(full)-test.offset)
				}
				var want []int
				skip := test.offset + test.read
				for _, size := range occurrences {
					if skip >= size {
						skip -= size
						continue
					}
					want = append(want, size-skip)
					skip = 0
				}
				if !slices.Equal(w.sizes, want) {
					t.Fatalf("Write sizes %v, want one Write per chunk: %v", w.sizes, want)
				}
			})
		}

		t.Run(name+"/hash mismatch", func(t *testing.T) {
			r := open(t, 0, WithExpectedFileHash(xet.FileHash{}))
			n, err := io.Copy(&recordingWriter{limit: math.MaxInt}, r)
			if err == nil || !strings.Contains(err.Error(), "file hash mismatch") || n != int64(len(full)) {
				t.Fatalf("io.Copy = %d, %v; want %d bytes and a file hash mismatch", n, err, len(full))
			}
		})

		errWrite := errors.New("write failed")
		for _, test := range []struct {
			name  string
			limit int
			err   error
		}{
			{"write error at a chunk end", 1000, errWrite},
			{"write error mid-chunk", 1700, errWrite},
			{"short write", 1700, nil},
		} {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				r := open(t, 0, WithExpectedFileHash(fileHash))
				w := &recordingWriter{limit: test.limit, err: test.err}
				n, err := io.Copy(w, r)
				want := test.err
				if want == nil {
					want = io.ErrShortWrite
				}
				if !errors.Is(err, want) || n != int64(test.limit) {
					t.Fatalf("io.Copy = %d, %v; want %d, %v", n, err, test.limit, want)
				}
				// Each chunk still counts once toward the file hash.
				rest, err := io.ReadAll(r)
				if got := append(w.data, rest...); err != nil || !bytes.Equal(got, full) {
					t.Fatalf("resumed read: got %d bytes in all, %v; want %d", len(got), err, len(full))
				}
			})
		}
	}
}

// TestReaderReadsEachChunkOnce pins that Read takes a chunk from the cache
// once, however small the reads: a change to the cache entry after the first
// byte is read does not reach the rest of that chunk.
func TestReaderReadsEachChunkOnce(t *testing.T) {
	chunk := randomChunks(1, 1, 1000)[0]
	for name, newReader := range rangeReaders {
		t.Run(name, func(t *testing.T) {
			cache := NewCacheManager(t.TempDir(), 0)
			path := writeRangeEntry(t, cache, testCacheHash, 0, 1, 0, 1023, []string{string(chunk)})
			r, err := newReader(&refusingClient{}, ChunkRange{Start: 0, End: 1}, ChunkRange{Start: 0, End: 1}, 1023, cache)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got := make([]byte, 1)
			if _, err := io.ReadFull(r, got); err != nil {
				t.Fatal(err)
			}
			corruptDataByte(t, path)
			rest, err := io.ReadAll(iotest.OneByteReader(r))
			if got = append(got, rest...); err != nil || !bytes.Equal(got, chunk) {
				t.Fatalf("got %d bytes, %v; want the chunk as first read", len(got), err)
			}
		})
	}
}

// recordingStorageAdapter captures what GetXorbURL receives.
type recordingStorageAdapter struct {
	ctx context.Context
}

func (r *recordingStorageAdapter) GetXorbURL(ctx context.Context, xorbHash xet.XorbHash) (string, error) {
	r.ctx = ctx
	return "/v1/xorbs/default/" + xorbHash.String(), nil
}

// GetXorbChunkOffsets gives every xorb three chunks, enough for the terms these tests build.
func (r *recordingStorageAdapter) GetXorbChunkOffsets(context.Context, xet.XorbHash) ([]uint64, error) {
	return []uint64{1, 2, 3}, nil
}

func TestBuildReconstructionForwardsContext(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "request")
	var fileHash xet.FileHash
	sh := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash, Entries: []shard.FileDataSequenceEntry{{UnpackedSegBytes: 1, ChunkIndexEnd: 1}}}}}
	build := map[string]func(StorageAdapter) error{
		"v1": func(st StorageAdapter) error {
			_, err := BuildReconstructionResponseV1(ctx, st, sh, fileHash, ParseByteRange("", 1))
			return err
		},
		"v2": func(st StorageAdapter) error {
			_, err := BuildReconstructionResponseV2(ctx, st, sh, fileHash, ParseByteRange("", 1))
			return err
		},
	}
	for name, fn := range build {
		st := &recordingStorageAdapter{}
		if err := fn(st); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if st.ctx != ctx {
			t.Fatalf("%s: GetXorbURL got ctx %v; want the caller's ctx", name, st.ctx)
		}
	}
}

func TestBuildReconstructionFileAbsentFromShardIsAnError(t *testing.T) {
	present, absent := xet.FileHash{1}, xet.FileHash{2}
	sh := &shard.Shard{Files: []shard.FileBlock{{FileHash: present, Entries: []shard.FileDataSequenceEntry{{UnpackedSegBytes: 1, ChunkIndexEnd: 1}}}}}
	// The handlers parse the Range against the absent file's size, 0.
	rng := ParseByteRange("", 0)
	build := map[string]func() error{
		"v1": func() error {
			_, err := BuildReconstructionResponseV1(context.Background(), &recordingStorageAdapter{}, sh, absent, rng)
			return err
		},
		"v2": func() error {
			_, err := BuildReconstructionResponseV2(context.Background(), &recordingStorageAdapter{}, sh, absent, rng)
			return err
		},
	}
	for name, fn := range build {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("building the %s response of a file absent from its shard panicked: %v", name, r)
				}
			}()
			if err := fn(); err == nil {
				t.Errorf("building the %s response of a file absent from its shard succeeded, want an error", name)
			}
		})
	}
}

// TestBuildReconstructionRangeHeader pins which whole terms a Range header,
// parsed against the file's size, selects; tails are never trimmed, so
// ExpectedLength rounds up to a term end.
func TestBuildReconstructionRangeHeader(t *testing.T) {
	var fileHash xet.FileHash
	var entries []shard.FileDataSequenceEntry
	var allTerms []Term
	var size int64
	for index, length := range []uint32{1000, 1000, 500} {
		entry := shard.FileDataSequenceEntry{
			CASHash:          xet.XorbHash{byte(index + 1)},
			UnpackedSegBytes: length,
			ChunkIndexStart:  uint32(index),
			ChunkIndexEnd:    uint32(index + 1),
		}
		entries = append(entries, entry)
		allTerms = append(allTerms, Term{
			Hash:           entry.CASHash.String(),
			UnpackedLength: uint64(length),
			Range:          ChunkRange{Start: entry.ChunkIndexStart, End: entry.ChunkIndexEnd},
		})
		size += int64(length)
	}
	sh := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash, Entries: entries}}}
	emptyShard := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash}}}

	type result struct {
		terms  []Term
		offset int64
		length int64
	}
	build := map[string]func(sh *shard.Shard, rng ByteRange) (result, error){
		"v1": func(sh *shard.Shard, rng ByteRange) (result, error) {
			resp, err := BuildReconstructionResponseV1(context.Background(), &recordingStorageAdapter{}, sh, fileHash, rng)
			if err != nil {
				return result{}, err
			}
			return result{resp.Terms, resp.OffsetIntoFirstRange, ExpectedLengthV1(resp)}, nil
		},
		"v2": func(sh *shard.Shard, rng ByteRange) (result, error) {
			resp, err := BuildReconstructionResponseV2(context.Background(), &recordingStorageAdapter{}, sh, fileHash, rng)
			if err != nil {
				return result{}, err
			}
			return result{resp.Terms, resp.OffsetIntoFirstRange, ExpectedLengthV2(resp)}, nil
		},
	}

	tests := []struct {
		name, header string
		first, last  int // selected entries [first, last)
		offset       int64
		length       int64
	}{
		{"no range", "", 0, 3, 0, 2500},
		{"open from start", "bytes=0-", 0, 3, 0, 2500},
		{"open interior", "bytes=1500-", 1, 3, 500, 1000},
		{"open at term boundary", "bytes=1000-", 1, 3, 0, 1500},
		{"open last byte", "bytes=2499-", 2, 3, 499, 1},
		{"open at end of file", "bytes=2500-", 0, 0, 0, 0},
		{"explicit", "bytes=500-1499", 0, 2, 500, 1500},
		{"explicit ending at a term start", "bytes=500-1000", 0, 2, 500, 1500},
		{"explicit clamped", "bytes=2400-9999", 2, 3, 400, 100},
		{"suffix", "bytes=-300", 2, 3, 200, 300},
		{"suffix at term boundary", "bytes=-500", 2, 3, 0, 500},
		{"suffix longer than file", "bytes=-99999", 0, 3, 0, 2500},
		{"suffix zero", "bytes=-0", 0, 0, 0, 0},
		{"start beyond end", "bytes=5000-6000", 0, 0, 0, 0},
		{"start after end", "bytes=1500-1000", 0, 3, 0, 2500},
		{"garbage", "bytes=abc", 0, 3, 0, 2500},
		{"multirange", "bytes=0-499,1000-1499", 0, 3, 0, 2500},
		{"overflow", "bytes=99999999999999999999-", 0, 3, 0, 2500},
		{"dash only", "bytes=-", 0, 3, 0, 2500},
		{"signed", "bytes=+5-", 0, 3, 0, 2500},
		{"other unit", "items=0-10", 0, 3, 0, 2500},
	}
	for name, fn := range build {
		for _, test := range tests {
			t.Run(name+"/"+test.name, func(t *testing.T) {
				got, err := fn(sh, ParseByteRange(test.header, size))
				if err != nil {
					t.Fatal(err)
				}
				want := result{allTerms[test.first:test.last], test.offset, test.length}
				if !slices.Equal(got.terms, want.terms) || got.offset != want.offset || got.length != want.length {
					t.Fatalf("Range %q: got %d terms %v offset %d length %d; want %d terms offset %d length %d",
						test.header, len(got.terms), got.terms, got.offset, got.length, len(want.terms), want.offset, want.length)
				}
			})
		}
		for _, header := range []string{"", "bytes=0-", "bytes=-1", "bytes=0-0"} {
			got, err := fn(emptyShard, ParseByteRange(header, 0))
			if err != nil {
				t.Fatal(err)
			}
			if len(got.terms) != 0 || got.offset != 0 || got.length != 0 {
				t.Fatalf("%s: empty file with Range %q: got %+v, want no terms", name, header, got)
			}
		}
	}
}
