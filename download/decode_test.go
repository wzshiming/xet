package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"slices"
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
var rangeReaders = map[string]func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager) (io.ReadCloser, error){
	"v1": func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager) (io.ReadCloser, error) {
		return NewReaderV1(context.Background(), client, &ReconstructionResponseV1{
			Terms: []Term{{Hash: testCacheHash, Range: term}},
			FetchInfo: map[string][]FetchInfoEntry{
				testCacheHash: {{Range: fetch, URL: "test://xorb", URLRange: ByteRange{Start: 0, End: bytesEnd}}},
			},
		}, WithCacheManager(cache))
	},
	"v2": func(client ClientAdapter, term, fetch ChunkRange, bytesEnd int64, cache *CacheManager) (io.ReadCloser, error) {
		return NewReaderV2(context.Background(), client, &ReconstructionResponseV2{
			Terms: []Term{{Hash: testCacheHash, Range: term}},
			Xorbs: map[string][]XorbMultiRangeFetch{
				testCacheHash: {{URL: "test://xorb", Ranges: []XorbRangeDescriptor{{Chunks: fetch, Bytes: ByteRange{Start: 0, End: bytesEnd}}}}},
			},
		}, WithCacheManager(cache))
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

// recordingStorageAdapter captures what GetXorbURL receives.
type recordingStorageAdapter struct {
	ctx       context.Context
	namespace string
}

func (r *recordingStorageAdapter) GetXorbURL(ctx context.Context, namespace string, xorbHash xet.XorbHash) (string, error) {
	r.ctx, r.namespace = ctx, namespace
	return "/v1/xorbs/" + namespace + "/" + xorbHash.String(), nil
}

func (r *recordingStorageAdapter) GetXorbDataRange(context.Context, string, xet.XorbHash, uint32, uint32) (int64, int64, error) {
	return 0, 0, nil
}

func TestBuildReconstructionForwardsContextAndNamespace(t *testing.T) {
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "request")
	var fileHash xet.FileHash
	sh := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash, Entries: []shard.FileDataSequenceEntry{{UnpackedSegBytes: 1, ChunkIndexEnd: 1}}}}}
	build := map[string]func(StorageAdapter) error{
		"v1": func(st StorageAdapter) error {
			_, err := BuildReconstructionResponseV1(ctx, st, "tenant", sh, fileHash, "")
			return err
		},
		"v2": func(st StorageAdapter) error {
			_, err := BuildReconstructionResponseV2(ctx, st, "tenant", sh, fileHash, "")
			return err
		},
	}
	for name, fn := range build {
		st := &recordingStorageAdapter{}
		if err := fn(st); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if st.ctx != ctx || st.namespace != "tenant" {
			t.Fatalf("%s: GetXorbURL got ctx %v, namespace %q; want the caller's ctx and %q", name, st.ctx, st.namespace, "tenant")
		}
	}
}

// TestBuildReconstructionRangeHeader pins which whole terms a Range header
// selects; tails are never trimmed, so ExpectedLength rounds up to a term end.
func TestBuildReconstructionRangeHeader(t *testing.T) {
	var fileHash xet.FileHash
	var entries []shard.FileDataSequenceEntry
	var allTerms []Term
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
	}
	sh := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash, Entries: entries}}}
	emptyShard := &shard.Shard{Files: []shard.FileBlock{{FileHash: fileHash}}}

	type result struct {
		terms  []Term
		offset int64
		length int64
	}
	build := map[string]func(sh *shard.Shard, header string) (result, error){
		"v1": func(sh *shard.Shard, header string) (result, error) {
			resp, err := BuildReconstructionResponseV1(context.Background(), &recordingStorageAdapter{}, "tenant", sh, fileHash, header)
			if err != nil {
				return result{}, err
			}
			return result{resp.Terms, resp.OffsetIntoFirstRange, ExpectedLengthV1(resp)}, nil
		},
		"v2": func(sh *shard.Shard, header string) (result, error) {
			resp, err := BuildReconstructionResponseV2(context.Background(), &recordingStorageAdapter{}, "tenant", sh, fileHash, header)
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
				got, err := fn(sh, test.header)
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
			got, err := fn(emptyShard, header)
			if err != nil {
				t.Fatal(err)
			}
			if len(got.terms) != 0 || got.offset != 0 || got.length != 0 {
				t.Fatalf("%s: empty file with Range %q: got %+v, want no terms", name, header, got)
			}
		}
	}
}
