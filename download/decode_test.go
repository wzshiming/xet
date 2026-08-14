package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"testing"
	"testing/iotest"

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
