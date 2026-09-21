package download

import (
	"bytes"
	"context"
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

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// fakeClientAdapter serves a pre-encoded xorb honoring the Range header.
type fakeClientAdapter struct {
	data  []byte
	calls atomic.Int32
}

func (f *fakeClientAdapter) DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error) {
	f.calls.Add(1)
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

func listTree(t *testing.T, root string) []string {
	t.Helper()
	var paths []string
	err := filepath.WalkDir(root, func(path string, _ fs.DirEntry, err error) error {
		paths = append(paths, path)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return paths
}

// TestReaderRejectsInvalidXorbHash pins that a server-supplied xorb hash is
// validated before it becomes a cache path: reader construction fails without
// a download, without touching anything outside the cache, and without pinning
// terms already served from it.
func TestReaderRejectsInvalidXorbHash(t *testing.T) {
	chunks := []string{"chunk-a", "chunk-b"}
	var raw [][]byte
	for _, c := range chunks {
		raw = append(raw, []byte(c))
	}
	encoded := buildTestXorb(t, raw)
	n := uint32(len(chunks))
	byteEnd := int64(len(encoded) - 1)
	want := []byte(strings.Join(chunks, ""))

	// Each hash becomes one term covering the whole xorb.
	newReaders := map[string]func(adapter ClientAdapter, cache *CacheManager, hashes ...string) (io.ReadCloser, error){
		"v1": func(adapter ClientAdapter, cache *CacheManager, hashes ...string) (io.ReadCloser, error) {
			recon := &ReconstructionResponseV1{FetchInfo: map[string][]FetchInfoEntry{}}
			for _, hash := range hashes {
				recon.Terms = append(recon.Terms, Term{Hash: hash, UnpackedLength: uint64(len(want)), Range: ChunkRange{End: n}})
				recon.FetchInfo[hash] = []FetchInfoEntry{{Range: ChunkRange{End: n}, URL: "test://xorb", URLRange: ByteRange{End: byteEnd}}}
			}
			return NewReaderV1(context.Background(), adapter, recon, WithCacheManager(cache))
		},
		"v2": func(adapter ClientAdapter, cache *CacheManager, hashes ...string) (io.ReadCloser, error) {
			recon := &ReconstructionResponseV2{Xorbs: map[string][]XorbMultiRangeFetch{}}
			for _, hash := range hashes {
				recon.Terms = append(recon.Terms, Term{Hash: hash, UnpackedLength: uint64(len(want)), Range: ChunkRange{End: n}})
				recon.Xorbs[hash] = []XorbMultiRangeFetch{{URL: "test://xorb", Ranges: []XorbRangeDescriptor{{Chunks: ChunkRange{End: n}, Bytes: ByteRange{End: byteEnd}}}}}
			}
			return NewReaderV2(context.Background(), adapter, recon, WithCacheManager(cache))
		},
	}

	fresh := strings.Repeat("fedcba9876543210", 4)
	invalid := []string{
		"../../victim/pwned",
		"",
		"abcd",
		"0123456789abcdef",
		fresh + "0",
		strings.Repeat("g", 64),
		".." + strings.Repeat("0", 62),
		"00/" + strings.Repeat("0", 61),
	}

	for name, newReader := range newReaders {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			cacheDir := filepath.Join(root, "cache")
			m := NewCacheManager(cacheDir, 0)
			cachedPath := writeRangeEntry(t, m, testCacheHash, 0, n, 0, byteEnd, chunks)
			// The traversal hash resolves to this cache-shaped file outside the cache root.
			sentinel := filepath.Join(root, "victim", "pwned", cacheFileName(0, n, 0, byteEnd))
			if err := os.MkdirAll(filepath.Dir(sentinel), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(sentinel, []byte("sentinel"), 0o644); err != nil {
				t.Fatal(err)
			}
			before := listTree(t, root)

			for _, hash := range invalid {
				adapter := &fakeClientAdapter{data: encoded}
				r, err := newReader(adapter, m, testCacheHash, hash)
				if err == nil {
					io.ReadAll(r) //nolint:errcheck
					r.Close()
					t.Errorf("hash %q: reader accepted", hash)
				}
				if calls := adapter.calls.Load(); calls != 0 {
					t.Errorf("hash %q: %d downloads, want none", hash, calls)
				}
				if got, err := os.ReadFile(sentinel); err != nil || string(got) != "sentinel" {
					t.Errorf("hash %q: sentinel = %q, %v", hash, got, err)
				}
				if after := listTree(t, root); !slices.Equal(after, before) {
					t.Errorf("hash %q: tree changed to %v", hash, after)
				}
				m.mu.Lock()
				v, ok := m.lru.Get(cachedPath)
				m.mu.Unlock()
				if ok && v.(*cacheEntry).refs != 0 {
					t.Errorf("hash %q: cached term kept %d refs after failed init", hash, v.(*cacheEntry).refs)
				}
			}

			adapter := &fakeClientAdapter{data: encoded}
			r, err := newReader(adapter, m, testCacheHash, fresh)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			r.Close()
			if err != nil || !bytes.Equal(got, bytes.Repeat(want, 2)) {
				t.Fatalf("valid hashes: %v, got %q", err, got)
			}
			if calls := adapter.calls.Load(); calls != 1 {
				t.Errorf("valid hashes: %d downloads, want one for the uncached term", calls)
			}
			if _, err := os.Stat(cacheFilePath(cacheDir, fresh, 0, n, 0, byteEnd)); err != nil {
				t.Errorf("downloaded term not cached under its fanout path: %v", err)
			}
			r, err = newReader(adapter, m, fresh)
			if err != nil {
				t.Fatal(err)
			}
			got, err = io.ReadAll(r)
			r.Close()
			if err != nil || !bytes.Equal(got, want) || adapter.calls.Load() != 1 {
				t.Fatalf("reopen: %v, got %q, %d downloads; want the cached bytes without a download", err, got, adapter.calls.Load())
			}
		})
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
