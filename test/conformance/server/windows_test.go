package server_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/server"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/local"
	"github.com/wzshiming/xet/storage/storagetest"
	"github.com/wzshiming/xet/test/conformance/rustref"
	"github.com/wzshiming/xet/xorb"
)

// TestServerWindowedDownloadConformance downloads files with xet-core, which queries reconstructions in byte
// windows and keys a xorb's fetches by start chunk: it fetches each chunk once, plus one per window edge, also
// when terms of a xorb share a start chunk.
func TestServerWindowedDownloadConformance(t *testing.T) {
	const chunkSize = 64 << 10
	// Windows of 90 chunks and 1000 bytes start the second one in chunk 90 of the first term.
	const window = 90*chunkSize + 1000
	none := storagetest.ChunkClasses[slices.IndexFunc(storagetest.ChunkClasses, func(c storagetest.ChunkClass) bool { return c.Compression == xorb.CompressionNone })]
	type term struct {
		x          int
		start, end uint32
	}
	for _, c := range []struct {
		name    string
		windows bool
		terms   []term // over xorbs of 150, 10 and 200 chunks
	}{
		// The second window trims the first term to chunk 90, where the third term starts.
		{"windows", true, []term{{0, 0, 100}, {1, 0, 10}, {0, 90, 150}, {2, 0, 200}}},
		// One window holds the file, whose first and third term start at chunk 0.
		{"terms sharing a start chunk", false, []term{{0, 0, 100}, {1, 0, 10}, {0, 0, 150}, {2, 0, 200}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			var fetched atomic.Int64
			var srv *server.Handler
			httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/xorbs/") {
					var start, end int64
					if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err == nil {
						fetched.Add(end - start + 1)
					}
				}
				srv.ServeHTTP(w, r)
			}))
			defer httpSrv.Close()
			stor, err := local.NewStorage(local.WithBasePath(t.TempDir()), local.WithBaseURL(httpSrv.URL))
			if err != nil {
				t.Fatal(err)
			}
			srv = server.NewHandler(server.WithStorage(stor))

			// whole is the packed bytes of all xorbs, edge those of their largest chunk.
			var whole, edge int64
			sh := shard.NewShard()
			chunks := make([][][]byte, 3)
			var hashes []xet.XorbHash
			for x, n := range []int{150, 10, 200} {
				var cb shard.CASBlock
				for i := range n {
					chunk := make([]byte, chunkSize)
					none.Chunk(x, i, chunk)
					chunks[x] = append(chunks[x], chunk)
					cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{ChunkHash: xet.ComputeChunkHash(chunk), ByteRangeStart: cb.NumBytesInCAS, UnpackedSegBytes: chunkSize})
					cb.NumBytesInCAS += chunkSize
				}
				encoded, hash := storagetest.EncodeXorb(t, true, chunks[x]...)
				if _, err := stor.PutXorb(t.Context(), hash, bytes.NewReader(encoded)); err != nil {
					t.Fatal(err)
				}
				cb.CASHash = hash
				sh.AddCASBlock(cb)
				hashes = append(hashes, hash)
				offsets, err := stor.GetXorbChunkOffsets(t.Context(), hash)
				if err != nil {
					t.Fatal(err)
				}
				for i := range uint32(n) {
					start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, i, i+1)
					if err != nil {
						t.Fatal(err)
					}
					whole += end - start + 1
					edge = max(edge, end-start+1)
				}
			}
			var content []byte
			var chunkHashes []xet.ChunkHash
			var chunkSizes []uint64
			file := shard.FileBlock{}
			for _, tm := range c.terms {
				entry := shard.FileDataSequenceEntry{CASHash: hashes[tm.x], ChunkIndexStart: tm.start, ChunkIndexEnd: tm.end}
				for _, chunk := range chunks[tm.x][tm.start:tm.end] {
					entry.UnpackedSegBytes += uint32(len(chunk))
					content = append(content, chunk...)
					chunkHashes = append(chunkHashes, xet.ComputeChunkHash(chunk))
					chunkSizes = append(chunkSizes, uint64(len(chunk)))
				}
				file.Entries = append(file.Entries, entry)
			}
			file.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
			sh.AddFile(file)
			if _, err := storage.PutShard(t.Context(), stor, sh); err != nil {
				t.Fatal(err)
			}

			windows := int64(1)
			if c.windows {
				windows = (int64(len(content)) + window - 1) / window
			}
			for _, protocol := range []rustref.ProtocolVersion{rustref.ProtocolV1, rustref.ProtocolV2} {
				t.Run(protocol.String(), func(t *testing.T) {
					if c.windows {
						t.Setenv("HF_XET_RECONSTRUCTION_MIN_RECONSTRUCTION_FETCH_SIZE", strconv.Itoa(window))
						t.Setenv("HF_XET_RECONSTRUCTION_MAX_RECONSTRUCTION_FETCH_SIZE", strconv.Itoa(window))
					}
					fetched.Store(0)
					dest := filepath.Join(t.TempDir(), "download.bin")
					req := []rustref.DownloadRequest{{DestinationPath: dest, Hash: file.FileHash.String(), FileSize: int64(len(content))}}
					if _, err := rustref.DownloadFilesWithVersion(req, httpSrv.URL, nil, protocol); err != nil {
						t.Fatalf("download with xet-core: %v", err)
					}
					got, err := os.ReadFile(dest)
					if err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(got, content) {
						t.Fatalf("xet-core downloaded %d bytes differing from the file's %d", len(got), len(content))
					}
					if limit := whole + (windows-1)*edge; fetched.Load() > limit {
						t.Errorf("xet-core fetched %d bytes of xorb data in %d window(s), want at most %d: each chunk once, plus one per window edge", fetched.Load(), windows, limit)
					}
				})
			}
		})
	}
}
