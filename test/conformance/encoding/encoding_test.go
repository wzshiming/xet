package encoding_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/wzshiming/xet"
	xetgo "github.com/wzshiming/xet-go"
	"github.com/wzshiming/xet/test/conformance/utils"
)

// chunkEntry holds the hash and size for a single chunk.
type chunkEntry struct {
	hash string
	size uint64
}

func TestConformance(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty file",
			data: []byte{},
		},
		{
			name: "Hello World",
			data: []byte("Hello World!"),
		},
		{
			name: "10MB",
			data: utils.MakeRandData(10 * 1024 * 1024),
		},
		{
			name: "10MB repeating",
			data: utils.MakeRepeatData(10 * 1024 * 1024),
		},
		{
			name: "100MB",
			data: utils.MakeRandData(100 * 1024 * 1024),
		},
		{
			name: "100MB repeating",
			data: utils.MakeRepeatData(100 * 1024 * 1024),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nativeChunks := getNativeChunks(t, tt.data)
			refChunks := getReferenceChunks(t, tt.data)

			t.Run("chunking", func(t *testing.T) {
				if len(nativeChunks) != len(refChunks) {
					t.Fatalf("chunk count mismatch: native=%d reference=%d",
						len(nativeChunks), len(refChunks))
				}
				for i := range nativeChunks {
					if nativeChunks[i].hash != refChunks[i].hash {
						t.Errorf("chunk[%d] hash mismatch: native=%s reference=%s",
							i, nativeChunks[i].hash, refChunks[i].hash)
					}
					if nativeChunks[i].size != refChunks[i].size {
						t.Errorf("chunk[%d] size mismatch: native=%d reference=%d",
							i, nativeChunks[i].size, refChunks[i].size)
					}
				}
			})

			if len(tt.data) == 0 {
				return
			}

			t.Run("file", func(t *testing.T) {
				nativeHash := getNativeFileHash(t, nativeChunks)
				refHash := getReferenceFileHash(t, tt.data)
				if nativeHash != refHash {
					t.Errorf("file hash mismatch: native=%s reference=%s",
						nativeHash, refHash)
				}
			})
		})
	}
}

// getNativeChunks splits data using the native Go implementation and returns
// the hash and size of each chunk.
func getNativeChunks(t *testing.T, data []byte) []chunkEntry {
	t.Helper()
	var chunks []chunkEntry
	err := xet.ChunkData(bytes.NewReader(data), func(_ int64, chunk xet.ChunkBytes) error {
		chunks = append(chunks, chunkEntry{hash: chunk.Hash().String(), size: chunk.Size()})
		return nil
	})
	if err != nil {
		t.Fatalf("native ChunkData: %v", err)
	}
	return chunks
}

// getReferenceChunks splits data using the xet-go (Rust) reference implementation
// and returns the hash and size of each chunk.
func getReferenceChunks(t *testing.T, data []byte) []chunkEntry {
	t.Helper()
	// xetgo.ChunkData does not accept empty input; empty data produces no chunks.
	if len(data) == 0 {
		return nil
	}
	raw, err := xetgo.ChunkData(data)
	if err != nil {
		t.Fatalf("reference ChunkData: %v", err)
	}
	chunks := make([]chunkEntry, len(raw))
	for i, c := range raw {
		chunks[i] = chunkEntry{hash: c.Hash, size: c.Size}
	}
	return chunks
}

// getNativeFileHash computes the file hash using the native Go implementation.
func getNativeFileHash(t *testing.T, chunks []chunkEntry) string {
	t.Helper()
	hashes := make([]xet.Hash, len(chunks))
	sizes := make([]uint64, len(chunks))
	for i, c := range chunks {
		h, err := xet.ParseHash(c.hash)
		if err != nil {
			t.Fatalf("parse hash: %v", err)
		}
		hashes[i] = h
		sizes[i] = c.size
	}
	return xet.ComputeFileHash(hashes, sizes).String()
}

// getReferenceFileHash computes the file hash using the xet-go (Rust) reference
// implementation by writing the data to a temporary file and calling HashFiles.
func getReferenceFileHash(t *testing.T, data []byte) string {
	t.Helper()
	f, err := os.CreateTemp("", "xet-conformance-*")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(data); err != nil {
		t.Fatalf("write temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close temp file: %v", err)
	}
	results, err := xetgo.HashFiles([]string{f.Name()})
	if err != nil {
		t.Fatalf("reference HashFiles: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("reference HashFiles returned no results")
	}
	return results[0].Hash
}
