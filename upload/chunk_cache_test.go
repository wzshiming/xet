package upload

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// testShard has two CAS blocks of nChunks chunks each; hashes are distinct
// per id.
func testShard(id byte, nChunks int) *shard.Shard {
	s := shard.NewShard()
	for b := range byte(2) {
		block := shard.CASBlock{CASHash: xet.XorbHash{0: id, 1: b}}
		for i := range nChunks {
			block.Chunks = append(block.Chunks, shard.CASChunkSequenceEntry{ChunkHash: xet.ChunkHash{0: id, 1: b, 2: byte(i)}, UnpackedSegBytes: 1})
		}
		s.CASInfos = append(s.CASInfos, block)
	}
	return s
}

func chunkHashes(s *shard.Shard) []xet.ChunkHash {
	var hashes []xet.ChunkHash
	for _, block := range s.CASInfos {
		for _, chunk := range block.Chunks {
			hashes = append(hashes, chunk.ChunkHash)
		}
	}
	return hashes
}

// testEndpoint scopes every test entry; tag names its directory under chunks/.
const testEndpoint = "http://cas.test/default"

var tag = endpointTag(testEndpoint)

func entryFile(dir string, h xet.ChunkHash) string {
	name := h.String()
	return filepath.Join(dir, "chunks", hex.EncodeToString(tag[:]), name[:2], name[2:4], name[4:])
}

func seed(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func touch(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// entries lists every file under <dir>/chunks at entry depth, across endpoints.
func entries(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "chunks", "*", "*", "*", "*"))
	if err != nil {
		t.Fatal(err)
	}
	return matches
}

// hits counts the chunks of s resolved in got and fails on a wrong location.
func hits(t *testing.T, got map[xet.ChunkHash]shard.ChunkLocation, s *shard.Shard) int {
	t.Helper()
	n := 0
	for _, block := range s.CASInfos {
		for i, chunk := range block.Chunks {
			loc, ok := got[chunk.ChunkHash]
			if !ok {
				continue
			}
			if want := (shard.ChunkLocation{XorbHash: block.CASHash, ChunkIndex: uint32(i)}); loc != want {
				t.Errorf("chunk %x = %+v, want %+v", chunk.ChunkHash[:3], loc, want)
			}
			n++
		}
	}
	return n
}

func TestChunkCacheStoreLookupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := NewCacheManager(dir, DefaultCacheSize)
	s := testShard(1, 3)
	if err := m.Store(testEndpoint, s.ChunkLocations()); err != nil {
		t.Fatal(err)
	}
	h := s.CASInfos[1].Chunks[2].ChunkHash
	entry, err := os.ReadFile(entryFile(dir, h))
	if err != nil {
		t.Fatal(err)
	}
	if want := append(slices.Clone(s.CASInfos[1].CASHash[:]), 2, 0, 0, 0); !bytes.Equal(entry, want) {
		t.Fatalf("entry = %x, want xorb hash and little-endian index %x", entry, want)
	}
	if got := entries(t, dir); len(got) != 6 {
		t.Fatalf("entries = %v; want one per chunk", got)
	}
	unknown := xet.ChunkHash{9}
	got := m.Lookup(t.Context(), testEndpoint, append(chunkHashes(s), unknown))
	if n := hits(t, got, s); n != 6 || len(got) != 6 {
		t.Fatalf("resolved %d chunks in %d results, want all 6 and nothing for the unknown hash", n, len(got))
	}
	if got := m.Lookup(t.Context(), testEndpoint, []xet.ChunkHash{h, h, h}); len(got) != 1 || got[h] != (shard.ChunkLocation{XorbHash: s.CASInfos[1].CASHash, ChunkIndex: 2}) {
		t.Fatalf("duplicate hashes resolved %v, want the one entry", got)
	}
	if got := m.Lookup(t.Context(), "http://other.test/default", chunkHashes(s)); len(got) != 0 {
		t.Fatalf("another endpoint resolved %d from this endpoint's entries", len(got))
	}
	if usage, err := m.Usage(t.Context()); err != nil || usage != (CacheUsage{Count: 6, Bytes: 6 * entrySize}) {
		t.Fatalf("usage = %+v, %v; want six %d-byte entries", usage, err, entrySize)
	}
}

func TestChunkCacheWrongSizeEntryRemovedOnLookup(t *testing.T) {
	dir := t.TempDir()
	h := xet.ChunkHash{1}
	path := entryFile(dir, h)
	seed(t, path, []byte("short"))
	if got := NewCacheManager(dir, 0).Lookup(t.Context(), testEndpoint, []xet.ChunkHash{h}); len(got) != 0 {
		t.Fatalf("wrong-size entry resolved %v", got)
	}
	for _, p := range []string{path, filepath.Dir(path), filepath.Dir(filepath.Dir(path))} {
		if _, err := os.Stat(p); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("%s after lookup: %v, want removed", p, err)
		}
	}
}

func TestChunkCacheNilReceiver(t *testing.T) {
	var m *CacheManager
	s := testShard(1, 1)
	if err := m.Store(testEndpoint, s.ChunkLocations()); err != nil {
		t.Fatal(err)
	}
	if got := m.Lookup(t.Context(), testEndpoint, chunkHashes(s)); got != nil {
		t.Fatalf("nil manager resolved %v", got)
	}
	if got := NewCacheManager(t.TempDir(), 0).Lookup(t.Context(), testEndpoint, nil); got != nil {
		t.Fatalf("no hashes resolved %v", got)
	}
}

func TestChunkCacheConcurrentStoreLookup(t *testing.T) {
	m := NewCacheManager(t.TempDir(), DefaultCacheSize)
	shared := testShard(0, 2)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 4 {
				s := testShard(byte(g*4+i+1), 2)
				for _, s := range []*shard.Shard{s, shared} {
					if err := m.Store(testEndpoint, s.ChunkLocations()); err != nil {
						t.Error(err)
						return
					}
					if n := hits(t, m.Lookup(t.Context(), testEndpoint, chunkHashes(s)), s); n != 4 {
						t.Errorf("goroutine %d resolved %d chunks, want 4", g, n)
					}
				}
			}
		})
	}
	wg.Wait()
}
