package download

import (
	"os"
	"path/filepath"
	"testing"
)

func TestChunkCachePutGet(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	xorbHash := "abc123"
	data := []byte("hello world")

	// Miss before Put
	if _, ok := cache.Get(xorbHash, 0); ok {
		t.Fatal("expected miss before put")
	}

	// Put and Get
	if err := cache.Put(xorbHash, 0, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok := cache.Get(xorbHash, 0)
	if !ok {
		t.Fatal("expected hit after put")
	}
	if string(got) != string(data) {
		t.Fatalf("got %q, want %q", got, data)
	}

	// Different chunk index is a miss
	if _, ok := cache.Get(xorbHash, 1); ok {
		t.Fatal("expected miss for different chunk index")
	}

	// Different xorb hash is a miss
	if _, ok := cache.Get("other", 0); ok {
		t.Fatal("expected miss for different xorb hash")
	}
}

func TestChunkCacheHasRange(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	xorbHash := "rangetest"

	// Empty range is always true
	if !cache.HasRange(xorbHash, 5, 5) {
		t.Fatal("empty range should return true")
	}

	// Missing chunks
	if cache.HasRange(xorbHash, 0, 3) {
		t.Fatal("expected false for missing range")
	}

	// Populate partial range
	for i := uint32(0); i < 2; i++ {
		if err := cache.Put(xorbHash, i, []byte{byte(i)}); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}
	if cache.HasRange(xorbHash, 0, 3) {
		t.Fatal("expected false for partially cached range")
	}

	// Complete the range
	if err := cache.Put(xorbHash, 2, []byte{2}); err != nil {
		t.Fatalf("Put(2): %v", err)
	}
	if !cache.HasRange(xorbHash, 0, 3) {
		t.Fatal("expected true for fully cached range")
	}
}

func TestChunkCachePersistence(t *testing.T) {
	dir := t.TempDir()

	// Write with one cache instance
	cache1, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}
	if err := cache1.Put("xorb1", 0, []byte("data0")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := cache1.Put("xorb1", 1, []byte("data1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Read with a new cache instance (simulates process restart)
	cache2, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}
	for i, want := range []string{"data0", "data1"} {
		got, ok := cache2.Get("xorb1", uint32(i))
		if !ok {
			t.Fatalf("chunk %d: expected hit after restart", i)
		}
		if string(got) != want {
			t.Fatalf("chunk %d: got %q, want %q", i, got, want)
		}
	}
}

func TestChunkCacheAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	// Write a chunk
	if err := cache.Put("atomic", 0, []byte("first")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Overwrite with different data
	if err := cache.Put("atomic", 0, []byte("second")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := cache.Get("atomic", 0)
	if !ok {
		t.Fatal("expected hit")
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}

	// Verify no temp files are left behind
	entries, err := os.ReadDir(filepath.Join(dir, "atomic"))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if len(name) > 0 && name[0] == '.' {
			t.Fatalf("temp file left behind: %s", name)
		}
	}
}

func TestChunkCachePersistentReadIntegration(t *testing.T) {
	dir := t.TempDir()
	cache, err := NewChunkCache(dir)
	if err != nil {
		t.Fatalf("NewChunkCache: %v", err)
	}

	xorbHash := "xorb42"
	chunkStart := uint32(10)
	numChunks := 5

	// Populate cache with chunks [10, 15)
	for i := uint32(0); i < uint32(numChunks); i++ {
		data := []byte{byte(i + 100)}
		if err := cache.Put(xorbHash, chunkStart+i, data); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Create a chunkCache backed by the persistent cache (as the prefetcher would)
	cc := &chunkCache{
		done: true,
		persistentRead: func(idx uint32) ([]byte, error) {
			data, ok := cache.Get(xorbHash, chunkStart+idx)
			if !ok {
				t.Fatalf("unexpected miss for chunk %d", chunkStart+idx)
			}
			return data, nil
		},
	}

	// Verify chunks can be read via local indices [0, 5)
	for i := uint32(0); i < uint32(numChunks); i++ {
		got, err := cc.Chunk(i)
		if err != nil {
			t.Fatalf("Chunk(%d): %v", i, err)
		}
		want := []byte{byte(i + 100)}
		if len(got) != 1 || got[0] != want[0] {
			t.Fatalf("Chunk(%d) = %v, want %v", i, got, want)
		}
	}

	// Done() should be a no-op since done is already true
	cc.Done()
}
