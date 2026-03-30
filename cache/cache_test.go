package cache

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/wzshiming/xet"
)

func TestMemoryCache(t *testing.T) {
	cache := NewMemoryCache()
	testChunkCache(t, cache)
}

func TestFileCache(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewFileCache(tmpDir)
	if err != nil {
		t.Fatalf("failed to create file cache: %v", err)
	}
	testChunkCache(t, cache)
}

func TestFileCacheStructure(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewFileCache(tmpDir)
	if err != nil {
		t.Fatalf("failed to create file cache: %v", err)
	}

	// Create a test hash
	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Store chunk
	if err := cache.Put(hash, data); err != nil {
		t.Fatalf("failed to put chunk: %v", err)
	}

	// Verify directory structure
	hashStr := hash.String()
	expectedPath := filepath.Join(tmpDir, hashStr[:2], hashStr)

	if _, err := os.Stat(expectedPath); err != nil {
		t.Errorf("chunk file not found at expected path: %s", expectedPath)
	}

	// Verify content
	content, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("failed to read chunk file: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("chunk content mismatch: got %v, want %v", content, data)
	}
}

func TestFileCacheEmptyDir(t *testing.T) {
	_, err := NewFileCache("")
	if err == nil {
		t.Error("expected error for empty cache directory")
	}
}

func TestNoOpCache(t *testing.T) {
	cache := NewNoOpCache()
	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Put should succeed but do nothing
	if err := cache.Put(hash, data); err != nil {
		t.Errorf("NoOpCache.Put failed: %v", err)
	}

	// Get should return nil
	result, err := cache.Get(hash)
	if err != nil {
		t.Errorf("NoOpCache.Get failed: %v", err)
	}
	if result != nil {
		t.Errorf("NoOpCache.Get should return nil, got %v", result)
	}

	// Has should return false
	if cache.Has(hash) {
		t.Error("NoOpCache.Has should return false")
	}
}

func TestReadThroughCache(t *testing.T) {
	primary := NewMemoryCache()
	secondary := NewMemoryCache()
	cache := NewReadThroughCache(primary, secondary)

	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Put in secondary only
	if err := secondary.Put(hash, data); err != nil {
		t.Fatalf("failed to put in secondary: %v", err)
	}

	// Get should retrieve from secondary and populate primary
	result, err := cache.Get(hash)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch: got %v, want %v", result, data)
	}

	// Verify primary now has the data
	primaryData, err := primary.Get(hash)
	if err != nil {
		t.Fatalf("failed to get from primary: %v", err)
	}
	if !bytes.Equal(primaryData, data) {
		t.Errorf("primary cache not populated: got %v, want %v", primaryData, data)
	}
}

func TestReadThroughCachePut(t *testing.T) {
	primary := NewMemoryCache()
	secondary := NewMemoryCache()
	cache := NewReadThroughCache(primary, secondary)

	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Put should write to both
	if err := cache.Put(hash, data); err != nil {
		t.Fatalf("failed to put: %v", err)
	}

	// Verify both have the data
	primaryData, err := primary.Get(hash)
	if err != nil {
		t.Fatalf("failed to get from primary: %v", err)
	}
	if !bytes.Equal(primaryData, data) {
		t.Errorf("primary data mismatch: got %v, want %v", primaryData, data)
	}

	secondaryData, err := secondary.Get(hash)
	if err != nil {
		t.Fatalf("failed to get from secondary: %v", err)
	}
	if !bytes.Equal(secondaryData, data) {
		t.Errorf("secondary data mismatch: got %v, want %v", secondaryData, data)
	}
}

func TestCopyChunk(t *testing.T) {
	src := NewMemoryCache()
	dst := NewMemoryCache()

	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Put in source
	if err := src.Put(hash, data); err != nil {
		t.Fatalf("failed to put in source: %v", err)
	}

	// Copy to destination
	if err := CopyChunk(dst, src, hash); err != nil {
		t.Fatalf("failed to copy chunk: %v", err)
	}

	// Verify destination has the data
	result, err := dst.Get(hash)
	if err != nil {
		t.Fatalf("failed to get from destination: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch: got %v, want %v", result, data)
	}
}

func TestCopyFromReader(t *testing.T) {
	cache := NewMemoryCache()
	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")
	reader := bytes.NewReader(data)

	if err := CopyFromReader(cache, hash, reader); err != nil {
		t.Fatalf("failed to copy from reader: %v", err)
	}

	result, err := cache.Get(hash)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch: got %v, want %v", result, data)
	}
}

// testChunkCache runs common tests on any ChunkCache implementation
func testChunkCache(t *testing.T, cache ChunkCache) {
	hash := xet.Hash{0x12, 0x34, 0x56, 0x78}
	data := []byte("test data")

	// Initially should not exist
	if cache.Has(hash) {
		t.Error("cache should not have chunk initially")
	}

	result, err := cache.Get(hash)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if result != nil {
		t.Errorf("Get should return nil for non-existent chunk")
	}

	// Put chunk
	if err := cache.Put(hash, data); err != nil {
		t.Errorf("Put failed: %v", err)
	}

	// Should now exist
	if !cache.Has(hash) {
		t.Error("cache should have chunk after Put")
	}

	// Get should return the data
	result, err = cache.Get(hash)
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("Get returned wrong data: got %v, want %v", result, data)
	}

	// Test overwrite
	newData := []byte("new data")
	if err := cache.Put(hash, newData); err != nil {
		t.Errorf("Put (overwrite) failed: %v", err)
	}

	result, err = cache.Get(hash)
	if err != nil {
		t.Errorf("Get after overwrite failed: %v", err)
	}
	if !bytes.Equal(result, newData) {
		t.Errorf("Get after overwrite returned wrong data: got %v, want %v", result, newData)
	}
}
