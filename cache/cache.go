package cache

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/wzshiming/xet"
)

// ChunkCache defines the interface for chunk caching implementations
type ChunkCache interface {
	// Get retrieves a chunk by its hash, returns nil if not found
	Get(hash xet.Hash) ([]byte, error)
	// Put stores a chunk with its hash
	Put(hash xet.Hash, data []byte) error
	// Has checks if a chunk exists in the cache
	Has(hash xet.Hash) bool
}

// MemoryCache implements an in-memory chunk cache
type MemoryCache struct {
	cache map[xet.Hash][]byte
}

// NewMemoryCache creates a new in-memory cache
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		cache: make(map[xet.Hash][]byte),
	}
}

func (m *MemoryCache) Get(hash xet.Hash) ([]byte, error) {
	if data, ok := m.cache[hash]; ok {
		return data, nil
	}
	return nil, nil
}

func (m *MemoryCache) Put(hash xet.Hash, data []byte) error {
	m.cache[hash] = data
	return nil
}

func (m *MemoryCache) Has(hash xet.Hash) bool {
	_, ok := m.cache[hash]
	return ok
}

// FileCache implements a persistent file-based chunk cache
// compatible with xet-core's cache directory structure
type FileCache struct {
	baseDir string
}

// NewFileCache creates a new file-based cache
// The cache directory structure follows xet-core's convention:
// <baseDir>/<first2chars>/<hash>
func NewFileCache(baseDir string) (*FileCache, error) {
	if baseDir == "" {
		return nil, fmt.Errorf("cache directory cannot be empty")
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	return &FileCache{
		baseDir: baseDir,
	}, nil
}

// getPath returns the file path for a given chunk hash
// Following xet-core's convention: <baseDir>/<first2chars>/<hash>
func (f *FileCache) getPath(hash xet.Hash) string {
	hashStr := hash.String()
	if len(hashStr) < 2 {
		return filepath.Join(f.baseDir, hashStr)
	}
	return filepath.Join(f.baseDir, hashStr[:2], hashStr)
}

func (f *FileCache) Get(hash xet.Hash) ([]byte, error) {
	path := f.getPath(hash)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read chunk from cache: %w", err)
	}
	return data, nil
}

func (f *FileCache) Put(hash xet.Hash, data []byte) error {
	path := f.getPath(hash)

	// Create parent directory if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create cache directory: %w", err)
	}

	// Write to a temporary file first, then rename for atomicity
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk to cache: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath) // Clean up temp file
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

func (f *FileCache) Has(hash xet.Hash) bool {
	path := f.getPath(hash)
	_, err := os.Stat(path)
	return err == nil
}

// NoOpCache is a cache implementation that does nothing (no caching)
type NoOpCache struct{}

func NewNoOpCache() *NoOpCache {
	return &NoOpCache{}
}

func (n *NoOpCache) Get(hash xet.Hash) ([]byte, error) {
	return nil, nil
}

func (n *NoOpCache) Put(hash xet.Hash, data []byte) error {
	return nil
}

func (n *NoOpCache) Has(hash xet.Hash) bool {
	return false
}

// ReadThroughCache wraps a ChunkCache and provides read-through functionality
type ReadThroughCache struct {
	primary   ChunkCache
	secondary ChunkCache
}

// NewReadThroughCache creates a cache that reads from secondary if primary misses
func NewReadThroughCache(primary, secondary ChunkCache) *ReadThroughCache {
	return &ReadThroughCache{
		primary:   primary,
		secondary: secondary,
	}
}

func (r *ReadThroughCache) Get(hash xet.Hash) ([]byte, error) {
	// Try primary first
	data, err := r.primary.Get(hash)
	if err != nil {
		return nil, err
	}
	if data != nil {
		return data, nil
	}

	// Try secondary
	data, err = r.secondary.Get(hash)
	if err != nil {
		return nil, err
	}
	if data != nil {
		// Populate primary cache
		_ = r.primary.Put(hash, data)
		return data, nil
	}

	return nil, nil
}

func (r *ReadThroughCache) Put(hash xet.Hash, data []byte) error {
	// Write to both caches
	if err := r.primary.Put(hash, data); err != nil {
		return err
	}
	return r.secondary.Put(hash, data)
}

func (r *ReadThroughCache) Has(hash xet.Hash) bool {
	return r.primary.Has(hash) || r.secondary.Has(hash)
}

// CopyChunk copies a chunk from one cache to another
func CopyChunk(dst, src ChunkCache, hash xet.Hash) error {
	data, err := src.Get(hash)
	if err != nil {
		return err
	}
	if data == nil {
		return fmt.Errorf("chunk not found in source cache")
	}
	return dst.Put(hash, data)
}

// CopyFromReader reads data from a reader and stores it in the cache
func CopyFromReader(cache ChunkCache, hash xet.Hash, reader io.Reader) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	return cache.Put(hash, data)
}
