package download

import (
	"fmt"
	"os"
	"path/filepath"
)

// ChunkCache is a persistent, disk-backed chunk cache keyed by
// (xorbHash, absoluteChunkIndex). It stores decoded chunk data as
// individual files under a directory hierarchy. The cache is safe for
// concurrent use across goroutines and processes. There is no eviction
// policy (no LRU, no maximum capacity).
//
// The life cycle of the cache belongs to the client, not to a single
// reader or download call. The same cache should be reused for any
// download entry and is recoverable after a process restart.
type ChunkCache struct {
	dir string
}

// NewChunkCache creates a new ChunkCache rooted at dir.
// The directory is created if it does not exist.
func NewChunkCache(dir string) (*ChunkCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk cache dir: %w", err)
	}
	return &ChunkCache{dir: dir}, nil
}

func (c *ChunkCache) chunkPath(xorbHash string, chunkIdx uint32) string {
	return filepath.Join(c.dir, xorbHash, fmt.Sprintf("%d", chunkIdx))
}

// Get retrieves the decoded chunk data for the given xorb and chunk index.
// Returns nil, false if the chunk is not cached.
func (c *ChunkCache) Get(xorbHash string, chunkIdx uint32) ([]byte, bool) {
	data, err := os.ReadFile(c.chunkPath(xorbHash, chunkIdx))
	if err != nil {
		return nil, false
	}
	return data, true
}

// Put stores decoded chunk data in the cache. It writes atomically
// (write to temp file, then rename) to avoid partial reads.
func (c *ChunkCache) Put(xorbHash string, chunkIdx uint32, data []byte) error {
	path := c.chunkPath(xorbHash, chunkIdx)
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".chunk-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	_, writeErr := tmp.Write(data)
	closeErr := tmp.Close()
	if writeErr != nil {
		os.Remove(tmpName)
		return writeErr
	}
	if closeErr != nil {
		os.Remove(tmpName)
		return closeErr
	}

	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// HasRange checks if all chunks in [chunkStart, chunkEnd) are cached.
func (c *ChunkCache) HasRange(xorbHash string, chunkStart, chunkEnd uint32) bool {
	for i := chunkStart; i < chunkEnd; i++ {
		if _, err := os.Stat(c.chunkPath(xorbHash, i)); err != nil {
			return false
		}
	}
	return true
}
