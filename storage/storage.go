package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// Storage defines the interface for storing and retrieving XET data
type Storage interface {
	// PutXorb stores an xorb by its hash
	PutXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash, r io.Reader) (bool, error)

	// GetXorbURL generates a URL for accessing xorb data
	GetXorbURL(namespace string, xorbHash xet.XorbHash) string

	// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error)

	// HasXorb checks whether an xorb exists.
	HasXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) (bool, error)

	// GetXorbDataRange returns the byte range within the stored xorb for the given chunk range
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)

	// PutShard stores a shard by its hash
	PutShard(ctx context.Context, shard *shard.Shard) (bool, error)

	// GetShardByFileHash retrieves a shard by file hash
	GetShardByFileHash(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
	GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error)
}

// FileStorage implements Storage using the filesystem
type FileStorage struct {
	basePath   string
	baseURL    string
	mu         sync.RWMutex
	shardIndex map[xet.FileHash]*shard.Shard               // file hash -> shard
	chunkIndex map[xet.ChunkHash]map[xet.FileHash]struct{} // chunk hash -> set of file hashes
}

type Option func(*FileStorage)

func WithBasePath(basePath string) Option {
	return func(fs *FileStorage) {
		fs.basePath = basePath
	}
}

func WithBaseURL(baseURL string) Option {
	return func(fs *FileStorage) {
		fs.baseURL = baseURL
	}
}

// NewFileStorage creates a new filesystem-based storage
func NewFileStorage(opts ...Option) (*FileStorage, error) {
	fs := &FileStorage{
		basePath:   "./xet",
		baseURL:    "",
		shardIndex: make(map[xet.FileHash]*shard.Shard),
		chunkIndex: make(map[xet.ChunkHash]map[xet.FileHash]struct{}),
	}

	for _, opt := range opts {
		opt(fs)
	}

	// Create directories
	dirs := []string{
		filepath.Join(fs.basePath, "xorbs"),
		filepath.Join(fs.basePath, "shards"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	// Load existing shards into memory for fast lookup
	if err := fs.loadShards(); err != nil {
		return nil, fmt.Errorf("load shards: %w", err)
	}

	return fs, nil
}

// loadShards loads all shards from disk into memory
func (fs *FileStorage) loadShards() error {
	shardsDir := filepath.Join(fs.basePath, "shards")
	entries, err := os.ReadDir(shardsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		f, err := os.Open(filepath.Join(shardsDir, entry.Name()))
		if err != nil {
			continue // Skip files we can't read
		}

		s := shard.NewShard()
		err = s.Decode(f, true)
		f.Close()
		if err != nil {
			continue // Skip invalid shards
		}

		// Index by file hash
		for _, fileBlock := range s.Files {
			fs.shardIndex[fileBlock.FileHash] = s

			// Index chunks for deduplication
			for _, casBlock := range s.CASInfos {
				for _, chunk := range casBlock.Chunks {
					if fs.chunkIndex[chunk.ChunkHash] == nil {
						fs.chunkIndex[chunk.ChunkHash] = make(map[xet.FileHash]struct{})
					}
					fs.chunkIndex[chunk.ChunkHash][fileBlock.FileHash] = struct{}{}
				}
			}
		}
	}

	return nil
}

// PutXorb stores an xorb
func (fs *FileStorage) PutXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	xorbDir := filepath.Join(fs.basePath, "xorbs", namespace)
	if err := os.MkdirAll(xorbDir, 0755); err != nil {
		return false, fmt.Errorf("create xorb directory: %w", err)
	}

	xorbPath := filepath.Join(xorbDir, xorbHash.String())

	// Check if xorb already exists
	if _, err := os.Stat(xorbPath); err == nil {
		return false, nil // Already exists
	}
	// Write xorb to disk using streaming
	f, err := os.OpenFile(xorbPath+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false, fmt.Errorf("create xorb file: %w", err)
	}

	err = xorb.Validate(io.TeeReader(r, f), xorbHash) // Validate xorb format before storing
	if err != nil {
		f.Close()
		os.Remove(xorbPath + ".tmp")
		return false, fmt.Errorf("validate xorb: %w", err)
	}
	f.Close()

	// Atomically rename temp file to final path
	if err := os.Rename(xorbPath+".tmp", xorbPath); err != nil {
		os.Remove(xorbPath + ".tmp")
		return false, fmt.Errorf("finalize xorb file: %w", err)
	}

	return true, nil
}

// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
func (fs *FileStorage) GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", namespace, xorbHash.String())

	f, err := os.Open(xorbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("xorb not found")
		}
		return nil, fmt.Errorf("open xorb file: %w", err)
	}

	return f, nil
}

// HasXorb checks whether an xorb exists.
func (fs *FileStorage) HasXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) (bool, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", namespace, xorbHash.String())

	_, err := os.Stat(xorbPath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, fmt.Errorf("check xorb file: %w", err)
}

// PutShard stores a shard
func (fs *FileStorage) PutShard(ctx context.Context, s *shard.Shard) (bool, error) {
	// Generate a unique filename (use first file hash)
	if len(s.Files) == 0 {
		return false, fmt.Errorf("shard has no file blocks")
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check if any file in the shard already exists
	alreadyExists := false
	for _, fileBlock := range s.Files {
		if _, exists := fs.shardIndex[fileBlock.FileHash]; exists {
			alreadyExists = true
			break
		}
	}

	if alreadyExists {
		return false, nil // Already exists
	}

	// Serialize shard with footer for storage
	r, err := s.Encode(true)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}

	shardPath := filepath.Join(fs.basePath, "shards", s.Files[0].FileHash.String())
	if _, err := os.Stat(shardPath); err == nil {
		return false, nil
	}

	f, err := os.OpenFile(shardPath+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false, fmt.Errorf("create shard file: %w", err)
	}

	_, err = io.Copy(f, r)
	if err != nil {
		f.Close()
		return false, fmt.Errorf("write shard to disk: %w", err)
	}
	f.Close()

	err = os.Rename(shardPath+".tmp", shardPath)
	if err != nil {
		return false, fmt.Errorf("finalize shard file: %w", err)
	}

	// Update in-memory indexes
	for _, fileBlock := range s.Files {
		fs.shardIndex[fileBlock.FileHash] = s

		// Index chunks for deduplication
		for _, casBlock := range s.CASInfos {
			for _, chunk := range casBlock.Chunks {
				if fs.chunkIndex[chunk.ChunkHash] == nil {
					fs.chunkIndex[chunk.ChunkHash] = make(map[xet.FileHash]struct{})
				}
				fs.chunkIndex[chunk.ChunkHash][fileBlock.FileHash] = struct{}{}
			}
		}
	}

	return true, nil
}

// GetShardByFileHash retrieves a shard by file hash
func (fs *FileStorage) GetShardByFileHash(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	s, exists := fs.shardIndex[fileHash]
	if !exists {
		return nil, fmt.Errorf("shard not found")
	}

	return s, nil
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (fs *FileStorage) GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	fileHashes, exists := fs.chunkIndex[chunkHash]
	if !exists || len(fileHashes) == 0 {
		return nil, fmt.Errorf("chunk not found")
	}

	// Return the shard for the first file containing this chunk
	for fileHash := range fileHashes {
		if s, ok := fs.shardIndex[fileHash]; ok {
			return s, nil
		}
	}

	return nil, fmt.Errorf("shard not found")
}

// GetXorbURL generates a URL for accessing xorb data
func (fs *FileStorage) GetXorbURL(namespace string, xorbHash xet.XorbHash) string {
	if fs.baseURL == "" {
		// If no base URL is configured, return a relative path
		return fmt.Sprintf("/v1/xorbs/%s/%s", namespace, xorbHash.String())
	}
	return fmt.Sprintf("%s/v1/xorbs/%s/%s", fs.baseURL, namespace, xorbHash.String())
}

// GetXorbDataRange returns the [start, end] byte range (inclusive) within
// the stored xorb binary for the given chunk range [chunkStart, chunkEnd).
// The returned range includes the 8-byte chunk header for each chunk, so that
// xet-core can parse the header (version, compressed/uncompressed size,
// compression type) when it downloads that byte range.
func (fs *FileStorage) GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", namespace, xorbHash.String())
	f, err := os.Open(xorbPath)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open xorb file: %w", err)
	}
	defer f.Close()

	startByte, endByte, err = xorb.ChunkDataRange(f, chunkStart, chunkEnd)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get chunk data range: %w", err)
	}
	return startByte, endByte, nil
}
