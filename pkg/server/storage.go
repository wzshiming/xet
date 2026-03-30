package server

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// Storage defines the interface for storing and retrieving XET data
type Storage interface {
	// StoreXorb stores an xorb by its hash
	StoreXorb(ctx context.Context, namespace string, xorbObj *xorb.Xorb) (bool, error)

	// GetXorb retrieves an xorb by its hash
	GetXorb(ctx context.Context, namespace string, xorbHash xet.Hash) (*xorb.Xorb, error)

	// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.Hash) (io.ReadSeekCloser, error)

	// StoreShard stores a shard
	StoreShard(ctx context.Context, shard *shard.Shard) (bool, error)

	// GetShardByFileHash retrieves a shard by file hash
	GetShardByFileHash(ctx context.Context, fileHash xet.Hash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
	GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.Hash) (*shard.Shard, error)

	// GetXorbURL generates a URL for accessing xorb data
	GetXorbURL(namespace string, xorbHash xet.Hash) string
}

// FileStorage implements Storage using the filesystem
type FileStorage struct {
	basePath   string
	baseURL    string
	mu         sync.RWMutex
	shardIndex map[xet.Hash]*shard.Shard          // file hash -> shard
	chunkIndex map[xet.Hash]map[xet.Hash]struct{} // chunk hash -> set of file hashes
}

// FileStorageOptions configures the file storage
type FileStorageOptions struct {
	BasePath string // Base directory for storage
	BaseURL  string // Base URL for serving xorb data
}

// NewFileStorage creates a new filesystem-based storage
func NewFileStorage(opts FileStorageOptions) (*FileStorage, error) {
	if opts.BasePath == "" {
		opts.BasePath = "./xet-data"
	}

	// Create directories
	dirs := []string{
		filepath.Join(opts.BasePath, "xorbs"),
		filepath.Join(opts.BasePath, "shards"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	fs := &FileStorage{
		basePath:   opts.BasePath,
		baseURL:    opts.BaseURL,
		shardIndex: make(map[xet.Hash]*shard.Shard),
		chunkIndex: make(map[xet.Hash]map[xet.Hash]struct{}),
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

		s, err := shard.Deserialize(f)
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
						fs.chunkIndex[chunk.ChunkHash] = make(map[xet.Hash]struct{})
					}
					fs.chunkIndex[chunk.ChunkHash][fileBlock.FileHash] = struct{}{}
				}
			}
		}
	}

	return nil
}

// StoreXorb stores an xorb
func (fs *FileStorage) StoreXorb(ctx context.Context, namespace string, xorbObj *xorb.Xorb) (bool, error) {
	xorbDir := filepath.Join(fs.basePath, "xorbs", namespace)
	if err := os.MkdirAll(xorbDir, 0755); err != nil {
		return false, fmt.Errorf("create xorb directory: %w", err)
	}

	xorbPath := filepath.Join(xorbDir, xorbObj.Hash.String())

	// Check if xorb already exists
	if _, err := os.Stat(xorbPath); err == nil {
		return false, nil // Already exists
	}

	// Serialize xorb to full format with footer for storage.
	// This ensures all clients can download the same format consistently.
	r, err := xorb.Serialize(xorbObj, false)
	if err != nil {
		return false, fmt.Errorf("serialize xorb: %w", err)
	}

	// Write xorb to disk using streaming
	f, err := os.OpenFile(xorbPath+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return false, fmt.Errorf("create xorb file: %w", err)
	}

	_, err = io.Copy(f, r)
	if err != nil {
		f.Close()
		os.Remove(xorbPath + ".tmp")
		return false, fmt.Errorf("write xorb: %w", err)
	}
	f.Close()

	// Atomically rename temp file to final path
	if err := os.Rename(xorbPath+".tmp", xorbPath); err != nil {
		os.Remove(xorbPath + ".tmp")
		return false, fmt.Errorf("finalize xorb file: %w", err)
	}

	return true, nil
}

// GetXorb retrieves an xorb
func (fs *FileStorage) GetXorb(ctx context.Context, namespace string, xorbHash xet.Hash) (*xorb.Xorb, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", namespace, xorbHash.String())

	// Open file for streaming deserialization
	f, err := os.Open(xorbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("xorb not found")
		}
		return nil, fmt.Errorf("open xorb file: %w", err)
	}
	defer f.Close()

	// Deserialize xorb from file using streaming.
	// The stored format always has the full XETBLOB footer (chunkOnly=false)
	// because StoreXorb normalizes all uploads to this format.
	xorbObj, err := xorb.Deserialize(f, false)
	if err != nil {
		return nil, fmt.Errorf("deserialize xorb: %w", err)
	}

	return xorbObj, nil
}

// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
func (fs *FileStorage) GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.Hash) (io.ReadSeekCloser, error) {
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

// StoreShard stores a shard
func (fs *FileStorage) StoreShard(ctx context.Context, s *shard.Shard) (bool, error) {
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

	// Ensure shard has a footer for storage
	if s.Footer == nil {
		s.Footer = &shard.Footer{
			Version:                1,
			FileInfoOffset:         0, // Will be set during serialization
			CASInfoOffset:          0, // Will be set during serialization
			FileLookupOffset:       0, // Will be set during serialization
			FileLookupNumEntries:   0,
			CASLookupOffset:        0, // Will be set during serialization
			CASLookupNumEntries:    0,
			ChunkLookupOffset:      0, // Will be set during serialization
			ChunkLookupNumEntries:  0,
			ShardCreationTimestamp: uint64(time.Now().Unix()),
			ShardKeyExpiry:         0,
			StoredBytesOnDisk:      0,
			MaterializedBytes:      0,
			StoredBytes:            0,
			FooterOffset:           0, // Will be set during serialization
		}
	}

	// Serialize shard with footer for storage
	r, err := s.SerializeWithFooter()
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
					fs.chunkIndex[chunk.ChunkHash] = make(map[xet.Hash]struct{})
				}
				fs.chunkIndex[chunk.ChunkHash][fileBlock.FileHash] = struct{}{}
			}
		}
	}

	return true, nil
}

// GetShardByFileHash retrieves a shard by file hash
func (fs *FileStorage) GetShardByFileHash(ctx context.Context, fileHash xet.Hash) (*shard.Shard, error) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()

	s, exists := fs.shardIndex[fileHash]
	if !exists {
		return nil, fmt.Errorf("shard not found")
	}

	return s, nil
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (fs *FileStorage) GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.Hash) (*shard.Shard, error) {
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
func (fs *FileStorage) GetXorbURL(namespace string, xorbHash xet.Hash) string {
	if fs.baseURL == "" {
		// If no base URL is configured, return a relative path
		return fmt.Sprintf("/v1/xorbs/%s/%s/data", namespace, xorbHash.String())
	}
	return fmt.Sprintf("%s/v1/xorbs/%s/%s/data", fs.baseURL, namespace, xorbHash.String())
}
