package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/golang/groupcache/lru"
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

	// GetShard retrieves a shard by file hash
	GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
	GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error)
}

// FileStorage implements Storage using the filesystem
type FileStorage struct {
	basePath   string
	baseURL    string
	mut        sync.RWMutex
	shardIndex *lru.Cache // bounded file hash -> shard cache
	chunkIndex *lru.Cache // bounded chunk hash -> file hash cache
}

const defaultShardCacheSize = 1024
const defaultChunkCacheSize = 4096

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

// WithShardCacheSize sets the maximum number of file-hash entries retained in
// the in-memory shard cache. Values less than one disable shard caching.
func WithShardCacheSize(size int) Option {
	return func(fs *FileStorage) {
		fs.shardIndex = lru.New(size)
	}
}

// WithChunkCacheSize sets the maximum number of chunk-hash entries retained
// in memory. Values less than one disable chunk-index caching.
func WithChunkCacheSize(size int) Option {
	return func(fs *FileStorage) {
		fs.chunkIndex = lru.New(size)
	}
}

// NewFileStorage creates a new filesystem-based storage
func NewFileStorage(opts ...Option) (*FileStorage, error) {
	fs := &FileStorage{
		basePath:   "./xet",
		baseURL:    "",
		shardIndex: lru.New(defaultShardCacheSize),
		chunkIndex: lru.New(defaultChunkCacheSize),
	}

	for _, opt := range opts {
		opt(fs)
	}

	// Create directories
	dirs := []string{
		filepath.Join(fs.basePath, "xorbs"),
		filepath.Join(fs.basePath, "shards"),
		filepath.Join(fs.basePath, "chunks"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	return fs, nil
}

// hasShard checks if a shard exists for the given file hash
func (fs *FileStorage) hasShard(fileHash xet.FileHash) (bool, error) {
	if _, exists := fs.shardIndex.Get(fileHash); exists {
		return true, nil
	}

	shardPath := filepath.Join(fs.basePath, "shards", fileHash.String())
	if _, err := os.Stat(shardPath); err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	} else {
		return false, fmt.Errorf("check shard: %w", err)
	}
}

// GetShard resolves fileHash with fixed paths only.
func (fs *FileStorage) getShard(fileHash xet.FileHash) (*shard.Shard, error) {
	if value, exists := fs.shardIndex.Get(fileHash); exists {
		return value.(*shard.Shard), nil
	}

	shardPath := filepath.Join(fs.basePath, "shards", fileHash.String())
	f, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := shard.NewShard()
	if err := s.Decode(f, true); err != nil {
		return nil, err
	}

	for _, fileBlock := range s.Files {
		fs.shardIndex.Add(fileBlock.FileHash, s)
	}
	return s, nil
}

// PutXorb stores an xorb
func (fs *FileStorage) PutXorb(ctx context.Context, _ string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", xorbHash.String())

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
func (fs *FileStorage) GetXorbReadSeekCloser(ctx context.Context, _ string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", xorbHash.String())

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
func (fs *FileStorage) HasXorb(ctx context.Context, _ string, xorbHash xet.XorbHash) (bool, error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", xorbHash.String())

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

	fs.mut.Lock()
	defer fs.mut.Unlock()

	// Check if any file in the shard already exists
	alreadyExists := false
	for _, fileBlock := range s.Files {
		if exists, err := fs.hasShard(fileBlock.FileHash); err == nil {
			if exists {
				alreadyExists = true
				break
			}
		} else if !os.IsNotExist(err) {
			return false, fmt.Errorf("check shard: %w", err)
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
	shardFileHash := []byte(s.Files[0].FileHash.String())

	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			chunkPath := filepath.Join(fs.basePath, "chunks", chunk.ChunkHash.String())
			err := writeIndexFile(chunkPath, shardFileHash)
			if err != nil {
				return true, fmt.Errorf("write chunk index: %w", err)
			}
		}
	}

	return true, nil
}

// GetShard retrieves a shard by file hash
func (fs *FileStorage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	fs.mut.RLock()
	defer fs.mut.RUnlock()

	return fs.getShard(fileHash)
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (fs *FileStorage) GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	fs.mut.RLock()
	value, exists := fs.chunkIndex.Get(chunkHash)
	fs.mut.RUnlock()
	if exists {
		return fs.GetShard(ctx, value.(xet.FileHash))
	}

	chunkPath := filepath.Join(fs.basePath, "chunks", chunkHash.String())
	b, err := os.ReadFile(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk not found")
		}
		return nil, fmt.Errorf("read chunk index: %w", err)
	}
	fileHash, err := xet.ParseFileHash(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("invalid chunk index: %w", err)
	}
	fs.mut.RLock()
	fs.chunkIndex.Add(chunkHash, fileHash)
	fs.mut.RUnlock()
	return fs.GetShard(ctx, fileHash)
}

func writeIndexFile(path string, value []byte) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, value, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
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
func (fs *FileStorage) GetXorbDataRange(ctx context.Context, _ string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	xorbPath := filepath.Join(fs.basePath, "xorbs", xorbHash.String())
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
