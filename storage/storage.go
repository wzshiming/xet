package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	// GetReconstructedFile returns a ReadSeekCloser for a file reconstructed from a shard by its SHA-256 digest.
	GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error)
}

// FileStorage implements Storage using the filesystem
type FileStorage struct {
	basePath    string
	baseURL     string
	fileIndex   *lru.Cache // bounded file hash -> shard hash
	shardIndex  *lru.Cache // bounded shard hash -> shard cache
	chunkIndex  *lru.Cache // bounded chunk hash -> shard hash
	sha256Index *lru.Cache // bounded SHA-256 -> file hash
	xorbIndex   *lru.Cache // bounded xorb hash -> *xorbFile

	fileMut   sync.Mutex // guards fileIndex
	shardMut  sync.Mutex // guards shardIndex
	chunkMut  sync.Mutex // guards chunkIndex
	sha256Mut sync.Mutex // guards sha256Index
	xorbMut   sync.Mutex // guards xorbIndex
}

// xorbFile wraps an open xorb handle with its own mutex so that only uses of
// the same xorb are serialized while different xorbs can be read in parallel.
type xorbFile struct {
	mut    sync.Mutex
	f      *os.File
	closed bool
}

const defaultFileCacheSize = 4096
const defaultShardCacheSize = 512
const defaultChunkCacheSize = 4096
const defaultSHA256CacheSize = 4096
const defaultXorbCacheSize = 512

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

// WithFileCacheSize sets the maximum number of file-to-shard mappings retained
// in memory. Values less than one disable file-index caching.
func WithFileCacheSize(size int) Option {
	return func(fs *FileStorage) {
		fs.fileIndex = lru.New(size)
	}
}

// WithShardCacheSize sets the maximum number of shards retained in memory.
// Values less than one disable shard caching.
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

// WithSHA256CacheSize sets the maximum number of SHA-256 entries retained in
// memory. Values less than one disable SHA-256 index caching.
func WithSHA256CacheSize(size int) Option {
	return func(fs *FileStorage) {
		fs.sha256Index = lru.New(size)
	}
}

// WithXorbCacheSize sets the maximum number of concurrently open xorb file
// handles retained in memory while computing shard SHA-256 digests. Values
// less than one disable xorb handle caching.
func WithXorbCacheSize(size int) Option {
	return func(fs *FileStorage) {
		fs.xorbIndex = lru.New(size)
	}
}

// NewFileStorage creates a new filesystem-based storage
func NewFileStorage(opts ...Option) (*FileStorage, error) {
	fs := &FileStorage{
		basePath:    "./xet",
		baseURL:     "",
		fileIndex:   lru.New(defaultFileCacheSize),
		shardIndex:  lru.New(defaultShardCacheSize),
		chunkIndex:  lru.New(defaultChunkCacheSize),
		sha256Index: lru.New(defaultSHA256CacheSize),
		xorbIndex:   lru.New(defaultXorbCacheSize),
	}

	for _, opt := range opts {
		opt(fs)
	}

	// Close evicted xorb handles when the LRU cache drops them. Blocking on
	// the handle's own lock ensures we never close a file while another
	// goroutine is reading through it.
	fs.xorbIndex.OnEvicted = func(_ lru.Key, value interface{}) {
		xf := value.(*xorbFile)
		xf.mut.Lock()
		defer xf.mut.Unlock()
		_ = xf.f.Close()
		xf.closed = true
	}

	// Create directories
	dirs := []string{
		filepath.Join(fs.basePath, "xorbs"),
		filepath.Join(fs.basePath, "shards"),
		filepath.Join(fs.basePath, "files"),
		filepath.Join(fs.basePath, "chunks"),
		filepath.Join(fs.basePath, "sha256"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	return fs, nil
}

// hasFile checks whether a file hash already has a shard mapping.
func (fs *FileStorage) hasFile(fileHash xet.FileHash) (bool, error) {
	fs.fileMut.Lock()
	_, exists := fs.fileIndex.Get(fileHash)
	fs.fileMut.Unlock()
	if exists {
		return true, nil
	}

	filePath := filepath.Join(fs.basePath, "files", fileHash.String())
	if _, err := os.Stat(filePath); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check file index: %w", err)
	}
	return false, nil
}

// getShard resolves a file hash through files/<file-hash>, whose contents are
// the hash of the serialized shard stored at shards/<shard-hash>.
func (fs *FileStorage) getShard(fileHash xet.FileHash) (*shard.Shard, error) {
	fs.fileMut.Lock()
	value, exists := fs.fileIndex.Get(fileHash)
	fs.fileMut.Unlock()
	if exists {
		return fs.getShardByHash(value.(string))
	}

	indexPath := filepath.Join(fs.basePath, "files", fileHash.String())
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("read file index: %w", err)
	}
	shardHash := strings.TrimSpace(string(indexData))
	fs.fileMut.Lock()
	fs.fileIndex.Add(fileHash, shardHash)
	fs.fileMut.Unlock()
	return fs.getShardByHash(shardHash)
}

func (fs *FileStorage) getShardByHash(shardHash string) (*shard.Shard, error) {
	fs.shardMut.Lock()
	value, exists := fs.shardIndex.Get(shardHash)
	fs.shardMut.Unlock()
	if exists {
		return value.(*shard.Shard), nil
	}

	shardPath := filepath.Join(fs.basePath, "shards", shardHash)
	f, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s := shard.NewShard()
	if err := s.Decode(f, true); err != nil {
		return nil, err
	}

	fs.shardMut.Lock()
	fs.shardIndex.Add(shardHash, s)
	fs.shardMut.Unlock()
	for _, fileBlock := range s.Files {
		fs.fileMut.Lock()
		fs.fileIndex.Add(fileBlock.FileHash, shardHash)
		fs.fileMut.Unlock()
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
	// Use a unique temporary file so concurrent uploads of the same xorb cannot
	// truncate or corrupt one another.
	f, err := os.CreateTemp(filepath.Dir(xorbPath), filepath.Base(xorbPath)+".tmp-")
	if err != nil {
		return false, fmt.Errorf("create xorb file: %w", err)
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if err := f.Chmod(0o644); err != nil {
		_ = f.Close()
		return false, fmt.Errorf("set xorb file permissions: %w", err)
	}

	err = xorb.Validate(io.TeeReader(r, f), xorbHash) // Validate xorb format before storing
	if err != nil {
		f.Close()
		return false, fmt.Errorf("validate xorb: %w", err)
	}
	if err := f.Close(); err != nil {
		return false, fmt.Errorf("close xorb file: %w", err)
	}

	// Atomically rename temp file to final path
	fs.mut.Lock()
	defer fs.mut.Unlock()
	if _, err := os.Stat(xorbPath); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check xorb before finalize: %w", err)
	}
	if err := os.Rename(tmpPath, xorbPath); err != nil {
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
	fs.xorbMut.Lock()
	_, ok := fs.xorbIndex.Get(xorbHash)
	fs.xorbMut.Unlock()
	if ok {
		return true, nil
	}

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
	if len(s.Files) == 0 {
		return false, fmt.Errorf("shard has no file blocks")
	}

	// Check if any file in the shard already exists
	alreadyExists := false
	for _, fileBlock := range s.Files {
		if exists, err := fs.hasFile(fileBlock.FileHash); err == nil {
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

	for i := range s.Files {
		computed, err := fs.computeFileSHA256(ctx, &s.Files[i])
		if err != nil {
			return false, fmt.Errorf("compute SHA-256 for file %s: %w", s.Files[i].FileHash.String(), err)
		}
		computedHash := shard.NewSHA256Hash(computed)
		if s.Files[i].MetadataExt != nil &&
			s.Files[i].MetadataExt.SHA256Hash != computedHash &&
			!isXETCoreEmptyFile(&s.Files[i]) {
			return false, fmt.Errorf("SHA-256 mismatch for file %s", s.Files[i].FileHash.String())
		}

		s.Files[i].MetadataExt = &shard.FileMetadataExt{SHA256Hash: computedHash}
		s.Files[i].Flags |= shard.FileWithMetadataExt
	}

	// Serialize the shard first, then hash those exact persisted bytes. The
	// shard object is addressed by this hash rather than by any contained file.
	r, err := s.Encode(true)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}

	shardsDir := filepath.Join(fs.basePath, "shards")
	f, err := os.CreateTemp(shardsDir, ".shard-*")
	if err != nil {
		return false, fmt.Errorf("create shard file: %w", err)
	}
	tmpPath := f.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpPath)
		}
	}()

	_, err = io.Copy(f, r)
	if err != nil {
		f.Close()
		return false, fmt.Errorf("write shard to disk: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return false, fmt.Errorf("rewind shard file: %w", err)
	}
	shardHash, err := computeShardHashFromReader(f)
	if err != nil {
		f.Close()
		return false, fmt.Errorf("hash shard file: %w", err)
	}
	if err := f.Chmod(0644); err != nil {
		f.Close()
		return false, fmt.Errorf("set shard file permissions: %w", err)
	}
	if err := f.Close(); err != nil {
		return false, fmt.Errorf("close shard file: %w", err)
	}

	shardPath := filepath.Join(shardsDir, shardHash)
	wasInserted := true
	if _, err := os.Stat(shardPath); err == nil {
		wasInserted = false
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check shard file: %w", err)
	} else if err := os.Rename(tmpPath, shardPath); err != nil {
		return false, fmt.Errorf("finalize shard file: %w", err)
	} else {
		removeTemp = false
	}

	shardHashData := []byte(shardHash)
	fs.shardMut.Lock()
	fs.shardIndex.Add(shardHash, s)
	fs.shardMut.Unlock()
	for _, file := range s.Files {
		fs.fileMut.Lock()
		fs.fileIndex.Add(file.FileHash, shardHash)
		fs.fileMut.Unlock()
		indexPath := filepath.Join(fs.basePath, "files", file.FileHash.String())
		if err := writeIndexFile(indexPath, shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
		}
	}

	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			chunkPath := filepath.Join(fs.basePath, "chunks", chunk.ChunkHash.String())
			err := writeIndexFile(chunkPath, shardHashData)
			if err != nil {
				return wasInserted, fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		sha256Path := filepath.Join(fs.basePath, "sha256", file.MetadataExt.SHA256Hash.String())
		if err := writeIndexFile(sha256Path, []byte(file.FileHash.String())); err != nil {
			return wasInserted, fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}

	return wasInserted, nil
}

// openXorb returns a cached read handle for the given xorb, opening it on
// first use. Handles are retained in the FileStorage-wide LRU cache so the
// number of open files stays bounded and handles are reused across shards;
// evicted handles are closed via the cache's OnEvicted callback. The handle's
// own lock must be held while seeking and reading through it.
func (fs *FileStorage) openXorb(casHash xet.XorbHash) (*xorbFile, error) {
	fs.xorbMut.Lock()
	defer fs.xorbMut.Unlock()

	v, ok := fs.xorbIndex.Get(casHash)
	if ok {
		xf := v.(*xorbFile)
		if !xf.closed {
			return xf, nil
		}
	}
	xorbPath := filepath.Join(fs.basePath, "xorbs", casHash.String())
	f, err := os.Open(xorbPath)
	if err != nil {
		return nil, fmt.Errorf("open xorb %s: %w", casHash.String(), err)
	}
	xf := &xorbFile{f: f}
	fs.xorbIndex.Add(casHash, xf)
	return xf, nil
}

// xet-core represents an empty file's computed SHA-256 as the zero value in
// shard metadata. Accept that wire-level sentinel and normalize it to the
// standard SHA-256 of empty content before persisting the shard.
func isXETCoreEmptyFile(file *shard.FileBlock) bool {
	return file.MetadataExt != nil &&
		file.FileHash == (xet.FileHash{}) &&
		len(file.Entries) == 0 &&
		file.MetadataExt.SHA256Hash == (shard.SHA256Hash{})
}

func (fs *FileStorage) computeFileSHA256(ctx context.Context, fileBlock *shard.FileBlock) ([32]byte, error) {
	h := sha256.New()
	buf := make([]byte, xet.MaxChunkSize)
	for _, entry := range fileBlock.Entries {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}

		// Cached handles are shared, and their seek offsets are mutable, so the
		// whole seek+read sequence is serialized per handle. Different xorbs
		// proceed in parallel.
		xf, err := fs.openXorb(entry.CASHash)
		if err != nil {
			return [32]byte{}, err
		}
		xf.mut.Lock()
		if _, err := xf.f.Seek(0, io.SeekStart); err != nil {
			xf.mut.Unlock()
			return [32]byte{}, fmt.Errorf("rewind xorb %s: %w", entry.CASHash.String(), err)
		}
		start, end, err := xorb.ChunkDataRange(xf.f, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			xf.mut.Unlock()
			return [32]byte{}, fmt.Errorf("locate xorb chunks: %w", err)
		}
		decoder := xorb.NewDecoder(io.NewSectionReader(xf.f, start, end-start+1), false)
		written, err := io.CopyBuffer(h, decoder, buf)
		xf.mut.Unlock()
		if err != nil {
			return [32]byte{}, fmt.Errorf("decode xorb chunks: %w", err)
		}
		if written != int64(entry.UnpackedSegBytes) {
			return [32]byte{}, fmt.Errorf("reconstructed term has %d bytes, expected %d", written, entry.UnpackedSegBytes)
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// GetShard retrieves a shard by file hash
func (fs *FileStorage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	return fs.getShard(fileHash)
}

func (fs *FileStorage) getShardBySHA256(ctx context.Context, _ string, digest [32]byte) (*shard.Shard, error) {
	fs.sha256Mut.Lock()
	value, exists := fs.sha256Index.Get(digest)
	fs.sha256Mut.Unlock()
	if exists {
		return fs.GetShard(ctx, value.(xet.FileHash))
	}

	indexPath := filepath.Join(fs.basePath, "sha256", hex.EncodeToString(digest[:]))
	b, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("SHA-256 not found")
		}
		return nil, fmt.Errorf("read SHA-256 index: %w", err)
	}
	fileHash, err := xet.ParseFileHash(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("invalid SHA-256 index: %w", err)
	}
	fs.sha256Mut.Lock()
	fs.sha256Index.Add(digest, fileHash)
	fs.sha256Mut.Unlock()
	return fs.GetShard(ctx, fileHash)
}

func (fs *FileStorage) GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error) {
	sh, err := fs.getShardBySHA256(ctx, namespace, sha256)
	if err != nil {
		return nil, fmt.Errorf("get shard by sha256: %w", err)
	}
	return newReconstructedFile(ctx, fs, namespace, sh, sha256)
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (fs *FileStorage) GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	fs.chunkMut.Lock()
	value, exists := fs.chunkIndex.Get(chunkHash)
	fs.chunkMut.Unlock()
	if exists {
		return fs.getShardByHash(value.(string))
	}

	chunkPath := filepath.Join(fs.basePath, "chunks", chunkHash.String())
	b, err := os.ReadFile(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk not found")
		}
		return nil, fmt.Errorf("read chunk index: %w", err)
	}
	shardHash := strings.TrimSpace(string(b))
	fs.chunkMut.Lock()
	fs.chunkIndex.Add(chunkHash, shardHash)
	fs.chunkMut.Unlock()
	return fs.getShardByHash(shardHash)
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
