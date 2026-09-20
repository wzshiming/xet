package local

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// Storage implements Storage using the filesystem
type Storage struct {
	basePath  string
	baseURL   string
	caches    *storage.IndexCaches
	xorbIndex *lru.Cache // bounded xorb hash -> *xorbFile
	xorbMut   sync.Mutex // guards xorbIndex
}

// xorbFile wraps an open xorb handle with its own mutex so that only uses of
// the same xorb are serialized while different xorbs can be read in parallel.
type xorbFile struct {
	mut    sync.Mutex
	f      *os.File
	closed bool
}

const defaultXorbCacheSize = 512

type Option func(*Storage)

func WithBasePath(basePath string) Option {
	return func(fs *Storage) {
		fs.basePath = basePath
	}
}

func WithBaseURL(baseURL string) Option {
	return func(fs *Storage) {
		fs.baseURL = baseURL
	}
}

// WithXorbCacheSize sets the maximum number of concurrently open xorb file
// handles retained in memory while computing shard SHA-256 digests. Values
// less than one disable xorb handle caching.
func WithXorbCacheSize(size int) Option {
	return func(fs *Storage) {
		fs.xorbIndex = lru.New(size)
	}
}

// NewStorage creates a new filesystem-based storage
func NewStorage(opts ...Option) (*Storage, error) {
	fs := &Storage{
		basePath:  "./xet",
		baseURL:   "",
		caches:    storage.NewIndexCaches(),
		xorbIndex: lru.New(defaultXorbCacheSize),
	}

	for _, opt := range opts {
		opt(fs)
	}

	// Close evicted xorb handles when the LRU cache drops them. Blocking on
	// the handle's own lock ensures we never close a file while another
	// goroutine is reading through it.
	fs.xorbIndex.OnEvicted = func(_ lru.Key, value any) {
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
		filepath.Join(fs.basePath, "index", "files"),
		filepath.Join(fs.basePath, "index", "chunks"),
		filepath.Join(fs.basePath, "index", "sha256"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("create directory %s: %w", dir, err)
		}
	}

	return fs, nil
}

// objectPath returns the on-disk location of a hash-named object under basePath.
func (fs *Storage) objectPath(kind, name string) string {
	return filepath.Join(fs.basePath, filepath.FromSlash(storage.ObjectKey(kind, name)))
}

// hasFile checks whether a file hash already has a shard mapping.
func (fs *Storage) hasFile(fileHash xet.FileHash) (bool, error) {
	if _, exists := fs.caches.Files.Get(fileHash); exists {
		return true, nil
	}

	filePath := fs.objectPath("index/files", fileHash.String())
	if _, err := os.Stat(filePath); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check file index: %w", err)
	}
	return false, nil
}

// getShard resolves a file hash through index/files/<file-hash>, whose contents
// are the hash of the serialized shard stored at shards/<shard-hash>.
func (fs *Storage) getShard(fileHash xet.FileHash) (*shard.Shard, error) {
	shardHash, err := fs.caches.Files.GetOrLoad(fileHash, func() (string, error) {
		indexData, err := os.ReadFile(fs.objectPath("index/files", fileHash.String()))
		if err != nil {
			return "", fmt.Errorf("read file index: %w", err)
		}
		return strings.TrimSpace(string(indexData)), nil
	})
	if err != nil {
		return nil, err
	}
	return fs.getShardByHash(shardHash)
}

func (fs *Storage) getShardByHash(shardHash string) (*shard.Shard, error) {
	return fs.caches.Shards.GetOrLoad(shardHash, func() (*shard.Shard, error) {
		return fs.loadShard(shardHash)
	})
}

// loadShard reads and decodes a stored shard object straight from disk.
func (fs *Storage) loadShard(shardHash string) (*shard.Shard, error) {
	shardPath := fs.objectPath("shards", shardHash)
	f, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := storage.DecodeStoredShard(f)
	if err != nil {
		return nil, err
	}
	if s.Footer == nil {
		info, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat shard file: %w", err)
		}
		// xet-core prunes cached dedup shards oldest-first by the footer creation
		// time, so pin it to the ingest time instead of the first-serve time.
		s.SetFooter(info.ModTime())
	}
	return s, nil
}

// LoadShard reads a stored shard object, bypassing the shard cache both
// ways so bulk scans cannot evict hot entries.
func (fs *Storage) LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	return fs.loadShard(shardHash)
}

// GetShardByHash loads a stored shard by the hash of its serialized bytes.
// The returned error wraps fs.ErrNotExist when the shard is absent.
func (fs *Storage) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	return fs.getShardByHash(shardHash)
}

// WalkFileIndex calls fn for every committed index/files entry.
func (fs *Storage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	root := filepath.Join(fs.basePath, "index", "files")
	return filepath.WalkDir(root, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			// Tolerate concurrent deletion anywhere under the index.
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fileHash := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		// Skip in-flight .tmp files and anything that is not a hash name.
		if len(fileHash) != 64 {
			return nil
		}
		if _, err := hex.DecodeString(fileHash); err != nil {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(fileHash, strings.TrimSpace(string(data)))
	})
}

// PutXorb stores an xorb
func (fs *Storage) PutXorb(ctx context.Context, _ string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	xorbPath := fs.objectPath("xorbs", xorbHash.String())

	// Check if xorb already exists. Dedup hits leave the stored object,
	// including its mod time, untouched.
	if _, err := os.Stat(xorbPath); err == nil {
		return false, nil // Already exists
	}
	if err := os.MkdirAll(filepath.Dir(xorbPath), 0755); err != nil {
		return false, fmt.Errorf("create xorb directory: %w", err)
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
func (fs *Storage) GetXorbReadSeekCloser(ctx context.Context, _ string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	xorbPath := fs.objectPath("xorbs", xorbHash.String())

	f, err := os.Open(xorbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("xorb %s: %w", xorbHash.String(), iofs.ErrNotExist)
		}
		return nil, fmt.Errorf("open xorb file: %w", err)
	}

	return f, nil
}

// HasXorb checks whether an xorb exists.
func (fs *Storage) HasXorb(ctx context.Context, _ string, xorbHash xet.XorbHash) (bool, error) {
	fs.xorbMut.Lock()
	_, ok := fs.xorbIndex.Get(xorbHash)
	fs.xorbMut.Unlock()
	if ok {
		return true, nil
	}

	xorbPath := fs.objectPath("xorbs", xorbHash.String())

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
func (fs *Storage) PutShard(ctx context.Context, s *shard.Shard) (bool, error) {
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

	if err := storage.PrepareShard(ctx, s, fs); err != nil {
		return false, err
	}

	encoded, shardHash, err := storage.EncodeShard(s)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}

	shardPath := fs.objectPath("shards", shardHash)
	wasInserted := true
	if _, err := os.Stat(shardPath); err == nil {
		wasInserted = false
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check shard file: %w", err)
	} else if err := overwriteIndexFile(shardPath, encoded); err != nil {
		return false, fmt.Errorf("write shard file: %w", err)
	}

	// Nothing is cached here; the read path populates the caches from the
	// stored objects, so a warm process serves exactly what a restarted one
	// would.
	err = storage.PutShardIndexes(ctx, s, shardHash, func(_ context.Context, kind, name string, value []byte) error {
		return writeIndexFile(fs.objectPath(kind, name), value)
	})
	return wasInserted, err
}

// openXorb returns a cached read handle for the given xorb, opening it on
// first use. Handles are retained in the FileStorage-wide LRU cache so the
// number of open files stays bounded and handles are reused across shards;
// evicted handles are closed via the cache's OnEvicted callback. The handle's
// own lock must be held while seeking and reading through it.
func (fs *Storage) openXorb(casHash xet.XorbHash) (*xorbFile, error) {
	fs.xorbMut.Lock()
	defer fs.xorbMut.Unlock()

	v, ok := fs.xorbIndex.Get(casHash)
	if ok {
		xf := v.(*xorbFile)
		if !xf.closed {
			return xf, nil
		}
	}
	xorbPath := fs.objectPath("xorbs", casHash.String())
	f, err := os.Open(xorbPath)
	if err != nil {
		return nil, fmt.Errorf("open xorb %s: %w", casHash.String(), err)
	}
	xf := &xorbFile{f: f}
	fs.xorbIndex.Add(casHash, xf)
	return xf, nil
}

// xorbChunkOffsets returns the cumulative packed end-offset of every chunk in
// the xorb, from the in-memory cache, the xorb footer, or a full header scan
// for footer-less xorbs. Once cached, chunk ranges are computed without
// touching the xorb file.
func (fs *Storage) xorbChunkOffsets(xorbHash xet.XorbHash) ([]uint64, error) {
	return fs.caches.Offsets.GetOrLoad(xorbHash, func() ([]uint64, error) {
		xf, err := fs.openXorb(xorbHash)
		if err != nil {
			return nil, err
		}
		xf.mut.Lock()
		offsets, err := storage.ReadXorbChunkOffsets(xf.f)
		xf.mut.Unlock()
		if err != nil {
			return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
		}
		return offsets, nil
	})
}

// Eviction waits until Close releases the handle lock.
type lockedXorbRange struct {
	io.Reader
	xf *xorbFile
}

func (r *lockedXorbRange) Close() error {
	if r.xf != nil {
		r.xf.mut.Unlock()
		r.xf = nil
	}
	return nil
}

// ReadXorbRange holds the cached handle lock until the returned reader closes.
func (fs *Storage) ReadXorbRange(_ context.Context, xorbHash xet.XorbHash, start, end int64) (io.ReadCloser, error) {
	xf, err := fs.openXorb(xorbHash)
	if err != nil {
		return nil, err
	}
	xf.mut.Lock()
	return &lockedXorbRange{Reader: io.NewSectionReader(xf.f, start, end-start+1), xf: xf}, nil
}

// GetShard retrieves a shard by file hash
func (fs *Storage) GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	return fs.getShard(fileHash)
}

// GetFileHashBySHA256 resolves a SHA-256 digest to the xet file hash recorded
// at ingest, loading the owning shard and matching its file metadata.
func (fs *Storage) GetFileHashBySHA256(ctx context.Context, _ string, digest [32]byte) (xet.FileHash, error) {
	sh, err := fs.getShardBySHA256(digest)
	if err != nil {
		return xet.FileHash{}, err
	}
	file := storage.FindFileBySHA256(sh, digest)
	if file == nil {
		return xet.FileHash{}, fmt.Errorf("SHA-256 is not present in shard")
	}
	return file.FileHash, nil
}

// getShardBySHA256 resolves a SHA-256 digest through index/sha256/<digest>,
// whose contents are the hash of the owning shard, reading the index through
// the bounded cache.
func (fs *Storage) getShardBySHA256(digest [32]byte) (*shard.Shard, error) {
	shardHash, err := fs.caches.SHA256.GetOrLoad(digest, func() (string, error) {
		b, err := os.ReadFile(fs.objectPath("index/sha256", hex.EncodeToString(digest[:])))
		if err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("SHA-256 not found")
			}
			return "", fmt.Errorf("read SHA-256 index: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	})
	if err != nil {
		return nil, err
	}
	return fs.getShardByHash(shardHash)
}

func (fs *Storage) GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error) {
	sh, err := fs.getShardBySHA256(sha256)
	if err != nil {
		return nil, fmt.Errorf("get shard by sha256: %w", err)
	}
	return storage.NewReconstructedFile(ctx, fs, namespace, sh, sha256)
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
func (fs *Storage) GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	shardHash, err := fs.caches.Chunks.GetOrLoad(chunkHash, func() (string, error) {
		b, err := os.ReadFile(fs.objectPath("index/chunks", chunkHash.String()))
		if err != nil {
			if os.IsNotExist(err) {
				return "", fmt.Errorf("chunk not found")
			}
			return "", fmt.Errorf("read chunk index: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	})
	if err != nil {
		return nil, err
	}
	return fs.getShardByHash(shardHash)
}

func writeIndexFile(path string, value []byte) error {
	_, err := os.Stat(path)
	if err == nil {
		return nil
	}
	return overwriteIndexFile(path, value)
}

// overwriteIndexFile writes an index entry unconditionally, replacing any
// existing value through an atomic rename. Each writer renames its own
// unique temp file, so concurrent same-key writers cannot steal each
// other's temp; the last rename wins.
func overwriteIndexFile(path string, value []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".idx-*")
	if err != nil {
		return err
	}
	tmpPath := f.Name()
	if _, err := f.Write(value); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Chmod(0644); err != nil {
		f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// walkHashedObjects calls fn for every hash-named object stored under kind,
// skipping in-flight temp files and tolerating concurrent deletion.
func (fs *Storage) walkHashedObjects(ctx context.Context, kind string, fn func(hash string, size int64, modTime time.Time) error) error {
	root := filepath.Join(fs.basePath, kind)
	return filepath.WalkDir(root, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		hash := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		if len(hash) != 64 {
			return nil
		}
		if _, err := hex.DecodeString(hash); err != nil {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(hash, info.Size(), info.ModTime())
	})
}

// WalkShards calls fn for every stored shard object.
func (fs *Storage) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	return fs.walkHashedObjects(ctx, "shards", fn)
}

// WalkXorbs calls fn for every stored xorb object.
func (fs *Storage) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return fs.walkHashedObjects(ctx, "xorbs", fn)
}

// WalkSHA256Index calls fn for every committed index/sha256 entry.
func (fs *Storage) WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error {
	root := filepath.Join(fs.basePath, "index", "sha256")
	return filepath.WalkDir(root, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			// Tolerate concurrent deletion anywhere under the index.
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		sha256Hex := strings.ReplaceAll(filepath.ToSlash(rel), "/", "")
		// Skip in-flight .tmp files and anything that is not a hash name.
		if len(sha256Hex) != 64 {
			return nil
		}
		if _, err := hex.DecodeString(sha256Hex); err != nil {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			return err
		}
		return fn(sha256Hex, strings.TrimSpace(string(data)))
	})
}

// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting
// whether it existed.
func (fs *Storage) DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	err := os.Remove(fs.objectPath("index/files", fileHash.String()))
	// Evicting after the delete narrows but does not close the re-cache window.
	fs.caches.Files.Remove(fileHash)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("delete file index: %w", err)
	}
	return true, nil
}

// GetFileIndexEntry returns the shard hash recorded for fileHash, or ""
// when the entry is absent, bypassing the cache so sweeps see stored state.
func (fs *Storage) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	b, err := os.ReadFile(fs.objectPath("index/files", fileHash.String()))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read file index: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// DeleteShard removes a stored shard object.
func (fs *Storage) DeleteShard(ctx context.Context, shardHash string) error {
	err := os.Remove(fs.objectPath("shards", shardHash))
	fs.caches.Shards.Remove(shardHash)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete shard: %w", err)
	}
	return nil
}

// DeleteXorb removes a stored xorb object.
func (fs *Storage) DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) error {
	// Evict before removing: OnEvicted closes the cached handle once any
	// in-flight read through it finishes, and Windows cannot delete a file
	// that still has an open handle.
	fs.xorbMut.Lock()
	fs.xorbIndex.Remove(xorbHash)
	fs.xorbMut.Unlock()
	fs.caches.Offsets.Remove(xorbHash)
	err := os.Remove(fs.objectPath("xorbs", xorbHash.String()))
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete xorb: %w", err)
	}
	return nil
}

// GetChunkIndexEntry returns the shard hash recorded for chunkHash, or ""
// when the entry is absent, bypassing the cache so sweeps see stored state.
func (fs *Storage) GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error) {
	b, err := os.ReadFile(fs.objectPath("index/chunks", chunkHash.String()))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read chunk index: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
func (fs *Storage) DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) error {
	err := os.Remove(fs.objectPath("index/chunks", chunkHash.String()))
	fs.caches.Chunks.Remove(chunkHash)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete chunk index: %w", err)
	}
	return nil
}

// evictSHA256 drops the cached mapping for a hex SHA-256 digest.
func (fs *Storage) evictSHA256(sha256Hex string) {
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil || len(raw) != 32 {
		return
	}
	var digest [32]byte
	copy(digest[:], raw)
	fs.caches.SHA256.Remove(digest)
}

// GetSHA256IndexEntry returns the shard hash recorded for the hex SHA-256
// digest, or "" when the entry is absent, bypassing the cache.
func (fs *Storage) GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error) {
	b, err := os.ReadFile(fs.objectPath("index/sha256", sha256Hex))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read SHA-256 index: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting whether
// it existed.
func (fs *Storage) DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) (bool, error) {
	err := os.Remove(fs.objectPath("index/sha256", sha256Hex))
	fs.evictSHA256(sha256Hex)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("delete SHA-256 index: %w", err)
	}
	return true, nil
}

// GetXorbURL generates a URL for accessing xorb data
func (fs *Storage) GetXorbURL(namespace string, xorbHash xet.XorbHash) (string, error) {
	if fs.baseURL == "" {
		// If no base URL is configured, return a relative path
		return fmt.Sprintf("/v1/xorbs/%s/%s", namespace, xorbHash.String()), nil
	}
	return fmt.Sprintf("%s/v1/xorbs/%s/%s", fs.baseURL, namespace, xorbHash.String()), nil
}

// GetXorbDataRange returns the [start, end] byte range (inclusive) within
// the stored xorb binary for the given chunk range [chunkStart, chunkEnd).
// The returned range includes the 8-byte chunk header for each chunk, so that
// xet-core can parse the header (version, compressed/uncompressed size,
// compression type) when it downloads that byte range.
// Ranges are computed from cached per-xorb chunk offsets, so the xorb file is
// only read (footer or full scan) the first time it is seen.
func (fs *Storage) GetXorbDataRange(ctx context.Context, _ string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	offsets, err := fs.xorbChunkOffsets(xorbHash)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get chunk data range: %w", err)
	}
	return xorb.ChunkDataRangeFromOffsets(offsets, chunkStart, chunkEnd)
}

// GetXorbChunkOffsets returns the xorb's chunk offset table; cached after
// the first read.
func (fs *Storage) GetXorbChunkOffsets(_ context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	return fs.xorbChunkOffsets(xorbHash)
}

var _ storage.Storage = (*Storage)(nil)
