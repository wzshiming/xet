package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
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
	"github.com/wzshiming/xet/xorb"
)

// Storage defines the interface for storing and retrieving XET data
type Storage interface {
	// PutXorb stores an xorb by its hash
	PutXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash, r io.Reader) (bool, error)

	// GetXorbURL generates a URL for accessing xorb data
	GetXorbURL(namespace string, xorbHash xet.XorbHash) (string, error)

	// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error)

	// HasXorb checks whether an xorb exists.
	HasXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) (bool, error)

	// GetXorbDataRange returns the byte range within the stored xorb for the given chunk range
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)

	// PutShard stores a shard, named by the SHA-256 of its stored bytes
	PutShard(ctx context.Context, shard *shard.Shard) (bool, error)

	// GetShard retrieves a shard by file hash
	GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
	GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error)

	// GetReconstructedFile returns a ReadSeekCloser for a file reconstructed from a shard by its SHA-256 digest.
	GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error)

	// GetFileHashBySHA256 resolves a file's SHA-256 digest to the xet file hash recorded at ingest.
	GetFileHashBySHA256(ctx context.Context, namespace string, sha256 [32]byte) (xet.FileHash, error)
}

// FileStorage implements Storage using the filesystem
type FileStorage struct {
	basePath     string
	baseURL      string
	fileIndex    *lru.Cache // bounded file hash -> shard hash
	shardIndex   *lru.Cache // bounded shard hash -> shard cache
	chunkIndex   *lru.Cache // bounded chunk hash -> shard hash
	sha256Index  *lru.Cache // bounded SHA-256 -> shard hash
	xorbIndex    *lru.Cache // bounded xorb hash -> *xorbFile
	offsetsIndex *lru.Cache // bounded xorb hash -> []uint64 packed chunk end-offsets

	fileMut    sync.Mutex // guards fileIndex
	shardMut   sync.Mutex // guards shardIndex
	chunkMut   sync.Mutex // guards chunkIndex
	sha256Mut  sync.Mutex // guards sha256Index
	xorbMut    sync.Mutex // guards xorbIndex
	offsetsMut sync.Mutex // guards offsetsIndex
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
const defaultOffsetsCacheSize = 512

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
		basePath:     "./xet",
		baseURL:      "",
		fileIndex:    lru.New(defaultFileCacheSize),
		shardIndex:   lru.New(defaultShardCacheSize),
		chunkIndex:   lru.New(defaultChunkCacheSize),
		sha256Index:  lru.New(defaultSHA256CacheSize),
		xorbIndex:    lru.New(defaultXorbCacheSize),
		offsetsIndex: lru.New(defaultOffsetsCacheSize),
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

// objectPath returns the git-style fanout path for a hash-named object:
// basePath/<kind>/<name[:2]>/<name[2:]>. The fanout keeps directory sizes
// bounded; a flat layout accumulates tens of thousands of entries per model.
func (fs *FileStorage) objectPath(kind, name string) string {
	if len(name) <= 2 {
		return filepath.Join(fs.basePath, kind, name)
	}
	return filepath.Join(fs.basePath, kind, name[:2], name[2:])
}

// hasFile checks whether a file hash already has a shard mapping.
func (fs *FileStorage) hasFile(fileHash xet.FileHash) (bool, error) {
	fs.fileMut.Lock()
	_, exists := fs.fileIndex.Get(fileHash)
	fs.fileMut.Unlock()
	if exists {
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
func (fs *FileStorage) getShard(fileHash xet.FileHash) (*shard.Shard, error) {
	fs.fileMut.Lock()
	value, exists := fs.fileIndex.Get(fileHash)
	fs.fileMut.Unlock()
	if exists {
		return fs.getShardByHash(value.(string))
	}

	indexPath := fs.objectPath("index/files", fileHash.String())
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

// shardHeaderSize is the fixed shard header; its last 8 bytes carry FooterSize.
const shardHeaderSize = 48

// decodeStoredShard decodes a stored shard object, following the FooterSize its
// header declares so objects written before shards went footerless still load.
func decodeStoredShard(r io.Reader) (*shard.Shard, error) {
	var header [shardHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read shard header: %w", err)
	}
	s := shard.NewShard()
	withFooter := binary.LittleEndian.Uint64(header[40:]) != 0
	if err := s.Decode(io.MultiReader(bytes.NewReader(header[:]), r), withFooter); err != nil {
		return nil, err
	}
	return s, nil
}

func (fs *FileStorage) getShardByHash(shardHash string) (*shard.Shard, error) {
	fs.shardMut.Lock()
	value, exists := fs.shardIndex.Get(shardHash)
	fs.shardMut.Unlock()
	if exists {
		return value.(*shard.Shard), nil
	}

	shardPath := fs.objectPath("shards", shardHash)
	f, err := os.Open(shardPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	s, err := decodeStoredShard(f)
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

	fs.shardMut.Lock()
	fs.shardIndex.Add(shardHash, s)
	fs.shardMut.Unlock()

	return s, nil
}

// GetShardByHash loads a stored shard by the hash of its serialized bytes.
// The returned error wraps fs.ErrNotExist when the shard is absent.
func (fs *FileStorage) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	return fs.getShardByHash(shardHash)
}

// WalkFileIndex calls fn for every committed index/files entry.
func (fs *FileStorage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
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
func (fs *FileStorage) PutXorb(ctx context.Context, _ string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	xorbPath := fs.objectPath("xorbs", xorbHash.String())

	// Check if xorb already exists
	if _, err := os.Stat(xorbPath); err == nil {
		// Refresh the mtime so deduplicated reuse stays inside the GC grace window.
		now := time.Now()
		_ = os.Chtimes(xorbPath, now, now)
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
func (fs *FileStorage) GetXorbReadSeekCloser(ctx context.Context, _ string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	xorbPath := fs.objectPath("xorbs", xorbHash.String())

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
		if s.Files[i].MetadataExt != nil && s.Files[i].MetadataExt.SHA256Hash != shard.NewSHA256Hash(computed) {
			return false, fmt.Errorf("SHA-256 mismatch for file %s", s.Files[i].FileHash.String())
		}

		s.Files[i].MetadataExt = &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(computed)}
		s.Files[i].Flags |= shard.FileWithMetadataExt
	}

	// Serialize without the footer (it embeds a creation timestamp), so the
	// stored bytes — and the sha256 hash addressing them — are deterministic
	// for identical shard content.
	r, err := s.Encode(false)
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

	shardPath := fs.objectPath("shards", shardHash)
	wasInserted := true
	if _, err := os.Stat(shardPath); err == nil {
		wasInserted = false
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("check shard file: %w", err)
	} else if err := os.MkdirAll(filepath.Dir(shardPath), 0755); err != nil {
		return false, fmt.Errorf("create shard directory: %w", err)
	} else if err := os.Rename(tmpPath, shardPath); err != nil {
		return false, fmt.Errorf("finalize shard file: %w", err)
	} else {
		removeTemp = false
	}

	// The index/files/ index is written last: hasFile treats it as the commit
	// marker, so a partial failure leaves a retryable shard instead of one
	// that reports "already exists" with missing chunk/sha256 indexes. Nothing
	// is cached here; the read path populates the caches from the stored
	// objects, so a warm process serves exactly what a restarted one would.
	shardHashData := []byte(shardHash)
	for _, casBlock := range s.CASInfos {
		for _, chunk := range casBlock.Chunks {
			chunkPath := fs.objectPath("index/chunks", chunk.ChunkHash.String())
			err := writeIndexFile(chunkPath, shardHashData)
			if err != nil {
				return wasInserted, fmt.Errorf("write chunk index: %w", err)
			}
		}
	}
	for _, file := range s.Files {
		sha256Path := fs.objectPath("index/sha256", file.MetadataExt.SHA256Hash.String())
		if err := writeIndexFile(sha256Path, shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write SHA-256 index for file %s: %w", file.FileHash.String(), err)
		}
	}
	for _, file := range s.Files {
		indexPath := fs.objectPath("index/files", file.FileHash.String())
		if err := writeIndexFile(indexPath, shardHashData); err != nil {
			return wasInserted, fmt.Errorf("write file index for file %s: %w", file.FileHash.String(), err)
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
func (fs *FileStorage) xorbChunkOffsets(xorbHash xet.XorbHash) ([]uint64, error) {
	fs.offsetsMut.Lock()
	if v, ok := fs.offsetsIndex.Get(xorbHash); ok {
		fs.offsetsMut.Unlock()
		return v.([]uint64), nil
	}
	fs.offsetsMut.Unlock()

	xf, err := fs.openXorb(xorbHash)
	if err != nil {
		return nil, err
	}
	xf.mut.Lock()
	offsets, err := xorb.ReadChunkOffsets(xf.f)
	if errors.Is(err, xorb.ErrNoFooter) {
		offsets, err = xorb.ScanChunkOffsets(xf.f)
	}
	xf.mut.Unlock()
	if err != nil {
		return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
	}

	fs.offsetsMut.Lock()
	fs.offsetsIndex.Add(xorbHash, offsets)
	fs.offsetsMut.Unlock()
	return offsets, nil
}

func (fs *FileStorage) computeFileSHA256(ctx context.Context, fileBlock *shard.FileBlock) ([32]byte, error) {
	if len(fileBlock.Entries) == 0 {
		return [32]byte{}, nil
	}

	h := sha256.New()
	buf := make([]byte, xet.MaxChunkSize)
	for _, entry := range fileBlock.Entries {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, err
		}

		offsets, err := fs.xorbChunkOffsets(entry.CASHash)
		if err != nil {
			return [32]byte{}, fmt.Errorf("locate xorb chunks: %w", err)
		}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			return [32]byte{}, fmt.Errorf("locate xorb chunks: %w", err)
		}
		// Cached handles are shared; holding the handle lock keeps eviction from
		// closing the file mid-read. Different xorbs proceed in parallel.
		xf, err := fs.openXorb(entry.CASHash)
		if err != nil {
			return [32]byte{}, err
		}
		xf.mut.Lock()
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

// GetFileHashBySHA256 resolves a SHA-256 digest to the xet file hash recorded
// at ingest, loading the owning shard and matching its file metadata.
func (fs *FileStorage) GetFileHashBySHA256(ctx context.Context, _ string, digest [32]byte) (xet.FileHash, error) {
	sh, err := fs.getShardBySHA256(digest)
	if err != nil {
		return xet.FileHash{}, err
	}
	file := findFileBySHA256(sh, digest)
	if file == nil {
		return xet.FileHash{}, fmt.Errorf("SHA-256 is not present in shard")
	}
	return file.FileHash, nil
}

// getShardBySHA256 resolves a SHA-256 digest through index/sha256/<digest>,
// whose contents are the hash of the owning shard, reading the index through
// the bounded cache.
func (fs *FileStorage) getShardBySHA256(digest [32]byte) (*shard.Shard, error) {
	fs.sha256Mut.Lock()
	value, exists := fs.sha256Index.Get(digest)
	fs.sha256Mut.Unlock()
	if exists {
		return fs.getShardByHash(value.(string))
	}

	indexPath := fs.objectPath("index/sha256", hex.EncodeToString(digest[:]))
	b, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("SHA-256 not found")
		}
		return nil, fmt.Errorf("read SHA-256 index: %w", err)
	}
	shardHash := strings.TrimSpace(string(b))
	fs.sha256Mut.Lock()
	fs.sha256Index.Add(digest, shardHash)
	fs.sha256Mut.Unlock()
	return fs.getShardByHash(shardHash)
}

func (fs *FileStorage) GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error) {
	sh, err := fs.getShardBySHA256(sha256)
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

	chunkPath := fs.objectPath("index/chunks", chunkHash.String())
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
	return overwriteIndexFile(path, value)
}

// overwriteIndexFile writes an index entry unconditionally, replacing any
// existing value through an atomic rename.
func overwriteIndexFile(path string, value []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
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

// walkHashedObjects calls fn for every hash-named object stored under kind,
// skipping in-flight temp files and tolerating concurrent deletion.
func (fs *FileStorage) walkHashedObjects(ctx context.Context, kind string, fn func(hash string, size int64, modTime time.Time) error) error {
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
func (fs *FileStorage) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	return fs.walkHashedObjects(ctx, "shards", fn)
}

// WalkXorbs calls fn for every stored xorb object.
func (fs *FileStorage) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return fs.walkHashedObjects(ctx, "xorbs", fn)
}

// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting
// whether it existed.
func (fs *FileStorage) DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	err := os.Remove(fs.objectPath("index/files", fileHash.String()))
	// Evicting after the delete narrows but does not close the re-cache window.
	fs.fileMut.Lock()
	fs.fileIndex.Remove(fileHash)
	fs.fileMut.Unlock()
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
func (fs *FileStorage) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
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
func (fs *FileStorage) DeleteShard(ctx context.Context, shardHash string) error {
	err := os.Remove(fs.objectPath("shards", shardHash))
	fs.shardMut.Lock()
	fs.shardIndex.Remove(shardHash)
	fs.shardMut.Unlock()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete shard: %w", err)
	}
	return nil
}

// DeleteXorb removes a stored xorb object.
func (fs *FileStorage) DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) error {
	err := os.Remove(fs.objectPath("xorbs", xorbHash.String()))
	// Evicting the handle closes it via the cache's OnEvicted callback; open
	// handles keep serving in-flight reads until then.
	fs.xorbMut.Lock()
	fs.xorbIndex.Remove(xorbHash)
	fs.xorbMut.Unlock()
	fs.offsetsMut.Lock()
	fs.offsetsIndex.Remove(xorbHash)
	fs.offsetsMut.Unlock()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete xorb: %w", err)
	}
	return nil
}

// GetChunkIndexEntry returns the shard hash recorded for chunkHash, or ""
// when the entry is absent, bypassing the cache so sweeps see stored state.
func (fs *FileStorage) GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error) {
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
func (fs *FileStorage) DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) error {
	err := os.Remove(fs.objectPath("index/chunks", chunkHash.String()))
	fs.chunkMut.Lock()
	fs.chunkIndex.Remove(chunkHash)
	fs.chunkMut.Unlock()
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete chunk index: %w", err)
	}
	return nil
}

// SetChunkIndexEntry force-writes the index/chunks entry for chunkHash.
func (fs *FileStorage) SetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) error {
	if err := overwriteIndexFile(fs.objectPath("index/chunks", chunkHash.String()), []byte(shardHash)); err != nil {
		return fmt.Errorf("write chunk index: %w", err)
	}
	fs.chunkMut.Lock()
	fs.chunkIndex.Remove(chunkHash)
	fs.chunkMut.Unlock()
	return nil
}

// evictSHA256 drops the cached mapping for a hex SHA-256 digest.
func (fs *FileStorage) evictSHA256(sha256Hex string) {
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil || len(raw) != 32 {
		return
	}
	var digest [32]byte
	copy(digest[:], raw)
	fs.sha256Mut.Lock()
	fs.sha256Index.Remove(digest)
	fs.sha256Mut.Unlock()
}

// GetSHA256IndexEntry returns the shard hash recorded for the hex SHA-256
// digest, or "" when the entry is absent, bypassing the cache.
func (fs *FileStorage) GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error) {
	b, err := os.ReadFile(fs.objectPath("index/sha256", sha256Hex))
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", fmt.Errorf("read SHA-256 index: %w", err)
	}
	return strings.TrimSpace(string(b)), nil
}

// DeleteSHA256IndexEntry removes the index/sha256 entry.
func (fs *FileStorage) DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) error {
	err := os.Remove(fs.objectPath("index/sha256", sha256Hex))
	fs.evictSHA256(sha256Hex)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete SHA-256 index: %w", err)
	}
	return nil
}

// SetSHA256IndexEntry force-writes the index/sha256 entry.
func (fs *FileStorage) SetSHA256IndexEntry(ctx context.Context, sha256Hex string, shardHash string) error {
	if err := overwriteIndexFile(fs.objectPath("index/sha256", sha256Hex), []byte(shardHash)); err != nil {
		return fmt.Errorf("write SHA-256 index: %w", err)
	}
	fs.evictSHA256(sha256Hex)
	return nil
}

// GetXorbURL generates a URL for accessing xorb data
func (fs *FileStorage) GetXorbURL(namespace string, xorbHash xet.XorbHash) (string, error) {
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
func (fs *FileStorage) GetXorbDataRange(ctx context.Context, _ string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	offsets, err := fs.xorbChunkOffsets(xorbHash)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get chunk data range: %w", err)
	}
	return xorb.ChunkDataRangeFromOffsets(offsets, chunkStart, chunkEnd)
}

// GetXorbChunkOffsets returns the xorb's chunk offset table; cached after
// the first read.
func (fs *FileStorage) GetXorbChunkOffsets(_ context.Context, xorbHash xet.XorbHash) ([]uint64, error) {
	return fs.xorbChunkOffsets(xorbHash)
}
