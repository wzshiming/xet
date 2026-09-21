// Package memory implements storage.Storage over process memory, for tests and ephemeral servers.
package memory

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	iofs "io/fs"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/xorb"
)

// object is one immutable stored blob; data is never mutated after insertion.
type object struct {
	data    []byte
	modTime time.Time
}

// Storage keeps xorbs, shards, and index entries in maps keyed by kind then name.
type Storage struct {
	baseURL string

	mu      sync.RWMutex
	objects map[string]map[string]object
}

type Option func(*Storage)

func WithBaseURL(baseURL string) Option {
	return func(s *Storage) {
		s.baseURL = baseURL
	}
}

// NewStorage creates an empty in-memory store.
func NewStorage(opts ...Option) *Storage {
	s := &Storage{objects: map[string]map[string]object{}}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Storage) get(kind, name string) (object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	obj, ok := s.objects[kind][name]
	return obj, ok
}

// put stores data under kind/name, reporting whether it was inserted; an
// existing entry is kept unless overwrite is set.
func (s *Storage) put(kind, name string, data []byte, overwrite bool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	objs := s.objects[kind]
	if objs == nil {
		objs = map[string]object{}
		s.objects[kind] = objs
	}
	if _, exists := objs[name]; exists && !overwrite {
		return false
	}
	objs[name] = object{data: data, modTime: time.Now()}
	return true
}

// delete removes kind/name, reporting whether it existed.
func (s *Storage) delete(kind, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objects[kind][name]
	delete(s.objects[kind], name)
	return ok
}

type entry struct {
	name string
	object
}

// snapshot returns kind's entries sorted by name, so walks run their callbacks outside the lock.
func (s *Storage) snapshot(kind string) []entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]entry, 0, len(s.objects[kind]))
	for name, obj := range s.objects[kind] {
		entries = append(entries, entry{name: name, object: obj})
	}
	slices.SortFunc(entries, func(a, b entry) int { return cmp.Compare(a.name, b.name) })
	return entries
}

func (s *Storage) walkObjects(ctx context.Context, kind string, fn func(hash string, size int64, modTime time.Time) error) error {
	for _, e := range s.snapshot(kind) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(e.name, int64(len(e.data)), e.modTime); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) walkIndex(ctx context.Context, kind string, fn func(name, shardHash string) error) error {
	for _, e := range s.snapshot(kind) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(e.name, string(e.data)); err != nil {
			return err
		}
	}
	return nil
}

// indexEntry returns the shard hash recorded under kind/name, "" when absent.
func (s *Storage) indexEntry(kind, name string) string {
	obj, _ := s.get(kind, name)
	return string(obj.data)
}

// loadShard decodes a fresh shard value per call; the footer pins the ingest time.
func (s *Storage) loadShard(shardHash string) (*shard.Shard, error) {
	obj, ok := s.get("shards", shardHash)
	if !ok {
		return nil, notFound("shard", shardHash)
	}
	sh, err := storage.DecodeStoredShard(bytes.NewReader(obj.data))
	if err != nil {
		return nil, err
	}
	if sh.Footer == nil {
		sh.SetFooter(obj.modTime)
	}
	return sh, nil
}

// shardByIndex resolves an index entry to its decoded shard.
func (s *Storage) shardByIndex(kind, name string) (*shard.Shard, error) {
	shardHash := s.indexEntry(kind, name)
	if shardHash == "" {
		return nil, notFound(kind, name)
	}
	return s.loadShard(shardHash)
}

type readSeekCloser struct {
	*bytes.Reader
}

func (readSeekCloser) Close() error { return nil }

func notFound(kind, name string) error {
	return fmt.Errorf("%s %s: %w", kind, name, iofs.ErrNotExist)
}

// PutXorb validates the stream into a private copy; dedup hits leave the stored object untouched.
func (s *Storage) PutXorb(ctx context.Context, _ string, xorbHash xet.XorbHash, r io.Reader) (bool, error) {
	if _, exists := s.get("xorbs", xorbHash.String()); exists {
		return false, nil
	}
	var buf bytes.Buffer
	if err := xorb.Validate(io.TeeReader(r, &buf), xorbHash); err != nil {
		return false, fmt.Errorf("validate xorb: %w", err)
	}
	return s.put("xorbs", xorbHash.String(), buf.Bytes(), false), nil
}

// GetXorbURL routes through the CAS server's xorb endpoint, absolute when a base URL is set.
func (s *Storage) GetXorbURL(_ context.Context, namespace string, xorbHash xet.XorbHash) (string, error) {
	return fmt.Sprintf("%s/v1/xorbs/%s/%s", s.baseURL, namespace, xorbHash.String()), nil
}

// GetXorbReadSeekCloser returns an independent reader over the stored xorb bytes.
func (s *Storage) GetXorbReadSeekCloser(ctx context.Context, _ string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	obj, ok := s.get("xorbs", xorbHash.String())
	if !ok {
		return nil, notFound("xorb", xorbHash.String())
	}
	return readSeekCloser{bytes.NewReader(obj.data)}, nil
}

// HasXorb checks whether an xorb exists.
func (s *Storage) HasXorb(_ context.Context, _ string, xorbHash xet.XorbHash) (bool, error) {
	_, ok := s.get("xorbs", xorbHash.String())
	return ok, nil
}

// GetXorbChunkOffsets returns the xorb's cumulative packed chunk end-offsets.
func (s *Storage) GetXorbChunkOffsets(_ context.Context, _ string, xorbHash xet.XorbHash) ([]uint64, error) {
	obj, ok := s.get("xorbs", xorbHash.String())
	if !ok {
		return nil, notFound("xorb", xorbHash.String())
	}
	offsets, err := storage.ReadXorbChunkOffsets(bytes.NewReader(obj.data))
	if err != nil {
		return nil, fmt.Errorf("read xorb chunk offsets: %w", err)
	}
	return offsets, nil
}

// GetXorbDataRange returns the inclusive [start, end] byte range of chunks [chunkStart, chunkEnd).
func (s *Storage) GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error) {
	offsets, err := s.GetXorbChunkOffsets(ctx, namespace, xorbHash)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get chunk data range: %w", err)
	}
	return xorb.ChunkDataRangeFromOffsets(offsets, chunkStart, chunkEnd)
}

// GetXorbRangeReadCloser streams the inclusive [start, end] byte range of a stored xorb.
func (s *Storage) GetXorbRangeReadCloser(_ context.Context, _ string, xorbHash xet.XorbHash, start, end int64) (io.ReadCloser, error) {
	obj, ok := s.get("xorbs", xorbHash.String())
	if !ok {
		return nil, notFound("xorb", xorbHash.String())
	}
	return io.NopCloser(io.NewSectionReader(bytes.NewReader(obj.data), start, end-start+1)), nil
}

// PutShard stores a shard and its index entries; like the file backend, existing entries keep their first writer.
func (s *Storage) PutShard(ctx context.Context, sh *shard.Shard) (bool, error) {
	for _, file := range sh.Files {
		if s.indexEntry("index/files", file.FileHash.String()) != "" {
			return false, nil
		}
	}
	if err := storage.PrepareShard(ctx, sh, s); err != nil {
		return false, err
	}
	encoded, shardHash, err := storage.EncodeShard(sh)
	if err != nil {
		return false, fmt.Errorf("serialize shard: %w", err)
	}
	inserted := s.put("shards", shardHash, encoded, false)
	err = storage.PutShardIndexes(ctx, sh, shardHash, func(_ context.Context, kind, name string, value []byte) error {
		s.put(kind, name, value, false)
		return nil
	})
	return inserted, err
}

// GetShard retrieves a shard by file hash.
func (s *Storage) GetShard(_ context.Context, fileHash xet.FileHash) (*shard.Shard, error) {
	return s.shardByIndex("index/files", fileHash.String())
}

// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication).
func (s *Storage) GetShardByChunkHash(_ context.Context, _ string, chunkHash xet.ChunkHash) (*shard.Shard, error) {
	return s.shardByIndex("index/chunks", chunkHash.String())
}

// GetFileHashBySHA256 resolves a SHA-256 digest to the xet file hash recorded at ingest.
func (s *Storage) GetFileHashBySHA256(_ context.Context, _ string, digest [32]byte) (xet.FileHash, error) {
	sh, err := s.shardByIndex("index/sha256", shard.NewSHA256Hash(digest).String())
	if err != nil {
		return xet.FileHash{}, err
	}
	file := storage.FindFileBySHA256(sh, digest)
	if file == nil {
		return xet.FileHash{}, fmt.Errorf("SHA-256 is not present in shard")
	}
	return file.FileHash, nil
}

// GetReconstructedFile returns a ReadSeekCloser for the file recorded under the SHA-256 digest.
func (s *Storage) GetReconstructedFile(ctx context.Context, namespace string, digest [32]byte) (io.ReadSeekCloser, error) {
	sh, err := s.shardByIndex("index/sha256", shard.NewSHA256Hash(digest).String())
	if err != nil {
		return nil, fmt.Errorf("get shard by sha256: %w", err)
	}
	return storage.NewReconstructedFile(ctx, s, namespace, sh, digest)
}

// GetShardByHash loads a stored shard by the hash of its serialized bytes; the error wraps fs.ErrNotExist when absent.
func (s *Storage) GetShardByHash(_ context.Context, shardHash string) (*shard.Shard, error) {
	return s.loadShard(shardHash)
}

// LoadShard is GetShardByHash; there is no read cache to bypass.
func (s *Storage) LoadShard(_ context.Context, shardHash string) (*shard.Shard, error) {
	return s.loadShard(shardHash)
}

// WalkShards calls fn for every stored shard object.
func (s *Storage) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	return s.walkObjects(ctx, "shards", fn)
}

// WalkXorbs calls fn for every stored xorb object.
func (s *Storage) WalkXorbs(ctx context.Context, _ string, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return s.walkObjects(ctx, "xorbs", fn)
}

func (s *Storage) Usage(ctx context.Context) (storage.Usage, error) {
	return storage.ComputeUsage(ctx, s.walkObjects)
}

// WalkFileIndex calls fn for every index/files entry.
func (s *Storage) WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error {
	return s.walkIndex(ctx, "index/files", fn)
}

// WalkSHA256Index calls fn for every index/sha256 entry.
func (s *Storage) WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error {
	return s.walkIndex(ctx, "index/sha256", fn)
}

// GetFileIndexEntry returns the shard hash recorded for fileHash, "" when absent.
func (s *Storage) GetFileIndexEntry(_ context.Context, fileHash xet.FileHash) (string, error) {
	return s.indexEntry("index/files", fileHash.String()), nil
}

// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting whether it existed.
func (s *Storage) DeleteFileIndexEntry(_ context.Context, fileHash xet.FileHash) (bool, error) {
	return s.delete("index/files", fileHash.String()), nil
}

// DeleteShard removes a stored shard object.
func (s *Storage) DeleteShard(_ context.Context, shardHash string) error {
	s.delete("shards", shardHash)
	return nil
}

// DeleteXorb removes a stored xorb object.
func (s *Storage) DeleteXorb(_ context.Context, _ string, xorbHash xet.XorbHash) error {
	s.delete("xorbs", xorbHash.String())
	return nil
}

// GetChunkIndexEntry returns the shard hash recorded for chunkHash, "" when absent.
func (s *Storage) GetChunkIndexEntry(_ context.Context, chunkHash xet.ChunkHash) (string, error) {
	return s.indexEntry("index/chunks", chunkHash.String()), nil
}

// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
func (s *Storage) DeleteChunkIndexEntry(_ context.Context, chunkHash xet.ChunkHash) error {
	s.delete("index/chunks", chunkHash.String())
	return nil
}

// GetSHA256IndexEntry returns the shard hash recorded for sha256Hex, "" when absent.
func (s *Storage) GetSHA256IndexEntry(_ context.Context, sha256Hex string) (string, error) {
	return s.indexEntry("index/sha256", sha256Hex), nil
}

// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting whether it existed.
func (s *Storage) DeleteSHA256IndexEntry(_ context.Context, sha256Hex string) (bool, error) {
	return s.delete("index/sha256", sha256Hex), nil
}

var _ storage.Storage = (*Storage)(nil)
