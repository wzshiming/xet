package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

var ErrInvalidShard = errors.New("invalid shard")

// Storage is the backend contract implemented by the local, s3 and memory
// packages; the package overview describes the shared layout and semantics.
type Storage interface {
	// Xorb objects: content-addressed chunk data under xorbs/.

	// PutXorb stores an xorb by its hash
	PutXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash, r io.Reader) (bool, error)

	// HasXorb checks whether an xorb exists.
	HasXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) (bool, error)

	// GetXorbURL generates a URL for accessing xorb data
	GetXorbURL(ctx context.Context, namespace string, xorbHash xet.XorbHash) (string, error)

	// GetXorbReadSeekCloser returns a ReadSeekCloser for the xorb data, which can be used for range requests.
	GetXorbReadSeekCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash) (io.ReadSeekCloser, error)

	// GetXorbRangeReadCloser streams the inclusive [start, end] byte range of a stored xorb.
	GetXorbRangeReadCloser(ctx context.Context, namespace string, xorbHash xet.XorbHash, start, end int64) (io.ReadCloser, error)

	// GetXorbDataRange returns the byte range within the stored xorb for the given chunk range
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)

	// GetXorbChunkOffsets returns cumulative packed chunk ends, wrapping fs.ErrNotExist when absent.
	GetXorbChunkOffsets(ctx context.Context, namespace string, xorbHash xet.XorbHash) ([]uint64, error)

	// WalkXorbs calls fn for every stored xorb object.
	WalkXorbs(ctx context.Context, namespace string, fn func(xorbHash string, size int64, modTime time.Time) error) error

	// DeleteXorb removes a stored xorb object.
	DeleteXorb(ctx context.Context, namespace string, xorbHash xet.XorbHash) error

	// Shard objects: serialized shards under shards/, resolved through the indexes.

	// PutShard stores a shard, named by the SHA-256 of its stored bytes
	PutShard(ctx context.Context, shard *shard.Shard) (bool, error)

	// GetShard retrieves a shard by file hash
	GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication)
	GetShardByChunkHash(ctx context.Context, namespace string, chunkHash xet.ChunkHash) (*shard.Shard, error)

	// GetShardByHash loads by serialized shard hash, wrapping fs.ErrNotExist when absent.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	// LoadShard reads a stored shard bypassing the read cache, which a whole-store sweep would evict.
	LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error)

	// WalkShards calls fn for every stored shard object.
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error

	// DeleteShard removes a stored shard object.
	DeleteShard(ctx context.Context, shardHash string) error

	// File lookups by content SHA-256 through index/sha256.

	// GetReconstructedFile returns a ReadSeekCloser for a file reconstructed from a shard by its SHA-256 digest.
	GetReconstructedFile(ctx context.Context, namespace string, sha256 [32]byte) (io.ReadSeekCloser, error)

	// GetFileHashBySHA256 resolves a file's SHA-256 digest to the xet file hash recorded at ingest.
	GetFileHashBySHA256(ctx context.Context, namespace string, sha256 [32]byte) (xet.FileHash, error)

	// GC index access: walks, cache-bypassing reads and deletes of index entries.

	// WalkFileIndex calls fn with each file hash and its owning shard hash from index/files.
	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error

	// GetFileIndexEntry returns the shard hash recorded for fileHash, "" when absent, bypassing caches.
	GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error)

	// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting whether it existed.
	DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error)

	// GetChunkIndexEntry returns the shard hash recorded for chunkHash, "" when absent, bypassing caches.
	GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error)

	// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
	DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) error

	// WalkSHA256Index calls fn for every index/sha256 entry.
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error

	// GetSHA256IndexEntry returns the shard hash recorded for sha256Hex, "" when absent, bypassing caches.
	GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error)

	// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting whether it existed.
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) (bool, error)
}
