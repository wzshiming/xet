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
// packages. Its methods store, read and delete single objects and index
// entries; the package's PutXorb, PutShard and PutFile functions validate and
// index uploads.
//
// On every backend, the Put, Delete and Walk methods follow one rule per kind:
//   - A Put writes only when its key is absent, and then reports true; when
//     the key exists it writes nothing and reports false. Concurrent Puts of
//     one absent key may all write and report true; the last write wins.
//   - A Delete removes its target and reports whether the target existed; an
//     absent target is not an error. Concurrent Deletes of one existing target
//     may all report true.
//   - A Walk gives fn the stored names as strings, not typed keys: an object's
//     name, or an index entry's key and the shard hash it records. A hash
//     key's name is its String form, not the hex of its bytes; a SHA-256
//     digest's is its hex encoding.
type Storage interface {
	// Xorb objects: content-addressed chunk data under xorbs/.

	// PutXorb stores r's bytes as the xorb; an error from r stores nothing.
	// It does not validate them, unlike storage.PutXorb.
	PutXorb(ctx context.Context, xorbHash xet.XorbHash, r io.Reader) (bool, error)

	// HasXorb checks whether an xorb exists.
	HasXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error)

	// GetXorbURL generates a URL for accessing xorb data.
	// A URL routed through the CAS carries xet-core's fixed /v1/xorbs/default/ prefix.
	GetXorbURL(ctx context.Context, xorbHash xet.XorbHash) (string, error)

	// GetXorbReadSeekCloser returns a seekable reader over the whole stored xorb, wrapping fs.ErrNotExist when absent.
	// An inclusive [start, end] from xorb.ChunkDataRangeFromOffsets is read by Seek(start) then reading end-start+1 bytes.
	GetXorbReadSeekCloser(ctx context.Context, xorbHash xet.XorbHash) (io.ReadSeekCloser, error)

	// GetXorbChunkOffsets returns cumulative packed chunk ends, wrapping fs.ErrNotExist when absent.
	GetXorbChunkOffsets(ctx context.Context, xorbHash xet.XorbHash) ([]uint64, error)

	// WalkXorbs calls fn for every stored xorb object.
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error

	// DeleteXorb removes a stored xorb object.
	DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) (bool, error)

	// Shard objects: serialized shards under shards/, resolved through the indexes.

	// PutShard stores the shard, named by the SHA-256 of its stored bytes (EncodeShard),
	// returning that name. storage.PutShard verifies and indexes a shard around this call.
	PutShard(ctx context.Context, shard *shard.Shard) (string, bool, error)

	// GetShard retrieves a shard by file hash
	GetShard(ctx context.Context, fileHash xet.FileHash) (*shard.Shard, error)

	// GetShardByChunkHash retrieves a shard by chunk hash (for deduplication).
	// storage.PutShard writes an entry only for a probe-able chunk of the shard
	// (shard.GlobalDedupEligibleChunks); a hash it did not index is not found.
	GetShardByChunkHash(ctx context.Context, chunkHash xet.ChunkHash) (*shard.Shard, error)

	// GetShardByHash loads by serialized shard hash, wrapping fs.ErrNotExist when absent.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	// LoadShard reads a stored shard bypassing the read cache, which a whole-store sweep would evict.
	LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error)

	// WalkShards calls fn for every stored shard object.
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error

	// DeleteShard removes a stored shard object.
	DeleteShard(ctx context.Context, shardHash string) (bool, error)

	// File lookups by content SHA-256 through index/sha256.

	// GetShardBySHA256 retrieves the shard holding the file with a SHA-256 digest, wrapping fs.ErrNotExist when the digest is unknown.
	// NewReconstructedFile reads the file from its FindFileBySHA256 entries.
	GetShardBySHA256(ctx context.Context, sha256 [32]byte) (*shard.Shard, error)

	// GetFileHashBySHA256 resolves a file's SHA-256 digest to the xet file hash recorded at ingest, wrapping fs.ErrNotExist when the digest is unknown.
	GetFileHashBySHA256(ctx context.Context, sha256 [32]byte) (xet.FileHash, error)

	// Index entries: writes, walks, cache-bypassing reads and deletes.

	// WalkFileIndex calls fn for every index/files entry.
	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string) error) error

	// PutFileIndexEntry records shardHash as the shard holding fileHash in index/files.
	PutFileIndexEntry(ctx context.Context, fileHash xet.FileHash, shardHash string) (bool, error)

	// GetFileIndexEntry returns the shard hash recorded for fileHash, "" when absent, bypassing caches.
	GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error)

	// DeleteFileIndexEntry removes the index/files entry for fileHash.
	DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error)

	// WalkChunkIndex calls fn for every index/chunks entry.
	WalkChunkIndex(ctx context.Context, fn func(chunkHash, shardHash string) error) error

	// PutChunkIndexEntry records shardHash as the shard holding chunkHash in index/chunks.
	PutChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) (bool, error)

	// GetChunkIndexEntry returns the shard hash recorded for chunkHash, "" when absent,
	// bypassing caches. storage.PutShard writes an entry only for a probe-able chunk of the shard
	// (shard.GlobalDedupEligibleChunks); a hash it did not index is not found.
	GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error)

	// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
	// Sweeps call it only for a dead shard's probe-able chunks.
	DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (bool, error)

	// WalkSHA256Index calls fn for every index/sha256 entry.
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error

	// PutSHA256IndexEntry records shardHash as the shard holding the file with SHA-256 digest sha256 in index/sha256.
	PutSHA256IndexEntry(ctx context.Context, sha256 [32]byte, shardHash string) (bool, error)

	// GetSHA256IndexEntry returns the shard hash recorded for sha256, "" when absent, bypassing caches.
	GetSHA256IndexEntry(ctx context.Context, sha256 [32]byte) (string, error)

	// DeleteSHA256IndexEntry removes the index/sha256 entry for sha256.
	DeleteSHA256IndexEntry(ctx context.Context, sha256 [32]byte) (bool, error)

	// Store-wide usage.

	// Usage excludes temporary objects and is not an atomic snapshot.
	Usage(ctx context.Context) (Usage, error)
}
