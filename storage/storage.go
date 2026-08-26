package storage

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
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
