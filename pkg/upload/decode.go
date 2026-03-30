package upload

import (
	"context"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// StorageAdapter provides access to storage operations needed for upload decoding
type StorageAdapter interface {
	// StoreXorb stores a xorb and returns whether it was newly inserted
	StoreXorb(ctx context.Context, namespace string, xorb *xorb.Xorb) (bool, error)
	// StoreShard stores a shard and returns whether it was newly inserted
	StoreShard(ctx context.Context, shard *shard.Shard) (bool, error)
}

// DecodeAndStoreXorb deserializes a xorb from a reader and stores it
// xorbHash is the expected hash from the URL, used for verification
func DecodeAndStoreXorb(ctx context.Context, storage StorageAdapter, namespace string, xorbHash xet.Hash, body io.Reader) (*XorbUploadResponse, error) {
	// Deserialize xorb with chunkOnly=true to accept both formats
	// (chunks-only from xet-core, full format with footer from Go client)
	deserializedXorb, err := xorb.Decode(body, true)
	if err != nil {
		return nil, fmt.Errorf("invalid xorb format: %w", err)
	}

	// Verify hash matches URL parameter
	if deserializedXorb.Hash != xorbHash {
		return nil, fmt.Errorf("hash mismatch: xorb has %s, URL has %s", deserializedXorb.Hash.String(), xorbHash.String())
	}

	// Store xorb (storage will normalize to full format with footer)
	wasInserted, err := storage.StoreXorb(ctx, namespace, deserializedXorb)
	if err != nil {
		return nil, fmt.Errorf("failed to store xorb: %w", err)
	}

	return &XorbUploadResponse{
		WasInserted: wasInserted,
	}, nil
}

// DecodeAndStoreShard deserializes a shard from a reader and stores it
func DecodeAndStoreShard(ctx context.Context, storage StorageAdapter, body io.Reader) (*ShardUploadResponse, error) {
	// Deserialize shard
	shardObj, err := shard.Decode(body)
	if err != nil {
		return nil, fmt.Errorf("invalid shard format: %w", err)
	}

	// Store shard
	wasInserted, err := storage.StoreShard(ctx, shardObj)
	if err != nil {
		return nil, fmt.Errorf("failed to store shard: %w", err)
	}

	// Convert boolean to result code
	result := 0
	if wasInserted {
		result = 1
	}

	return &ShardUploadResponse{
		Result: result,
	}, nil
}
