package download

import (
	"context"

	"github.com/wzshiming/xet"
)

// StorageAdapter provides access to storage operations needed for reconstruction encoding
type StorageAdapter interface {
	GetXorbURL(namespace string, xorbHash xet.Hash) string
	CompressedDataRange(ctx context.Context, namespace string, xorbHash xet.Hash, chunkStart, chunkEnd uint32) (startByte, endByte int64)
}
