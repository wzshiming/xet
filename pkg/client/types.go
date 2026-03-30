package client

import (
	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/pkg/upload"
)

// XorbUploadResponse is an alias for upload.XorbUploadResponse.
type XorbUploadResponse = upload.XorbUploadResponse

// ShardUploadResponse is an alias for upload.ShardUploadResponse.
type ShardUploadResponse = upload.ShardUploadResponse

// DeduplicationInfo represents deduplication information for a chunk
type DeduplicationInfo struct {
	XorbHash   xet.Hash
	ChunkIndex uint32
	Found      bool
}
