package client

import (
	"github.com/wzshiming/xet"
)

// DeduplicationInfo represents deduplication information for a chunk
type DeduplicationInfo struct {
	XorbHash   xet.Hash
	ChunkIndex uint32
	Found      bool
}
