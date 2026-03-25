package client

import "github.com/wzshiming/xet/pkg/xet"

// ReconstructionResponse represents the response from the file reconstruction API
type ReconstructionResponse struct {
	OffsetIntoFirstRange int64  `json:"offset_into_first_range"`
	Terms                []Term `json:"terms"`
	FetchInfo            map[string][]FetchInfoEntry `json:"fetch_info"`
}

// Term represents a single term in the file reconstruction
type Term struct {
	Hash           string     `json:"hash"`
	UnpackedLength uint64     `json:"unpacked_length"`
	Range          ChunkRange `json:"range"`
}

// ChunkRange represents a chunk index range [start, end)
type ChunkRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"` // Exclusive
}

// FetchInfoEntry represents fetch information for downloading xorb data
type FetchInfoEntry struct {
	Range    ChunkRange `json:"range"`
	URL      string     `json:"url"`
	URLRange ByteRange  `json:"url_range"`
}

// ByteRange represents a byte range [start, end] (inclusive)
type ByteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"` // Inclusive
}

// XorbUploadResponse represents the response from uploading an xorb
type XorbUploadResponse struct {
	WasInserted bool `json:"was_inserted"`
}

// ShardUploadResponse represents the response from uploading a shard
type ShardUploadResponse struct {
	Result int `json:"result"` // 0 = already exists, 1 = was registered
}

// DeduplicationInfo represents deduplication information for a chunk
type DeduplicationInfo struct {
	XorbHash   xet.Hash
	ChunkIndex uint32
	Found      bool
}
