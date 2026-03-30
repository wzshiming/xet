package upload

import (
	"io"

	"github.com/wzshiming/xet/pkg/shard"
	"github.com/wzshiming/xet/pkg/xorb"
)

// EncodeXorb serializes a xorb for upload (full format with footer).
// Used by the client when uploading xorbs to the server.
func EncodeXorb(xorbObj *xorb.Xorb) (io.Reader, error) {
	return xorb.Encode(xorbObj, false)
}

// EncodeShard serializes a shard for upload (full format with footer).
// Used by the client when uploading shards to the server.
func EncodeShard(shardObj *shard.Shard) (io.Reader, error) {
	return shard.Encode(shardObj, false)
}

// EncodeChunkQueryResponse serializes a shard for a chunk deduplication
// query response (without footer). Used by the server when responding
// to chunk dedup queries.
func EncodeChunkQueryResponse(shardObj *shard.Shard) (io.Reader, error) {
	return shard.Encode(shardObj, false)
}
