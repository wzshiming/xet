package upload

import (
	"io"

	"github.com/wzshiming/xet/shard"
)

// EncodeShard serializes a shard for upload (without footer).
// Used by the client when uploading shards to the server.
func EncodeShard(shardObj *shard.Shard) (io.Reader, error) {
	return shard.Encode(shardObj, false)
}
