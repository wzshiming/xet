package upload

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// DecodeXorb deserializes a xorb from an upload request body and verifies
// that the computed hash matches the expected hash. Used by the server when
// receiving xorb uploads.
func DecodeXorb(body io.Reader, expectedHash xet.Hash) (*xorb.Xorb, error) {
	xorbObj, err := xorb.Decode(body, true)
	if err != nil {
		return nil, fmt.Errorf("invalid xorb format: %w", err)
	}

	if xorbObj.Hash != expectedHash {
		return nil, fmt.Errorf("hash mismatch: xorb has %s, expected %s", xorbObj.Hash.String(), expectedHash.String())
	}

	return xorbObj, nil
}

// DecodeShard deserializes a shard from an upload request body.
// Used by the server when receiving shard uploads.
func DecodeShard(body io.Reader) (*shard.Shard, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("read request body: %w", err)
	}

	shardObj, err := shard.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("invalid shard format: %w", err)
	}

	return shardObj, nil
}

// DecodeChunkQueryResponse deserializes a shard from a chunk deduplication
// query response. Used by the client when reading the server's response
// to chunk dedup queries.
func DecodeChunkQueryResponse(body io.Reader) (*shard.Shard, error) {
	return shard.Decode(body)
}
