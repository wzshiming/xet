package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// ObjectKey returns the slash-separated two-level fanout key of a hash-named
// object: <kind>/<name[:2]>/<name[2:4]>/<name[4:]>. The fanout keeps
// directory sizes bounded; a flat layout accumulates tens of thousands of
// entries per model.
func ObjectKey(kind, name string) string {
	if len(name) <= 4 {
		return kind + "/" + name
	}
	return kind + "/" + name[:2] + "/" + name[2:4] + "/" + name[4:]
}

// shardHeaderSize is the fixed shard header; its last 8 bytes carry FooterSize.
const shardHeaderSize = 48

// DecodeStoredShard decodes a stored shard object, following the FooterSize its
// header declares so objects written before shards went footerless still load.
func DecodeStoredShard(r io.Reader) (*shard.Shard, error) {
	var header [shardHeaderSize]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, fmt.Errorf("read shard header: %w", err)
	}
	s := shard.NewShard()
	withFooter := binary.LittleEndian.Uint64(header[40:]) != 0
	if err := s.Decode(io.MultiReader(bytes.NewReader(header[:]), r), withFooter); err != nil {
		return nil, err
	}
	return s, nil
}

// EncodeShard serializes s without the footer (it embeds a creation
// timestamp), so the stored bytes — and the hex SHA-256 naming them — are
// deterministic for identical shard content.
func EncodeShard(s *shard.Shard) ([]byte, string, error) {
	r, err := s.Encode(false)
	if err != nil {
		return nil, "", err
	}
	encoded, err := io.ReadAll(r)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(encoded)
	return encoded, hex.EncodeToString(sum[:]), nil
}

// ReadXorbChunkOffsets also supports stored xorbs without a footer.
func ReadXorbChunkOffsets(r io.ReadSeeker) ([]uint64, error) {
	offsets, err := xorb.ReadChunkOffsets(r)
	if errors.Is(err, xorb.ErrNoFooter) {
		offsets, err = xorb.ScanChunkOffsets(r)
	}
	return offsets, err
}
