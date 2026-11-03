// Package testutil contains helpers shared by the conformance test packages.
package testutil

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/test/conformance/rustref"
)

// UploadFileWithProtocol uploads a file through the shard API version selected
// by protocol.
func UploadFileWithProtocol(ctx context.Context, c *client.Client, protocol rustref.ProtocolVersion, r io.ReadSeeker) (xet.FileHash, error) {
	if protocol == rustref.ProtocolV2 {
		return c.UploadFileV2(ctx, r)
	}
	return c.UploadFileV1(ctx, r)
}

// DownloadFileWithProtocol downloads a file through the reconstruction API
// version selected by protocol.
func DownloadFileWithProtocol(ctx context.Context, c *client.Client, protocol rustref.ProtocolVersion, hash xet.FileHash, w io.WriteSeeker) error {
	if protocol == rustref.ProtocolV2 {
		return c.DownloadFileV2(ctx, hash, w)
	}
	return c.DownloadFileV1(ctx, hash, w)
}

// LocalChunkHashes returns the hashes of data's chunks in file order.
func LocalChunkHashes(t *testing.T, data []byte) []xet.ChunkHash {
	t.Helper()
	var hashes []xet.ChunkHash
	err := xet.ChunkData(bytes.NewReader(data), func(offset int64, chunk []byte) error {
		hashes = append(hashes, xet.ComputeChunkHash(chunk))
		return nil
	})
	if err != nil {
		t.Fatalf("chunk fixture data: %v", err)
	}
	return hashes
}

// DedupProbeTargets returns a file's first chunk and the first later chunk
// that is neither that hash nor hash-sampled; ok is false if there is none.
func DedupProbeTargets(hashes []xet.ChunkHash) (fileStart, nonProbeable xet.ChunkHash, ok bool) {
	if len(hashes) == 0 {
		return fileStart, nonProbeable, false
	}
	fileStart = hashes[0]
	for _, h := range hashes[1:] {
		if h != fileStart && !shard.IsChunkGlobalDedupEligible(h, false, 0) {
			return fileStart, h, true
		}
	}
	return fileStart, nonProbeable, false
}

// DecodeDedupShard decodes a dedup-query body with or without a footer, as its header says: the
// Go server and xet-core ≥ 1.7.1 re-encode with one, xet-core ≤ 1.6.0 keeps the footerless upload.
func DecodeDedupShard(body []byte) (*shard.Shard, error) {
	// The 48-byte shard header ends with the uint64 footer size, 0 when there is no footer.
	const headerSize = 48
	if len(body) < headerSize {
		return nil, fmt.Errorf("dedup shard body is %d bytes, shorter than the %d-byte shard header", len(body), headerSize)
	}
	withFooter := binary.LittleEndian.Uint64(body[40:headerSize]) != 0
	s := shard.NewShard()
	if err := s.Decode(bytes.NewReader(body), withFooter); err != nil {
		return nil, err
	}
	return s, nil
}
