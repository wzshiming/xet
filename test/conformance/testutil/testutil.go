// Package testutil contains helpers shared by the conformance test packages.
package testutil

import (
	"context"
	"io"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/client"
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
