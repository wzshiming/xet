package download

import (
	"context"

	"github.com/wzshiming/xet/pkg/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, reqOpts ...interface{}) (*xorb.Xorb, error)
}
