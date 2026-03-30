package download

import (
	"context"
	"net/http"

	"github.com/wzshiming/xet/pkg/xorb"
)

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (*xorb.Xorb, error)
}
