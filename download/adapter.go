package download

import (
	"context"
	"io"
	"mime/multipart"
	"net/http"

	"github.com/wzshiming/xet"
)

// StorageAdapter provides access to storage operations needed for reconstruction encoding
type StorageAdapter interface {
	GetXorbURL(namespace string, xorbHash xet.Hash) string
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.Hash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)
}

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorb(ctx context.Context, url string, header http.Header) (io.ReadCloser, error)
	DownloadXorbsMultipart(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error)
}
