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
	GetXorbURL(namespace string, xorbHash xet.XorbHash) (string, error)
	GetXorbDataRange(ctx context.Context, namespace string, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32) (startByte, endByte int64, err error)
}

// ClientAdapter provides access to client operations needed for reconstruction decoding
type ClientAdapter interface {
	DownloadXorbWithURL(ctx context.Context, url string, header http.Header) (io.ReadCloser, error)
	DownloadXorbsMultipartWithURL(ctx context.Context, url string, header http.Header) (*multipart.Reader, io.Closer, error)
}
