package s3

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

func TestStorage(t *testing.T) {
	direct := s3Backend("s3")
	direct.UploadGranted = storagetest.DirectUpload
	storagetest.Run(t, direct)
	storagetest.Run(t, s3Backend("s3-routed", WithPresign(false)))
}

// s3Backend adapts gofakes3-backed stores configured with opts to the suite.
func s3Backend(name string, opts ...Option) storagetest.Backend {
	return storagetest.Backend{
		Name: name,
		New: func(t *testing.T) storage.Storage {
			return newTestS3Storage(t, opts...)
		},
		// S3 grants live as long as its presigned URLs.
		NewWithUploadGrantTTL: func(t *testing.T, ttl time.Duration) storage.Storage {
			return newTestS3Storage(t, append([]Option{WithPresignExpiry(ttl)}, opts...)...)
		},
		SetIndexEntry: func(t *testing.T, st storage.Storage, kind, name, shardHash string) {
			ss := st.(*Storage)
			if err := ss.putIndexObject(context.Background(), ss.objectKey(kind, name), []byte(shardHash)); err != nil {
				t.Fatal(err)
			}
		},
		PutRawShardObject: func(t *testing.T, ctx context.Context, st storage.Storage, hash string, raw []byte) {
			ss := st.(*Storage)
			if err := ss.putObject(ctx, ss.objectKey("shards", hash), bytes.NewReader(raw)); err != nil {
				t.Fatal(err)
			}
		},
	}
}
