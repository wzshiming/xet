package s3

import (
	"bytes"
	"context"
	"testing"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

func TestStorage(t *testing.T) {
	storagetest.Run(t, storagetest.Backend{
		Name: "s3",
		New: func(t *testing.T) storage.Storage {
			return newTestS3Storage(t)
		},
		PutRawShardObject: func(t *testing.T, ctx context.Context, st storage.Storage, hash string, raw []byte) {
			ss := st.(*Storage)
			if err := ss.putObject(ctx, ss.objectKey("shards", hash), bytes.NewReader(raw)); err != nil {
				t.Fatal(err)
			}
		},
	})
}
