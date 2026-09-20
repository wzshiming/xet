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
	})
}
