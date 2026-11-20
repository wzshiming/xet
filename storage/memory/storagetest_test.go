package memory

import (
	"context"
	"testing"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

func TestStorage(t *testing.T) {
	storagetest.Run(t, storagetest.Backend{
		Name: "memory",
		New: func(t *testing.T) storage.Storage {
			return NewStorage()
		},
		PutRawShardObject: func(t *testing.T, _ context.Context, st storage.Storage, hash string, raw []byte) {
			st.(*Storage).put("shards", hash, raw, true)
		},
	})
}
