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
		SetIndexEntry: func(t *testing.T, st storage.Storage, kind, name, shardHash string) {
			st.(*Storage).put(kind, name, []byte(shardHash), true)
		},
		PutRawShardObject: func(t *testing.T, _ context.Context, st storage.Storage, hash string, raw []byte) {
			st.(*Storage).put("shards", hash, raw, true)
		},
	})
}
