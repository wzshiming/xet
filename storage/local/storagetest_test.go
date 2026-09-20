package local

import (
	"context"
	"testing"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

func TestStorage(t *testing.T) {
	storagetest.Run(t, storagetest.Backend{
		Name: "file",
		New: func(t *testing.T) storage.Storage {
			st, err := NewStorage(WithBasePath(t.TempDir()))
			if err != nil {
				t.Fatal(err)
			}
			return st
		},
		SetIndexEntry: func(t *testing.T, st storage.Storage, kind, name, shardHash string) {
			if err := overwriteIndexFile(st.(*Storage).objectPath(kind, name), []byte(shardHash)); err != nil {
				t.Fatal(err)
			}
		},
		PutRawShardObject: func(t *testing.T, _ context.Context, st storage.Storage, hash string, raw []byte) {
			if err := overwriteIndexFile(st.(*Storage).objectPath("shards", hash), raw); err != nil {
				t.Fatal(err)
			}
		},
	})
}
