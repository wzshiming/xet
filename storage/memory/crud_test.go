package memory

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// indexKind reaches one key of an index through the Storage methods.
type indexKind struct {
	name string
	put  func(st *Storage, shardHash string) (bool, error)
	get  func(st *Storage) (string, error)
	del  func(st *Storage) (bool, error)
}

func indexKinds() []indexKind {
	ctx := context.Background()
	fileHash, chunkHash, digest := xet.FileHash{1}, xet.ChunkHash{2}, [32]byte{3}
	return []indexKind{{
		name: "files",
		put: func(st *Storage, shardHash string) (bool, error) {
			return st.PutFileIndexEntry(ctx, fileHash, shardHash)
		},
		get: func(st *Storage) (string, error) { return st.GetFileIndexEntry(ctx, fileHash) },
		del: func(st *Storage) (bool, error) { return st.DeleteFileIndexEntry(ctx, fileHash) },
	}, {
		name: "chunks",
		put: func(st *Storage, shardHash string) (bool, error) {
			return st.PutChunkIndexEntry(ctx, chunkHash, shardHash)
		},
		get: func(st *Storage) (string, error) { return st.GetChunkIndexEntry(ctx, chunkHash) },
		del: func(st *Storage) (bool, error) { return st.DeleteChunkIndexEntry(ctx, chunkHash) },
	}, {
		name: "sha256",
		put: func(st *Storage, shardHash string) (bool, error) {
			return st.PutSHA256IndexEntry(ctx, digest, shardHash)
		},
		get: func(st *Storage) (string, error) { return st.GetSHA256IndexEntry(ctx, digest) },
		del: func(st *Storage) (bool, error) { return st.DeleteSHA256IndexEntry(ctx, digest) },
	}}
}

// deleteTarget stores one object or index entry and deletes it through the Storage methods.
type deleteTarget struct {
	name  string
	store func(st *Storage) (bool, error)
	del   func(st *Storage) (bool, error)
}

func deleteTargets(t *testing.T) []deleteTarget {
	ctx := context.Background()
	xorbHash, sh := xet.XorbHash{4}, shard.NewShard()
	_, shardHash, err := storage.EncodeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
	targets := []deleteTarget{{
		name:  "xorb",
		store: func(st *Storage) (bool, error) { return st.PutXorb(ctx, xorbHash, strings.NewReader("xorb bytes")) },
		del:   func(st *Storage) (bool, error) { return st.DeleteXorb(ctx, xorbHash) },
	}, {
		name: "shard",
		store: func(st *Storage) (bool, error) {
			_, inserted, err := st.PutShard(ctx, sh)
			return inserted, err
		},
		del: func(st *Storage) (bool, error) { return st.DeleteShard(ctx, shardHash) },
	}}
	for _, kind := range indexKinds() {
		targets = append(targets, deleteTarget{
			name:  "index/" + kind.name,
			store: func(st *Storage) (bool, error) { return kind.put(st, "shard") },
			del:   kind.del,
		})
	}
	return targets
}

// concurrently releases n goroutines at once, the i-th running call(i), and
// returns the i of every call that reported true.
func concurrently(t *testing.T, n int, call func(i int) (bool, error)) []int {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		trues []int
	)
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			ok, err := call(i)
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				trues = append(trues, i)
				mu.Unlock()
			}
		})
	}
	close(start)
	wg.Wait()
	return trues
}

func TestConcurrentIndexPutsOfOneKeyInsertOnce(t *testing.T) {
	const callers, rounds = 16, 100
	for _, kind := range indexKinds() {
		t.Run(kind.name, func(t *testing.T) {
			for range rounds {
				st := NewStorage()
				inserted := concurrently(t, callers, func(i int) (bool, error) {
					return kind.put(st, fmt.Sprint("shard ", i))
				})
				if len(inserted) != 1 {
					t.Fatalf("%d of %d concurrent Puts of one key reported true, want 1", len(inserted), callers)
				}
				want := fmt.Sprint("shard ", inserted[0])
				if got, err := kind.get(st); err != nil || got != want {
					t.Fatalf("Get() = %q, %v; want %q from the Put that reported true", got, err, want)
				}
			}
		})
	}
}

func TestConcurrentDeletesOfOneTargetReportOnce(t *testing.T) {
	const callers, rounds = 16, 100
	for _, target := range deleteTargets(t) {
		t.Run(target.name, func(t *testing.T) {
			for range rounds {
				st := NewStorage()
				if _, err := target.store(st); err != nil {
					t.Fatal(err)
				}
				existed := concurrently(t, callers, func(int) (bool, error) { return target.del(st) })
				if len(existed) != 1 {
					t.Fatalf("%d of %d concurrent Deletes of one target reported true, want 1", len(existed), callers)
				}
			}
		})
	}
}
