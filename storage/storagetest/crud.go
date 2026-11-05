package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

// indexKind reaches one index through its Storage methods. Its test keys are
// numbered: n stands for the n-th key of the index's key type.
type indexKind struct {
	name    string              // the index under index/
	keyName func(n byte) string // the name the index's Walk gives key n
	put     func(st storage.Storage, ctx context.Context, n byte, shardHash string) (bool, error)
	get     func(st storage.Storage, ctx context.Context, n byte) (string, error)
	del     func(st storage.Storage, ctx context.Context, n byte) (bool, error)
	walk    func(st storage.Storage, ctx context.Context, fn func(name, shardHash string) error) error
}

// indexKinds are the three indexes. A hash key's walk name is its String
// form, a SHA-256 digest's its hex encoding.
var indexKinds = []indexKind{{
	name:    "files",
	keyName: func(n byte) string { return xet.FileHash{n}.String() },
	put: func(st storage.Storage, ctx context.Context, n byte, shardHash string) (bool, error) {
		return st.PutFileIndexEntry(ctx, xet.FileHash{n}, shardHash)
	},
	get: func(st storage.Storage, ctx context.Context, n byte) (string, error) {
		return st.GetFileIndexEntry(ctx, xet.FileHash{n})
	},
	del: func(st storage.Storage, ctx context.Context, n byte) (bool, error) {
		return st.DeleteFileIndexEntry(ctx, xet.FileHash{n})
	},
	walk: storage.Storage.WalkFileIndex,
}, {
	name:    "chunks",
	keyName: func(n byte) string { return xet.ChunkHash{n}.String() },
	put: func(st storage.Storage, ctx context.Context, n byte, shardHash string) (bool, error) {
		return st.PutChunkIndexEntry(ctx, xet.ChunkHash{n}, shardHash)
	},
	get: func(st storage.Storage, ctx context.Context, n byte) (string, error) {
		return st.GetChunkIndexEntry(ctx, xet.ChunkHash{n})
	},
	del: func(st storage.Storage, ctx context.Context, n byte) (bool, error) {
		return st.DeleteChunkIndexEntry(ctx, xet.ChunkHash{n})
	},
	walk: storage.Storage.WalkChunkIndex,
}, {
	name: "sha256",
	keyName: func(n byte) string {
		digest := [32]byte{n}
		return hex.EncodeToString(digest[:])
	},
	put: func(st storage.Storage, ctx context.Context, n byte, shardHash string) (bool, error) {
		return st.PutSHA256IndexEntry(ctx, [32]byte{n}, shardHash)
	},
	get: func(st storage.Storage, ctx context.Context, n byte) (string, error) {
		return st.GetSHA256IndexEntry(ctx, [32]byte{n})
	},
	del: func(st storage.Storage, ctx context.Context, n byte) (bool, error) {
		return st.DeleteSHA256IndexEntry(ctx, [32]byte{n})
	},
	walk: storage.Storage.WalkSHA256Index,
}}

// shardName returns a well-formed shard hash made from label; no shard of that name is stored.
func shardName(label string) string {
	sum := sha256.Sum256([]byte(label))
	return hex.EncodeToString(sum[:])
}

// indexEntries returns the "name -> shard hash" pairs walk gives over st, sorted.
func indexEntries(tb testing.TB, ctx context.Context, st storage.Storage, walk func(storage.Storage, context.Context, func(name, shardHash string) error) error) []string {
	tb.Helper()
	var entries []string
	if err := walk(st, ctx, func(name, shardHash string) error {
		entries = append(entries, name+" -> "+shardHash)
		return nil
	}); err != nil {
		tb.Fatalf("walk: %v", err)
	}
	slices.Sort(entries)
	return entries
}

// testIndexPutKeepsExistingEntry: an index Put writes an absent key and
// reports true; on an existing key it writes nothing and reports false.
func testIndexPutKeepsExistingEntry(t *testing.T, b Backend) {
	for _, ix := range indexKinds {
		t.Run(ix.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			first, second := shardName("first"), shardName("second")
			if inserted, err := ix.put(st, ctx, 1, first); err != nil || !inserted {
				t.Fatalf("Put(absent key) = %v, %v; want true", inserted, err)
			}
			if inserted, err := ix.put(st, ctx, 1, second); err != nil || inserted {
				t.Fatalf("Put(existing key) = %v, %v; want false", inserted, err)
			}
			if got, err := ix.get(st, ctx, 1); err != nil || got != first {
				t.Fatalf("Get() = %q, %v; want the first Put's %q", got, err, first)
			}
			if inserted, err := ix.put(st, ctx, 1, first); err != nil || inserted {
				t.Fatalf("Put(existing key, same shard hash) = %v, %v; want false", inserted, err)
			}
		})
	}
}

// testIndexDeleteReportsWhetherEntryExisted: an index Delete of a key never
// written reports false; one of an existing entry removes it and reports
// true; once it is gone, Get gives "" and another Delete reports false. None
// of them is an error.
func testIndexDeleteReportsWhetherEntryExisted(t *testing.T, b Backend) {
	for _, ix := range indexKinds {
		t.Run(ix.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			if removed, err := ix.del(st, ctx, 1); err != nil || removed {
				t.Fatalf("Delete(absent entry) = %v, %v; want false", removed, err)
			}
			if _, err := ix.put(st, ctx, 1, shardName("deleted")); err != nil {
				t.Fatal(err)
			}
			if removed, err := ix.del(st, ctx, 1); err != nil || !removed {
				t.Fatalf("Delete(existing entry) = %v, %v; want true", removed, err)
			}
			if got, err := ix.get(st, ctx, 1); err != nil || got != "" {
				t.Fatalf("Get() after Delete = %q, %v; want \"\"", got, err)
			}
			if removed, err := ix.del(st, ctx, 1); err != nil || removed {
				t.Fatalf("Delete(deleted entry) = %v, %v; want false", removed, err)
			}
		})
	}
}

// testIndexPutAfterDeleteWritesNewEntry: a deleted entry's key is absent
// again, so the next Put writes its new shard hash and reports true.
func testIndexPutAfterDeleteWritesNewEntry(t *testing.T, b Backend) {
	for _, ix := range indexKinds {
		t.Run(ix.name, func(t *testing.T) {
			ctx := context.Background()
			st := b.New(t)
			if _, err := ix.put(st, ctx, 1, shardName("deleted")); err != nil {
				t.Fatal(err)
			}
			if _, err := ix.del(st, ctx, 1); err != nil {
				t.Fatal(err)
			}
			again := shardName("written after the delete")
			if inserted, err := ix.put(st, ctx, 1, again); err != nil || !inserted {
				t.Fatalf("Put(deleted key) = %v, %v; want true", inserted, err)
			}
			if got, err := ix.get(st, ctx, 1); err != nil || got != again {
				t.Fatalf("Get() = %q, %v; want the new %q", got, err, again)
			}
		})
	}
}

// testIndexWalkGivesCurrentEntries: with all three indexes written to, each
// index Walk gives exactly the entries its index holds now, by name and with
// their current shard hashes.
func testIndexWalkGivesCurrentEntries(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	want := map[string][]string{}
	for _, ix := range indexKinds {
		shardHashes := map[byte]string{}
		for n := byte(1); n <= 3; n++ {
			shardHashes[n] = shardName(fmt.Sprintf("%s entry %d", ix.name, n))
			if _, err := ix.put(st, ctx, n, shardHashes[n]); err != nil {
				t.Fatal(err)
			}
		}
		// Key 2 goes; key 3 comes back with another shard hash.
		for _, n := range []byte{2, 3} {
			if _, err := ix.del(st, ctx, n); err != nil {
				t.Fatal(err)
			}
		}
		shardHashes[3] = shardName(ix.name + " entry 3, written again")
		if _, err := ix.put(st, ctx, 3, shardHashes[3]); err != nil {
			t.Fatal(err)
		}
		want[ix.name] = []string{ix.keyName(1) + " -> " + shardHashes[1], ix.keyName(3) + " -> " + shardHashes[3]}
		slices.Sort(want[ix.name])
	}
	for _, ix := range indexKinds {
		t.Run(ix.name, func(t *testing.T) {
			if got := indexEntries(t, ctx, st, ix.walk); !slices.Equal(got, want[ix.name]) {
				t.Errorf("walk = %q, want %q", got, want[ix.name])
			}
		})
	}
}

// testStoragePutXorbStoresGivenBytes: the bare PutXorb validates nothing; it
// stores exactly the bytes it reads and reports true for an absent xorb.
func testStoragePutXorbStoresGivenBytes(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	xorbHash := xet.XorbHash{1}
	data := []byte("no xorb encoding, and not hashing to its name")
	if inserted, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(data)); err != nil || !inserted {
		t.Fatalf("PutXorb() = %v, %v; want true", inserted, err)
	}
	assertXorbBytes(t, ctx, st, xorbHash, data)
}

// testStoragePutXorbKeepsExistingXorb: a PutXorb of a stored xorb writes
// nothing and reports false.
func testStoragePutXorbKeepsExistingXorb(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	xorbHash := xet.XorbHash{1}
	first := []byte("the bytes stored first")
	if _, err := st.PutXorb(ctx, xorbHash, bytes.NewReader(first)); err != nil {
		t.Fatal(err)
	}
	if inserted, err := st.PutXorb(ctx, xorbHash, strings.NewReader("other bytes for the same xorb")); err != nil || inserted {
		t.Fatalf("PutXorb(stored xorb) = %v, %v; want false", inserted, err)
	}
	assertXorbBytes(t, ctx, st, xorbHash, first)
}

// testStoragePutXorbReadErrorStoresNothing: a PutXorb whose reader fails part
// way returns an error and leaves no xorb behind.
func testStoragePutXorbReadErrorStoresNothing(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	xorbHash := xet.XorbHash{1}
	r := io.MultiReader(strings.NewReader("the bytes read before the failure"), iotest.ErrReader(errors.New("connection reset")))
	if inserted, err := st.PutXorb(ctx, xorbHash, r); err == nil || inserted {
		t.Fatalf("PutXorb(failing reader) = %v, %v; want an error", inserted, err)
	}
	if ok, err := st.HasXorb(ctx, xorbHash); err != nil || ok {
		t.Fatalf("HasXorb() after the failed PutXorb = %v, %v; want false", ok, err)
	}
	var walked []string
	if err := st.WalkXorbs(ctx, func(xorbHash string, _ int64, _ time.Time) error {
		walked = append(walked, xorbHash)
		return nil
	}); err != nil || len(walked) != 0 {
		t.Fatalf("WalkXorbs() after the failed PutXorb gave %q, %v; want nothing", walked, err)
	}
}

// assertXorbBytes fails unless the xorb stored as xorbHash holds exactly want.
func assertXorbBytes(tb testing.TB, ctx context.Context, st storage.Storage, xorbHash xet.XorbHash, want []byte) {
	tb.Helper()
	rc, err := st.GetXorbReadSeekCloser(ctx, xorbHash)
	if err != nil {
		tb.Fatalf("GetXorbReadSeekCloser(): %v", err)
	}
	defer rc.Close()
	if got, err := io.ReadAll(rc); err != nil || !bytes.Equal(got, want) {
		tb.Fatalf("stored xorb = %q, %v; want %q", got, err, want)
	}
}

// testStoragePutShardNamesShardByEncodeShard: the bare PutShard stores the
// shard as the bytes and under the name storage.EncodeShard gives it, returns
// that name and reports true; a repeated call returns the same name and
// reports false.
func testStoragePutShardNamesShardByEncodeShard(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	AddFileBlock(t, ctx, st, sh, [][]byte{[]byte("stored by the bare PutShard")})
	encoded, want, err := storage.EncodeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
	if name, inserted, err := st.PutShard(ctx, sh); err != nil || name != want || !inserted {
		t.Fatalf("PutShard() = %q, %v, %v; want %q, true", name, inserted, err, want)
	}
	if name, inserted, err := st.PutShard(ctx, sh); err != nil || name != want || inserted {
		t.Fatalf("repeated PutShard() = %q, %v, %v; want %q, false", name, inserted, err, want)
	}
	if _, err := st.LoadShard(ctx, want); err != nil {
		t.Fatalf("LoadShard(%s): %v", want, err)
	}
	stored := map[string]int64{}
	if err := st.WalkShards(ctx, func(shardHash string, size int64, _ time.Time) error {
		stored[shardHash] = size
		return nil
	}); err != nil || len(stored) != 1 || stored[want] != int64(len(encoded)) {
		t.Fatalf("WalkShards() = %v, %v; want only %s holding the %d bytes of EncodeShard", stored, err, want, len(encoded))
	}
}

// testStoragePutShardWritesNoIndexEntry: the bare PutShard stores the shard
// object only, even for a shard storage.PutShard would index in full.
func testStoragePutShardWritesNoIndexEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	sh := shard.NewShard()
	AddFileBlock(t, ctx, st, sh, [][]byte{[]byte("stored without index entries")})
	if err := storage.PrepareShard(ctx, sh, st); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.PutShard(ctx, sh); err != nil {
		t.Fatal(err)
	}
	for _, ix := range indexKinds {
		if got := indexEntries(t, ctx, st, ix.walk); len(got) != 0 {
			t.Errorf("index/%s holds %q after PutShard, want nothing", ix.name, got)
		}
	}
}

// testObjectDeleteReportsWhetherObjectExisted: DeleteXorb and DeleteShard
// report true for a stored object and false for an absent one, neither an
// error.
func testObjectDeleteReportsWhetherObjectExisted(t *testing.T, b Backend) {
	ctx := context.Background()
	xorbHash, sh := xet.XorbHash{1}, shard.NewShard()
	_, shardHash, err := storage.EncodeShard(sh)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct {
		kind  string
		store func(st storage.Storage) error
		del   func(st storage.Storage) (bool, error)
	}{{
		kind: "xorb",
		store: func(st storage.Storage) error {
			_, err := st.PutXorb(ctx, xorbHash, strings.NewReader("xorb bytes"))
			return err
		},
		del: func(st storage.Storage) (bool, error) { return st.DeleteXorb(ctx, xorbHash) },
	}, {
		kind: "shard",
		store: func(st storage.Storage) error {
			_, _, err := st.PutShard(ctx, sh)
			return err
		},
		del: func(st storage.Storage) (bool, error) { return st.DeleteShard(ctx, shardHash) },
	}} {
		t.Run(target.kind, func(t *testing.T) {
			st := b.New(t)
			if existed, err := target.del(st); err != nil || existed {
				t.Fatalf("Delete(absent) = %v, %v; want false", existed, err)
			}
			if err := target.store(st); err != nil {
				t.Fatal(err)
			}
			if existed, err := target.del(st); err != nil || !existed {
				t.Fatalf("Delete(stored) = %v, %v; want true", existed, err)
			}
			if existed, err := target.del(st); err != nil || existed {
				t.Fatalf("Delete(deleted) = %v, %v; want false", existed, err)
			}
		})
	}
}
