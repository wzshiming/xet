package local

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/storage/storagetest"
)

// hookedGCStore wraps a GCStore with callbacks fired at sweep-visible points,
// simulating uploads that commit while a sweep is running. A non-zero age
// backdates every modTime the object walks report.
type hookedGCStore struct {
	storage.GCStore
	age                time.Duration
	beforeFileEntryGet func() // consumed on first fire
	beforeShardLoad    func() // consumed on first fire
	// onFileEntryGet fires before every call of the wrapped getter with a
	// 1-based call count, unlike the before* hooks consumed on first fire.
	onFileEntryGet   func(n int)
	fileEntryGets    int
	beforeWalkShards func() // fired before every WalkShards delegation
	beforeWalkXorbs  func() // fired before every WalkXorbs delegation
	loadShardErrs    map[string]error
}

// walkTime substitutes the aged modTime when aging is enabled.
func (h *hookedGCStore) walkTime(modTime time.Time) time.Time {
	if h.age == 0 {
		return modTime
	}
	return time.Now().Add(-h.age)
}

func (h *hookedGCStore) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	if h.beforeWalkShards != nil {
		h.beforeWalkShards()
	}
	return h.GCStore.WalkShards(ctx, func(shardHash string, size int64, modTime time.Time) error {
		return fn(shardHash, size, h.walkTime(modTime))
	})
}

func (h *hookedGCStore) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	if h.beforeWalkXorbs != nil {
		h.beforeWalkXorbs()
	}
	return h.GCStore.WalkXorbs(ctx, func(xorbHash string, size int64, modTime time.Time) error {
		return fn(xorbHash, size, h.walkTime(modTime))
	})
}

func (h *hookedGCStore) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	if h.beforeFileEntryGet != nil {
		cb := h.beforeFileEntryGet
		h.beforeFileEntryGet = nil
		cb()
	}
	if h.onFileEntryGet != nil {
		h.fileEntryGets++
		h.onFileEntryGet(h.fileEntryGets)
	}
	return h.GCStore.GetFileIndexEntry(ctx, fileHash)
}

func (h *hookedGCStore) LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	if h.beforeShardLoad != nil {
		cb := h.beforeShardLoad
		h.beforeShardLoad = nil
		cb()
	}
	if err, ok := h.loadShardErrs[shardHash]; ok {
		return nil, err
	}
	return h.GCStore.LoadShard(ctx, shardHash)
}

// TestSweepSkipsReuploadCommittedDuringMark: a re-upload commits between the
// index walks and the shard walk, so the shard looks unreferenced at the
// mark; sweepShard's guards must read the fresh entries and spare it, and
// phase 2's walk must shield its xorb.
func TestSweepSkipsReuploadCommittedDuringMark(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed during the sweep walks")}
	f := storagetest.PutFile(t, ctx, st, parts)
	storagetest.UnlinkFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	committed := false
	hooked.beforeWalkShards = func() {
		if committed {
			return
		}
		committed = true
		storagetest.PutFile(t, ctx, st, parts)
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: storagetest.NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !committed {
		t.Fatal("hook did not run")
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	storagetest.AssertFileIntact(t, ctx, st, f)
}

// TestSweepSkipsReuploadCommittedBeforeDelete: a writer in another process
// (a second store on the same directory) commits right before the shard
// delete; sweepShard's files guard must abort before the shard, its
// entries, or its xorbs are touched.
func TestSweepSkipsReuploadCommittedBeforeDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed right before the delete")}
	f := storagetest.PutFile(t, ctx, st, parts)
	storagetest.UnlinkFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeFileEntryGet = func() { storagetest.PutFile(t, ctx, writer, parts) }
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: storagetest.NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	if got, err := st.GetChunkIndexEntry(ctx, f.ChunkHashes[0]); err != nil || got != f.ShardHash {
		t.Fatalf("chunk entry = %q, %v; want %q", got, err, f.ShardHash)
	}
	if got, err := st.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.ShardHash)
	}
	storagetest.AssertFileIntact(t, ctx, st, f)
}

// TestSweepShardVanishedBeforeDelete: the shard object disappears between
// the walk and its delete; the sweep counts nothing for it and still sweeps
// the now-unreferenced xorb.
func TestSweepShardVanishedBeforeDelete(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("vanishes before the delete")})
	storagetest.UnlinkFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeShardLoad = func() {
		if err := os.Remove(st.objectPath("shards", f.ShardHash)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: storagetest.NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 {
		t.Fatalf("vanished shard reported swept: %+v", res.SweptShards)
	}
	if got, want := storagetest.SweptHashes(res.SweptXorbs), []string{f.XorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.ReclaimedBytes != res.SweptXorbs[0].Size {
		t.Fatalf("ReclaimedBytes = %d, want %d (xorb only)", res.ReclaimedBytes, res.SweptXorbs[0].Size)
	}
}

// TestSweepGraceShieldsDedupedXorbs: an in-grace shard with no entries is an
// upload mid-commit; the xorbs it deduplicated against, older than the
// grace window themselves, must survive the sweep via phase 2's walk.
func TestSweepGraceShieldsDedupedXorbs(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("deduplicated old payload")})
	storagetest.UnlinkFile(t, ctx, st, f)

	// Age the original objects out of the grace window.
	old := time.Now().Add(-2 * time.Hour)
	for _, p := range []string{
		st.objectPath("shards", f.ShardHash),
		st.objectPath("xorbs", f.XorbHashes[0].String()),
	} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate an upload mid-commit: its shard object is stored (fresh
	// mtime) and references the old xorb, but no index entry exists yet.
	pending := shard.NewShard()
	pending.AddFile(shard.FileBlock{
		FileHash: f.FileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: f.XorbHashes[0], UnpackedSegBytes: uint32(len(f.Content)), ChunkIndexEnd: 1},
		},
	})
	encoded, pendingHash, err := storage.EncodeShard(pending)
	if err != nil {
		t.Fatal(err)
	}
	if err := overwriteIndexFile(st.objectPath("shards", pendingHash), encoded); err != nil {
		t.Fatal(err)
	}

	res, err := storage.Sweep(ctx, st, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := storagetest.SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if len(res.SweptXorbs) != 0 {
		t.Fatalf("deduplicated xorb swept: %+v", res.SweptXorbs)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("shielded xorb removed")
	}
}

// TestSweepStepBudgetProgress: a vanishing budget cannot expire before the
// first sweep, so every step still sweeps at least one object and repeated
// stepping always terminates.
func TestSweepStepBudgetProgress(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	storagetest.PutUnlinkedFiles(t, ctx, st, "budget file one", "budget file two")

	g := storage.NewGC(st)
	opts := storage.SweepOptions{Grace: storagetest.NoGrace, Budget: time.Nanosecond}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || res.RemainingShards != 1 || res.RemainingXorbs != 0 {
		t.Fatalf("first step = done %v, remaining %d/%d; want not done, 1/0 (budget honored)", res.Done, res.RemainingShards, res.RemainingXorbs)
	}
	shards, xorbs := len(res.SweptShards), len(res.SweptXorbs)
	for steps := 0; !res.Done; steps++ {
		if steps > 6 {
			t.Fatalf("stepping not done after %d steps", steps)
		}
		if res, err = g.SweepStep(ctx, opts); err != nil {
			t.Fatalf("SweepStep: %v", err)
		}
		shards += len(res.SweptShards)
		xorbs += len(res.SweptXorbs)
	}
	if shards != 2 || xorbs != 2 {
		t.Fatalf("cumulative result = %d shards, %d xorbs; want 2/2", shards, xorbs)
	}
}

// TestSweepStepPhase2WalkNotChargedToBudget: phase 2's walks always run
// whole and their wall time must not count against the step's Budget — a
// walk slower than the whole budget still leaves the step its full
// allowance for the xorb queue. The first step's bounds run out exactly at
// the shard queue's drain, so it skips phase 2 and reports RemainingXorbs
// 0; the second step reaches the xorb phase unexhausted.
func TestSweepStepPhase2WalkNotChargedToBudget(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	storagetest.PutUnlinkedFiles(t, ctx, st, "budgeted walk one", "budgeted walk two")

	hooked := &hookedGCStore{GCStore: st}
	g := storage.NewGC(hooked)
	res, err := g.SweepStep(ctx, storage.SweepOptions{Grace: storagetest.NoGrace, MaxDeletes: 2})
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || res.RemainingShards != 0 || res.RemainingXorbs != 0 || len(res.SweptShards) != 2 {
		t.Fatalf("first step = done %v, remaining %d/%d; want both shards swept and phase 2 skipped (0/0)", res.Done, res.RemainingShards, res.RemainingXorbs)
	}

	// Both walks of the next step outlast the entire budget; both queued
	// xorbs must still be consumed within it.
	hooked.beforeWalkShards = func() { time.Sleep(300 * time.Millisecond) }
	hooked.beforeWalkXorbs = func() { time.Sleep(300 * time.Millisecond) }
	res, err = g.SweepStep(ctx, storage.SweepOptions{Grace: storagetest.NoGrace, Budget: 250 * time.Millisecond})
	if err != nil {
		t.Fatalf("second SweepStep: %v", err)
	}
	if !res.Done || res.RemainingXorbs != 0 || len(res.SweptXorbs) != 2 {
		t.Fatalf("second step = done %v, remaining xorbs %d, swept xorbs %d; want done 0/2 (the walk must not be charged to the budget)",
			res.Done, res.RemainingXorbs, len(res.SweptXorbs))
	}
}

// TestSweepStepAbortsOnContextCancel: a step whose context dies mid-pass
// returns the error; deletions already performed stick, and the next call
// re-marks from scratch and finishes the job.
func TestSweepStepAbortsOnContextCancel(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	files := storagetest.PutUnlinkedFiles(t, ctx, st, "canceled step one", "canceled step two")

	hooked := &hookedGCStore{GCStore: st}
	g := storage.NewGC(hooked)
	opts := storage.SweepOptions{Grace: storagetest.NoGrace}

	// The cancel fires while the first dead shard is being swept; the file
	// backend ignores contexts, so that shard still completes and the
	// loop's own check stops the pass before the second one.
	stepCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hooked.onFileEntryGet = func(n int) {
		if n == 1 {
			cancel()
		}
	}
	if _, err := g.SweepStep(stepCtx, opts); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SweepStep = %v, want context.Canceled", err)
	}
	hooked.onFileEntryGet = nil

	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep after cancel: %v", err)
	}
	if !res.Done {
		t.Fatalf("second step = %+v, want done", res)
	}
	for _, f := range files {
		if _, err := st.GetShardByHash(ctx, f.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("shard load = %v, want ErrNotExist", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); ok {
			t.Fatal("xorb still stored after both steps")
		}
	}
}

// TestSweepLeavesShardCacheCold: a sweep over a store whose shard cache is
// empty leaves it empty — LoadShard populates nothing, so hot entries of a
// serving process survive its sweeps untouched.
func TestSweepLeavesShardCacheCold(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	fLive := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("cold cache live")})
	fDead := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("cold cache dead")})
	storagetest.UnlinkFile(t, ctx, st, fDead)

	// A second view over the same directory starts with a cold cache.
	sweeper, err := NewStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	res, err := storage.Sweep(ctx, &hookedGCStore{GCStore: sweeper, age: 2 * time.Hour}, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := storagetest.SweptHashes(res.SweptShards), []string{fDead.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if n := sweeper.caches.Shards.Len(); n != 0 {
		t.Fatalf("shard cache holds %d entries after the sweep, want 0", n)
	}
	storagetest.AssertFileIntact(t, ctx, st, fLive)
}

// TestSweepLoadContextErrorAborts: a load failing with the pass's own dying
// context aborts the sweep with that error and must not brand the shard
// unreadable or delete anything of it.
func TestSweepLoadContextErrorAborts(t *testing.T) {
	ctx := context.Background()
	st, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := storagetest.PutFile(t, ctx, st, [][]byte{[]byte("canceled mid-load")})
	storagetest.UnlinkFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.loadShardErrs = map[string]error{f.ShardHash: fmt.Errorf("get object: %w", context.Canceled)}
	if _, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: storagetest.NoGrace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sweep = %v, want context.Canceled", err)
	}
	if _, err := st.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard gone after aborted sweep: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb gone after aborted sweep")
	}
}

// TestSweepAnchorSHA256KeepsNilMetadataFileShards: the unanchorable-file
// exemption's other branch — a stored shard whose file block carries no
// MetadataExt at all. PutShard always installs metadata, so the shard
// object is written directly; file backend only, the sweep logic under
// test is backend-independent.
func TestSweepAnchorSHA256KeepsNilMetadataFileShards(t *testing.T) {
	ctx := context.Background()
	fs, err := NewStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	shardObj := shard.NewShard()
	fileHash, xorbHashes, _ := storagetest.AddFileBlock(t, ctx, fs, shardObj, [][]byte{[]byte("no metadata ext")})
	raw, shardHash, err := storage.EncodeShard(shardObj)
	if err != nil {
		t.Fatal(err)
	}
	path := fs.objectPath("shards", shardHash)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatal(err)
	}
	if err := overwriteIndexFile(fs.objectPath("index/files", fileHash.String()), []byte(shardHash)); err != nil {
		t.Fatal(err)
	}

	// File-referenced and metadata-less: exempt under AnchorSHA256.
	res, err := storage.Sweep(ctx, fs, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: storagetest.NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || res.DeletedFileEntries != 0 {
		t.Fatalf("nil-metadata shard not exempt: %+v", res)
	}
	if got, gerr := fs.GetFileIndexEntry(ctx, fileHash); gerr != nil || got != shardHash {
		t.Fatalf("file entry = %q, %v; want %q", got, gerr, shardHash)
	}

	// Without the file entry nothing anchors or exempts it.
	if _, err := fs.DeleteFileIndexEntry(ctx, fileHash); err != nil {
		t.Fatal(err)
	}
	res, err = storage.Sweep(ctx, fs, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: storagetest.NoGrace})
	if err != nil {
		t.Fatalf("Sweep after unlink: %v", err)
	}
	if got, want := storagetest.SweptHashes(res.SweptShards), []string{shardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := storagetest.SweptHashes(res.SweptXorbs), []string{xorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
}
