package storagetest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	iofs "io/fs"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/storage"
)

func testUnlinkRemovesFileIndexEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("unlink me")})

	// Warm the file index cache so the unlink must evict it.
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard before unlink: %v", err)
	}

	removed, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("Unlink: %v", err)
	}
	if !removed {
		t.Fatal("Unlink reported the entry missing")
	}

	if _, err := st.GetShard(ctx, f.FileHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetShard after unlink = %v, want ErrNotExist", err)
	}

	removed, err = storage.NewGC(gcs).Unlink(ctx, f.FileHash)
	if err != nil {
		t.Fatalf("second Unlink: %v", err)
	}
	if removed {
		t.Fatal("second Unlink reported an entry")
	}

	// The shard, xorbs, and sha256/chunk indexes survive until a sweep.
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard should survive unlink: %v", err)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("sha256 entry after unlink = %q, %v; want %q", got, err, f.ShardHash)
	}
}

// testUnlinkSHA256RemovesEntry: UnlinkSHA256 drops only the index/sha256
// entry — SHA-256 lookups stop resolving at once while file-hash access
// keeps working — and reports existence like Unlink; the all-zero digest is
// rejected.
func testUnlinkSHA256RemovesEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("unlink my sha256")})

	// Warm the sha256 cache so the unlink must evict it.
	rc, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()

	removed, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex))
	if err != nil {
		t.Fatalf("UnlinkSHA256: %v", err)
	}
	if !removed {
		t.Fatal("UnlinkSHA256 reported the entry missing")
	}

	if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != "" {
		t.Fatalf("sha256 entry after unlink = %q, %v; want removed", got, err)
	}
	if _, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex)); err == nil {
		t.Fatal("sha256 reconstruction still resolves")
	}
	if _, err := st.GetFileHashBySHA256(ctx, "default", SHA256Digest(f.SHA256Hex)); err == nil {
		t.Fatal("GetFileHashBySHA256 still resolves")
	}

	// File-hash paths keep working; nothing else was touched.
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard after UnlinkSHA256: %v", err)
	}
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard should survive UnlinkSHA256: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb removed by UnlinkSHA256")
	}

	// Second unlink through the GC delegate reports the entry gone.
	removed, err = storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex))
	if err != nil {
		t.Fatalf("second UnlinkSHA256: %v", err)
	}
	if removed {
		t.Fatal("second UnlinkSHA256 reported an entry")
	}

	if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, [32]byte{}); err == nil {
		t.Fatal("all-zero digest accepted")
	}
}

// testSweepNeedsBothUnlinks: a shard stays live while either its file entry
// or its non-zero sha256 entry remains; only unlinking both lets a sweep
// reclaim it.
func testSweepNeedsBothUnlinks(t *testing.T, b Backend) {
	t.Run("file-unlink-only", func(t *testing.T) {
		ctx := context.Background()
		st := b.New(t)
		gcs := st.(storage.GCStore)

		f := PutFile(t, ctx, st, [][]byte{[]byte("file entry unlinked only")})
		if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
			t.Fatal(err)
		}
		res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
			t.Fatalf("sha-anchored shard swept: %+v", res)
		}
		// The content stays resolvable through its SHA-256.
		if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
			t.Fatalf("shard should survive: %v", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
			t.Fatal("xorb swept")
		}
		rc, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex))
		if err != nil {
			t.Fatalf("GetReconstructedFile: %v", err)
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil || !bytes.Equal(data, f.Content) {
			t.Fatalf("reconstruction corrupted: %v", err)
		}
	})
	t.Run("sha-unlink-only", func(t *testing.T) {
		ctx := context.Background()
		st := b.New(t)
		gcs := st.(storage.GCStore)

		f := PutFile(t, ctx, st, [][]byte{[]byte("sha entry unlinked only")})
		if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
			t.Fatal(err)
		}
		res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
			t.Fatalf("file-anchored shard swept: %+v", res)
		}
		// File-hash access keeps working.
		if _, err := st.GetShard(ctx, f.FileHash); err != nil {
			t.Fatalf("GetShard: %v", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
			t.Fatal("xorb swept")
		}
	})
	t.Run("both-unlinked", func(t *testing.T) {
		ctx := context.Background()
		st := b.New(t)
		gcs := st.(storage.GCStore)

		f := PutFile(t, ctx, st, [][]byte{[]byte("both entries unlinked")})
		UnlinkFile(t, ctx, gcs, f)
		res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
			t.Fatalf("SweptShards = %v, want %v", got, want)
		}
		if got, want := SweptHashes(res.SweptXorbs), []string{f.XorbHashes[0].String()}; !slices.Equal(got, want) {
			t.Fatalf("SweptXorbs = %v, want %v", got, want)
		}
		if res.DeletedChunkEntries != 1 {
			t.Fatalf("DeletedChunkEntries = %d, want 1", res.DeletedChunkEntries)
		}
		// Both entries were unlinked up front; no zero entry exists, so
		// the shard's cleanup deletes no sha256 entries.
		if res.DeletedSHA256Entries != 0 {
			t.Fatalf("DeletedSHA256Entries = %d, want 0", res.DeletedSHA256Entries)
		}
		if _, err := gcs.GetShardByHash(ctx, f.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("dead shard load = %v, want ErrNotExist", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); ok {
			t.Fatal("xorb still stored")
		}
		if got, _ := gcs.GetChunkIndexEntry(ctx, f.ChunkHashes[0]); got != "" {
			t.Fatalf("chunk entry = %q, want removed", got)
		}
	})
}

func testSweepRemovesOrphanedObjects(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	shared := []byte("chunk shared by both files")
	fileA := PutFile(t, ctx, st, [][]byte{shared, []byte("exclusive to A")})
	fileB := PutFile(t, ctx, st, [][]byte{shared, []byte("exclusive to B")})
	if fileA.XorbHashes[0] != fileB.XorbHashes[0] {
		t.Fatal("test setup: shared part must map to one xorb")
	}

	// Warm every cache the sweep must invalidate.
	if _, err := st.GetShard(ctx, fileA.FileHash); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetShardByChunkHash(ctx, "default", fileA.ChunkHashes[1]); err != nil {
		t.Fatal(err)
	}
	rc, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(fileA.SHA256Hex))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()

	UnlinkFile(t, ctx, gcs, fileA)
	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if got, want := SweptHashes(res.SweptShards), []string{fileA.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptXorbs), []string{fileA.XorbHashes[1].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.ReclaimedBytes <= 0 {
		t.Fatalf("ReclaimedBytes = %d, want > 0", res.ReclaimedBytes)
	}
	// The sha256 entry was unlinked up front; only chunk entries of
	// the dead shard remain to clean.
	if res.DeletedSHA256Entries != 0 {
		t.Fatalf("DeletedSHA256Entries = %d, want 0", res.DeletedSHA256Entries)
	}
	if res.DeletedChunkEntries < 1 {
		t.Fatalf("DeletedChunkEntries = %d, want >= 1", res.DeletedChunkEntries)
	}

	// Dead shard and its exclusive xorb are gone, shared xorb stays.
	if _, err := gcs.GetShardByHash(ctx, fileA.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", fileA.XorbHashes[1]); ok {
		t.Fatal("exclusive xorb of the dead shard still stored")
	}
	if ok, _ := st.HasXorb(ctx, "default", fileA.XorbHashes[0]); !ok {
		t.Fatal("shared xorb was swept while file B references it")
	}

	// Stale caches must not resurrect swept state.
	if _, err := st.GetShardByChunkHash(ctx, "default", fileA.ChunkHashes[1]); err == nil {
		t.Fatal("chunk lookup for the dead shard's exclusive chunk still resolves")
	}
	if _, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(fileA.SHA256Hex)); err == nil {
		t.Fatal("sha256 lookup for the swept file still resolves")
	}

	// Chunk entries never point at the dead shard: an entry it owned
	// is deleted (a dedup miss until rewritten), one owned by the
	// live shard survives.
	if got, err := gcs.GetChunkIndexEntry(ctx, fileA.ChunkHashes[0]); err != nil || got == fileA.ShardHash {
		t.Fatalf("shared chunk entry = %q, %v; must not point at the dead shard", got, err)
	}
	if got, err := gcs.GetChunkIndexEntry(ctx, fileA.ChunkHashes[1]); err != nil || got != "" {
		t.Fatalf("exclusive chunk entry = %q, %v; want removed", got, err)
	}

	// File B is untouched.
	if _, err := st.GetShard(ctx, fileB.FileHash); err != nil {
		t.Fatalf("GetShard(fileB): %v", err)
	}
	rc, err = st.GetReconstructedFile(ctx, "default", SHA256Digest(fileB.SHA256Hex))
	if err != nil {
		t.Fatalf("GetReconstructedFile(fileB): %v", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(data, fileB.Content) {
		t.Fatalf("file B reconstruction corrupted: %v", err)
	}
}

func testSweepDryRunDeletesNothing(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("dry run target")})
	UnlinkFile(t, ctx, gcs, f)

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace, DryRun: true})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !res.DryRun {
		t.Fatal("result not marked dry run")
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptXorbs), []string{f.XorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.DeletedChunkEntries != 0 || res.DeletedSHA256Entries != 0 {
		t.Fatalf("dry run reported entry mutations: %+v", res)
	}

	// Everything is still stored.
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard removed by dry run: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb removed by dry run")
	}
	if got, _ := gcs.GetChunkIndexEntry(ctx, f.ChunkHashes[0]); got != f.ShardHash {
		t.Fatalf("chunk entry = %q, want %q", got, f.ShardHash)
	}
}

// testSweepDryRunParity: over a mixed store — a dead file, a live file, and
// a live empty file — a dry run reports exactly what a real pass then
// removes, byte for byte, while counting no entry deletions.
func testSweepDryRunParity(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	doomed := PutFile(t, ctx, st, [][]byte{[]byte("doomed by both unlinks")})
	UnlinkFile(t, ctx, gcs, doomed)
	kept := PutFile(t, ctx, st, [][]byte{[]byte("kept alive by its entries")})
	empty := PutFile(t, ctx, st, nil)

	dry, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace, DryRun: true})
	if err != nil {
		t.Fatalf("dry Sweep: %v", err)
	}
	if !dry.DryRun {
		t.Fatal("result not marked dry run")
	}
	wet, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	if got, want := SweptHashes(dry.SweptShards), SweptHashes(wet.SweptShards); !slices.Equal(got, want) {
		t.Fatalf("dry SweptShards = %v, real = %v", got, want)
	}
	if got, want := SweptHashes(dry.SweptXorbs), SweptHashes(wet.SweptXorbs); !slices.Equal(got, want) {
		t.Fatalf("dry SweptXorbs = %v, real = %v", got, want)
	}
	if dry.ReclaimedBytes != wet.ReclaimedBytes {
		t.Fatalf("dry ReclaimedBytes = %d, real %d", dry.ReclaimedBytes, wet.ReclaimedBytes)
	}
	if got, want := SweptHashes(wet.SweptShards), []string{doomed.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("real SweptShards = %v, want %v", got, want)
	}
	// Dry runs never count entry deletions; the real pass does.
	if dry.DeletedChunkEntries != 0 {
		t.Fatalf("dry DeletedChunkEntries = %d, want 0", dry.DeletedChunkEntries)
	}
	if wet.DeletedChunkEntries != 1 {
		t.Fatalf("real DeletedChunkEntries = %d, want 1", wet.DeletedChunkEntries)
	}

	// The real sweep removed exactly the doomed file and spared the
	// anchored and empty shards.
	AssertFileIntact(t, ctx, st, kept)
	if _, err := st.GetShard(ctx, empty.FileHash); err != nil {
		t.Fatalf("GetShard(empty): %v", err)
	}
	if _, err := st.GetShard(ctx, doomed.FileHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetShard(doomed) = %v, want ErrNotExist", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", doomed.XorbHashes[0]); ok {
		t.Fatal("doomed xorb still stored")
	}
}

func testSweepGraceWindow(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("fresh object")})
	UnlinkFile(t, ctx, gcs, f)

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("grace window ignored: %+v", res)
	}
	// The shard is counted; its xorb is shielded by the in-grace
	// shard's presence in the phase-2 walk, not by the window.
	if res.SkippedInGrace != 1 {
		t.Fatalf("SkippedInGrace = %d, want 1", res.SkippedInGrace)
	}

	res, err = storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep without grace: %v", err)
	}
	if len(res.SweptShards) != 1 || len(res.SweptXorbs) != 1 {
		t.Fatalf("sweep without grace missed objects: %+v", res)
	}
}

// testSweepNegativeGraceSentinelSweepsFreshObjects: a negative grace (the
// HTTP "window disabled" sentinel) puts the cutoff just past now; the
// object walks must compare against it untruncated — flooring a future
// cutoff to the whole second would wrongly shield objects written in the
// current second, re-enabling a window the caller disabled.
func testSweepNegativeGraceSentinelSweepsFreshObjects(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)
	f := PutFile(t, ctx, st, [][]byte{[]byte("fresh but window disabled")})
	UnlinkFile(t, ctx, gcs, f)

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: -time.Nanosecond})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptXorbs), []string{f.XorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.SkippedInGrace != 0 {
		t.Fatalf("SkippedInGrace = %d, want 0", res.SkippedInGrace)
	}
}

// testSweepDeletesSharedChunkEntry: two files share chunk A while each also
// has its own chunk. Sweeping the shard that owns A's entry deletes the
// entry outright — a dedup miss until a future upload rewrites it, never
// data loss — and the live shard's data stays resolvable by file hash.
func testSweepDeletesSharedChunkEntry(t *testing.T, b Backend) {
	partA := []byte("chunk shared by both shards")
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f1 := PutFile(t, ctx, st, [][]byte{partA, []byte("unique to file one")})
	f2 := PutFile(t, ctx, st, [][]byte{partA, []byte("unique to file two")})
	if f1.ChunkHashes[0] != f2.ChunkHashes[0] {
		t.Fatal("test setup: shared part must map to one chunk hash")
	}
	// FileStorage keeps the first writer (f1), S3 the last (f2); kill
	// the owner so the shared-entry deletion runs on both backends.
	owner, err := gcs.GetChunkIndexEntry(ctx, f1.ChunkHashes[0])
	if err != nil {
		t.Fatal(err)
	}
	dead, live := f1, f2
	if owner == f2.ShardHash {
		dead, live = f2, f1
	} else if owner != f1.ShardHash {
		t.Fatalf("chunk entry owner = %q, want one of the two shards", owner)
	}
	// Warm the chunk cache so the delete must evict it.
	if _, err := st.GetShardByChunkHash(ctx, "default", f1.ChunkHashes[0]); err != nil {
		t.Fatal(err)
	}

	UnlinkFile(t, ctx, gcs, dead)
	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	// Both entries the dead shard owned — shared and exclusive.
	if res.DeletedChunkEntries != 2 {
		t.Fatalf("DeletedChunkEntries = %d, want 2", res.DeletedChunkEntries)
	}

	// The shared entry is gone (dedup miss accepted), not repointed.
	if got, err := gcs.GetChunkIndexEntry(ctx, live.ChunkHashes[0]); err != nil || got != "" {
		t.Fatalf("shared chunk entry = %q, %v; want removed", got, err)
	}
	if _, err := st.GetShardByChunkHash(ctx, "default", live.ChunkHashes[0]); err == nil {
		t.Fatal("shared chunk lookup still resolves after the entry delete")
	}
	// The dead shard's exclusive entry is gone; the live shard's own
	// exclusive entry is untouched.
	if got, err := gcs.GetChunkIndexEntry(ctx, dead.ChunkHashes[1]); err != nil || got != "" {
		t.Fatalf("dead exclusive chunk entry = %q, %v; want removed", got, err)
	}
	if got, err := gcs.GetChunkIndexEntry(ctx, live.ChunkHashes[1]); err != nil || got != live.ShardHash {
		t.Fatalf("live exclusive chunk entry = %q, %v; want %q", got, err, live.ShardHash)
	}

	// The live file itself is whole: file hash and SHA-256 resolve.
	AssertFileIntact(t, ctx, st, live)
	if ok, _ := st.HasXorb(ctx, "default", live.XorbHashes[0]); !ok {
		t.Fatal("shared xorb was swept")
	}
}

func testSweepReportsDanglingFileEntries(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	fileHash := strings.Repeat("ab", 32)
	b.SetIndexEntry(t, st, "index/files", fileHash, strings.Repeat("cd", 32))

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := res.DanglingFileEntries, []string{fileHash}; !slices.Equal(got, want) {
		t.Fatalf("DanglingFileEntries = %v, want %v", got, want)
	}

	// The entry is reported, never deleted.
	found := false
	if err := gcs.WalkFileIndex(ctx, func(fh, _ string) error {
		if fh == fileHash {
			found = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("dangling file entry was deleted")
	}
}

// testSweepReportsDanglingSHA256Entries: deleting shard objects directly
// orphans their file and sha256 entries; both kinds are reported — sorted,
// in dry runs and real sweeps alike — and never deleted.
func testSweepReportsDanglingSHA256Entries(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	fA := PutFile(t, ctx, st, [][]byte{[]byte("dangling sha one")})
	fB := PutFile(t, ctx, st, [][]byte{[]byte("dangling sha two")})
	for _, f := range []File{fA, fB} {
		if err := gcs.DeleteShard(ctx, f.ShardHash); err != nil {
			t.Fatal(err)
		}
	}

	wantSHA := []string{fA.SHA256Hex, fB.SHA256Hex}
	slices.Sort(wantSHA)
	wantFiles := []string{fA.FileHash.String(), fB.FileHash.String()}
	slices.Sort(wantFiles)
	for _, dryRun := range []bool{true, false} {
		res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace, DryRun: dryRun})
		if err != nil {
			t.Fatalf("Sweep(dryRun=%v): %v", dryRun, err)
		}
		if !slices.Equal(res.DanglingSHA256Entries, wantSHA) {
			t.Fatalf("DanglingSHA256Entries(dryRun=%v) = %v, want %v", dryRun, res.DanglingSHA256Entries, wantSHA)
		}
		if !slices.Equal(res.DanglingFileEntries, wantFiles) {
			t.Fatalf("DanglingFileEntries(dryRun=%v) = %v, want %v", dryRun, res.DanglingFileEntries, wantFiles)
		}
	}

	// The entries are reported, never deleted.
	for _, f := range []File{fA, fB} {
		if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
			t.Fatalf("sha256 entry %s = %q, %v; want %q", f.SHA256Hex, got, err, f.ShardHash)
		}
		if got, err := gcs.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != f.ShardHash {
			t.Fatalf("file entry %s = %q, %v; want %q", f.FileHash.String(), got, err, f.ShardHash)
		}
	}
}

func testSweepThenReuploadResurrects(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	parts := [][]byte{[]byte("sweep, then upload again")}
	f := PutFile(t, ctx, st, parts)
	UnlinkFile(t, ctx, gcs, f)
	if _, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace}); err != nil {
		t.Fatal(err)
	}

	again := PutFile(t, ctx, st, parts)
	if again.FileHash != f.FileHash || again.ShardHash != f.ShardHash {
		t.Fatal("re-upload produced different hashes")
	}
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard after re-upload: %v", err)
	}
	rc, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex))
	if err != nil {
		t.Fatalf("GetReconstructedFile after re-upload: %v", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(data, f.Content) {
		t.Fatalf("reconstruction after re-upload corrupted: %v", err)
	}
}

// Park the first sweep to check contention through the public API.
func testGCSweepStepSingleFlight(t *testing.T, b Backend) {
	ctx := context.Background()
	hooked := &hookedGCStore{GCStore: b.New(t).(storage.GCStore)}
	g := storage.NewGC(hooked)
	opts := storage.SweepOptions{Grace: NoGrace}

	entered := make(chan struct{})
	release := make(chan struct{})
	var blockOnce, releaseOnce sync.Once
	hooked.beforeWalkShards = func() {
		blockOnce.Do(func() {
			close(entered)
			<-release
		})
	}
	var firstErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, firstErr = g.SweepStep(ctx, opts)
	}()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		<-done
	})

	<-entered
	if _, err := g.SweepStep(ctx, opts); !errors.Is(err, storage.ErrGCBusy) {
		t.Fatalf("concurrent SweepStep = %v, want ErrGCBusy", err)
	}
	releaseOnce.Do(func() { close(release) })
	<-done
	if firstErr != nil {
		t.Fatalf("blocked SweepStep: %v", firstErr)
	}

	if _, err := g.SweepStep(ctx, opts); err != nil {
		t.Fatalf("SweepStep after release: %v", err)
	}
}

// testSweepShieldsCommitDuringShardDeletePhase: an upload sharing a doomed
// xorb commits during the shard-delete phase; phase 2's fresh walk over the
// stored shards must shield the shared xorb.
func testSweepShieldsCommitDuringShardDeletePhase(t *testing.T, b Backend) {
	partA := []byte("shared payload the sweep must keep")
	partB := []byte("unique to the late upload")
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f1 := PutFile(t, ctx, st, [][]byte{partA})
	UnlinkFile(t, ctx, gcs, f1)

	// The hook fires inside sweepShard for the dead shard; file2
	// dedup-hits file1's only xorb.
	var f2 File
	hooked := &hookedGCStore{GCStore: gcs}
	hooked.beforeFileEntryGet = func() {
		f2 = PutFile(t, ctx, st, [][]byte{partA, partB})
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if f2.ShardHash == "" {
		t.Fatal("hook did not run")
	}

	if got, want := SweptHashes(res.SweptShards), []string{f1.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if len(res.SweptXorbs) != 0 {
		t.Fatalf("SweptXorbs = %v, want none", SweptHashes(res.SweptXorbs))
	}
	if ok, _ := st.HasXorb(ctx, "default", f1.XorbHashes[0]); !ok {
		t.Fatal("xorb shared with the late upload was swept")
	}
	AssertFileIntact(t, ctx, st, f2)

	// file1 itself stays swept.
	if _, err := st.GetShard(ctx, f1.FileHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("GetShard(file1) = %v, want ErrNotExist", err)
	}
	if _, err := gcs.GetShardByHash(ctx, f1.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
}

// testSweepStepDrainsInBatches: MaxDeletes=1 steps each run an independent
// pass that re-marks and sweeps exactly one object; repeated until Done,
// their union matches a single full pass over an identically prepared
// store.
func testSweepStepDrainsInBatches(t *testing.T, b Backend) {
	contents := []string{"batched sweep one", "batched sweep two", "batched sweep three"}
	ctx := context.Background()
	st := b.New(t)
	PutUnlinkedFiles(t, ctx, st, contents...)

	g := storage.NewGC(st.(storage.GCStore))
	opts := storage.SweepOptions{Grace: NoGrace, MaxDeletes: 1}
	var sweptShards, sweptXorbs []storage.SweptObject
	var chunkEntries int
	var reclaimed int64
	steps := 0
	for {
		res, err := g.SweepStep(ctx, opts)
		if err != nil {
			t.Fatalf("SweepStep %d: %v", steps, err)
		}
		steps++
		if got := len(res.SweptShards) + len(res.SweptXorbs); got != 1 {
			t.Fatalf("step %d swept %d objects, want exactly 1", steps, got)
		}
		if steps == 1 {
			// The shard queue is cut short before any xorb is judged.
			if res.Done || res.RemainingShards != 2 || res.RemainingXorbs != 0 {
				t.Fatalf("first step = done %v, remaining %d/%d; want not done, 2/0", res.Done, res.RemainingShards, res.RemainingXorbs)
			}
		}
		sweptShards = append(sweptShards, res.SweptShards...)
		sweptXorbs = append(sweptXorbs, res.SweptXorbs...)
		chunkEntries += res.DeletedChunkEntries
		reclaimed += res.ReclaimedBytes
		if res.Done {
			if res.RemainingShards != 0 || res.RemainingXorbs != 0 {
				t.Fatalf("done with remaining %d/%d", res.RemainingShards, res.RemainingXorbs)
			}
			break
		}
		if steps > 20 {
			t.Fatal("stepping did not finish in 20 steps")
		}
	}
	if want := len(contents) * 2; steps != want {
		t.Fatalf("steps = %d, want %d (one item per step)", steps, want)
	}

	// Walk order differs per backend, so compare the results as sets.
	st2 := b.New(t)
	PutUnlinkedFiles(t, ctx, st2, contents...)
	full, err := storage.Sweep(ctx, st2.(storage.GCStore), storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !full.Done || full.RemainingShards != 0 || full.RemainingXorbs != 0 {
		t.Fatalf("full pass progress = %v %d/%d, want done 0/0", full.Done, full.RemainingShards, full.RemainingXorbs)
	}
	if got, want := sortedSwept(sweptShards), sortedSwept(full.SweptShards); !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := sortedSwept(sweptXorbs), sortedSwept(full.SweptXorbs); !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if chunkEntries != full.DeletedChunkEntries {
		t.Fatalf("DeletedChunkEntries = %d, want %d", chunkEntries, full.DeletedChunkEntries)
	}
	if reclaimed != full.ReclaimedBytes {
		t.Fatalf("ReclaimedBytes = %d, want %d", reclaimed, full.ReclaimedBytes)
	}
}

// testSweepStepRecommitBetweenSteps: a re-upload commits between two
// bounded steps; the next step's fresh mark sees the new entries, spares
// the shard, and phase 2 shields its xorb — stateless re-marking replaces
// any carried-over queue.
func testSweepStepRecommitBetweenSteps(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	files := PutUnlinkedFiles(t, ctx, st, "mid-step commit one", "mid-step commit two")

	// Aged mtimes leave the index re-marks as the only shield.
	g := storage.NewGC(agedStore(gcs))
	opts := storage.SweepOptions{Grace: time.Hour, MaxDeletes: 1}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || len(res.SweptShards) != 1 {
		t.Fatalf("first step = %+v, want one swept shard and not done", res)
	}

	// Re-upload the file whose shard is still stored: PutShard
	// recommits its entries.
	kept, gone := files[0], files[1]
	if res.SweptShards[0].Hash == kept.ShardHash {
		kept, gone = gone, kept
	}
	if again := PutFile(t, ctx, st, [][]byte{kept.Content}); again.ShardHash != kept.ShardHash {
		t.Fatal("re-upload produced different hashes")
	}

	for i := 0; !res.Done; i++ {
		if i > 20 {
			t.Fatal("stepping did not finish in 20 steps")
		}
		if res, err = g.SweepStep(ctx, opts); err != nil {
			t.Fatalf("SweepStep: %v", err)
		}
	}

	// The re-linked file survives with its xorb; the other is gone.
	AssertFileIntact(t, ctx, st, kept)
	if ok, _ := st.HasXorb(ctx, "default", kept.XorbHashes[0]); !ok {
		t.Fatal("xorb of the re-uploaded file was swept")
	}
	if _, err := gcs.GetShardByHash(ctx, gone.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", gone.XorbHashes[0]); ok {
		t.Fatal("dead file's xorb still stored")
	}
}

// testSweepStepDryRunIgnoresBounds: a dry-run step reports one full
// unbounded pass whatever the limits say, deleting nothing.
func testSweepStepDryRunIgnoresBounds(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	files := PutUnlinkedFiles(t, ctx, st, "dry bounds one", "dry bounds two")
	g := storage.NewGC(gcs)
	dry, err := g.SweepStep(ctx, storage.SweepOptions{Grace: NoGrace, DryRun: true, MaxDeletes: 1, Budget: time.Nanosecond})
	if err != nil {
		t.Fatalf("dry SweepStep: %v", err)
	}
	if !dry.DryRun || !dry.Done {
		t.Fatalf("dry step = %+v, want a done dry-run report", dry)
	}
	if len(dry.SweptShards) != 2 || len(dry.SweptXorbs) != 2 {
		t.Fatalf("dry report = %d shards, %d xorbs; want 2/2 (bounds ignored)", len(dry.SweptShards), len(dry.SweptXorbs))
	}

	// Nothing was deleted.
	for _, f := range files {
		if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
			t.Fatalf("shard removed by dry step: %v", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
			t.Fatal("xorb removed by dry step")
		}
	}
}

// testSweepNeverTouchesShardCache: every shard load a sweep performs — the
// dead-shard load and phase 2's walk loads — must go through LoadShard,
// never the cache-populating GetShardByHash, so bulk sweeps cannot evict
// hot serving entries.
func testSweepNeverTouchesShardCache(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	fLive := PutFile(t, ctx, st, [][]byte{[]byte("cache-cold live file")})
	fDead := PutFile(t, ctx, st, [][]byte{[]byte("cache-cold dead file")})
	UnlinkFile(t, ctx, gcs, fDead)
	fGrace := PutFile(t, ctx, st, [][]byte{[]byte("cache-cold in-grace upload")})
	UnlinkFile(t, ctx, gcs, fGrace)

	hooked := agedStore(gcs)
	hooked.shardModTimes = map[string]time.Time{
		fGrace.ShardHash: time.Now().Add(-30 * time.Minute),
	}
	// A commit landing mid-sweep exercises the phase-2 load too.
	hooked.beforeFileEntryGet = func() {
		PutFile(t, ctx, st, [][]byte{[]byte("cache-cold mid-sweep commit")})
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{fDead.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if hooked.walkShardsCalls != 2 {
		t.Fatalf("WalkShards called %d times, want 2 (phase 1 + phase 2)", hooked.walkShardsCalls)
	}
	if hooked.cachedShardGets != 0 {
		t.Fatalf("GetShardByHash called %d times during the sweep, want 0 (loads must bypass the cache)", hooked.cachedShardGets)
	}
	AssertFileIntact(t, ctx, st, fLive)
	if ok, _ := st.HasXorb(ctx, "default", fGrace.XorbHashes[0]); !ok {
		t.Fatal("in-grace shard's xorb was swept")
	}
}

// testSweepReportsUnreadableDeadShard: a dead shard whose object cannot be
// decoded does not fail the sweep — it is reported, treated as live, and
// nothing of it (object, entries, xorb) is deleted.
func testSweepReportsUnreadableDeadShard(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("unreadable dead shard")})
	UnlinkFile(t, ctx, gcs, f)

	hooked := agedStore(gcs)
	hooked.loadShardErrs = map[string]error{f.ShardHash: errors.New("decode stored shard: corrupt")}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep = %v, want the unreadable shard skipped, not fail-stop", err)
	}
	if got, want := res.UnreadableShards, []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("UnreadableShards = %v, want %v", got, want)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("swept %v/%v, want nothing", SweptHashes(res.SweptShards), SweptHashes(res.SweptXorbs))
	}
	if !res.Done || res.RemainingXorbs != 0 {
		t.Fatalf("progress = done %v, remaining xorbs %d; want done 0 (xorb phase skipped)", res.Done, res.RemainingXorbs)
	}
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("unreadable shard object gone: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("unreadable shard's xorb was swept")
	}
	if got, err := gcs.GetChunkIndexEntry(ctx, f.ChunkHashes[0]); err != nil || got != f.ShardHash {
		t.Fatalf("chunk entry = %q, %v; want untouched %q", got, err, f.ShardHash)
	}
}

// testSweepUnreadableShardSuppressesXorbSweep: one unreadable live shard
// must not stop dead-shard cleanup, but no xorb may be deleted — the
// unreadable shard could reference any of them.
func testSweepUnreadableShardSuppressesXorbSweep(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	fSick := PutFile(t, ctx, st, [][]byte{[]byte("unreadable live shard")})
	fDead := PutFile(t, ctx, st, [][]byte{[]byte("healthy dead shard")})
	UnlinkFile(t, ctx, gcs, fDead)

	hooked := agedStore(gcs)
	hooked.loadShardErrs = map[string]error{fSick.ShardHash: errors.New("decode stored shard: corrupt")}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := res.UnreadableShards, []string{fSick.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("UnreadableShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptShards), []string{fDead.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v (dead-shard cleanup must proceed)", got, want)
	}
	if len(res.SweptXorbs) != 0 || res.RemainingXorbs != 0 {
		t.Fatalf("xorbs swept %v remaining %d, want none (xorb phase poisoned)", SweptHashes(res.SweptXorbs), res.RemainingXorbs)
	}
	if ok, _ := st.HasXorb(ctx, "default", fDead.XorbHashes[0]); !ok {
		t.Fatal("queued xorb swept despite an unreadable shard")
	}
	if _, err := gcs.GetShardByHash(ctx, fDead.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
	if got, err := gcs.GetFileIndexEntry(ctx, fSick.FileHash); err != nil || got != fSick.ShardHash {
		t.Fatalf("unreadable shard's file entry = %q, %v; want untouched", got, err)
	}
}

// testSweepEmptyFileZeroSHA256Cleanup: empty files store all-zero SHA-256
// metadata and their shared index/sha256 entry never anchors, so the file
// entry alone keeps the shard alive; once it is unlinked — Unlink alone
// suffices for empty files — the shard falls and takes the zero entry with
// it.
func testSweepEmptyFileZeroSHA256Cleanup(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	// An empty file: PutShard computes and stores the all-zero
	// SHA-256 metadata and the shared zero sha256 index entry.
	f := PutFile(t, ctx, st, nil)
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("zero sha256 entry = %q, %v; want %q", got, err, f.ShardHash)
	}

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 {
		t.Fatalf("file-anchored shard swept: %+v", res.SweptShards)
	}
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard after sweep: %v", err)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("zero sha256 entry after sweep = %q, %v; want untouched", got, err)
	}

	// The zero entry never anchors: Unlink alone frees the shard.
	if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
		t.Fatal(err)
	}
	res, err = storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep after unlink: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if res.DeletedSHA256Entries != 1 {
		t.Fatalf("DeletedSHA256Entries = %d, want 1 (the zero entry goes with its shard)", res.DeletedSHA256Entries)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != "" {
		t.Fatalf("zero sha256 entry = %q, %v; want removed", got, err)
	}
}

// testSweepDeleteLoopAbortsOnRacingFileEntry: a commit lands after the mark
// queued the dead shard, recreating its file entry right before the
// file-entry guard loop's read — the shard's only read of that key under
// AnchorBoth. The sweep must abort the shard's deletion — before anything
// was deleted — and keep the commit whole.
func testSweepDeleteLoopAbortsOnRacingFileEntry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("racing file-entry recommit")})
	UnlinkFile(t, ctx, gcs, f)

	// Aged out of grace; the commit rewrites the entry right before
	// the guard loop's only read (call 1).
	hooked := agedStore(gcs)
	recommitted := false
	hooked.onFileEntryGet = func(n int) {
		if n != 1 {
			return
		}
		recommitted = true
		b.SetIndexEntry(t, st, "index/files", f.FileHash.String(), f.ShardHash)
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !recommitted {
		t.Fatal("hook did not fire")
	}

	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("racing commit's objects swept: %+v", res)
	}
	if got, err := gcs.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != f.ShardHash {
		t.Fatalf("file entry = %q, %v; want %q", got, err, f.ShardHash)
	}
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard destroyed under the racing commit: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb destroyed under the racing commit")
	}
	assertChunkEntriesIntact(t, ctx, gcs, f, res)
}

// testSweepDeleteLoopAbortsOnRacingSHA256Entry: a commit's sha256 entry —
// PutShard commits sha256 entries before file entries — becomes visible
// after the file-entry guard loop but right before the sha256 guard loop's
// read, the shard's only read of that key under AnchorBoth. The sweep must
// abort the shard's deletion and leave the entry in place.
func testSweepDeleteLoopAbortsOnRacingSHA256Entry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("racing sha256-entry recommit")})
	UnlinkFile(t, ctx, gcs, f)

	// The commit lands right before the sha guard loop's only
	// read (get 1).
	hooked := &hookedGCStore{GCStore: gcs}
	recommitted := false
	hooked.onSHA256EntryGet = func(n int) {
		if n != 1 {
			return
		}
		recommitted = true
		b.SetIndexEntry(t, st, "index/sha256", f.SHA256Hex, f.ShardHash)
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !recommitted {
		t.Fatal("hook did not fire")
	}

	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("racing commit's objects swept: %+v", res)
	}
	if res.DeletedSHA256Entries != 0 {
		t.Fatalf("racing commit's sha256 entry touched: %+v", res)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.ShardHash)
	}
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
		t.Fatalf("shard destroyed under the racing commit: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb destroyed under the racing commit")
	}
	assertChunkEntriesIntact(t, ctx, gcs, f, res)
}

// testSweepAbortPreservesZeroSHA256Entry: on a shard whose empty file
// precedes a non-empty one, a racing non-zero sha256 entry must abort the
// deletion BEFORE the shared zero empty-file entry is deleted — every
// non-zero guard fires first, so the revived shard keeps its marker.
func testSweepAbortPreservesZeroSHA256Entry(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	// One shard, empty file first: file order is preserved by the
	// shard encoding, so the sha loops meet the zero digest first.
	content := []byte("empty-then-full recommit")
	shardObj := shard.NewShard()
	emptyHash, _, _ := AddFileBlock(t, ctx, st, shardObj, nil)
	fullHash, fullXorbs, _ := AddFileBlock(t, ctx, st, shardObj, [][]byte{content})
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	var shardHash string
	if err := gcs.WalkFileIndex(ctx, func(fileHash, sh string) error {
		if fileHash == fullHash.String() {
			shardHash = sh
		}
		return nil
	}); err != nil || shardHash == "" {
		t.Fatalf("shard hash lookup: %q, %v", shardHash, err)
	}
	digest := sha256.Sum256(content)
	nonZeroHex := hex.EncodeToString(digest[:])

	g := storage.NewGC(gcs)
	for _, fh := range []xet.FileHash{emptyHash, fullHash} {
		if _, err := g.Unlink(ctx, fh); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := g.UnlinkSHA256(ctx, digest); err != nil {
		t.Fatal(err)
	}

	// The commit lands right before the guard loop's non-zero read
	// (get 1). The zero entry's read would be get 2 — it must never
	// happen.
	hooked := &hookedGCStore{GCStore: gcs}
	recommitted := false
	hooked.onSHA256EntryGet = func(n int) {
		if n != 1 {
			return
		}
		recommitted = true
		b.SetIndexEntry(t, st, "index/sha256", nonZeroHex, shardHash)
	}
	res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !recommitted {
		t.Fatal("hook did not fire")
	}

	if len(res.SweptShards) != 0 || res.DeletedSHA256Entries != 0 {
		t.Fatalf("racing commit's shard or zero entry touched: %+v", res)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != shardHash {
		t.Fatalf("zero sha256 entry = %q, %v; want untouched %q", got, err, shardHash)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, nonZeroHex); err != nil || got != shardHash {
		t.Fatalf("racing sha256 entry = %q, %v; want %q", got, err, shardHash)
	}
	if _, err := gcs.GetShardByHash(ctx, shardHash); err != nil {
		t.Fatalf("shard destroyed under the racing commit: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", fullXorbs[0]); !ok {
		t.Fatal("xorb destroyed under the racing commit")
	}
}

// testSweepAnchorSHA256LFSLifecycle: a store behind Git-LFS is managed by
// SHA-256 OIDs only — the managing layer calls UnlinkSHA256, never Unlink.
// An AnchorSHA256 sweep reclaims the sha-dead shard, deletes its stale file
// entries as the designed cleanup, leaves sha-anchored files whole, and an
// identical re-upload of the swept content recommits everything.
func testSweepAnchorSHA256LFSLifecycle(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	parts1 := [][]byte{[]byte("lfs-managed content one")}
	f1 := PutFile(t, ctx, st, parts1)
	f2 := PutFile(t, ctx, st, [][]byte{[]byte("lfs-managed content two")})

	// The LFS layer knows only the OID: no Unlink call ever.
	if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f1.SHA256Hex)); err != nil {
		t.Fatal(err)
	}

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f1.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptXorbs), []string{f1.XorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.DeletedFileEntries != 1 {
		t.Fatalf("DeletedFileEntries = %d, want 1 (the stale file entry goes with its shard)", res.DeletedFileEntries)
	}
	if got, err := gcs.GetFileIndexEntry(ctx, f1.FileHash); err != nil || got != "" {
		t.Fatalf("file entry = %q, %v; want removed", got, err)
	}
	if _, err := gcs.GetShardByHash(ctx, f1.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f1.XorbHashes[0]); ok {
		t.Fatal("xorb still stored")
	}

	// File 2 keeps both access paths: sha256 and file hash.
	AssertFileIntact(t, ctx, st, f2)
	if got, err := gcs.GetFileIndexEntry(ctx, f2.FileHash); err != nil || got != f2.ShardHash {
		t.Fatalf("f2 file entry = %q, %v; want %q", got, err, f2.ShardHash)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, f2.SHA256Hex); err != nil || got != f2.ShardHash {
		t.Fatalf("f2 sha256 entry = %q, %v; want %q", got, err, f2.ShardHash)
	}

	// hasFile self-heal: the file entry is gone, so PutShard treats
	// the content as absent and recommits it all.
	if again := PutFile(t, ctx, st, parts1); again.ShardHash != f1.ShardHash {
		t.Fatal("re-upload produced different hashes")
	}
	if _, err := st.GetShard(ctx, f1.FileHash); err != nil {
		t.Fatalf("GetShard after re-upload: %v", err)
	}
}

// testSweepAnchorSHA256KeepsUnanchorableFileShards: an empty file can never
// be sha-anchored — UnlinkSHA256 rejects the all-zero digest — so while its
// file entry points at the shard an AnchorSHA256 sweep spares it (the
// accepted leak); the mark cannot see that, so a dry run still reports the
// shard as its upper bound. After Unlink the sweep reclaims it, zero entry
// included.
func testSweepAnchorSHA256KeepsUnanchorableFileShards(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, nil)
	opts := storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace}

	dry, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace, DryRun: true})
	if err != nil {
		t.Fatalf("dry Sweep: %v", err)
	}
	if got, want := SweptHashes(dry.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("dry SweptShards = %v, want %v (mark-time upper bound)", got, want)
	}

	res, err := storage.Sweep(ctx, gcs, opts)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || res.DeletedFileEntries != 0 || res.DeletedSHA256Entries != 0 {
		t.Fatalf("file-referenced unanchorable shard touched: %+v", res)
	}
	if _, err := st.GetShard(ctx, f.FileHash); err != nil {
		t.Fatalf("GetShard after sweep: %v", err)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("zero sha256 entry = %q, %v; want untouched", got, err)
	}

	// Unlink is the only way out for such shards.
	if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
		t.Fatal(err)
	}
	res, err = storage.Sweep(ctx, gcs, opts)
	if err != nil {
		t.Fatalf("Sweep after unlink: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if res.DeletedSHA256Entries != 1 {
		t.Fatalf("DeletedSHA256Entries = %d, want 1 (the zero entry goes with its shard)", res.DeletedSHA256Entries)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != "" {
		t.Fatalf("zero sha256 entry = %q, %v; want removed", got, err)
	}
}

// testSweepAnchorFilesUnlinkAloneReclaims: under AnchorFiles a still-present
// non-zero sha256 entry neither anchors nor is walked; Unlink alone lets the
// sweep reclaim the shard and its xorbs, deleting the sha256 entry with them.
func testSweepAnchorFilesUnlinkAloneReclaims(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("files-anchored content")})
	if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
		t.Fatal(err)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
		t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.ShardHash)
	}

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorFiles, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := SweptHashes(res.SweptXorbs), []string{f.XorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.DeletedSHA256Entries != 1 {
		t.Fatalf("DeletedSHA256Entries = %d, want 1 (the non-zero entry goes with its shard)", res.DeletedSHA256Entries)
	}
	if len(res.DanglingSHA256Entries) != 0 {
		t.Fatalf("DanglingSHA256Entries = %v, want empty (sha256 index not walked)", res.DanglingSHA256Entries)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != "" {
		t.Fatalf("sha256 entry after sweep = %q, %v; want removed", got, err)
	}
	if _, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(f.SHA256Hex)); err == nil {
		t.Fatal("sha256 lookup still resolves")
	}
}

// testSweepAnchorFilesDeletesSharedSHA256Entry: identical content stored
// under two chunkings shares one sha256 entry, owned by whichever PutShard
// the backend keeps (FileStorage the first, S3 the last). An AnchorFiles
// sweep of the owner deletes the entry outright: SHA-256 lookup misses, and
// re-uploading the live chunking cannot heal it (PutShard's hasFile gate
// skips every index write), while the live file stays whole by file hash;
// re-uploading the dead chunking rewrites the entry.
func testSweepAnchorFilesDeletesSharedSHA256Entry(t *testing.T, b Backend) {
	content := []byte("same bytes, two chunkings, one sha256 entry")
	wholeParts := [][]byte{content}
	splitParts := [][]byte{content[:16], content[16:]}
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	whole := PutFile(t, ctx, st, wholeParts)
	split := PutFile(t, ctx, st, splitParts)
	if whole.SHA256Hex != split.SHA256Hex || whole.FileHash == split.FileHash {
		t.Fatal("test setup: chunkings must share the digest but not the file hash")
	}
	owner, err := gcs.GetSHA256IndexEntry(ctx, whole.SHA256Hex)
	if err != nil {
		t.Fatal(err)
	}
	dead, live, deadParts, liveParts := whole, split, wholeParts, splitParts
	if owner == split.ShardHash {
		dead, live, deadParts, liveParts = split, whole, splitParts, wholeParts
	} else if owner != whole.ShardHash {
		t.Fatalf("sha256 entry owner = %q, want one of the two shards", owner)
	}

	if _, err := storage.NewGC(gcs).Unlink(ctx, dead.FileHash); err != nil {
		t.Fatal(err)
	}
	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorFiles, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{dead.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if res.DeletedSHA256Entries != 1 {
		t.Fatalf("DeletedSHA256Entries = %d, want 1 (the shared entry goes with its owner)", res.DeletedSHA256Entries)
	}
	if _, err := st.GetShard(ctx, live.FileHash); err != nil {
		t.Fatalf("GetShard(live): %v", err)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, live.SHA256Hex); err != nil || got != "" {
		t.Fatalf("shared sha256 entry = %q, %v; want removed", got, err)
	}
	if _, err := st.GetReconstructedFile(ctx, "default", SHA256Digest(live.SHA256Hex)); err == nil {
		t.Fatal("SHA-256 lookup still resolves after the entry delete")
	}

	// The live chunking's re-upload is a no-op behind hasFile.
	PutFile(t, ctx, st, liveParts)
	if got, err := gcs.GetSHA256IndexEntry(ctx, live.SHA256Hex); err != nil || got != "" {
		t.Fatalf("sha256 entry after live re-upload = %q, %v; want still removed", got, err)
	}
	// The dead chunking's re-upload rewrites it.
	if again := PutFile(t, ctx, st, deadParts); again.ShardHash != dead.ShardHash {
		t.Fatalf("re-upload shard = %s, want %s", again.ShardHash, dead.ShardHash)
	}
	if got, err := gcs.GetSHA256IndexEntry(ctx, live.SHA256Hex); err != nil || got != dead.ShardHash {
		t.Fatalf("sha256 entry after dead re-upload = %q, %v; want %q", got, err, dead.ShardHash)
	}
	AssertFileIntact(t, ctx, st, live)
}

// testSweepAnchorFilesSkipsSHAWalk: a dangling sha256 entry is invisible to
// an AnchorFiles sweep — the sha256 index is not walked at all — while an
// AnchorBoth sweep over the same store reports it.
func testSweepAnchorFilesSkipsSHAWalk(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("dangling sha, files anchor")})
	if err := gcs.DeleteShard(ctx, f.ShardHash); err != nil {
		t.Fatal(err)
	}

	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorFiles, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.DanglingSHA256Entries) != 0 {
		t.Fatalf("DanglingSHA256Entries = %v, want empty (walk skipped)", res.DanglingSHA256Entries)
	}
	// The file entry dangles under every anchor.
	if got, want := res.DanglingFileEntries, []string{f.FileHash.String()}; !slices.Equal(got, want) {
		t.Fatalf("DanglingFileEntries = %v, want %v", got, want)
	}

	both, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep(AnchorBoth): %v", err)
	}
	if got, want := both.DanglingSHA256Entries, []string{f.SHA256Hex}; !slices.Equal(got, want) {
		t.Fatalf("DanglingSHA256Entries = %v, want %v", got, want)
	}
}

// testSweepAnchorSHA256AbortsOnRacingSHA256Entry: under AnchorSHA256 the two
// sha scans bound the file-entry deletions. A recommit visible to the
// pre-check's scan (get 1) aborts before anything is deleted; one landing
// only after it (get 2) still aborts the shard's deletion but has lost its
// file entries to the stale cleanup — the documented degraded outcome of
// accepted race (f), healed by an identical re-upload.
func testSweepAnchorSHA256AbortsOnRacingSHA256Entry(t *testing.T, b Backend) {
	t.Run("pre-check-abort", func(t *testing.T) {
		ctx := context.Background()
		st := b.New(t)
		gcs := st.(storage.GCStore)

		f := PutFile(t, ctx, st, [][]byte{[]byte("sha recommit at pre-check")})
		if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
			t.Fatal(err)
		}

		hooked := &hookedGCStore{GCStore: gcs}
		recommitted := false
		hooked.onSHA256EntryGet = func(n int) {
			if n != 1 {
				return
			}
			recommitted = true
			b.SetIndexEntry(t, st, "index/sha256", f.SHA256Hex, f.ShardHash)
		}
		res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace})
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if !recommitted {
			t.Fatal("hook did not fire")
		}
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 || res.DeletedFileEntries != 0 {
			t.Fatalf("pre-check abort came too late: %+v", res)
		}
		// The abort fired before any deletion: the file entry survives.
		if got, err := gcs.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != f.ShardHash {
			t.Fatalf("file entry = %q, %v; want %q", got, err, f.ShardHash)
		}
		assertChunkEntriesIntact(t, ctx, gcs, f, res)
		AssertFileIntact(t, ctx, st, f)
	})
	t.Run("post-files-loop-abort", func(t *testing.T) {
		ctx := context.Background()
		st := b.New(t)
		gcs := st.(storage.GCStore)

		parts := [][]byte{[]byte("sha recommit after file deletes")}
		f := PutFile(t, ctx, st, parts)
		if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
			t.Fatal(err)
		}

		hooked := &hookedGCStore{GCStore: gcs}
		recommitted := false
		hooked.onSHA256EntryGet = func(n int) {
			if n != 2 {
				return
			}
			recommitted = true
			b.SetIndexEntry(t, st, "index/sha256", f.SHA256Hex, f.ShardHash)
		}
		res, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace})
		if err != nil {
			t.Fatalf("Sweep: %v", err)
		}
		if !recommitted {
			t.Fatal("hook did not fire")
		}
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
			t.Fatalf("shard swept under the racing sha256 entry: %+v", res)
		}
		if res.DeletedFileEntries != 1 {
			t.Fatalf("DeletedFileEntries = %d, want 1 (stale cleanup ran before the abort)", res.DeletedFileEntries)
		}
		// The degraded-but-consistent outcome: the shard survives
		// sha-resolvable, its file entry is gone.
		if got, err := gcs.GetSHA256IndexEntry(ctx, f.SHA256Hex); err != nil || got != f.ShardHash {
			t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.ShardHash)
		}
		if got, err := gcs.GetFileIndexEntry(ctx, f.FileHash); err != nil || got != "" {
			t.Fatalf("file entry = %q, %v; want removed", got, err)
		}
		if _, err := gcs.GetShardByHash(ctx, f.ShardHash); err != nil {
			t.Fatalf("shard destroyed under the racing commit: %v", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
			t.Fatal("xorb destroyed under the racing commit")
		}
		assertChunkEntriesIntact(t, ctx, gcs, f, res)

		// An identical re-upload rewrites the lost entries (self-heal).
		if again := PutFile(t, ctx, st, parts); again.ShardHash != f.ShardHash {
			t.Fatal("re-upload produced different hashes")
		}
		if _, err := st.GetShard(ctx, f.FileHash); err != nil {
			t.Fatalf("GetShard after re-upload: %v", err)
		}
	})
}

// testSweepUnknownAnchorFails: both entry points validate the anchor before
// touching the store, and a failed SweepStep releases the single-flight
// lock for the next call.
func testSweepUnknownAnchorFails(t *testing.T, b Backend) {
	ctx := context.Background()
	hooked := &hookedGCStore{GCStore: b.New(t).(storage.GCStore)}
	g := storage.NewGC(hooked)
	if _, err := storage.Sweep(ctx, hooked, storage.SweepOptions{Anchor: "bogus", Grace: NoGrace}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
		t.Fatalf("Sweep = %v, want unknown-anchor error", err)
	}
	if _, err := g.SweepStep(ctx, storage.SweepOptions{Anchor: "bogus", Grace: NoGrace}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
		t.Fatalf("SweepStep = %v, want unknown-anchor error", err)
	}
	if hooked.walkShardsCalls != 0 {
		t.Fatalf("walkShardsCalls = %d after failed sweeps, want 0 (fail before store access)", hooked.walkShardsCalls)
	}
	// The failed step must not leak the single-flight lock.
	if _, err := g.SweepStep(ctx, storage.SweepOptions{Grace: NoGrace}); err != nil {
		t.Fatalf("SweepStep after failed anchor: %v", err)
	}
	if hooked.walkShardsCalls == 0 {
		t.Fatal("valid SweepStep did not reach the store")
	}
}

// testSweepAnchorBothUnchanged: passing AnchorBoth explicitly is the zero
// value — a shard stays live while either entry remains, exactly like the
// default-mode lifecycles in TestSweepNeedsBothUnlinks, and no file entries
// are ever deleted.
func testSweepAnchorBothUnchanged(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)
	f := PutFile(t, ctx, st, [][]byte{[]byte("explicit anchor both")})
	if _, err := storage.NewGC(gcs).Unlink(ctx, f.FileHash); err != nil {
		t.Fatal(err)
	}
	res, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorBoth, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 {
		t.Fatalf("sha-anchored shard swept under explicit AnchorBoth: %+v", res)
	}
	if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
		t.Fatal(err)
	}
	res, err = storage.Sweep(ctx, gcs, storage.SweepOptions{Anchor: storage.AnchorBoth, Grace: NoGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if res.DeletedFileEntries != 0 {
		t.Fatalf("DeletedFileEntries = %d, want 0 (only AnchorSHA256 produces it)", res.DeletedFileEntries)
	}
}

// testSweepStepUnreadableShardCannotLivelock: unswept queue items charge no
// bound. With MaxDeletes=1 and an undecodable dead shard sorting first in
// the queue, the first step must still sweep the healthy dead shard behind
// it — per-item accounting would burn the only slot on the corrupt object
// on every stateless pass, forever — and repeated steps reach Done with the
// corrupt shard reported unreadable, nothing more to do.
func testSweepStepUnreadableShardCannotLivelock(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("healthy dead shard")})
	UnlinkFile(t, ctx, gcs, f)
	// The all-zero name sorts before any real hash on both backends.
	corrupt := strings.Repeat("0", 64)
	if corrupt >= f.ShardHash {
		t.Fatalf("test setup: %s must sort before %s", corrupt, f.ShardHash)
	}
	b.PutRawShardObject(t, ctx, st, corrupt, []byte("not a decodable shard"))

	g := storage.NewGC(gcs)
	opts := storage.SweepOptions{Grace: NoGrace, MaxDeletes: 1}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("first step SweptShards = %v, want %v (progress past the corrupt shard)", got, want)
	}
	if got, want := res.UnreadableShards, []string{corrupt}; !slices.Equal(got, want) {
		t.Fatalf("UnreadableShards = %v, want %v", got, want)
	}
	for steps := 0; !res.Done; steps++ {
		if steps > 4 {
			t.Fatalf("stepping did not reach Done; last %+v", res)
		}
		if res, err = g.SweepStep(ctx, opts); err != nil {
			t.Fatalf("SweepStep: %v", err)
		}
		if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
			t.Fatalf("later step swept %v/%v, want nothing left", SweptHashes(res.SweptShards), SweptHashes(res.SweptXorbs))
		}
	}
	if got, want := res.UnreadableShards, []string{corrupt}; !slices.Equal(got, want) {
		t.Fatalf("done step UnreadableShards = %v, want %v", got, want)
	}
	// The poisoned passes judge no xorb: the healthy shard's xorb
	// stays until the corrupt object is repaired or removed.
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); !ok {
		t.Fatal("xorb swept despite the unreadable shard")
	}
	foundCorrupt := false
	if err := gcs.WalkShards(ctx, func(hash string, _ int64, _ time.Time) error {
		if hash == corrupt {
			foundCorrupt = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !foundCorrupt {
		t.Fatal("corrupt shard object deleted")
	}
}

// testSweepStepSparedUnanchorableShardCannotLivelock: under AnchorSHA256 a
// file-referenced shard carrying an unanchorable file is re-queued dead on
// every pass — the mark cannot see file refs — and spared by sweepShard's
// pre-check. Spared items charge no bound, so with MaxDeletes=1 the first
// step must still sweep the sha-dead shard behind it and repeated steps
// reach Done with the spared shard intact.
func testSweepStepSparedUnanchorableShardCannotLivelock(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	f := PutFile(t, ctx, st, [][]byte{[]byte("sha-dead healthy shard")})
	if _, err := storage.NewGC(gcs).UnlinkSHA256(ctx, SHA256Digest(f.SHA256Hex)); err != nil {
		t.Fatal(err)
	}

	// A valid shard whose file block has no MetadataExt, stored
	// under the all-zero name so it sorts before the sha-dead
	// shard, with a file entry pointing at it: unanchorable and
	// spared, yet queued dead by every sha-mode mark.
	spared := strings.Repeat("0", 64)
	if spared >= f.ShardHash {
		t.Fatalf("test setup: %s must sort before %s", spared, f.ShardHash)
	}
	sparedShard := shard.NewShard()
	sparedFile, sparedXorbs, _ := AddFileBlock(t, ctx, st, sparedShard, [][]byte{[]byte("unanchorable payload")})
	r, err := sparedShard.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	b.PutRawShardObject(t, ctx, st, spared, raw)
	b.SetIndexEntry(t, st, "index/files", sparedFile.String(), spared)

	g := storage.NewGC(gcs)
	opts := storage.SweepOptions{Anchor: storage.AnchorSHA256, Grace: NoGrace, MaxDeletes: 1}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{f.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("first step SweptShards = %v, want %v (progress past the spared shard)", got, want)
	}
	if res.DeletedFileEntries != 1 {
		t.Fatalf("DeletedFileEntries = %d, want 1 (the sha-dead shard's stale entry)", res.DeletedFileEntries)
	}
	for steps := 0; !res.Done; steps++ {
		if steps > 4 {
			t.Fatalf("stepping did not reach Done; last %+v", res)
		}
		if res, err = g.SweepStep(ctx, opts); err != nil {
			t.Fatalf("SweepStep: %v", err)
		}
		if len(res.SweptShards) != 0 {
			t.Fatalf("later step swept shards %v, want none", SweptHashes(res.SweptShards))
		}
	}

	// The spared shard, its file entry, and its xorb all survive.
	if _, err := gcs.LoadShard(ctx, spared); err != nil {
		t.Fatalf("spared shard gone: %v", err)
	}
	if got, err := gcs.GetFileIndexEntry(ctx, sparedFile); err != nil || got != spared {
		t.Fatalf("spared file entry = %q, %v; want %q", got, err, spared)
	}
	if ok, _ := st.HasXorb(ctx, "default", sparedXorbs[0]); !ok {
		t.Fatal("spared shard's xorb swept")
	}
	// The sha-dead shard and its xorb are gone.
	if _, err := gcs.GetShardByHash(ctx, f.ShardHash); !errors.Is(err, iofs.ErrNotExist) {
		t.Fatalf("dead shard load = %v, want ErrNotExist", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.XorbHashes[0]); ok {
		t.Fatal("dead shard's xorb still stored")
	}
}

// testSweepStepExhaustedAtShardDrainSkipsXorbPhase: a step whose bounds run
// out exactly as the shard queue drains returns at once — no phase-2 walk,
// no shard loads beyond the dead shard's own — reporting Done false with
// nothing measured for xorbs.
func testSweepStepExhaustedAtShardDrainSkipsXorbPhase(t *testing.T, b Backend) {
	ctx := context.Background()
	st := b.New(t)
	gcs := st.(storage.GCStore)

	dead := PutFile(t, ctx, st, [][]byte{[]byte("drained dead shard")})
	UnlinkFile(t, ctx, gcs, dead)
	live := PutFile(t, ctx, st, [][]byte{[]byte("live shard phase 2 would load")})

	hooked := &hookedGCStore{GCStore: gcs}
	g := storage.NewGC(hooked)
	res, err := g.SweepStep(ctx, storage.SweepOptions{Grace: NoGrace, MaxDeletes: 1})
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if got, want := SweptHashes(res.SweptShards), []string{dead.ShardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if res.Done || res.RemainingShards != 0 || res.RemainingXorbs != 0 {
		t.Fatalf("step = done %v, remaining %d/%d; want not done, 0/0 (xorb phase skipped)", res.Done, res.RemainingShards, res.RemainingXorbs)
	}
	if hooked.walkShardsCalls != 1 {
		t.Fatalf("WalkShards called %d times, want 1 (no phase-2 walk)", hooked.walkShardsCalls)
	}
	if hooked.loadShardCalls != 1 {
		t.Fatalf("LoadShard called %d times, want 1 (the dead shard only)", hooked.loadShardCalls)
	}
	// The dead shard's xorb was never judged; a follow-up unbounded
	// step finishes the job.
	if ok, _ := st.HasXorb(ctx, "default", dead.XorbHashes[0]); !ok {
		t.Fatal("xorb swept by a step that skipped the xorb phase")
	}
	res, err = g.SweepStep(ctx, storage.SweepOptions{Grace: NoGrace})
	if err != nil {
		t.Fatalf("second SweepStep: %v", err)
	}
	if !res.Done || len(res.SweptXorbs) != 1 {
		t.Fatalf("second step = done %v, %d xorbs; want done with the xorb swept", res.Done, len(res.SweptXorbs))
	}
	AssertFileIntact(t, ctx, st, live)
}

// testSweepCanceledContextFailsBeforeWork: a pass entered with a dead
// context reports the cancellation, even over an empty store where no walk
// or load would ever notice it.
func testSweepCanceledContextFailsBeforeWork(t *testing.T, b Backend) {
	st := b.New(t)
	gcs := st.(storage.GCStore)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := storage.Sweep(ctx, gcs, storage.SweepOptions{Grace: NoGrace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sweep = %v, want context.Canceled", err)
	}
	if _, err := storage.NewGC(gcs).SweepStep(ctx, storage.SweepOptions{Grace: NoGrace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("SweepStep = %v, want context.Canceled", err)
	}
}
