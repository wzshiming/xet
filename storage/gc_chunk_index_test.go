package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
)

// removeShardObjectOutOfBand deletes the stored shard object directly,
// simulating operator loss the store's own DeleteShard never sees.
func removeShardObjectOutOfBand(t *testing.T, ctx context.Context, st Storage, shardHash string) {
	t.Helper()
	switch b := st.(type) {
	case *FileStorage:
		if err := os.Remove(b.objectPath("shards", shardHash)); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.deleteObject(ctx, b.objectKey("shards", shardHash)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported backend %T", st)
	}
}

// chunkPhaseSweeper returns a sweep store with the given chunk entries aged
// past a one-hour grace: real Chtimes on FileStorage, walk-time aging on S3,
// whose entry times cannot be set through the API.
func chunkPhaseSweeper(t *testing.T, st Storage, aged []xet.ChunkHash) *hookedGCStore {
	t.Helper()
	gcs := st.(GCStore)
	fs, ok := st.(*FileStorage)
	if !ok {
		return agedStore(gcs)
	}
	old := time.Now().Add(-2 * time.Hour)
	for _, ch := range aged {
		p := fs.objectPath("index/chunks", ch.String())
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}
	return &hookedGCStore{GCStore: gcs}
}

// TestWalkChunkIndexRoundtrip: the walk yields exactly the entries a stored
// shard committed, each with a modification time inside the test's window.
func TestWalkChunkIndexRoundtrip(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// Two-second slack: S3 reports times at second precision.
			before := time.Now().Add(-2 * time.Second)
			f := putGCFile(t, ctx, st, [][]byte{[]byte("walk chunk one"), []byte("walk chunk two")})
			after := time.Now().Add(2 * time.Second)

			got := map[string]string{}
			times := map[string]time.Time{}
			if err := gcs.WalkChunkIndex(ctx, func(chunkHash, shardHash string, modTime time.Time) error {
				got[chunkHash] = shardHash
				times[chunkHash] = modTime
				return nil
			}); err != nil {
				t.Fatalf("WalkChunkIndex: %v", err)
			}

			if len(got) != len(f.chunkHashes) {
				t.Fatalf("walked %d entries, want %d: %v", len(got), len(f.chunkHashes), got)
			}
			for _, ch := range f.chunkHashes {
				if got[ch.String()] != f.shardHash {
					t.Fatalf("entry %s = %q, want %q", ch.String(), got[ch.String()], f.shardHash)
				}
				if mt := times[ch.String()]; mt.Before(before) || mt.After(after) {
					t.Fatalf("entry %s modTime %v outside [%v, %v]", ch.String(), mt, before, after)
				}
			}
		})
	}
}

// TestHasShardObject: stored hashes probe true; absent and malformed names —
// including "" and 2-hex prefixes that map to the kind root and fanout
// directories on disk — read as absent, never as an error.
func TestHasShardObject(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("has shard object")})
			if ok, err := gcs.HasShardObject(ctx, f.shardHash); err != nil || !ok {
				t.Fatalf("HasShardObject(stored) = %v, %v; want true, nil", ok, err)
			}
			for _, name := range []string{strings.Repeat("ef", 32), "", "ab", f.shardHash[:2]} {
				if ok, err := gcs.HasShardObject(ctx, name); err != nil || ok {
					t.Fatalf("HasShardObject(%q) = %v, %v; want false, nil", name, ok, err)
				}
			}
		})
	}
}

// TestSweepCleansOrphanChunkEntries: entries orphaned by an out-of-band
// shard object loss are deleted by the opt-in chunk pass once aged, while
// the dangling file and sha256 entries stay reported, never deleted.
func TestSweepCleansOrphanChunkEntries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("orphan chunk one"), []byte("orphan chunk two")})
			removeShardObjectOutOfBand(t, ctx, st, f.shardHash)
			sweeper := chunkPhaseSweeper(t, st, f.chunkHashes)

			res, err := Sweep(ctx, sweeper, SweepOptions{Grace: time.Hour, CleanChunkIndex: true})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if res.DeletedChunkEntries != len(f.chunkHashes) {
				t.Fatalf("DeletedChunkEntries = %d, want %d", res.DeletedChunkEntries, len(f.chunkHashes))
			}
			if res.RepointedChunkEntries != 0 {
				t.Fatalf("RepointedChunkEntries = %d, want 0", res.RepointedChunkEntries)
			}
			for _, ch := range f.chunkHashes {
				if got, err := gcs.GetChunkIndexEntry(ctx, ch); err != nil || got != "" {
					t.Fatalf("chunk entry %s = %q, %v; want removed", ch.String(), got, err)
				}
			}

			// Dangling file/sha entries keep the existing report-only fate.
			if len(res.DanglingFileEntries) != 1 || res.DanglingFileEntries[0] != f.fileHash.String() {
				t.Fatalf("DanglingFileEntries = %v, want [%s]", res.DanglingFileEntries, f.fileHash.String())
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want kept as %q", got, err, f.shardHash)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want kept as %q", got, err, f.shardHash)
			}
		})
	}
}

// orphanSharedChunkFixture stores two shards sharing one chunk, removes the
// shared entry's owner shard object out-of-band, and returns dead (the
// vanished owner) and live (the surviving shard carrying the same chunk).
func orphanSharedChunkFixture(t *testing.T, ctx context.Context, st Storage, shared []byte, uniqueOne, uniqueTwo string) (dead, live gcFile) {
	t.Helper()
	gcs := st.(GCStore)
	f1 := putGCFile(t, ctx, st, [][]byte{shared, []byte(uniqueOne)})
	f2 := putGCFile(t, ctx, st, [][]byte{shared, []byte(uniqueTwo)})
	if f1.chunkHashes[0] != f2.chunkHashes[0] {
		t.Fatal("test setup: shared part must map to one chunk hash")
	}
	// FileStorage keeps the first writer, S3 the last; whichever owns the
	// shared entry loses its object so the entry dangles either way.
	owner, err := gcs.GetChunkIndexEntry(ctx, f1.chunkHashes[0])
	if err != nil {
		t.Fatal(err)
	}
	dead, live = f1, f2
	if owner == f2.shardHash {
		dead, live = f2, f1
	} else if owner != f1.shardHash {
		t.Fatalf("chunk entry owner = %q, want one of the two shards", owner)
	}
	// Warm the chunk cache so a repoint must evict it.
	if _, err := st.GetShardByChunkHash(ctx, "default", dead.chunkHashes[0]); err != nil {
		t.Fatal(err)
	}
	removeShardObjectOutOfBand(t, ctx, st, dead.shardHash)
	return dead, live
}

// TestSweepRepointsOrphanChunkEntriesToLiveOwner: an orphaned entry whose
// chunk a live shard also carries is repointed there, restoring dedup; the
// vanished shard's exclusive entry is deleted.
func TestSweepRepointsOrphanChunkEntriesToLiveOwner(t *testing.T) {
	shared := []byte("chunk shared by an orphan entry and a live shard")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			dead, live := orphanSharedChunkFixture(t, ctx, st, shared, "repoint unique one", "repoint unique two")
			sweeper := chunkPhaseSweeper(t, st, dead.chunkHashes)

			res, err := Sweep(ctx, sweeper, SweepOptions{Grace: time.Hour, CleanChunkIndex: true})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if res.RepointedChunkEntries != 1 {
				t.Fatalf("RepointedChunkEntries = %d, want 1", res.RepointedChunkEntries)
			}
			if res.DeletedChunkEntries != 1 {
				t.Fatalf("DeletedChunkEntries = %d, want 1 (the exclusive entry)", res.DeletedChunkEntries)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, live.chunkHashes[0]); err != nil || got != live.shardHash {
				t.Fatalf("shared chunk entry = %q, %v; want %q", got, err, live.shardHash)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, dead.chunkHashes[1]); err != nil || got != "" {
				t.Fatalf("exclusive chunk entry = %q, %v; want removed", got, err)
			}
			// The dedup lookup resolves through the repointed entry again.
			if _, err := st.GetShardByChunkHash(ctx, "default", live.chunkHashes[0]); err != nil {
				t.Fatalf("GetShardByChunkHash(shared) after repoint: %v", err)
			}
			assertFileIntact(t, ctx, st, live)
		})
	}
}

// TestSweepChunkPhaseOwnerVanishedFallsBackToDelete: the repoint target was
// marked live but its object vanishes before the chunk pass probes it; the
// entry is deleted, not repointed at another missing shard.
func TestSweepChunkPhaseOwnerVanishedFallsBackToDelete(t *testing.T) {
	shared := []byte("chunk whose every owner vanishes")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			dead, live := orphanSharedChunkFixture(t, ctx, st, shared, "fallback unique one", "fallback unique two")
			sweeper := chunkPhaseSweeper(t, st, dead.chunkHashes)
			sweeper.beforeWalkChunkIndex = func() {
				removeShardObjectOutOfBand(t, ctx, st, live.shardHash)
			}

			res, err := Sweep(ctx, sweeper, SweepOptions{Grace: time.Hour, CleanChunkIndex: true})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if res.RepointedChunkEntries != 0 {
				t.Fatalf("RepointedChunkEntries = %d, want 0", res.RepointedChunkEntries)
			}
			// The shared entry and the dead shard's exclusive entry.
			if res.DeletedChunkEntries != 2 {
				t.Fatalf("DeletedChunkEntries = %d, want 2", res.DeletedChunkEntries)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, dead.chunkHashes[0]); err != nil || got != "" {
				t.Fatalf("shared chunk entry = %q, %v; want removed, not repointed at a vanished owner", got, err)
			}
		})
	}
}

// TestSweepChunkPhaseFreshEntrySpared: a fresh orphan entry stays inside the
// grace window — counted as SkippedInGrace, kept stored.
func TestSweepChunkPhaseFreshEntrySpared(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh orphan entry")})
	removeShardObjectOutOfBand(t, ctx, st, f.shardHash)
	// Age the orphaned xorb so the fresh chunk entry is the only grace skip.
	old := time.Now().Add(-2 * DefaultSweepGrace)
	if err := os.Chtimes(st.objectPath("xorbs", f.xorbHashes[0].String()), old, old); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(ctx, st, SweepOptions{CleanChunkIndex: true})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.SkippedInGrace != 1 {
		t.Fatalf("SkippedInGrace = %d, want 1", res.SkippedInGrace)
	}
	if res.DeletedChunkEntries != 0 || res.RepointedChunkEntries != 0 {
		t.Fatalf("fresh entry touched: %+v", res)
	}
	if got, err := st.GetChunkIndexEntry(ctx, f.chunkHashes[0]); err != nil || got != f.shardHash {
		t.Fatalf("chunk entry = %q, %v; want kept as %q", got, err, f.shardHash)
	}
}

// TestSweepChunkPhaseOptIn: without CleanChunkIndex the pass never walks the
// chunk index and an otherwise collectable orphan entry survives.
func TestSweepChunkPhaseOptIn(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("opt-in only")})
			removeShardObjectOutOfBand(t, ctx, st, f.shardHash)
			sweeper := chunkPhaseSweeper(t, st, f.chunkHashes)

			res, err := Sweep(ctx, sweeper, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if sweeper.walkChunkIndexCalls != 0 {
				t.Fatalf("default sweep walked the chunk index %d times", sweeper.walkChunkIndexCalls)
			}
			if res.DeletedChunkEntries != 0 || res.RepointedChunkEntries != 0 {
				t.Fatalf("default sweep touched chunk entries: %+v", res)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, f.chunkHashes[0]); err != nil || got != f.shardHash {
				t.Fatalf("chunk entry = %q, %v; want kept as %q", got, err, f.shardHash)
			}
		})
	}
}

// TestSweepChunkPhaseDryRun: a dry run with CleanChunkIndex counts the
// repoints and deletes the real pass would perform, writing nothing.
func TestSweepChunkPhaseDryRun(t *testing.T) {
	shared := []byte("dry-run shared chunk")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			dead, live := orphanSharedChunkFixture(t, ctx, st, shared, "dry unique one", "dry unique two")
			sweeper := chunkPhaseSweeper(t, st, dead.chunkHashes)

			res, err := Sweep(ctx, sweeper, SweepOptions{Grace: time.Hour, DryRun: true, CleanChunkIndex: true})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if res.RepointedChunkEntries != 1 || res.DeletedChunkEntries != 1 {
				t.Fatalf("dry-run chunk counters = %d repointed, %d deleted; want 1, 1",
					res.RepointedChunkEntries, res.DeletedChunkEntries)
			}
			// Both entries still point at the vanished shard.
			for _, ch := range dead.chunkHashes {
				if got, err := gcs.GetChunkIndexEntry(ctx, ch); err != nil || got != dead.shardHash {
					t.Fatalf("chunk entry %s = %q, %v; want untouched %q", ch.String(), got, err, dead.shardHash)
				}
			}
			_ = live
		})
	}
}

// chunkWalkCanceller cancels its context once the walk callback has
// consumed `after` entries, simulating a caller vanishing mid-pass.
type chunkWalkCanceller struct {
	GCStore
	after  int
	cancel context.CancelFunc
}

func (s *chunkWalkCanceller) WalkChunkIndex(ctx context.Context, fn func(chunkHash, shardHash string, modTime time.Time) error) error {
	n := 0
	return s.GCStore.WalkChunkIndex(ctx, func(chunkHash, shardHash string, modTime time.Time) error {
		if err := fn(chunkHash, shardHash, modTime); err != nil {
			return err
		}
		n++
		if n == s.after && s.cancel != nil {
			s.cancel()
			s.cancel = nil
		}
		return nil
	})
}

// ReapTemps forwards to the wrapped store: the embedded interface hides the
// optional tempReaper surface the reap assertion depends on.
func (s *chunkWalkCanceller) ReapTemps(ctx context.Context, olderThan time.Time) (int, error) {
	return s.GCStore.(tempReaper).ReapTemps(ctx, olderThan)
}

// TestSweepStepParksInChunkPhaseAndResumes: a context abort mid-chunk-walk
// parks the cycle in the chunk phase before the temp reap; the resumed step
// re-runs the whole walk without double-counting the entry already deleted,
// and only the finished cycle reaps.
func TestSweepStepParksInChunkPhaseAndResumes(t *testing.T) {
	ctx := context.Background()
	basePath := t.TempDir()
	st, err := NewFileStorage(WithBasePath(basePath))
	if err != nil {
		t.Fatal(err)
	}

	f := putGCFile(t, ctx, st, [][]byte{[]byte("chunk park one"), []byte("chunk park two")})
	removeShardObjectOutOfBand(t, ctx, st, f.shardHash)
	// A stranded temp proves the reap waits for the completed walk.
	tmpPath := filepath.Join(basePath, "shards", ".shard-parked")
	if err := os.WriteFile(tmpPath, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	stepCtx, cancel := context.WithCancel(ctx)
	g := NewGC(&chunkWalkCanceller{GCStore: st, after: 1, cancel: cancel})
	opts := SweepOptions{Grace: noGrace, CleanChunkIndex: true}
	if _, err := g.SweepStep(stepCtx, opts); !errors.Is(err, context.Canceled) {
		t.Fatalf("aborted step = %v, want context.Canceled", err)
	}
	status := g.Status()
	if !status.Parked || status.ParkedPhase != "chunks" {
		t.Fatalf("status after abort = %+v, want parked in the chunk phase", status)
	}
	if _, err := os.Stat(tmpPath); err != nil {
		t.Fatalf("temp reaped before the cycle finished: %v", err)
	}

	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("resumed SweepStep: %v", err)
	}
	if !res.Done {
		t.Fatalf("resumed step not done: %+v", res)
	}
	if res.DeletedChunkEntries != len(f.chunkHashes) {
		t.Fatalf("cumulative DeletedChunkEntries = %d, want %d (no double count)",
			res.DeletedChunkEntries, len(f.chunkHashes))
	}
	for _, ch := range f.chunkHashes {
		if got, err := st.GetChunkIndexEntry(ctx, ch); err != nil || got != "" {
			t.Fatalf("chunk entry %s = %q, %v; want removed", ch.String(), got, err)
		}
	}
	if res.ReapedTempFiles != 1 {
		t.Fatalf("ReapedTempFiles = %d, want 1", res.ReapedTempFiles)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("stranded temp survived the finished cycle: %v", err)
	}
}

// TestSweepStepChunkFlagChangeRestarts: a parked cycle resumed with a
// different CleanChunkIndex is discarded and re-marked, so the flagged step
// both finishes the dead-object work and cleans the orphan entry.
func TestSweepStepChunkFlagChangeRestarts(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			putUnlinkedGCFiles(t, ctx, st, "flag change one", "flag change two")
			orphan := putGCFile(t, ctx, st, [][]byte{[]byte("flag change orphan")})
			removeShardObjectOutOfBand(t, ctx, st, orphan.shardHash)

			g := NewGC(gcs)
			res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, Anchor: AnchorFiles, MaxDeletes: 1})
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.Done || len(res.SweptShards) != 1 {
				t.Fatalf("first step = %+v, want one shard consumed and a parked cycle", res)
			}

			res, err = g.SweepStep(ctx, SweepOptions{Grace: noGrace, Anchor: AnchorFiles, CleanChunkIndex: true})
			if err != nil {
				t.Fatalf("flagged SweepStep: %v", err)
			}
			if !res.Done {
				t.Fatalf("flagged step not done: %+v", res)
			}
			// A fresh accumulator proves the re-mark; a resumed cycle would
			// report both shards.
			if len(res.SweptShards) != 1 {
				t.Fatalf("flagged step swept %d shards, want 1 from a re-mark", len(res.SweptShards))
			}
			// The re-marked shard's own entry plus the orphan entry.
			if res.DeletedChunkEntries != 2 {
				t.Fatalf("DeletedChunkEntries = %d, want 2", res.DeletedChunkEntries)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, orphan.chunkHashes[0]); err != nil || got != "" {
				t.Fatalf("orphan chunk entry = %q, %v; want cleaned by the re-marked flagged cycle", got, err)
			}
		})
	}
}
