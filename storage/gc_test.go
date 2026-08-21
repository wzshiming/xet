package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// noGrace makes every stored object immediately sweepable.
const noGrace = -time.Hour

// gcFile captures everything a stored test file owns so sweeps can be
// asserted object by object.
type gcFile struct {
	fileHash    xet.FileHash
	shardHash   string
	xorbHashes  []xet.XorbHash
	chunkHashes []xet.ChunkHash
	sha256Hex   string
	content     []byte
}

// putGCFile stores one file chunked as parts (one single-chunk xorb per
// part). Identical parts across files share the same xorb.
func putGCFile(t *testing.T, ctx context.Context, st Storage, parts [][]byte) gcFile {
	t.Helper()
	shardObj := shard.NewShard()
	fileBlock := shard.FileBlock{}
	var f gcFile
	var chunkSizes []uint64
	for _, part := range parts {
		var encoded bytes.Buffer
		encoder := xorb.NewEncoder(&encoded, true)
		if _, err := encoder.Write(part); err != nil {
			t.Fatal(err)
		}
		if err := encoder.Close(); err != nil {
			t.Fatal(err)
		}
		xorbHash := encoder.SummoryHash()
		if _, err := st.PutXorb(ctx, "default", xorbHash, bytes.NewReader(encoded.Bytes())); err != nil {
			t.Fatal(err)
		}
		chunkHash := xet.ComputeChunkHash(part)
		f.xorbHashes = append(f.xorbHashes, xorbHash)
		f.chunkHashes = append(f.chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		f.content = append(f.content, part...)
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	f.fileHash = xet.ComputeFileHash(f.chunkHashes, chunkSizes)
	fileBlock.FileHash = f.fileHash
	shardObj.AddFile(fileBlock)
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(f.content)
	f.sha256Hex = hex.EncodeToString(digest[:])

	gcs := st.(GCStore)
	if err := gcs.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		if fileHash == f.fileHash.String() {
			f.shardHash = shardHash
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if f.shardHash == "" {
		t.Fatalf("no file index entry for %s", f.fileHash.String())
	}
	return f
}

func sweptHashes(objs []SweptObject) []string {
	hashes := make([]string, 0, len(objs))
	for _, obj := range objs {
		hashes = append(hashes, obj.Hash)
	}
	slices.Sort(hashes)
	return hashes
}

func sha256Digest(hexDigest string) [32]byte {
	raw, _ := hex.DecodeString(hexDigest)
	var digest [32]byte
	copy(digest[:], raw)
	return digest
}

func TestUnlinkRemovesFileIndexEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("unlink me")})

			// Warm the file index cache so the unlink must evict it.
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard before unlink: %v", err)
			}

			removed, err := Unlink(ctx, gcs, f.fileHash)
			if err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			if !removed {
				t.Fatal("Unlink reported the entry missing")
			}

			if _, err := st.GetShard(ctx, f.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard after unlink = %v, want ErrNotExist", err)
			}

			removed, err = Unlink(ctx, gcs, f.fileHash)
			if err != nil {
				t.Fatalf("second Unlink: %v", err)
			}
			if removed {
				t.Fatal("second Unlink reported an entry")
			}

			// The shard, xorbs, and sha256/chunk indexes survive until a sweep.
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard should survive unlink: %v", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry after unlink = %q, %v; want %q", got, err, f.shardHash)
			}
		})
	}
}

func TestSweepRemovesOrphanedObjects(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := []byte("chunk shared by both files")
			fileA := putGCFile(t, ctx, st, [][]byte{shared, []byte("exclusive to A")})
			fileB := putGCFile(t, ctx, st, [][]byte{shared, []byte("exclusive to B")})
			if fileA.xorbHashes[0] != fileB.xorbHashes[0] {
				t.Fatal("test setup: shared part must map to one xorb")
			}

			// Warm every cache the sweep must invalidate.
			if _, err := st.GetShard(ctx, fileA.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := st.GetShardByChunkHash(ctx, "default", fileA.chunkHashes[1]); err != nil {
				t.Fatal(err)
			}
			rc, err := st.GetReconstructedFile(ctx, "default", sha256Digest(fileA.sha256Hex))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(rc); err != nil {
				t.Fatal(err)
			}
			_ = rc.Close()

			if _, err := Unlink(ctx, gcs, fileA.fileHash); err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			res, err := Sweep(ctx, gcs, noGrace, false)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			if got, want := sweptHashes(res.SweptShards), []string{fileA.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{fileA.xorbHashes[1].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.ReclaimedBytes <= 0 {
				t.Fatalf("ReclaimedBytes = %d, want > 0", res.ReclaimedBytes)
			}
			if res.DeletedSHA256Entries != 1 {
				t.Fatalf("DeletedSHA256Entries = %d, want 1", res.DeletedSHA256Entries)
			}
			if res.DeletedChunkEntries < 1 {
				t.Fatalf("DeletedChunkEntries = %d, want >= 1", res.DeletedChunkEntries)
			}

			// Dead shard and its exclusive xorb are gone, shared xorb stays.
			if _, err := gcs.GetShardByHash(ctx, fileA.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", fileA.xorbHashes[1]); ok {
				t.Fatal("exclusive xorb of the dead shard still stored")
			}
			if ok, _ := st.HasXorb(ctx, "default", fileA.xorbHashes[0]); !ok {
				t.Fatal("shared xorb was swept while file B references it")
			}

			// Stale caches must not resurrect swept state.
			if _, err := st.GetShardByChunkHash(ctx, "default", fileA.chunkHashes[1]); err == nil {
				t.Fatal("chunk lookup for the dead shard's exclusive chunk still resolves")
			}
			if _, err := st.GetReconstructedFile(ctx, "default", sha256Digest(fileA.sha256Hex)); err == nil {
				t.Fatal("sha256 lookup for the swept file still resolves")
			}

			// Chunk entries never point at the dead shard; whether the shared
			// entry survives depends on which shard wrote it last.
			if got, err := gcs.GetChunkIndexEntry(ctx, fileA.chunkHashes[0]); err != nil || got == fileA.shardHash {
				t.Fatalf("shared chunk entry = %q, %v; must not point at the dead shard", got, err)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, fileA.chunkHashes[1]); err != nil || got != "" {
				t.Fatalf("exclusive chunk entry = %q, %v; want removed", got, err)
			}

			// File B is untouched.
			if _, err := st.GetShard(ctx, fileB.fileHash); err != nil {
				t.Fatalf("GetShard(fileB): %v", err)
			}
			rc, err = st.GetReconstructedFile(ctx, "default", sha256Digest(fileB.sha256Hex))
			if err != nil {
				t.Fatalf("GetReconstructedFile(fileB): %v", err)
			}
			data, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil || !bytes.Equal(data, fileB.content) {
				t.Fatalf("file B reconstruction corrupted: %v", err)
			}
		})
	}
}

func TestSweepDryRunDeletesNothing(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("dry run target")})
			if _, err := Unlink(ctx, gcs, f.fileHash); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, noGrace, true)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !res.DryRun {
				t.Fatal("result not marked dry run")
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedChunkEntries != 0 || res.DeletedSHA256Entries != 0 {
				t.Fatalf("dry run reported entry mutations: %+v", res)
			}

			// Everything is still stored.
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard removed by dry run: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb removed by dry run")
			}
			if got, _ := gcs.GetChunkIndexEntry(ctx, f.chunkHashes[0]); got != f.shardHash {
				t.Fatalf("chunk entry = %q, want %q", got, f.shardHash)
			}
			if got, _ := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); got != f.shardHash {
				t.Fatalf("sha256 entry = %q, want %q", got, f.shardHash)
			}
		})
	}
}

func TestSweepGraceWindow(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh object")})
			if _, err := Unlink(ctx, gcs, f.fileHash); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, time.Hour, false)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("grace window ignored: %+v", res)
			}
			if res.SkippedInGrace != 2 {
				t.Fatalf("SkippedInGrace = %d, want 2", res.SkippedInGrace)
			}

			res, err = Sweep(ctx, gcs, noGrace, false)
			if err != nil {
				t.Fatalf("Sweep without grace: %v", err)
			}
			if len(res.SweptShards) != 1 || len(res.SweptXorbs) != 1 {
				t.Fatalf("sweep without grace missed objects: %+v", res)
			}
		})
	}
}

// TestSweepRepointsSharedSHA256 stores identical content under two
// chunkings, so two shards share one sha256 entry. When the sweep removes
// the shard owning the entry while the other still lives, the entry is
// repointed to the live shard and the SHA-256 lookup keeps working.
func TestSweepRepointsSharedSHA256(t *testing.T) {
	content := []byte("same content, two different chunkings")
	for _, backend := range listBackends() {
		for _, unlinkSecond := range []bool{false, true} {
			name := backend.name + "/unlink-first"
			if unlinkSecond {
				name = backend.name + "/unlink-second"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				st := backend.newStore(t)
				gcs := st.(GCStore)

				f1 := putGCFile(t, ctx, st, [][]byte{content})
				f2 := putGCFile(t, ctx, st, [][]byte{content[:11], content[11:]})
				if f1.sha256Hex != f2.sha256Hex {
					t.Fatal("test setup: contents must share a SHA-256")
				}
				// Which shard owns the shared entry is a backend index-write
				// detail (FileStorage keeps the first writer, S3 the last).
				owner, err := gcs.GetSHA256IndexEntry(ctx, f1.sha256Hex)
				if err != nil {
					t.Fatal(err)
				}
				if owner != f1.shardHash && owner != f2.shardHash {
					t.Fatalf("sha256 entry owner = %q, want one of the two shards", owner)
				}

				dead, live := f1, f2
				if unlinkSecond {
					dead, live = f2, f1
				}
				if _, err := Unlink(ctx, gcs, dead.fileHash); err != nil {
					t.Fatal(err)
				}
				res, err := Sweep(ctx, gcs, noGrace, false)
				if err != nil {
					t.Fatalf("Sweep: %v", err)
				}

				wantRepointed := 0
				if owner == dead.shardHash {
					wantRepointed = 1
				}
				if res.RepointedSHA256Entries != wantRepointed {
					t.Fatalf("RepointedSHA256Entries = %d, want %d", res.RepointedSHA256Entries, wantRepointed)
				}
				if res.DeletedSHA256Entries != 0 {
					t.Fatalf("DeletedSHA256Entries = %d, want 0", res.DeletedSHA256Entries)
				}

				// The entry now points at the live shard either way and the
				// SHA-256 read path resolves through it.
				if got, err := gcs.GetSHA256IndexEntry(ctx, live.sha256Hex); err != nil || got != live.shardHash {
					t.Fatalf("sha256 entry = %q, %v; want %q", got, err, live.shardHash)
				}
				gotHash, err := st.GetFileHashBySHA256(ctx, "default", sha256Digest(live.sha256Hex))
				if err != nil {
					t.Fatalf("GetFileHashBySHA256: %v", err)
				}
				if gotHash != live.fileHash {
					t.Fatalf("GetFileHashBySHA256 = %s, want %s", gotHash.String(), live.fileHash.String())
				}
				rc, err := st.GetReconstructedFile(ctx, "default", sha256Digest(live.sha256Hex))
				if err != nil {
					t.Fatalf("GetReconstructedFile: %v", err)
				}
				data, err := io.ReadAll(rc)
				_ = rc.Close()
				if err != nil || !bytes.Equal(data, content) {
					t.Fatalf("reconstruction after sweep corrupted: %v", err)
				}

				// The live file stays reachable by file hash too.
				if _, err := st.GetShard(ctx, live.fileHash); err != nil {
					t.Fatalf("GetShard(live): %v", err)
				}
			})
		}
	}
}

// TestSweepRepointsSharedChunks: two files share chunk A while each also has
// its own chunk; sweeping one must repoint A's entry to the live shard when
// the dead shard owned it, and delete the dead shard's exclusive entry.
func TestSweepRepointsSharedChunks(t *testing.T) {
	partA := []byte("chunk shared by both shards")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f1 := putGCFile(t, ctx, st, [][]byte{partA, []byte("unique to file one")})
			f2 := putGCFile(t, ctx, st, [][]byte{partA, []byte("unique to file two")})
			if f1.chunkHashes[0] != f2.chunkHashes[0] {
				t.Fatal("test setup: shared part must map to one chunk hash")
			}
			// FileStorage keeps the first writer (f1), S3 the last (f2); unlink
			// the owner so the repoint path runs on both backends.
			owner, err := gcs.GetChunkIndexEntry(ctx, f1.chunkHashes[0])
			if err != nil {
				t.Fatal(err)
			}
			dead, live := f1, f2
			if owner == f2.shardHash {
				dead, live = f2, f1
			} else if owner != f1.shardHash {
				t.Fatalf("chunk entry owner = %q, want one of the two shards", owner)
			}
			// Warm the chunk cache so a repoint must evict it.
			if _, err := st.GetShardByChunkHash(ctx, "default", f1.chunkHashes[0]); err != nil {
				t.Fatal(err)
			}

			if _, err := Unlink(ctx, gcs, dead.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, noGrace, false)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			if res.RepointedChunkEntries != 1 {
				t.Fatalf("RepointedChunkEntries = %d, want 1", res.RepointedChunkEntries)
			}

			// The shared chunk entry now points at the surviving shard.
			if got, err := gcs.GetChunkIndexEntry(ctx, live.chunkHashes[0]); err != nil || got != live.shardHash {
				t.Fatalf("shared chunk entry = %q, %v; want %q", got, err, live.shardHash)
			}
			if _, err := st.GetShardByChunkHash(ctx, "default", live.chunkHashes[0]); err != nil {
				t.Fatalf("GetShardByChunkHash(shared): %v", err)
			}
			// The dead shard's exclusive chunk entry is deleted, not repointed.
			if got, err := gcs.GetChunkIndexEntry(ctx, dead.chunkHashes[1]); err != nil || got != "" {
				t.Fatalf("exclusive chunk entry = %q, %v; want removed", got, err)
			}
			if res.DeletedChunkEntries != 1 {
				t.Fatalf("DeletedChunkEntries = %d, want 1", res.DeletedChunkEntries)
			}
			assertFileIntact(t, ctx, st, live)
		})
	}
}

func TestSweepReportsDanglingFileEntries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			fileHash := strings.Repeat("ab", 32)
			backend.writeDangling(t, st, fileHash, strings.Repeat("cd", 32))

			res, err := Sweep(ctx, gcs, noGrace, false)
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
		})
	}
}

func TestSweepThenReuploadResurrects(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts := [][]byte{[]byte("sweep, then upload again")}
			f := putGCFile(t, ctx, st, parts)
			if _, err := Unlink(ctx, gcs, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := Sweep(ctx, gcs, noGrace, false); err != nil {
				t.Fatal(err)
			}

			again := putGCFile(t, ctx, st, parts)
			if again.fileHash != f.fileHash || again.shardHash != f.shardHash {
				t.Fatal("re-upload produced different hashes")
			}
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after re-upload: %v", err)
			}
			rc, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex))
			if err != nil {
				t.Fatalf("GetReconstructedFile after re-upload: %v", err)
			}
			data, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil || !bytes.Equal(data, f.content) {
				t.Fatalf("reconstruction after re-upload corrupted: %v", err)
			}
		})
	}
}

func TestGCSweepSingleFlight(t *testing.T) {
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	g := NewGC(st)

	g.mu.Lock()
	if _, err := g.Sweep(context.Background(), noGrace, false); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("concurrent Sweep = %v, want ErrGCBusy", err)
	}
	g.mu.Unlock()

	if _, err := g.Sweep(context.Background(), noGrace, false); err != nil {
		t.Fatalf("Sweep after release: %v", err)
	}
}

// hookedGCStore wraps a GCStore with callbacks fired at sweep-visible points,
// simulating uploads that commit while a sweep is running.
type hookedGCStore struct {
	GCStore
	afterWalkXorbs     func()
	beforeFileEntryGet func()
	beforeShardLoad    func()
}

func (h *hookedGCStore) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	err := h.GCStore.WalkXorbs(ctx, fn)
	if err == nil && h.afterWalkXorbs != nil {
		cb := h.afterWalkXorbs
		h.afterWalkXorbs = nil
		cb()
	}
	return err
}

func (h *hookedGCStore) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error) {
	if h.beforeFileEntryGet != nil {
		cb := h.beforeFileEntryGet
		h.beforeFileEntryGet = nil
		cb()
	}
	return h.GCStore.GetFileIndexEntry(ctx, fileHash)
}

func (h *hookedGCStore) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	if h.beforeShardLoad != nil {
		cb := h.beforeShardLoad
		h.beforeShardLoad = nil
		cb()
	}
	return h.GCStore.GetShardByHash(ctx, shardHash)
}

func assertFileIntact(t *testing.T, ctx context.Context, st Storage, f gcFile) {
	t.Helper()
	if _, err := st.GetShard(ctx, f.fileHash); err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	rc, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex))
	if err != nil {
		t.Fatalf("GetReconstructedFile: %v", err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || !bytes.Equal(data, f.content) {
		t.Fatalf("reconstruction corrupted: %v", err)
	}
}

// TestSweepSkipsReuploadCommittedDuringMark: a re-upload commits its file
// entry between the walks and the delete phase; the second file-index look
// must drop the shard and its xorbs from the dead sets.
func TestSweepSkipsReuploadCommittedDuringMark(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed during the sweep walks")}
	f := putGCFile(t, ctx, st, parts)
	if _, err := Unlink(ctx, st, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.afterWalkXorbs = func() { putGCFile(t, ctx, st, parts) }
	res, err := Sweep(ctx, hooked, noGrace, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	assertFileIntact(t, ctx, st, f)
}

// TestSweepSkipsReuploadCommittedBeforeDelete: the commit lands after the
// re-check, in the window right before the shard is deleted; the per-shard
// file-entry look must skip the shard, its entries, and its xorbs.
func TestSweepSkipsReuploadCommittedBeforeDelete(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed right before the delete")}
	f := putGCFile(t, ctx, st, parts)
	if _, err := Unlink(ctx, st, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeFileEntryGet = func() { putGCFile(t, ctx, st, parts) }
	res, err := Sweep(ctx, hooked, noGrace, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	if got, err := st.GetChunkIndexEntry(ctx, f.chunkHashes[0]); err != nil || got != f.shardHash {
		t.Fatalf("chunk entry = %q, %v; want %q", got, err, f.shardHash)
	}
	if got, err := st.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
		t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.shardHash)
	}
	assertFileIntact(t, ctx, st, f)
}

// TestSweepShieldsCommitDuringShardDeletePhase: an upload sharing a doomed
// xorb commits during the shard-delete phase, after the file-index re-check;
// the final re-shield before the xorb deletes must keep the shared xorb.
func TestSweepShieldsCommitDuringShardDeletePhase(t *testing.T) {
	partA := []byte("shared payload the sweep must keep")
	partB := []byte("unique to the late upload")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f1 := putGCFile(t, ctx, st, [][]byte{partA})
			if _, err := Unlink(ctx, gcs, f1.fileHash); err != nil {
				t.Fatal(err)
			}

			// The hook fires inside sweepShard for the dead shard, i.e. after
			// the recommitted re-check; file2 dedup-hits file1's only xorb.
			var f2 gcFile
			hooked := &hookedGCStore{GCStore: gcs}
			hooked.beforeFileEntryGet = func() {
				f2 = putGCFile(t, ctx, st, [][]byte{partA, partB})
			}
			res, err := Sweep(ctx, hooked, noGrace, false)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if f2.shardHash == "" {
				t.Fatal("hook did not run")
			}

			if got, want := sweptHashes(res.SweptShards), []string{f1.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if len(res.SweptXorbs) != 0 {
				t.Fatalf("SweptXorbs = %v, want none", sweptHashes(res.SweptXorbs))
			}
			if ok, _ := st.HasXorb(ctx, "default", f1.xorbHashes[0]); !ok {
				t.Fatal("xorb shared with the late upload was swept")
			}
			assertFileIntact(t, ctx, st, f2)

			// file1 itself stays swept.
			if _, err := st.GetShard(ctx, f1.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard(file1) = %v, want ErrNotExist", err)
			}
			if _, err := gcs.GetShardByHash(ctx, f1.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
		})
	}
}

// TestSweepShardVanishedBeforeDelete: the shard object disappears between
// the walk and its delete; the sweep counts nothing for it and still sweeps
// the now-unreferenced xorb.
func TestSweepShardVanishedBeforeDelete(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("vanishes before the delete")})
	if _, err := Unlink(ctx, st, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeShardLoad = func() {
		if err := os.Remove(st.objectPath("shards", f.shardHash)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := Sweep(ctx, hooked, noGrace, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 {
		t.Fatalf("vanished shard reported swept: %+v", res.SweptShards)
	}
	if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.ReclaimedBytes != res.SweptXorbs[0].Size {
		t.Fatalf("ReclaimedBytes = %d, want %d (xorb only)", res.ReclaimedBytes, res.SweptXorbs[0].Size)
	}
}

// TestSweepGraceShieldsDedupedXorbs: an in-grace shard with no file entry is
// an upload mid-commit; the xorbs it deduplicated against, older than the
// grace window themselves, must survive the sweep.
func TestSweepGraceShieldsDedupedXorbs(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("deduplicated old payload")})
	if _, err := Unlink(ctx, st, f.fileHash); err != nil {
		t.Fatal(err)
	}

	// Age the original objects out of the grace window.
	old := time.Now().Add(-2 * time.Hour)
	for _, p := range []string{
		st.objectPath("shards", f.shardHash),
		st.objectPath("xorbs", f.xorbHashes[0].String()),
	} {
		if err := os.Chtimes(p, old, old); err != nil {
			t.Fatal(err)
		}
	}

	// Simulate an upload mid-commit: its shard object is stored (fresh
	// mtime) and references the old xorb, but no file entry exists yet.
	pending := shard.NewShard()
	pending.AddFile(shard.FileBlock{
		FileHash: f.fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: f.xorbHashes[0], UnpackedSegBytes: uint32(len(f.content)), ChunkIndexEnd: 1},
		},
	})
	r, err := pending.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	pendingHash, err := computeShardHashFromReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	pendingPath := st.objectPath("shards", pendingHash)
	if err := os.MkdirAll(filepath.Dir(pendingPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pendingPath, encoded, 0644); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(ctx, st, time.Hour, false)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if len(res.SweptXorbs) != 0 {
		t.Fatalf("deduplicated xorb swept: %+v", res.SweptXorbs)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
		t.Fatal("shielded xorb removed")
	}
}
