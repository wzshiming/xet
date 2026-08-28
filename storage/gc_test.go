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

// addGCFileBlock stores one single-chunk xorb per part and appends the
// matching file and CAS blocks to shardObj, returning the file's hashes.
func addGCFileBlock(t *testing.T, ctx context.Context, st Storage, shardObj *shard.Shard, parts [][]byte) (xet.FileHash, []xet.XorbHash, []xet.ChunkHash) {
	t.Helper()
	fileBlock := shard.FileBlock{}
	var xorbHashes []xet.XorbHash
	var chunkHashes []xet.ChunkHash
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
		xorbHashes = append(xorbHashes, xorbHash)
		chunkHashes = append(chunkHashes, chunkHash)
		chunkSizes = append(chunkSizes, uint64(len(part)))
		fileBlock.Entries = append(fileBlock.Entries, shard.FileDataSequenceEntry{
			CASHash: xorbHash, UnpackedSegBytes: uint32(len(part)), ChunkIndexEnd: 1,
		})
		shardObj.AddCASBlock(shard.CASBlock{
			CASHash: xorbHash,
			Chunks:  []shard.CASChunkSequenceEntry{{ChunkHash: chunkHash, UnpackedSegBytes: uint32(len(part))}},
		})
	}
	fileBlock.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fileBlock)
	return fileBlock.FileHash, xorbHashes, chunkHashes
}

// putGCFile stores one file chunked as parts (one single-chunk xorb per
// part). Identical parts across files share the same xorb.
func putGCFile(t *testing.T, ctx context.Context, st Storage, parts [][]byte) gcFile {
	t.Helper()
	shardObj := shard.NewShard()
	var f gcFile
	f.fileHash, f.xorbHashes, f.chunkHashes = addGCFileBlock(t, ctx, st, shardObj, parts)
	for _, part := range parts {
		f.content = append(f.content, part...)
	}
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

			removed, err := NewGC(gcs).Unlink(ctx, f.fileHash)
			if err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			if !removed {
				t.Fatal("Unlink reported the entry missing")
			}

			if _, err := st.GetShard(ctx, f.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard after unlink = %v, want ErrNotExist", err)
			}

			removed, err = NewGC(gcs).Unlink(ctx, f.fileHash)
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

			if _, err := NewGC(gcs).Unlink(ctx, fileA.fileHash); err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, DryRun: true, Anchor: AnchorFiles})
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
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: time.Hour, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("grace window ignored: %+v", res)
			}
			if res.SkippedInGrace != 2 {
				t.Fatalf("SkippedInGrace = %d, want 2", res.SkippedInGrace)
			}

			res, err = Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("Sweep without grace: %v", err)
			}
			if len(res.SweptShards) != 1 || len(res.SweptXorbs) != 1 {
				t.Fatalf("sweep without grace missed objects: %+v", res)
			}
		})
	}
}

// TestSweepNegativeGraceSentinelSweepsFreshObjects: a negative grace (the
// HTTP "window disabled" sentinel) puts the cutoff just past now; the
// object walks must compare against it untruncated — flooring a future
// cutoff to the whole second would wrongly shield objects written in the
// current second, re-enabling a window the caller disabled.
func TestSweepNegativeGraceSentinelSweepsFreshObjects(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh but window disabled")})
	if _, err := NewGC(st).Unlink(ctx, f.fileHash); err != nil {
		t.Fatal(err)
	}

	res, err := Sweep(ctx, st, SweepOptions{Grace: -time.Nanosecond, Anchor: AnchorFiles})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
	if res.SkippedInGrace != 0 {
		t.Fatalf("SkippedInGrace = %d, want 0", res.SkippedInGrace)
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
				if _, err := NewGC(gcs).Unlink(ctx, dead.fileHash); err != nil {
					t.Fatal(err)
				}
				res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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

			if _, err := NewGC(gcs).Unlink(ctx, dead.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
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

// TestSweepReportsDanglingSHA256Entries: sha256 entries whose shard object
// is missing are reported — sorted, in dry runs and real sweeps alike —
// and never deleted, mirroring DanglingFileEntries.
func TestSweepReportsDanglingSHA256Entries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shaA := strings.Repeat("aa", 32)
			shaB := strings.Repeat("bb", 32)
			missingA := strings.Repeat("d1", 32)
			missingB := strings.Repeat("e2", 32)
			if err := gcs.SetSHA256IndexEntry(ctx, shaB, missingB); err != nil {
				t.Fatal(err)
			}
			if err := gcs.SetSHA256IndexEntry(ctx, shaA, missingA); err != nil {
				t.Fatal(err)
			}

			want := []string{shaA, shaB}
			for _, dryRun := range []bool{true, false} {
				res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, DryRun: dryRun})
				if err != nil {
					t.Fatalf("Sweep(dryRun=%v): %v", dryRun, err)
				}
				if !slices.Equal(res.DanglingSHA256Entries, want) {
					t.Fatalf("DanglingSHA256Entries(dryRun=%v) = %v, want %v", dryRun, res.DanglingSHA256Entries, want)
				}
			}

			// The entries are reported, never deleted.
			for sha, missing := range map[string]string{shaA: missingA, shaB: missingB} {
				if got, err := gcs.GetSHA256IndexEntry(ctx, sha); err != nil || got != missing {
					t.Fatalf("sha256 entry %s = %q, %v; want %q", sha, got, err, missing)
				}
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
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorFiles}); err != nil {
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

func TestGCSweepStepSingleFlight(t *testing.T) {
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	g := NewGC(fs)

	g.mu.Lock()
	if _, err := g.SweepStep(context.Background(), SweepOptions{Grace: noGrace}); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("concurrent SweepStep = %v, want ErrGCBusy", err)
	}
	g.mu.Unlock()

	if _, err := g.SweepStep(context.Background(), SweepOptions{Grace: noGrace}); err != nil {
		t.Fatalf("SweepStep after release: %v", err)
	}
}

// hookedGCStore wraps a GCStore with callbacks fired at sweep-visible points,
// simulating uploads that commit while a sweep is running. A non-zero age
// backdates every modTime the walks and file-entry reads report, simulating
// aged objects on backends whose timestamps cannot be set (S3); file hashes
// in freshFileEntries are exempt and keep their true entry mtimes.
type hookedGCStore struct {
	GCStore
	age                time.Duration
	freshFileEntries   map[string]bool
	afterWalkXorbs     func()
	beforeFileEntryGet func()
	beforeShardLoad    func()
	// onFileEntryGet / onSHA256EntryGet fire before every call of the
	// wrapped getter with a 1-based call count, unlike the before* hooks
	// consumed on first fire.
	onFileEntryGet   func(n int)
	fileEntryGets    int
	onSHA256EntryGet func(n int)
	sha256EntryGets  int
	beforeWalkShards func() // fired before every WalkShards delegation
	walkShardsCalls  int    // WalkShards invocations
}

// agedStore wraps st so every stored object looks written two hours ago.
func agedStore(st GCStore) *hookedGCStore {
	return &hookedGCStore{GCStore: st, age: 2 * time.Hour}
}

// walkTime substitutes the aged modTime when aging is enabled.
func (h *hookedGCStore) walkTime(modTime time.Time) time.Time {
	if h.age == 0 {
		return modTime
	}
	return time.Now().Add(-h.age)
}

func (h *hookedGCStore) WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error {
	h.walkShardsCalls++
	if h.beforeWalkShards != nil {
		h.beforeWalkShards()
	}
	return h.GCStore.WalkShards(ctx, func(shardHash string, size int64, modTime time.Time) error {
		return fn(shardHash, size, h.walkTime(modTime))
	})
}

func (h *hookedGCStore) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	err := h.GCStore.WalkXorbs(ctx, func(xorbHash string, size int64, modTime time.Time) error {
		return fn(xorbHash, size, h.walkTime(modTime))
	})
	if err == nil && h.afterWalkXorbs != nil {
		cb := h.afterWalkXorbs
		h.afterWalkXorbs = nil
		cb()
	}
	return err
}

func (h *hookedGCStore) GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, time.Time, error) {
	if h.beforeFileEntryGet != nil {
		cb := h.beforeFileEntryGet
		h.beforeFileEntryGet = nil
		cb()
	}
	if h.onFileEntryGet != nil {
		h.fileEntryGets++
		h.onFileEntryGet(h.fileEntryGets)
	}
	shardHash, modTime, err := h.GCStore.GetFileIndexEntry(ctx, fileHash)
	if err == nil && shardHash != "" {
		if h.freshFileEntries[fileHash.String()] {
			// True entry time at S3's second precision.
			modTime = modTime.Truncate(time.Second)
		} else {
			modTime = h.walkTime(modTime)
		}
	}
	return shardHash, modTime, err
}

func (h *hookedGCStore) GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error) {
	if h.onSHA256EntryGet != nil {
		h.sha256EntryGets++
		h.onSHA256EntryGet(h.sha256EntryGets)
	}
	return h.GCStore.GetSHA256IndexEntry(ctx, sha256Hex)
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
	if _, err := NewGC(st).Unlink(ctx, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.afterWalkXorbs = func() { putGCFile(t, ctx, st, parts) }
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	assertFileIntact(t, ctx, st, f)
}

// TestSweepSkipsReuploadCommittedBeforeDelete: a writer in another process
// (a second store on the same directory) commits right before the shard
// delete; the per-shard file-entry look must skip the shard, its entries,
// and its xorbs.
func TestSweepSkipsReuploadCommittedBeforeDelete(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewFileStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewFileStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed right before the delete")}
	f := putGCFile(t, ctx, st, parts)
	if _, err := NewGC(st).Unlink(ctx, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeFileEntryGet = func() { putGCFile(t, ctx, writer, parts) }
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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
			if _, err := NewGC(gcs).Unlink(ctx, f1.fileHash); err != nil {
				t.Fatal(err)
			}

			// The hook fires inside sweepShard for the dead shard, i.e. after
			// the recommitted re-check; file2 dedup-hits file1's only xorb.
			var f2 gcFile
			hooked := &hookedGCStore{GCStore: gcs}
			hooked.beforeFileEntryGet = func() {
				f2 = putGCFile(t, ctx, st, [][]byte{partA, partB})
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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
	if _, err := NewGC(st).Unlink(ctx, f.fileHash); err != nil {
		t.Fatal(err)
	}

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeShardLoad = func() {
		if err := os.Remove(st.objectPath("shards", f.shardHash)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
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
	if _, err := NewGC(st).Unlink(ctx, f.fileHash); err != nil {
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

	res, err := Sweep(ctx, st, SweepOptions{Grace: time.Hour, Anchor: AnchorFiles})
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

func sortedSwept(objs []SweptObject) []SweptObject {
	out := slices.Clone(objs)
	slices.SortFunc(out, func(a, b SweptObject) int { return strings.Compare(a.Hash, b.Hash) })
	return out
}

// putUnlinkedGCFiles stores one single-part file per content and unlinks it,
// leaving its shard and xorb unreferenced.
func putUnlinkedGCFiles(t *testing.T, ctx context.Context, st Storage, contents ...string) []gcFile {
	t.Helper()
	gcs := st.(GCStore)
	files := make([]gcFile, 0, len(contents))
	for _, content := range contents {
		f := putGCFile(t, ctx, st, [][]byte{[]byte(content)})
		if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
	}
	return files
}

// TestSweepStepDrainsInBatches: MaxDeletes=1 steps drain one cycle item by
// item, and the Done step's cumulative result matches a single full pass
// over an identically prepared store.
func TestSweepStepDrainsInBatches(t *testing.T) {
	contents := []string{"batched sweep one", "batched sweep two", "batched sweep three"}
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			putUnlinkedGCFiles(t, ctx, st, contents...)

			g := NewGC(st.(GCStore))
			opts := SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorFiles}
			var res *SweepResult
			steps := 0
			remaining := len(contents) * 2 // one shard and one xorb per file
			for {
				var err error
				res, err = g.SweepStep(ctx, opts)
				if err != nil {
					t.Fatalf("SweepStep %d: %v", steps, err)
				}
				steps++
				if steps == 1 && res.Done {
					t.Fatal("first step already done")
				}
				left := res.RemainingShards + res.RemainingXorbs
				if left != remaining-1 {
					t.Fatalf("step %d: remaining %d, want exactly %d (one item per step)", steps, left, remaining-1)
				}
				remaining = left
				if res.Done {
					if left != 0 {
						t.Fatalf("done with remaining %d/%d", res.RemainingShards, res.RemainingXorbs)
					}
					break
				}
				if steps > 20 {
					t.Fatal("cycle did not finish in 20 steps")
				}
			}
			if steps <= 1 {
				t.Fatalf("steps = %d, want > 1", steps)
			}

			// Walk order differs per backend, so compare the results as sets.
			st2 := backend.newStore(t)
			putUnlinkedGCFiles(t, ctx, st2, contents...)
			full, err := Sweep(ctx, st2.(GCStore), SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !full.Done || full.RemainingShards != 0 || full.RemainingXorbs != 0 {
				t.Fatalf("full pass progress = %v %d/%d, want done 0/0", full.Done, full.RemainingShards, full.RemainingXorbs)
			}
			if got, want := sortedSwept(res.SweptShards), sortedSwept(full.SweptShards); !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sortedSwept(res.SweptXorbs), sortedSwept(full.SweptXorbs); !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedChunkEntries != full.DeletedChunkEntries ||
				res.DeletedSHA256Entries != full.DeletedSHA256Entries ||
				res.RepointedChunkEntries != full.RepointedChunkEntries ||
				res.RepointedSHA256Entries != full.RepointedSHA256Entries {
				t.Fatalf("entry counters diverge: stepped %+v, full %+v", res, full)
			}
			if res.ReclaimedBytes != full.ReclaimedBytes {
				t.Fatalf("ReclaimedBytes = %d, want %d", res.ReclaimedBytes, full.ReclaimedBytes)
			}
			if res.SkippedInGrace != full.SkippedInGrace {
				t.Fatalf("SkippedInGrace = %d, want %d", res.SkippedInGrace, full.SkippedInGrace)
			}
		})
	}
}

// TestSweepStepMidCycleCommitSurvives: a re-upload commits between steps;
// the per-shard file-entry re-read and the final re-shield keep the
// still-queued objects and the re-linked file stays reconstructable.
func TestSweepStepMidCycleCommitSurvives(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			files := putUnlinkedGCFiles(t, ctx, st, "mid-cycle commit one", "mid-cycle commit two")

			// Aged mtimes leave the re-checks as the only shield.
			g := NewGC(agedStore(gcs))
			opts := SweepOptions{Grace: time.Hour, MaxDeletes: 1, Anchor: AnchorFiles}
			res, err := g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.Done || len(res.SweptShards) != 1 {
				t.Fatalf("first step = %+v, want one swept shard and not done", res)
			}

			// Re-upload the file whose shard is still queued: PutShard
			// recommits its file entry.
			kept, gone := files[0], files[1]
			if res.SweptShards[0].Hash == kept.shardHash {
				kept, gone = gone, kept
			}
			if again := putGCFile(t, ctx, st, [][]byte{kept.content}); again.shardHash != kept.shardHash {
				t.Fatal("re-upload produced different hashes")
			}

			// The skipped shard is still one consumed item against MaxDeletes.
			res, err = g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.Done || res.RemainingShards != 0 || res.RemainingXorbs != 2 {
				t.Fatalf("skip step = done %v, remaining %d/%d; want not done, 0/2", res.Done, res.RemainingShards, res.RemainingXorbs)
			}
			if len(res.SweptShards) != 1 {
				t.Fatalf("SweptShards = %d, want 1 (the re-uploaded shard skipped, not swept)", len(res.SweptShards))
			}

			for i := 0; !res.Done; i++ {
				if i > 20 {
					t.Fatal("cycle did not finish in 20 steps")
				}
				if res, err = g.SweepStep(ctx, opts); err != nil {
					t.Fatalf("SweepStep: %v", err)
				}
			}

			// The re-linked file survives with its xorb; the other is gone.
			assertFileIntact(t, ctx, st, kept)
			if ok, _ := st.HasXorb(ctx, "default", kept.xorbHashes[0]); !ok {
				t.Fatal("xorb of the re-uploaded file was swept")
			}
			if _, err := gcs.GetShardByHash(ctx, gone.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", gone.xorbHashes[0]); ok {
				t.Fatal("dead file's xorb still stored")
			}
		})
	}
}

// TestSweepStepOptionChangeRestarts: a step with a different window discards
// the half-consumed cycle and marks afresh, picking up objects unlinked
// after the first mark.
func TestSweepStepOptionChangeRestarts(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			putUnlinkedGCFiles(t, ctx, st, "restart file one", "restart file two")
			g := NewGC(gcs)
			res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.RemainingShards != 1 || res.RemainingXorbs != 2 {
				t.Fatalf("first step remaining = %d/%d, want 1/2", res.RemainingShards, res.RemainingXorbs)
			}

			// A third unlinked file lands after the first mark.
			putUnlinkedGCFiles(t, ctx, st, "restart file three")

			// A different Grace: the old cycle is discarded and re-marked.
			res, err = g.SweepStep(ctx, SweepOptions{Grace: -2 * time.Hour, MaxDeletes: 1, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if len(res.SweptShards) != 1 {
				t.Fatalf("re-marked step swept %d shards, want a fresh accumulator with 1", len(res.SweptShards))
			}
			if res.RemainingShards != 1 || res.RemainingXorbs != 3 {
				t.Fatalf("re-marked remaining = %d/%d, want 1/3 (new file included)", res.RemainingShards, res.RemainingXorbs)
			}
		})
	}
}

// TestSweepStepDryRunLeavesCycle: a dry-run step reports a full stateless
// pass without consuming the in-progress cycle.
func TestSweepStepDryRunLeavesCycle(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			files := putUnlinkedGCFiles(t, ctx, st, "dry step file one", "dry step file two")
			g := NewGC(gcs)
			opts := SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorFiles}
			res, err := g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.RemainingShards != 1 || res.RemainingXorbs != 2 {
				t.Fatalf("first step remaining = %d/%d, want 1/2", res.RemainingShards, res.RemainingXorbs)
			}
			queued := files[0]
			if res.SweptShards[0].Hash == files[0].shardHash {
				queued = files[1]
			}

			dryOpts := opts
			dryOpts.DryRun = true
			dry, err := g.SweepStep(ctx, dryOpts)
			if err != nil {
				t.Fatalf("dry SweepStep: %v", err)
			}
			if !dry.DryRun || !dry.Done {
				t.Fatalf("dry step = %+v, want a done dry-run report", dry)
			}
			// The report still sees everything the real cycle has not consumed.
			if got, want := sweptHashes(dry.SweptShards), []string{queued.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("dry SweptShards = %v, want %v", got, want)
			}
			wantXorbs := []string{files[0].xorbHashes[0].String(), files[1].xorbHashes[0].String()}
			slices.Sort(wantXorbs)
			if got := sweptHashes(dry.SweptXorbs); !slices.Equal(got, wantXorbs) {
				t.Fatalf("dry SweptXorbs = %v, want %v", got, wantXorbs)
			}

			// The real cycle resumes where it stopped.
			res, err = g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep after dry run: %v", err)
			}
			if len(res.SweptShards) != 2 {
				t.Fatalf("cumulative SweptShards = %d, want 2 (old cycle kept)", len(res.SweptShards))
			}
			if res.RemainingShards != 0 || res.RemainingXorbs != 2 {
				t.Fatalf("remaining after resume = %d/%d, want 0/2", res.RemainingShards, res.RemainingXorbs)
			}
		})
	}
}

// TestSweepStepBudgetProgress: a vanishing budget still consumes one item
// per step, so the cycle always terminates.
func TestSweepStepBudgetProgress(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st, "budget file one", "budget file two")

	g := NewGC(st)
	opts := SweepOptions{Grace: noGrace, Budget: time.Nanosecond, Anchor: AnchorFiles}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || res.RemainingShards != 1 || res.RemainingXorbs != 2 {
		t.Fatalf("first step = done %v, remaining %d/%d; want not done, 1/2 (budget honored)", res.Done, res.RemainingShards, res.RemainingXorbs)
	}
	for steps := 0; !res.Done; steps++ {
		if steps > 4 {
			t.Fatalf("cycle not done after %d steps", steps)
		}
		if res, err = g.SweepStep(ctx, opts); err != nil {
			t.Fatalf("SweepStep: %v", err)
		}
	}
	if len(res.SweptShards) != 2 || len(res.SweptXorbs) != 2 {
		t.Fatalf("cumulative result = %d shards, %d xorbs; want 2/2", len(res.SweptShards), len(res.SweptXorbs))
	}
}

// TestSweepStepExpiredCycleRemarks: a cycle parked for one grace window
// after its mark is discarded and re-marked on the next step, so its shield
// state can never go staler than one window; a non-positive grace is the
// explicit no-safety-window mode and never expires.
func TestSweepStepExpiredCycleRemarks(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st, "expiring cycle one", "expiring cycle two")

	// Aged mtimes make the objects sweepable despite the window.
	g := NewGC(agedStore(st))
	opts := SweepOptions{Grace: time.Hour, MaxDeletes: 1, Anchor: AnchorFiles}
	res, err := g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || res.RemainingShards+res.RemainingXorbs == 0 {
		t.Fatalf("first step = done %v, remaining %d/%d; want a parked cycle", res.Done, res.RemainingShards, res.RemainingXorbs)
	}
	if g.cycle == nil {
		t.Fatal("no parked cycle after the first step")
	}

	// Parked past its window: the next step must discard it and mark afresh.
	g.cycle.marked = time.Now().Add(-2 * time.Hour)
	res, err = g.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep after expiry: %v", err)
	}
	if got := len(res.SweptShards) + len(res.SweptXorbs); got != 1 {
		t.Fatalf("swept objects after expiry = %d, want 1 (a fresh accumulation, not the resumed cycle's 2)", got)
	}

	st2, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st2, "immortal cycle one", "immortal cycle two")
	g2 := NewGC(st2)
	opts = SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorFiles}
	res, err = g2.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || len(res.SweptShards) != 1 {
		t.Fatalf("first step = %+v, want one swept shard and not done", res)
	}
	if g2.cycle == nil {
		t.Fatal("no parked cycle after the first step")
	}
	g2.cycle.marked = time.Now().Add(-1000 * time.Hour)
	res, err = g2.SweepStep(ctx, opts)
	if err != nil {
		t.Fatalf("SweepStep after aging: %v", err)
	}
	if len(res.SweptShards) != 2 {
		t.Fatalf("cumulative SweptShards = %d, want 2 (cycle resumed under a non-positive grace)", len(res.SweptShards))
	}
}

// TestSweepStepResumeReshieldsLateCommit: a stepped cycle parks in the xorb
// phase with an aged xorb still queued; a commit landing during the park
// dedup-reuses that xorb. The resuming step must re-run the re-shield before
// consuming the queue, so the late commit's xorb survives the cycle.
func TestSweepStepResumeReshieldsLateCommit(t *testing.T) {
	partShared := []byte("aged payload a late commit reuses")
	partExtra := []byte("unique to the late commit")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f1 := putGCFile(t, ctx, st, [][]byte{partShared})
			if _, err := NewGC(gcs).Unlink(ctx, f1.fileHash); err != nil {
				t.Fatal(err)
			}

			// Aged mtimes make the unlinked shard and xorb sweepable.
			g := NewGC(agedStore(gcs))
			opts := SweepOptions{Grace: time.Hour, MaxDeletes: 1, Anchor: AnchorFiles}
			res, err := g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.Done || res.RemainingShards != 0 || res.RemainingXorbs != 1 {
				t.Fatalf("first step = done %v, remaining %d/%d; want parked with 0/1", res.Done, res.RemainingShards, res.RemainingXorbs)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f1.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			// The commit below only exercises the resume re-shield if the
			// cycle really parked in the xorb phase.
			if g.cycle == nil || g.cycle.phase != sweepPhaseXorbs {
				t.Fatal("cycle not parked in the xorb phase")
			}

			// A commit lands during the park, dedup-reusing the queued xorb.
			f2 := putGCFile(t, ctx, st, [][]byte{partShared, partExtra})
			if f2.xorbHashes[0] != f1.xorbHashes[0] {
				t.Fatal("test setup: shared part must map to one xorb")
			}

			res, err = g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("resumed SweepStep: %v", err)
			}
			if !res.Done || res.RemainingXorbs != 0 {
				t.Fatalf("resumed step = done %v, remaining xorbs %d; want done 0", res.Done, res.RemainingXorbs)
			}
			if len(res.SweptXorbs) != 0 {
				t.Fatalf("SweptXorbs = %v, want none (the late commit reuses the queued xorb)", sweptHashes(res.SweptXorbs))
			}
			if ok, _ := st.HasXorb(ctx, "default", f1.xorbHashes[0]); !ok {
				t.Fatal("xorb reused by the late commit was swept")
			}
			assertFileIntact(t, ctx, st, f2)
		})
	}
}

// TestSweepReshieldSkippedWithoutDeadXorbs: a sweep with dead shards but an
// empty dead-xorb queue has nothing to re-shield, so the transition walk is
// skipped — the stored shards are walked exactly once, by the mark's dead
// collection — while the dead shard is still reclaimed.
func TestSweepReshieldSkippedWithoutDeadXorbs(t *testing.T) {
	shared := []byte("xorb shared with a live file")
	extra := []byte("exclusive to the live file")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// The dead shard's only xorb is shared with a live file, so the
			// dead-xorb queue stays empty.
			f1 := putGCFile(t, ctx, st, [][]byte{shared})
			f2 := putGCFile(t, ctx, st, [][]byte{shared, extra})
			if f2.xorbHashes[0] != f1.xorbHashes[0] {
				t.Fatal("test setup: shared part must map to one xorb")
			}
			if _, err := NewGC(gcs).Unlink(ctx, f1.fileHash); err != nil {
				t.Fatal(err)
			}

			hooked := &hookedGCStore{GCStore: gcs}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if hooked.walkShardsCalls != 1 {
				t.Fatalf("WalkShards called %d times, want 1 (mark only; re-shield skipped)", hooked.walkShardsCalls)
			}

			// The sweep itself still works: dead shard gone, nothing else.
			if got, want := sweptHashes(res.SweptShards), []string{f1.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if len(res.SweptXorbs) != 0 {
				t.Fatalf("SweptXorbs = %v, want none", sweptHashes(res.SweptXorbs))
			}
			if _, err := gcs.GetShardByHash(ctx, f1.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f1.xorbHashes[0]); !ok {
				t.Fatal("shared xorb was swept")
			}
			if ok, _ := st.HasXorb(ctx, "default", f2.xorbHashes[1]); !ok {
				t.Fatal("live file's exclusive xorb was swept")
			}
			assertFileIntact(t, ctx, st, f2)
		})
	}
}

// TestSweepStepReshieldNotChargedToBudget: the resume re-shield always runs
// whole and its wall time must not count against the step's Budget — a walk
// slower than the whole budget still leaves the step its full allowance.
func TestSweepStepReshieldNotChargedToBudget(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st, "budgeted walk one", "budgeted walk two")

	hooked := &hookedGCStore{GCStore: st}
	g := NewGC(hooked)
	res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 2, Anchor: AnchorFiles})
	if err != nil {
		t.Fatalf("SweepStep: %v", err)
	}
	if res.Done || res.RemainingShards != 0 || res.RemainingXorbs != 2 {
		t.Fatalf("first step = done %v, remaining %d/%d; want parked with 0/2", res.Done, res.RemainingShards, res.RemainingXorbs)
	}
	if g.cycle == nil || g.cycle.phase != sweepPhaseXorbs {
		t.Fatal("cycle not parked in the xorb phase")
	}

	// The resume re-shield outlasts the entire budget; both queued xorbs
	// must still be consumed within it.
	hooked.beforeWalkShards = func() { time.Sleep(500 * time.Millisecond) }
	res, err = g.SweepStep(ctx, SweepOptions{Grace: noGrace, Budget: 250 * time.Millisecond, Anchor: AnchorFiles})
	if err != nil {
		t.Fatalf("resumed SweepStep: %v", err)
	}
	if !res.Done || res.RemainingXorbs != 0 || len(res.SweptXorbs) != 2 {
		t.Fatalf("resumed step = done %v, remaining xorbs %d, swept xorbs %d; want done 0/2 (re-shield charged to the budget)",
			res.Done, res.RemainingXorbs, len(res.SweptXorbs))
	}
}

// putShardObject stores an encoded shard object directly, bypassing PutShard
// and its index writes, and returns its hash.
func putShardObject(t *testing.T, ctx context.Context, st Storage, sh *shard.Shard) string {
	t.Helper()
	r, err := sh.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := computeShardHashFromReader(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	switch b := st.(type) {
	case *FileStorage:
		path := b.objectPath("shards", hash)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0644); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.putObject(ctx, b.objectKey("shards", hash), bytes.NewReader(encoded)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported backend %T", st)
	}
	return hash
}

// TestUnlinkSHA256RemovesEntry: UnlinkSHA256 drops only the index/sha256
// entry — SHA-256 lookups stop resolving at once while file-hash access
// keeps working — and reports existence like Unlink; the all-zero digest is
// rejected.
func TestUnlinkSHA256RemovesEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("unlink my sha256")})

			// Warm the sha256 cache so the unlink must evict it.
			rc, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.ReadAll(rc); err != nil {
				t.Fatal(err)
			}
			_ = rc.Close()

			removed, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex))
			if err != nil {
				t.Fatalf("UnlinkSHA256: %v", err)
			}
			if !removed {
				t.Fatal("UnlinkSHA256 reported the entry missing")
			}

			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != "" {
				t.Fatalf("sha256 entry after unlink = %q, %v; want removed", got, err)
			}
			if _, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex)); err == nil {
				t.Fatal("sha256 reconstruction still resolves")
			}
			if _, err := st.GetFileHashBySHA256(ctx, "default", sha256Digest(f.sha256Hex)); err == nil {
				t.Fatal("GetFileHashBySHA256 still resolves")
			}

			// File-hash paths keep working; nothing else was touched.
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after UnlinkSHA256: %v", err)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard should survive UnlinkSHA256: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb removed by UnlinkSHA256")
			}

			// Second unlink through the GC delegate reports the entry gone.
			removed, err = NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex))
			if err != nil {
				t.Fatalf("second UnlinkSHA256: %v", err)
			}
			if removed {
				t.Fatal("second UnlinkSHA256 reported an entry")
			}

			if _, err := NewGC(gcs).UnlinkSHA256(ctx, [32]byte{}); err == nil {
				t.Fatal("all-zero digest accepted")
			}
		})
	}
}

// TestSweepDefaultAnchorNeedsBothUnlinks: under the default anchor a shard
// stays live while either its file entry or its sha256 entry remains; only
// unlinking both lets a sweep reclaim it.
func TestSweepDefaultAnchorNeedsBothUnlinks(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name+"/file-unlink-only", func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("file entry unlinked only")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("sha-anchored shard swept: %+v", res)
			}
			// The content stays resolvable through its SHA-256.
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard should survive: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb swept")
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
		})
		t.Run(backend.name+"/sha-unlink-only", func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("sha entry unlinked only")})
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("file-anchored shard swept: %+v", res)
			}
			// File-hash access keeps working.
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb swept")
			}
		})
		t.Run(backend.name+"/both-unlinked", func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("both entries unlinked")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedChunkEntries != 1 {
				t.Fatalf("DeletedChunkEntries = %d, want 1", res.DeletedChunkEntries)
			}
			// Both entries were unlinked up front, so the reverse-clean finds
			// nothing left to remove.
			if res.DeletedFileEntries != 0 || res.RepointedFileEntries != 0 || res.DeletedSHA256Entries != 0 {
				t.Fatalf("reverse-clean counters nonzero: %+v", res)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); ok {
				t.Fatal("xorb still stored")
			}
			if got, _ := gcs.GetChunkIndexEntry(ctx, f.chunkHashes[0]); got != "" {
				t.Fatalf("chunk entry = %q, want removed", got)
			}
		})
	}
}

// TestSweepAnchorSHA256ReclaimsDespiteFileEntry: under AnchorSHA256 a live
// file entry does not anchor; once the sha256 entry is unlinked the shard
// falls and the reverse-clean removes its file entry with it.
func TestSweepAnchorSHA256ReclaimsDespiteFileEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("sha-anchored only")})
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedFileEntries != 1 {
				t.Fatalf("DeletedFileEntries = %d, want 1", res.DeletedFileEntries)
			}
			if res.RepointedFileEntries != 0 {
				t.Fatalf("RepointedFileEntries = %d, want 0", res.RepointedFileEntries)
			}

			// Unreachable both ways.
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != "" {
				t.Fatalf("file entry = %q, %v; want removed", got, err)
			}
			if _, err := st.GetShard(ctx, f.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard = %v, want ErrNotExist", err)
			}
			if _, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex)); err == nil {
				t.Fatal("sha256 reconstruction still resolves")
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); ok {
				t.Fatal("xorb still stored")
			}
		})
	}
}

// TestSweepAnchorSHA256CollapsesDuplicateChunkings: identical content stored
// under two chunkings shares one sha256 entry; an AnchorSHA256 sweep keeps
// only the shard the entry points at and removes the loser's file entry,
// while the SHA-256 lookup keeps resolving.
func TestSweepAnchorSHA256CollapsesDuplicateChunkings(t *testing.T) {
	content := []byte("duplicate content stored under two chunkings")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f1 := putGCFile(t, ctx, st, [][]byte{content})
			f2 := putGCFile(t, ctx, st, [][]byte{content[:13], content[13:]})
			if f1.sha256Hex != f2.sha256Hex {
				t.Fatal("test setup: contents must share a SHA-256")
			}
			// FileStorage keeps the first writer, S3 the last: read the entry
			// to learn which shard won.
			ownerHash, err := gcs.GetSHA256IndexEntry(ctx, f1.sha256Hex)
			if err != nil {
				t.Fatal(err)
			}
			winner, loser := f1, f2
			if ownerHash == f2.shardHash {
				winner, loser = f2, f1
			} else if ownerHash != f1.shardHash {
				t.Fatalf("sha256 entry owner = %q, want one of the two shards", ownerHash)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{loser.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}

			// The loser collapsed: chunkings differ, so the winner does not
			// carry its file hash and the entry is deleted, not repointed.
			if got, _, err := gcs.GetFileIndexEntry(ctx, loser.fileHash); err != nil || got != "" {
				t.Fatalf("loser file entry = %q, %v; want removed", got, err)
			}
			if _, err := st.GetShard(ctx, loser.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard(loser) = %v, want ErrNotExist", err)
			}
			for _, xh := range loser.xorbHashes {
				if ok, _ := st.HasXorb(ctx, "default", xh); ok {
					t.Fatalf("loser xorb %s still stored", xh.String())
				}
			}

			// The winner survives with the shared SHA-256 lookup intact.
			assertFileIntact(t, ctx, st, winner)
			gotHash, err := st.GetFileHashBySHA256(ctx, "default", sha256Digest(winner.sha256Hex))
			if err != nil || gotHash != winner.fileHash {
				t.Fatalf("GetFileHashBySHA256 = %s, %v; want %s", gotHash.String(), err, winner.fileHash.String())
			}
			for _, xh := range winner.xorbHashes {
				if ok, _ := st.HasXorb(ctx, "default", xh); !ok {
					t.Fatalf("winner xorb %s swept", xh.String())
				}
			}
		})
	}
}

// TestSweepAnchorSHA256RepointsFileEntry: two stored shard objects carry the
// same file block; the file entry points at the doomed one while the live
// one is sha-anchored. The sweep repoints the entry to the live owner
// instead of deleting it.
func TestSweepAnchorSHA256RepointsFileEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("repoint my file entry")})

			// A second shard object carrying the same file block, minus the
			// CAS info so it hashes differently. PutShard would no-op on the
			// existing file entry, so store the object directly and point
			// the file entry at it.
			twin := shard.NewShard()
			twin.AddFile(shard.FileBlock{
				FileHash: f.fileHash,
				Flags:    shard.FileWithMetadataExt,
				Entries: []shard.FileDataSequenceEntry{
					{CASHash: f.xorbHashes[0], UnpackedSegBytes: uint32(len(f.content)), ChunkIndexEnd: 1},
				},
				MetadataExt: &shard.FileMetadataExt{SHA256Hash: shard.NewSHA256Hash(sha256Digest(f.sha256Hex))},
			})
			twinHash := putShardObject(t, ctx, st, twin)
			if twinHash == f.shardHash {
				t.Fatal("test setup: twin must be a distinct shard object")
			}
			if err := gcs.SetFileIndexEntry(ctx, f.fileHash, twinHash); err != nil {
				t.Fatal(err)
			}

			// The twin holds the file entry but no sha ref; the original is
			// sha-anchored and carries the same file: repoint, don't delete.
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{twinHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if res.RepointedFileEntries != 1 {
				t.Fatalf("RepointedFileEntries = %d, want 1", res.RepointedFileEntries)
			}
			if res.DeletedFileEntries != 0 {
				t.Fatalf("DeletedFileEntries = %d, want 0", res.DeletedFileEntries)
			}
			if len(res.SweptXorbs) != 0 {
				t.Fatalf("SweptXorbs = %v, want none (the xorb serves the live shard)", sweptHashes(res.SweptXorbs))
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want repointed to %q", got, err, f.shardHash)
			}
			assertFileIntact(t, ctx, st, f)
		})
	}
}

// TestSweepAnchorSHA256ExemptsZeroDigestFiles: empty files store all-zero
// SHA-256 metadata and their shared index/sha256 entry never anchors, so
// under AnchorSHA256 a live file entry must exempt such a shard from
// collection; once the file entry is unlinked too, the shard falls and
// takes the zero entry with it.
func TestSweepAnchorSHA256ExemptsZeroDigestFiles(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// An empty file: PutShard computes and stores the all-zero
			// SHA-256 metadata and the shared zero sha256 index entry.
			f := putGCFile(t, ctx, st, nil)
			if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.shardHash {
				t.Fatalf("zero sha256 entry = %q, %v; want %q", got, err, f.shardHash)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 {
				t.Fatalf("exempt shard swept: %+v", res.SweptShards)
			}
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after sweep: %v", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.shardHash {
				t.Fatalf("zero sha256 entry after sweep = %q, %v; want untouched", got, err)
			}

			// Without the file entry the exemption lapses.
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err = Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep after unlink: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if res.DeletedSHA256Entries != 1 {
				t.Fatalf("DeletedSHA256Entries = %d, want 1 (the zero entry goes with its shard)", res.DeletedSHA256Entries)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != "" {
				t.Fatalf("zero sha256 entry = %q, %v; want removed", got, err)
			}
		})
	}
}

// TestSweepStepAnchorChangeRestarts: a step with a different anchor discards
// the half-consumed cycle and marks afresh under the new rules.
func TestSweepStepAnchorChangeRestarts(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			files := putUnlinkedGCFiles(t, ctx, st, "anchor restart one", "anchor restart two")
			g := NewGC(gcs)
			res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorFiles})
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if res.Done || res.RemainingShards != 1 || res.RemainingXorbs != 2 {
				t.Fatalf("first step = done %v, remaining %d/%d; want not done, 1/2", res.Done, res.RemainingShards, res.RemainingXorbs)
			}
			swept, kept := files[0], files[1]
			if res.SweptShards[0].Hash == files[1].shardHash {
				swept, kept = files[1], files[0]
			}

			// A different Anchor discards the cycle and re-marks under the
			// new rules: the kept file's sha256 entry now anchors its shard,
			// leaving only the already-swept file's orphaned xorb dead.
			res, err = g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 1, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("SweepStep after anchor change: %v", err)
			}
			if len(res.SweptShards) != 0 {
				t.Fatalf("re-marked step swept %d shards, want a fresh accumulator with 0", len(res.SweptShards))
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{swept.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v (only the orphaned xorb)", got, want)
			}
			if !res.Done {
				t.Fatal("re-marked cycle not done after draining the one dead xorb")
			}

			// The sha-anchored shard and its xorb survived the anchor change.
			if _, err := gcs.GetShardByHash(ctx, kept.shardHash); err != nil {
				t.Fatalf("kept shard: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", kept.xorbHashes[0]); !ok {
				t.Fatal("kept xorb swept")
			}
		})
	}
}

// TestSweepAnchorSHA256DryRunParity: a dry run under AnchorSHA256 reports
// exactly what the real sweep then removes, including the zero-digest
// exemption keeping the empty file's shard alive.
func TestSweepAnchorSHA256DryRunParity(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// doomed: sha unlinked, file entry live → dead under AnchorSHA256.
			doomed := putGCFile(t, ctx, st, [][]byte{[]byte("doomed under sha anchor")})
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(doomed.sha256Hex)); err != nil {
				t.Fatal(err)
			}
			// kept: sha-anchored. empty: exempt through its zero digest.
			kept := putGCFile(t, ctx, st, [][]byte{[]byte("kept under sha anchor")})
			empty := putGCFile(t, ctx, st, nil)

			opts := SweepOptions{Grace: noGrace, Anchor: AnchorSHA256, DryRun: true}
			dry, err := Sweep(ctx, gcs, opts)
			if err != nil {
				t.Fatalf("dry Sweep: %v", err)
			}
			if !dry.DryRun {
				t.Fatal("result not marked dry run")
			}
			opts.DryRun = false
			wet, err := Sweep(ctx, gcs, opts)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			if got, want := sweptHashes(dry.SweptShards), sweptHashes(wet.SweptShards); !slices.Equal(got, want) {
				t.Fatalf("dry SweptShards = %v, real = %v", got, want)
			}
			if got, want := sweptHashes(dry.SweptXorbs), sweptHashes(wet.SweptXorbs); !slices.Equal(got, want) {
				t.Fatalf("dry SweptXorbs = %v, real = %v", got, want)
			}
			if dry.ReclaimedBytes != wet.ReclaimedBytes {
				t.Fatalf("dry ReclaimedBytes = %d, real %d", dry.ReclaimedBytes, wet.ReclaimedBytes)
			}
			if got, want := sweptHashes(wet.SweptShards), []string{doomed.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("real SweptShards = %v, want %v", got, want)
			}

			// The real sweep removed exactly the doomed file and spared the
			// anchored and exempt shards.
			assertFileIntact(t, ctx, st, kept)
			if _, err := st.GetShard(ctx, empty.fileHash); err != nil {
				t.Fatalf("GetShard(empty): %v", err)
			}
			if _, err := st.GetShard(ctx, doomed.fileHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("GetShard(doomed) = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", doomed.xorbHashes[0]); ok {
				t.Fatal("doomed xorb still stored")
			}
		})
	}
}

// TestSweepUnknownAnchorFails: an unrecognized anchor fails fast at mark.
func TestSweepUnknownAnchorFails(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			gcs := backend.newStore(t).(GCStore)
			if _, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, Anchor: "bogus"}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
				t.Fatalf("Sweep with bogus anchor = %v, want unknown-anchor error", err)
			}
			g := NewGC(gcs)
			if _, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, Anchor: "bogus"}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
				t.Fatalf("SweepStep with bogus anchor = %v, want unknown-anchor error", err)
			}
		})
	}
}

// TestSweepAnchorSHA256RevivesMultiFileRecommit: a sha-dead multi-file shard
// (its digests owned by live duplicate shards) is recommitted between the
// mark walks: one file entry is unlinked and PutShard rewrites both. The
// second look must treat the previously-absent entry as fresh and revive the
// shard — freshness is per entry→shard pair, not per shard.
func TestSweepAnchorSHA256RevivesMultiFileRecommit(t *testing.T) {
	contentA := []byte("first duplicate payload aye")
	contentB := []byte("second duplicate payload bee")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// Whole-content shards own the sha256 entries, leaving the two-file
			// shard below sha-dead.
			fa := putGCFile(t, ctx, st, [][]byte{contentA})
			fb := putGCFile(t, ctx, st, [][]byte{contentB})
			// Pin the sha entries on the whole-content shards: backends differ
			// in which writer their index keeps.
			pinSHAOwners := func() {
				for _, f := range []gcFile{fa, fb} {
					if err := gcs.SetSHA256IndexEntry(ctx, f.sha256Hex, f.shardHash); err != nil {
						t.Fatal(err)
					}
				}
			}

			buildShard := func() (*shard.Shard, xet.FileHash, xet.FileHash) {
				shardObj := shard.NewShard()
				f1, _, _ := addGCFileBlock(t, ctx, st, shardObj, [][]byte{contentA[:11], contentA[11:]})
				f2, _, _ := addGCFileBlock(t, ctx, st, shardObj, [][]byte{contentB[:9], contentB[9:]})
				return shardObj, f1, f2
			}
			shardObj, file1, file2 := buildShard()
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}
			pinSHAOwners()
			// Only file1's entry exists at the mark.
			if _, err := NewGC(gcs).Unlink(ctx, file2); err != nil {
				t.Fatal(err)
			}

			// Between the walks a recommit rewrites both file entries (PutShard
			// only proceeds once no file of the shard has an entry).
			hooked := &hookedGCStore{GCStore: gcs}
			hooked.afterWalkXorbs = func() {
				if _, err := NewGC(gcs).Unlink(ctx, file1); err != nil {
					t.Fatal(err)
				}
				again, _, _ := buildShard()
				if _, err := st.PutShard(ctx, again); err != nil {
					t.Fatal(err)
				}
				pinSHAOwners()
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("recommitted shard swept: %+v", res)
			}
			if _, err := st.GetShard(ctx, file1); err != nil {
				t.Fatalf("GetShard(file1) after sweep: %v", err)
			}
			if _, err := st.GetShard(ctx, file2); err != nil {
				t.Fatalf("GetShard(file2) after sweep: %v", err)
			}
		})
	}
}

// setupSHA256RecommitRace stores identical content under two chunkings with
// the shared sha256 entry pinned on the winner, and arms hooked with the
// unlink-and-recommit ABA sequence fired between the mark walks and the
// deletes: the loser's file entry is deleted and the identical shard
// recommitted, recreating byte-identical entry→shard pairs the pair-level
// second look cannot tell from the pre-mark state. The winner's sha
// ownership is re-pinned (the recommit overwrites it on backends whose
// index keeps the last writer) and the recreated entries keep their true
// fresh mtimes under an aged wrapper.
func setupSHA256RecommitRace(t *testing.T, ctx context.Context, st Storage, content []byte) (winner, loser gcFile, hooked *hookedGCStore) {
	t.Helper()
	gcs := st.(GCStore)

	winner = putGCFile(t, ctx, st, [][]byte{content})
	loser = putGCFile(t, ctx, st, [][]byte{content[:14], content[14:]})
	if winner.sha256Hex != loser.sha256Hex {
		t.Fatal("test setup: contents must share a SHA-256")
	}
	pinWinner := func() {
		if err := gcs.SetSHA256IndexEntry(ctx, winner.sha256Hex, winner.shardHash); err != nil {
			t.Fatal(err)
		}
	}
	pinWinner()

	hooked = &hookedGCStore{GCStore: gcs}
	hooked.afterWalkXorbs = func() {
		if _, err := NewGC(gcs).Unlink(ctx, loser.fileHash); err != nil {
			t.Fatal(err)
		}
		rebuilt := shard.NewShard()
		addGCFileBlock(t, ctx, st, rebuilt, [][]byte{content[:14], content[14:]})
		if _, err := st.PutShard(ctx, rebuilt); err != nil {
			t.Fatal(err)
		}
		pinWinner()
		hooked.freshFileEntries = map[string]bool{loser.fileHash.String(): true}
	}
	return winner, loser, hooked
}

// TestSweepAnchorSHA256FreshEntryShieldsRecommit: between the mark and the
// deletes the sha-dead loser's file entry is unlinked and the identical
// shard recommitted. The recreated entry→shard pairs look exactly like the
// pre-mark state, so only their fresh mtimes betray the completed commit:
// with a positive grace the freshness shield must keep the recommitted
// upload — entry, shard, and xorbs — whole.
func TestSweepAnchorSHA256FreshEntryShieldsRecommit(t *testing.T) {
	content := []byte("recommitted duplicate payload")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			winner, loser, hooked := setupSHA256RecommitRace(t, ctx, st, content)

			// Age the stored objects and file entries past the cutoff.
			switch b := st.(type) {
			case *FileStorage:
				old := time.Now().Add(-2 * time.Hour)
				paths := []string{
					b.objectPath("shards", winner.shardHash),
					b.objectPath("shards", loser.shardHash),
					b.objectPath("index/files", winner.fileHash.String()),
					b.objectPath("index/files", loser.fileHash.String()),
				}
				for _, f := range []gcFile{winner, loser} {
					for _, xh := range f.xorbHashes {
						paths = append(paths, b.objectPath("xorbs", xh.String()))
					}
				}
				for _, p := range paths {
					if err := os.Chtimes(p, old, old); err != nil {
						t.Fatal(err)
					}
				}
			case *S3Storage:
				// gofakes3 timestamps cannot be set: the wrapper backdates
				// every walk and entry time instead, sparing the entries the
				// recommit recreates.
				hooked.age = 2 * time.Hour
			}

			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("recommitted upload destroyed: %+v", res)
			}
			if res.DeletedFileEntries != 0 || res.RepointedFileEntries != 0 {
				t.Fatalf("recommitted file entries touched: %+v", res)
			}

			// The recommitted upload survived whole and stays reachable.
			if got, _, err := gcs.GetFileIndexEntry(ctx, loser.fileHash); err != nil || got != loser.shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, loser.shardHash)
			}
			for _, xh := range loser.xorbHashes {
				if ok, _ := st.HasXorb(ctx, "default", xh); !ok {
					t.Fatalf("xorb %s of the recommitted shard swept", xh.String())
				}
			}
			assertFileIntact(t, ctx, st, loser)
			if got, err := gcs.GetSHA256IndexEntry(ctx, winner.sha256Hex); err != nil || got != winner.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, winner.shardHash)
			}
			assertFileIntact(t, ctx, st, winner)

			// The freshness shield is judged at the mark too: a dry run over
			// the post-recommit state reports nothing sweepable.
			dry, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour, Anchor: AnchorSHA256, DryRun: true})
			if err != nil {
				t.Fatalf("dry Sweep: %v", err)
			}
			if len(dry.SweptShards) != 0 || len(dry.SweptXorbs) != 0 {
				t.Fatalf("dry run reports the shielded recommit sweepable: %+v", dry)
			}
		})
	}
}

// TestSweepAnchorSHA256RecommitCollectedWithoutGrace: the same ABA sequence
// with the window disabled collects the recommitted shard — the documented
// accepted race — proving the freshness shield is grace-gated.
func TestSweepAnchorSHA256RecommitCollectedWithoutGrace(t *testing.T) {
	content := []byte("recommitted duplicate payload")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			winner, loser, hooked := setupSHA256RecommitRace(t, ctx, st, content)

			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{loser.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, loser.fileHash); err != nil || got != "" {
				t.Fatalf("file entry = %q, %v; want removed", got, err)
			}
			if _, err := gcs.GetShardByHash(ctx, loser.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("recommitted shard load = %v, want ErrNotExist", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, winner.sha256Hex); err != nil || got != winner.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, winner.shardHash)
			}
			assertFileIntact(t, ctx, st, winner)
		})
	}
}

// TestSweepDeleteLoopAbortsOnRacingFileEntry: a commit lands between the
// per-shard liveness re-read and the file-entry delete loop, recreating the
// dead shard's file entry. Under the default anchor such an entry
// contradicts the re-read, so the sweep must abort the shard's deletion and
// keep the commit whole instead of silently destroying its entry.
func TestSweepDeleteLoopAbortsOnRacingFileEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("racing file-entry recommit")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			// Aged out of grace; the re-read (call 1) sees no entry, then the
			// commit rewrites it right before the delete loop's read (call 2).
			hooked := agedStore(gcs)
			recommitted := false
			hooked.onFileEntryGet = func(n int) {
				if n != 2 {
					return
				}
				recommitted = true
				if err := gcs.SetFileIndexEntry(ctx, f.fileHash, f.shardHash); err != nil {
					t.Fatal(err)
				}
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !recommitted {
				t.Fatal("hook did not fire")
			}

			if len(res.SweptShards) != 0 {
				t.Fatalf("SweptShards = %v, want none", sweptHashes(res.SweptShards))
			}
			if res.DeletedFileEntries != 0 || res.RepointedFileEntries != 0 {
				t.Fatalf("racing commit's file entry touched: %+v", res)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the racing commit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the racing commit")
			}
		})
	}
}

// TestSweepAnchorSHA256DeleteLoopSparesFreshRecommit: a sha-dead shard's
// stale file entry is judged deletable at the mark (call 1) and the re-read
// (call 2), but a recommit rewrites it fresh before the delete loop's read
// (call 3). With a positive grace the loop must treat the fresh entry as a
// completed commit and abort the shard's deletion.
func TestSweepAnchorSHA256DeleteLoopSparesFreshRecommit(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh recommit under sha anchor")})
			// Sha-dead: only the aged file entry still points at the shard.
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			hooked := agedStore(gcs)
			recommitted := false
			hooked.onFileEntryGet = func(n int) {
				if n != 3 {
					return
				}
				recommitted = true
				if err := gcs.SetFileIndexEntry(ctx, f.fileHash, f.shardHash); err != nil {
					t.Fatal(err)
				}
				// The rewritten entry keeps its true fresh mtime.
				hooked.freshFileEntries = map[string]bool{f.fileHash.String(): true}
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour, Anchor: AnchorSHA256})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !recommitted {
				t.Fatal("hook did not fire")
			}

			if len(res.SweptShards) != 0 {
				t.Fatalf("SweptShards = %v, want none", sweptHashes(res.SweptShards))
			}
			if res.DeletedFileEntries != 0 || res.RepointedFileEntries != 0 {
				t.Fatalf("fresh recommit's file entry touched: %+v", res)
			}
			if got, _, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the fresh recommit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the fresh recommit")
			}
		})
	}
}

// TestSweepDeleteLoopAbortsOnRacingSHA256Entry: a commit's sha256 entry —
// PutShard commits sha256 entries before file entries — becomes visible
// after the file-entry loop but before the sha256 loop's read. Under the
// default anchor that entry contradicts the liveness re-read, so the sweep
// must abort the shard's deletion and leave the entry in place.
func TestSweepDeleteLoopAbortsOnRacingSHA256Entry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("racing sha256-entry recommit")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			// Read 1 is the liveness re-read; the commit lands before the
			// delete loop's read 2.
			hooked := &hookedGCStore{GCStore: gcs}
			recommitted := false
			hooked.onSHA256EntryGet = func(n int) {
				if n != 2 {
					return
				}
				recommitted = true
				if err := gcs.SetSHA256IndexEntry(ctx, f.sha256Hex, f.shardHash); err != nil {
					t.Fatal(err)
				}
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !recommitted {
				t.Fatal("hook did not fire")
			}

			if len(res.SweptShards) != 0 {
				t.Fatalf("SweptShards = %v, want none", sweptHashes(res.SweptShards))
			}
			if res.DeletedSHA256Entries != 0 || res.RepointedSHA256Entries != 0 {
				t.Fatalf("racing commit's sha256 entry touched: %+v", res)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the racing commit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the racing commit")
			}
		})
	}
}

// TestFreshCutoffTruncationInvariant: sweep freshness compares mtimes against
// the cutoff truncated to whole seconds. For every sub-second cutoff phase,
// an entry modified at or after the raw cutoff must still read fresh when
// its mtime is reported at S3's second precision — truncation may only widen
// the shield, never miss a genuinely fresh entry.
func TestFreshCutoffTruncationInvariant(t *testing.T) {
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	for phaseMs := 0; phaseMs < 1000; phaseMs += 25 {
		cutoff := base.Add(time.Duration(phaseMs) * time.Millisecond)
		freshCutoff := cutoff.Truncate(time.Second)
		for deltaMs := 0; deltaMs < 2500; deltaMs += 25 {
			event := cutoff.Add(time.Duration(deltaMs) * time.Millisecond)
			reported := event.Truncate(time.Second)
			if reported.Before(freshCutoff) {
				t.Fatalf("event at cutoff+%dms (phase %dms) reads stale: reported %v < freshCutoff %v",
					deltaMs, phaseMs, reported, freshCutoff)
			}
		}
	}
}
