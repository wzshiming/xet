package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
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

// unlinkGCFile removes both of f's index entries — file and SHA-256 — so a
// sweep can prove the shard dead. Not for empty files (zero digest).
func unlinkGCFile(t *testing.T, ctx context.Context, gcs GCStore, f gcFile) {
	t.Helper()
	if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
		t.Fatal(err)
	}
	if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
		t.Fatal(err)
	}
}

// setIndexEntry writes an index entry directly on the backend — the GC
// surface has no entry writers — simulating a racing PutShard commit.
func setIndexEntry(t *testing.T, st Storage, kind, name, shardHash string) {
	t.Helper()
	switch b := st.(type) {
	case *FileStorage:
		if err := overwriteIndexFile(b.objectPath(kind, name), []byte(shardHash)); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.putIndexObject(context.Background(), b.objectKey(kind, name), []byte(shardHash)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported backend %T", st)
	}
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

// TestSweepNeedsBothUnlinks: a shard stays live while either its file entry
// or its non-zero sha256 entry remains; only unlinking both lets a sweep
// reclaim it.
func TestSweepNeedsBothUnlinks(t *testing.T) {
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
			unlinkGCFile(t, ctx, gcs, f)
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
			// Both entries were unlinked up front; no zero entry exists, so
			// the shard's cleanup deletes no sha256 entries.
			if res.DeletedSHA256Entries != 0 {
				t.Fatalf("DeletedSHA256Entries = %d, want 0", res.DeletedSHA256Entries)
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

			unlinkGCFile(t, ctx, gcs, fileA)
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
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
			// The sha256 entry was unlinked up front; only chunk entries of
			// the dead shard remain to clean.
			if res.DeletedSHA256Entries != 0 {
				t.Fatalf("DeletedSHA256Entries = %d, want 0", res.DeletedSHA256Entries)
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

			// Chunk entries never point at the dead shard: an entry it owned
			// is deleted (a dedup miss until rewritten), one owned by the
			// live shard survives.
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
			unlinkGCFile(t, ctx, gcs, f)

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, DryRun: true})
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
		})
	}
}

// TestSweepDryRunParity: over a mixed store — a dead file, a live file, and
// a live empty file — a dry run reports exactly what a real pass then
// removes, byte for byte, while counting no entry deletions.
func TestSweepDryRunParity(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			doomed := putGCFile(t, ctx, st, [][]byte{[]byte("doomed by both unlinks")})
			unlinkGCFile(t, ctx, gcs, doomed)
			kept := putGCFile(t, ctx, st, [][]byte{[]byte("kept alive by its entries")})
			empty := putGCFile(t, ctx, st, nil)

			dry, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, DryRun: true})
			if err != nil {
				t.Fatalf("dry Sweep: %v", err)
			}
			if !dry.DryRun {
				t.Fatal("result not marked dry run")
			}
			wet, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
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
			// Dry runs never count entry deletions; the real pass does.
			if dry.DeletedChunkEntries != 0 {
				t.Fatalf("dry DeletedChunkEntries = %d, want 0", dry.DeletedChunkEntries)
			}
			if wet.DeletedChunkEntries != 1 {
				t.Fatalf("real DeletedChunkEntries = %d, want 1", wet.DeletedChunkEntries)
			}

			// The real sweep removed exactly the doomed file and spared the
			// anchored and empty shards.
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

func TestSweepGraceWindow(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh object")})
			unlinkGCFile(t, ctx, gcs, f)

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: time.Hour})
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

			res, err = Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
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
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			f := putGCFile(t, ctx, st, [][]byte{[]byte("fresh but window disabled")})
			unlinkGCFile(t, ctx, gcs, f)

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: -time.Nanosecond})
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
		})
	}
}

// TestSweepDeletesSharedChunkEntry: two files share chunk A while each also
// has its own chunk. Sweeping the shard that owns A's entry deletes the
// entry outright — a dedup miss until a future upload rewrites it, never
// data loss — and the live shard's data stays resolvable by file hash.
func TestSweepDeletesSharedChunkEntry(t *testing.T) {
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
			// FileStorage keeps the first writer (f1), S3 the last (f2); kill
			// the owner so the shared-entry deletion runs on both backends.
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
			// Warm the chunk cache so the delete must evict it.
			if _, err := st.GetShardByChunkHash(ctx, "default", f1.chunkHashes[0]); err != nil {
				t.Fatal(err)
			}

			unlinkGCFile(t, ctx, gcs, dead)
			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			// Both entries the dead shard owned — shared and exclusive.
			if res.DeletedChunkEntries != 2 {
				t.Fatalf("DeletedChunkEntries = %d, want 2", res.DeletedChunkEntries)
			}

			// The shared entry is gone (dedup miss accepted), not repointed.
			if got, err := gcs.GetChunkIndexEntry(ctx, live.chunkHashes[0]); err != nil || got != "" {
				t.Fatalf("shared chunk entry = %q, %v; want removed", got, err)
			}
			if _, err := st.GetShardByChunkHash(ctx, "default", live.chunkHashes[0]); err == nil {
				t.Fatal("shared chunk lookup still resolves after the entry delete")
			}
			// The dead shard's exclusive entry is gone; the live shard's own
			// exclusive entry is untouched.
			if got, err := gcs.GetChunkIndexEntry(ctx, dead.chunkHashes[1]); err != nil || got != "" {
				t.Fatalf("dead exclusive chunk entry = %q, %v; want removed", got, err)
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, live.chunkHashes[1]); err != nil || got != live.shardHash {
				t.Fatalf("live exclusive chunk entry = %q, %v; want %q", got, err, live.shardHash)
			}

			// The live file itself is whole: file hash and SHA-256 resolve.
			assertFileIntact(t, ctx, st, live)
			if ok, _ := st.HasXorb(ctx, "default", live.xorbHashes[0]); !ok {
				t.Fatal("shared xorb was swept")
			}
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

// TestSweepReportsDanglingSHA256Entries: deleting shard objects directly
// orphans their file and sha256 entries; both kinds are reported — sorted,
// in dry runs and real sweeps alike — and never deleted.
func TestSweepReportsDanglingSHA256Entries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			fA := putGCFile(t, ctx, st, [][]byte{[]byte("dangling sha one")})
			fB := putGCFile(t, ctx, st, [][]byte{[]byte("dangling sha two")})
			for _, f := range []gcFile{fA, fB} {
				if err := gcs.DeleteShard(ctx, f.shardHash); err != nil {
					t.Fatal(err)
				}
			}

			wantSHA := []string{fA.sha256Hex, fB.sha256Hex}
			slices.Sort(wantSHA)
			wantFiles := []string{fA.fileHash.String(), fB.fileHash.String()}
			slices.Sort(wantFiles)
			for _, dryRun := range []bool{true, false} {
				res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace, DryRun: dryRun})
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
			for _, f := range []gcFile{fA, fB} {
				if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
					t.Fatalf("sha256 entry %s = %q, %v; want %q", f.sha256Hex, got, err, f.shardHash)
				}
				if got, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
					t.Fatalf("file entry %s = %q, %v; want %q", f.fileHash.String(), got, err, f.shardHash)
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
			unlinkGCFile(t, ctx, gcs, f)
			if _, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace}); err != nil {
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
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			g := NewGC(backend.newStore(t).(GCStore))

			g.mu.Lock()
			if _, err := g.SweepStep(context.Background(), SweepOptions{Grace: noGrace}); !errors.Is(err, ErrGCBusy) {
				t.Fatalf("concurrent SweepStep = %v, want ErrGCBusy", err)
			}
			g.mu.Unlock()

			if _, err := g.SweepStep(context.Background(), SweepOptions{Grace: noGrace}); err != nil {
				t.Fatalf("SweepStep after release: %v", err)
			}
		})
	}
}

// hookedGCStore wraps a GCStore with callbacks fired at sweep-visible points,
// simulating uploads that commit while a sweep is running. A non-zero age
// backdates every modTime the object walks report, simulating aged objects
// on backends whose timestamps cannot be set (S3).
type hookedGCStore struct {
	GCStore
	age                time.Duration
	shardModTimes      map[string]time.Time // per-shard walk mtime overrides
	beforeFileEntryGet func()               // consumed on first fire
	beforeShardLoad    func()               // consumed on first fire
	// onFileEntryGet / onSHA256EntryGet fire before every call of the
	// wrapped getter with a 1-based call count, unlike the before* hooks
	// consumed on first fire.
	onFileEntryGet   func(n int)
	fileEntryGets    int
	onSHA256EntryGet func(n int)
	sha256EntryGets  int
	beforeWalkShards func() // fired before every WalkShards delegation
	beforeWalkXorbs  func() // fired before every WalkXorbs delegation
	walkShardsCalls  int    // WalkShards invocations
	loadShardCalls   int    // LoadShard invocations
	cachedShardGets  int    // GetShardByHash invocations (GC must not)
	loadShardErrs    map[string]error
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
		if t, ok := h.shardModTimes[shardHash]; ok {
			return fn(shardHash, size, t)
		}
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

func (h *hookedGCStore) GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error) {
	if h.onSHA256EntryGet != nil {
		h.sha256EntryGets++
		h.onSHA256EntryGet(h.sha256EntryGets)
	}
	return h.GCStore.GetSHA256IndexEntry(ctx, sha256Hex)
}

func (h *hookedGCStore) GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error) {
	h.cachedShardGets++
	return h.GCStore.GetShardByHash(ctx, shardHash)
}

func (h *hookedGCStore) LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error) {
	h.loadShardCalls++
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

// assertChunkEntriesIntact fails when an aborted shard deletion touched the
// chunk entries a racing commit relies on for dedup.
func assertChunkEntriesIntact(t *testing.T, ctx context.Context, gcs GCStore, f gcFile, res *SweepResult) {
	t.Helper()
	if res.DeletedChunkEntries != 0 {
		t.Fatalf("racing commit's chunk entries touched: %+v", res)
	}
	for _, chunkHash := range f.chunkHashes {
		if got, err := gcs.GetChunkIndexEntry(ctx, chunkHash); err != nil || got != f.shardHash {
			t.Fatalf("chunk entry = %q, %v; want %q", got, err, f.shardHash)
		}
	}
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

// TestSweepSkipsReuploadCommittedDuringMark: a re-upload commits between the
// index walks and the shard walk, so the shard looks unreferenced at the
// mark; sweepShard's guards must read the fresh entries and spare it, and
// phase 2's walk must shield its xorb.
func TestSweepSkipsReuploadCommittedDuringMark(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{[]byte("committed during the sweep walks")}
	f := putGCFile(t, ctx, st, parts)
	unlinkGCFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	committed := false
	hooked.beforeWalkShards = func() {
		if committed {
			return
		}
		committed = true
		putGCFile(t, ctx, st, parts)
	}
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if !committed {
		t.Fatal("hook did not run")
	}
	if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
		t.Fatalf("re-committed objects swept: %+v", res)
	}
	assertFileIntact(t, ctx, st, f)
}

// TestSweepSkipsReuploadCommittedBeforeDelete: a writer in another process
// (a second store on the same directory) commits right before the shard
// delete; sweepShard's files guard must abort before the shard, its
// entries, or its xorbs are touched.
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
	unlinkGCFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeFileEntryGet = func() { putGCFile(t, ctx, writer, parts) }
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
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
// xorb commits during the shard-delete phase; phase 2's fresh walk over the
// stored shards must shield the shared xorb.
func TestSweepShieldsCommitDuringShardDeletePhase(t *testing.T) {
	partA := []byte("shared payload the sweep must keep")
	partB := []byte("unique to the late upload")
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f1 := putGCFile(t, ctx, st, [][]byte{partA})
			unlinkGCFile(t, ctx, gcs, f1)

			// The hook fires inside sweepShard for the dead shard; file2
			// dedup-hits file1's only xorb.
			var f2 gcFile
			hooked := &hookedGCStore{GCStore: gcs}
			hooked.beforeFileEntryGet = func() {
				f2 = putGCFile(t, ctx, st, [][]byte{partA, partB})
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
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
	unlinkGCFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.beforeShardLoad = func() {
		if err := os.Remove(st.objectPath("shards", f.shardHash)); err != nil {
			t.Fatal(err)
		}
	}
	res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
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
	putRawShardObject(t, ctx, st, hash, encoded)
	return hash
}

// putRawShardObject stores raw bytes as a shard object under the given hash
// name, bypassing PutShard and its index writes.
func putRawShardObject(t *testing.T, ctx context.Context, st Storage, hash string, raw []byte) {
	t.Helper()
	switch b := st.(type) {
	case *FileStorage:
		path := b.objectPath("shards", hash)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0644); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.putObject(ctx, b.objectKey("shards", hash), bytes.NewReader(raw)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported backend %T", st)
	}
}

// TestSweepGraceShieldsDedupedXorbs: an in-grace shard with no entries is an
// upload mid-commit; the xorbs it deduplicated against, older than the
// grace window themselves, must survive the sweep via phase 2's walk.
func TestSweepGraceShieldsDedupedXorbs(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("deduplicated old payload")})
	unlinkGCFile(t, ctx, st, f)

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
	// mtime) and references the old xorb, but no index entry exists yet.
	pending := shard.NewShard()
	pending.AddFile(shard.FileBlock{
		FileHash: f.fileHash,
		Entries: []shard.FileDataSequenceEntry{
			{CASHash: f.xorbHashes[0], UnpackedSegBytes: uint32(len(f.content)), ChunkIndexEnd: 1},
		},
	})
	putShardObject(t, ctx, st, pending)

	res, err := Sweep(ctx, st, SweepOptions{Grace: time.Hour})
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

// putUnlinkedGCFiles stores one single-part file per content and unlinks
// both its entries, leaving its shard and xorb unreferenced.
func putUnlinkedGCFiles(t *testing.T, ctx context.Context, st Storage, contents ...string) []gcFile {
	t.Helper()
	gcs := st.(GCStore)
	files := make([]gcFile, 0, len(contents))
	for _, content := range contents {
		f := putGCFile(t, ctx, st, [][]byte{[]byte(content)})
		unlinkGCFile(t, ctx, gcs, f)
		files = append(files, f)
	}
	return files
}

// TestSweepStepDrainsInBatches: MaxDeletes=1 steps each run an independent
// pass that re-marks and sweeps exactly one object; repeated until Done,
// their union matches a single full pass over an identically prepared
// store.
func TestSweepStepDrainsInBatches(t *testing.T) {
	contents := []string{"batched sweep one", "batched sweep two", "batched sweep three"}
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			putUnlinkedGCFiles(t, ctx, st, contents...)

			g := NewGC(st.(GCStore))
			opts := SweepOptions{Grace: noGrace, MaxDeletes: 1}
			var sweptShards, sweptXorbs []SweptObject
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
			st2 := backend.newStore(t)
			putUnlinkedGCFiles(t, ctx, st2, contents...)
			full, err := Sweep(ctx, st2.(GCStore), SweepOptions{Grace: noGrace})
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
		})
	}
}

// TestSweepStepRecommitBetweenSteps: a re-upload commits between two
// bounded steps; the next step's fresh mark sees the new entries, spares
// the shard, and phase 2 shields its xorb — stateless re-marking replaces
// any carried-over queue.
func TestSweepStepRecommitBetweenSteps(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			files := putUnlinkedGCFiles(t, ctx, st, "mid-step commit one", "mid-step commit two")

			// Aged mtimes leave the index re-marks as the only shield.
			g := NewGC(agedStore(gcs))
			opts := SweepOptions{Grace: time.Hour, MaxDeletes: 1}
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
			if res.SweptShards[0].Hash == kept.shardHash {
				kept, gone = gone, kept
			}
			if again := putGCFile(t, ctx, st, [][]byte{kept.content}); again.shardHash != kept.shardHash {
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

// TestSweepStepDryRunIgnoresBounds: a dry-run step reports one full
// unbounded pass whatever the limits say, deleting nothing.
func TestSweepStepDryRunIgnoresBounds(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			files := putUnlinkedGCFiles(t, ctx, st, "dry bounds one", "dry bounds two")
			g := NewGC(gcs)
			dry, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, DryRun: true, MaxDeletes: 1, Budget: time.Nanosecond})
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
				if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
					t.Fatalf("shard removed by dry step: %v", err)
				}
				if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
					t.Fatal("xorb removed by dry step")
				}
			}
		})
	}
}

// TestSweepStepBudgetProgress: a vanishing budget cannot expire before the
// first sweep, so every step still sweeps at least one object and repeated
// stepping always terminates.
func TestSweepStepBudgetProgress(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st, "budget file one", "budget file two")

	g := NewGC(st)
	opts := SweepOptions{Grace: noGrace, Budget: time.Nanosecond}
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
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putUnlinkedGCFiles(t, ctx, st, "budgeted walk one", "budgeted walk two")

	hooked := &hookedGCStore{GCStore: st}
	g := NewGC(hooked)
	res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 2})
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
	res, err = g.SweepStep(ctx, SweepOptions{Grace: noGrace, Budget: 250 * time.Millisecond})
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
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	files := putUnlinkedGCFiles(t, ctx, st, "canceled step one", "canceled step two")

	hooked := &hookedGCStore{GCStore: st}
	g := NewGC(hooked)
	opts := SweepOptions{Grace: noGrace}

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
		if _, err := st.GetShardByHash(ctx, f.shardHash); !errors.Is(err, iofs.ErrNotExist) {
			t.Fatalf("shard load = %v, want ErrNotExist", err)
		}
		if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); ok {
			t.Fatal("xorb still stored after both steps")
		}
	}
}

// TestSweepNeverTouchesShardCache: every shard load a sweep performs — the
// dead-shard load and phase 2's walk loads — must go through LoadShard,
// never the cache-populating GetShardByHash, so bulk sweeps cannot evict
// hot serving entries.
func TestSweepNeverTouchesShardCache(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			fLive := putGCFile(t, ctx, st, [][]byte{[]byte("cache-cold live file")})
			fDead := putGCFile(t, ctx, st, [][]byte{[]byte("cache-cold dead file")})
			unlinkGCFile(t, ctx, gcs, fDead)
			fGrace := putGCFile(t, ctx, st, [][]byte{[]byte("cache-cold in-grace upload")})
			unlinkGCFile(t, ctx, gcs, fGrace)

			hooked := agedStore(gcs)
			hooked.shardModTimes = map[string]time.Time{
				fGrace.shardHash: time.Now().Add(-30 * time.Minute),
			}
			// A commit landing mid-sweep exercises the phase-2 load too.
			hooked.beforeFileEntryGet = func() {
				putGCFile(t, ctx, st, [][]byte{[]byte("cache-cold mid-sweep commit")})
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{fDead.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if hooked.walkShardsCalls != 2 {
				t.Fatalf("WalkShards called %d times, want 2 (phase 1 + phase 2)", hooked.walkShardsCalls)
			}
			if hooked.cachedShardGets != 0 {
				t.Fatalf("GetShardByHash called %d times during the sweep, want 0 (loads must bypass the cache)", hooked.cachedShardGets)
			}
			assertFileIntact(t, ctx, st, fLive)
			if ok, _ := st.HasXorb(ctx, "default", fGrace.xorbHashes[0]); !ok {
				t.Fatal("in-grace shard's xorb was swept")
			}
		})
	}
}

// TestSweepLeavesShardCacheCold: a sweep over a store whose shard cache is
// empty leaves it empty — LoadShard populates nothing, so hot entries of a
// serving process survive its sweeps untouched.
func TestSweepLeavesShardCacheCold(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewFileStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	fLive := putGCFile(t, ctx, st, [][]byte{[]byte("cold cache live")})
	fDead := putGCFile(t, ctx, st, [][]byte{[]byte("cold cache dead")})
	unlinkGCFile(t, ctx, st, fDead)

	// A second view over the same directory starts with a cold cache.
	sweeper, err := NewFileStorage(WithBasePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	res, err := Sweep(ctx, agedStore(sweeper), SweepOptions{Grace: time.Hour})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if got, want := sweptHashes(res.SweptShards), []string{fDead.shardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if n := sweeper.shardIndex.Len(); n != 0 {
		t.Fatalf("shard cache holds %d entries after the sweep, want 0", n)
	}
	assertFileIntact(t, ctx, st, fLive)
}

// TestSweepReportsUnreadableDeadShard: a dead shard whose object cannot be
// decoded does not fail the sweep — it is reported, treated as live, and
// nothing of it (object, entries, xorb) is deleted.
func TestSweepReportsUnreadableDeadShard(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("unreadable dead shard")})
			unlinkGCFile(t, ctx, gcs, f)

			hooked := agedStore(gcs)
			hooked.loadShardErrs = map[string]error{f.shardHash: errors.New("decode stored shard: corrupt")}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep = %v, want the unreadable shard skipped, not fail-stop", err)
			}
			if got, want := res.UnreadableShards, []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("UnreadableShards = %v, want %v", got, want)
			}
			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("swept %v/%v, want nothing", sweptHashes(res.SweptShards), sweptHashes(res.SweptXorbs))
			}
			if !res.Done || res.RemainingXorbs != 0 {
				t.Fatalf("progress = done %v, remaining xorbs %d; want done 0 (xorb phase skipped)", res.Done, res.RemainingXorbs)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("unreadable shard object gone: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("unreadable shard's xorb was swept")
			}
			if got, err := gcs.GetChunkIndexEntry(ctx, f.chunkHashes[0]); err != nil || got != f.shardHash {
				t.Fatalf("chunk entry = %q, %v; want untouched %q", got, err, f.shardHash)
			}
		})
	}
}

// TestSweepUnreadableShardSuppressesXorbSweep: one unreadable live shard
// must not stop dead-shard cleanup, but no xorb may be deleted — the
// unreadable shard could reference any of them.
func TestSweepUnreadableShardSuppressesXorbSweep(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			fSick := putGCFile(t, ctx, st, [][]byte{[]byte("unreadable live shard")})
			fDead := putGCFile(t, ctx, st, [][]byte{[]byte("healthy dead shard")})
			unlinkGCFile(t, ctx, gcs, fDead)

			hooked := agedStore(gcs)
			hooked.loadShardErrs = map[string]error{fSick.shardHash: errors.New("decode stored shard: corrupt")}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := res.UnreadableShards, []string{fSick.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("UnreadableShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptShards), []string{fDead.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v (dead-shard cleanup must proceed)", got, want)
			}
			if len(res.SweptXorbs) != 0 || res.RemainingXorbs != 0 {
				t.Fatalf("xorbs swept %v remaining %d, want none (xorb phase poisoned)", sweptHashes(res.SweptXorbs), res.RemainingXorbs)
			}
			if ok, _ := st.HasXorb(ctx, "default", fDead.xorbHashes[0]); !ok {
				t.Fatal("queued xorb swept despite an unreadable shard")
			}
			if _, err := gcs.GetShardByHash(ctx, fDead.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if got, err := gcs.GetFileIndexEntry(ctx, fSick.fileHash); err != nil || got != fSick.shardHash {
				t.Fatalf("unreadable shard's file entry = %q, %v; want untouched", got, err)
			}
		})
	}
}

// TestSweepLoadContextErrorAborts: a load failing with the pass's own dying
// context aborts the sweep with that error and must not brand the shard
// unreadable or delete anything of it.
func TestSweepLoadContextErrorAborts(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	f := putGCFile(t, ctx, st, [][]byte{[]byte("canceled mid-load")})
	unlinkGCFile(t, ctx, st, f)

	hooked := &hookedGCStore{GCStore: st}
	hooked.loadShardErrs = map[string]error{f.shardHash: fmt.Errorf("get object: %w", context.Canceled)}
	if _, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sweep = %v, want context.Canceled", err)
	}
	if _, err := st.GetShardByHash(ctx, f.shardHash); err != nil {
		t.Fatalf("shard gone after aborted sweep: %v", err)
	}
	if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
		t.Fatal("xorb gone after aborted sweep")
	}
}

// TestSweepEmptyFileZeroSHA256Cleanup: empty files store all-zero SHA-256
// metadata and their shared index/sha256 entry never anchors, so the file
// entry alone keeps the shard alive; once it is unlinked — Unlink alone
// suffices for empty files — the shard falls and takes the zero entry with
// it.
func TestSweepEmptyFileZeroSHA256Cleanup(t *testing.T) {
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

			res, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 {
				t.Fatalf("file-anchored shard swept: %+v", res.SweptShards)
			}
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after sweep: %v", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.shardHash {
				t.Fatalf("zero sha256 entry after sweep = %q, %v; want untouched", got, err)
			}

			// The zero entry never anchors: Unlink alone frees the shard.
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err = Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
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

// TestSweepDeleteLoopAbortsOnRacingFileEntry: a commit lands after the mark
// queued the dead shard, recreating its file entry right before the
// file-entry guard loop's read — the shard's only read of that key under
// AnchorBoth. The sweep must abort the shard's deletion — before anything
// was deleted — and keep the commit whole.
func TestSweepDeleteLoopAbortsOnRacingFileEntry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("racing file-entry recommit")})
			unlinkGCFile(t, ctx, gcs, f)

			// Aged out of grace; the commit rewrites the entry right before
			// the guard loop's only read (call 1).
			hooked := agedStore(gcs)
			recommitted := false
			hooked.onFileEntryGet = func(n int) {
				if n != 1 {
					return
				}
				recommitted = true
				setIndexEntry(t, st, "index/files", f.fileHash.String(), f.shardHash)
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: time.Hour})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if !recommitted {
				t.Fatal("hook did not fire")
			}

			if len(res.SweptShards) != 0 || len(res.SweptXorbs) != 0 {
				t.Fatalf("racing commit's objects swept: %+v", res)
			}
			if got, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the racing commit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the racing commit")
			}
			assertChunkEntriesIntact(t, ctx, gcs, f, res)
		})
	}
}

// TestSweepDeleteLoopAbortsOnRacingSHA256Entry: a commit's sha256 entry —
// PutShard commits sha256 entries before file entries — becomes visible
// after the file-entry guard loop but right before the sha256 guard loop's
// read, the shard's only read of that key under AnchorBoth. The sweep must
// abort the shard's deletion and leave the entry in place.
func TestSweepDeleteLoopAbortsOnRacingSHA256Entry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("racing sha256-entry recommit")})
			unlinkGCFile(t, ctx, gcs, f)

			// The commit lands right before the sha guard loop's only
			// read (get 1).
			hooked := &hookedGCStore{GCStore: gcs}
			recommitted := false
			hooked.onSHA256EntryGet = func(n int) {
				if n != 1 {
					return
				}
				recommitted = true
				setIndexEntry(t, st, "index/sha256", f.sha256Hex, f.shardHash)
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
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
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the racing commit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the racing commit")
			}
			assertChunkEntriesIntact(t, ctx, gcs, f, res)
		})
	}
}

// TestSweepAbortPreservesZeroSHA256Entry: on a shard whose empty file
// precedes a non-empty one, a racing non-zero sha256 entry must abort the
// deletion BEFORE the shared zero empty-file entry is deleted — every
// non-zero guard fires first, so the revived shard keeps its marker.
func TestSweepAbortPreservesZeroSHA256Entry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// One shard, empty file first: file order is preserved by the
			// shard encoding, so the sha loops meet the zero digest first.
			content := []byte("empty-then-full recommit")
			shardObj := shard.NewShard()
			emptyHash, _, _ := addGCFileBlock(t, ctx, st, shardObj, nil)
			fullHash, fullXorbs, _ := addGCFileBlock(t, ctx, st, shardObj, [][]byte{content})
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

			g := NewGC(gcs)
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
				setIndexEntry(t, st, "index/sha256", nonZeroHex, shardHash)
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Grace: noGrace})
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
		})
	}
}

// walks compare mtimes against the cutoff truncated to whole seconds. For
// every sub-second cutoff phase, an object written at or after the raw
// cutoff must still read in-grace when its mtime is reported at S3's second
// precision — truncation may only widen the shield, never shrink it.
func TestObjectCutoffTruncationInvariant(t *testing.T) {
	base := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	for phaseMs := 0; phaseMs < 1000; phaseMs += 25 {
		cutoff := base.Add(time.Duration(phaseMs) * time.Millisecond)
		objCutoff := cutoff.Truncate(time.Second)
		for deltaMs := 0; deltaMs < 2500; deltaMs += 25 {
			event := cutoff.Add(time.Duration(deltaMs) * time.Millisecond)
			reported := event.Truncate(time.Second)
			if reported.Before(objCutoff) {
				t.Fatalf("object at cutoff+%dms (phase %dms) reads dead: reported %v < objCutoff %v",
					deltaMs, phaseMs, reported, objCutoff)
			}
		}
	}
}

// TestSweepAnchorSHA256LFSLifecycle: a store behind Git-LFS is managed by
// SHA-256 OIDs only — the managing layer calls UnlinkSHA256, never Unlink.
// An AnchorSHA256 sweep reclaims the sha-dead shard, deletes its stale file
// entries as the designed cleanup, leaves sha-anchored files whole, and an
// identical re-upload of the swept content recommits everything.
func TestSweepAnchorSHA256LFSLifecycle(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts1 := [][]byte{[]byte("lfs-managed content one")}
			f1 := putGCFile(t, ctx, st, parts1)
			f2 := putGCFile(t, ctx, st, [][]byte{[]byte("lfs-managed content two")})

			// The LFS layer knows only the OID: no Unlink call ever.
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f1.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f1.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f1.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedFileEntries != 1 {
				t.Fatalf("DeletedFileEntries = %d, want 1 (the stale file entry goes with its shard)", res.DeletedFileEntries)
			}
			if got, err := gcs.GetFileIndexEntry(ctx, f1.fileHash); err != nil || got != "" {
				t.Fatalf("file entry = %q, %v; want removed", got, err)
			}
			if _, err := gcs.GetShardByHash(ctx, f1.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f1.xorbHashes[0]); ok {
				t.Fatal("xorb still stored")
			}

			// File 2 keeps both access paths: sha256 and file hash.
			assertFileIntact(t, ctx, st, f2)
			if got, err := gcs.GetFileIndexEntry(ctx, f2.fileHash); err != nil || got != f2.shardHash {
				t.Fatalf("f2 file entry = %q, %v; want %q", got, err, f2.shardHash)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f2.sha256Hex); err != nil || got != f2.shardHash {
				t.Fatalf("f2 sha256 entry = %q, %v; want %q", got, err, f2.shardHash)
			}

			// hasFile self-heal: the file entry is gone, so PutShard treats
			// the content as absent and recommits it all.
			if again := putGCFile(t, ctx, st, parts1); again.shardHash != f1.shardHash {
				t.Fatal("re-upload produced different hashes")
			}
			if _, err := st.GetShard(ctx, f1.fileHash); err != nil {
				t.Fatalf("GetShard after re-upload: %v", err)
			}
		})
	}
}

// TestSweepAnchorSHA256KeepsUnanchorableFileShards: an empty file can never
// be sha-anchored — UnlinkSHA256 rejects the all-zero digest — so while its
// file entry points at the shard an AnchorSHA256 sweep spares it (the
// accepted leak); the mark cannot see that, so a dry run still reports the
// shard as its upper bound. After Unlink the sweep reclaims it, zero entry
// included.
func TestSweepAnchorSHA256KeepsUnanchorableFileShards(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, nil)
			opts := SweepOptions{Anchor: AnchorSHA256, Grace: noGrace}

			dry, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace, DryRun: true})
			if err != nil {
				t.Fatalf("dry Sweep: %v", err)
			}
			if got, want := sweptHashes(dry.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("dry SweptShards = %v, want %v (mark-time upper bound)", got, want)
			}

			res, err := Sweep(ctx, gcs, opts)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 || res.DeletedFileEntries != 0 || res.DeletedSHA256Entries != 0 {
				t.Fatalf("file-referenced unanchorable shard touched: %+v", res)
			}
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after sweep: %v", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, zeroSHA256Hex); err != nil || got != f.shardHash {
				t.Fatalf("zero sha256 entry = %q, %v; want untouched", got, err)
			}

			// Unlink is the only way out for such shards.
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err = Sweep(ctx, gcs, opts)
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

// TestSweepAnchorFilesUnlinkAloneReclaims: under AnchorFiles a still-present
// non-zero sha256 entry neither anchors nor is walked; Unlink alone lets the
// sweep reclaim the shard and its xorbs, deleting the sha256 entry with them.
func TestSweepAnchorFilesUnlinkAloneReclaims(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("files-anchored content")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.shardHash)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorFiles, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if got, want := sweptHashes(res.SweptXorbs), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("SweptXorbs = %v, want %v", got, want)
			}
			if res.DeletedSHA256Entries != 1 {
				t.Fatalf("DeletedSHA256Entries = %d, want 1 (the non-zero entry goes with its shard)", res.DeletedSHA256Entries)
			}
			if len(res.DanglingSHA256Entries) != 0 {
				t.Fatalf("DanglingSHA256Entries = %v, want empty (sha256 index not walked)", res.DanglingSHA256Entries)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != "" {
				t.Fatalf("sha256 entry after sweep = %q, %v; want removed", got, err)
			}
			if _, err := st.GetReconstructedFile(ctx, "default", sha256Digest(f.sha256Hex)); err == nil {
				t.Fatal("sha256 lookup still resolves")
			}
		})
	}
}

// TestSweepAnchorFilesDeletesSharedSHA256Entry: identical content stored
// under two chunkings shares one sha256 entry, owned by whichever PutShard
// the backend keeps (FileStorage the first, S3 the last). An AnchorFiles
// sweep of the owner deletes the entry outright: SHA-256 lookup misses, and
// re-uploading the live chunking cannot heal it (PutShard's hasFile gate
// skips every index write), while the live file stays whole by file hash;
// re-uploading the dead chunking rewrites the entry.
func TestSweepAnchorFilesDeletesSharedSHA256Entry(t *testing.T) {
	content := []byte("same bytes, two chunkings, one sha256 entry")
	wholeParts := [][]byte{content}
	splitParts := [][]byte{content[:16], content[16:]}
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			whole := putGCFile(t, ctx, st, wholeParts)
			split := putGCFile(t, ctx, st, splitParts)
			if whole.sha256Hex != split.sha256Hex || whole.fileHash == split.fileHash {
				t.Fatal("test setup: chunkings must share the digest but not the file hash")
			}
			owner, err := gcs.GetSHA256IndexEntry(ctx, whole.sha256Hex)
			if err != nil {
				t.Fatal(err)
			}
			dead, live, deadParts, liveParts := whole, split, wholeParts, splitParts
			if owner == split.shardHash {
				dead, live, deadParts, liveParts = split, whole, splitParts, wholeParts
			} else if owner != whole.shardHash {
				t.Fatalf("sha256 entry owner = %q, want one of the two shards", owner)
			}

			if _, err := NewGC(gcs).Unlink(ctx, dead.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorFiles, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{dead.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if res.DeletedSHA256Entries != 1 {
				t.Fatalf("DeletedSHA256Entries = %d, want 1 (the shared entry goes with its owner)", res.DeletedSHA256Entries)
			}
			if _, err := st.GetShard(ctx, live.fileHash); err != nil {
				t.Fatalf("GetShard(live): %v", err)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, live.sha256Hex); err != nil || got != "" {
				t.Fatalf("shared sha256 entry = %q, %v; want removed", got, err)
			}
			if _, err := st.GetReconstructedFile(ctx, "default", sha256Digest(live.sha256Hex)); err == nil {
				t.Fatal("SHA-256 lookup still resolves after the entry delete")
			}

			// The live chunking's re-upload is a no-op behind hasFile.
			putGCFile(t, ctx, st, liveParts)
			if got, err := gcs.GetSHA256IndexEntry(ctx, live.sha256Hex); err != nil || got != "" {
				t.Fatalf("sha256 entry after live re-upload = %q, %v; want still removed", got, err)
			}
			// The dead chunking's re-upload rewrites it.
			if again := putGCFile(t, ctx, st, deadParts); again.shardHash != dead.shardHash {
				t.Fatalf("re-upload shard = %s, want %s", again.shardHash, dead.shardHash)
			}
			if got, err := gcs.GetSHA256IndexEntry(ctx, live.sha256Hex); err != nil || got != dead.shardHash {
				t.Fatalf("sha256 entry after dead re-upload = %q, %v; want %q", got, err, dead.shardHash)
			}
			assertFileIntact(t, ctx, st, live)
		})
	}
}

// TestSweepAnchorFilesSkipsSHAWalk: a dangling sha256 entry is invisible to
// an AnchorFiles sweep — the sha256 index is not walked at all — while an
// AnchorBoth sweep over the same store reports it.
func TestSweepAnchorFilesSkipsSHAWalk(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("dangling sha, files anchor")})
			if err := gcs.DeleteShard(ctx, f.shardHash); err != nil {
				t.Fatal(err)
			}

			res, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorFiles, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.DanglingSHA256Entries) != 0 {
				t.Fatalf("DanglingSHA256Entries = %v, want empty (walk skipped)", res.DanglingSHA256Entries)
			}
			// The file entry dangles under every anchor.
			if got, want := res.DanglingFileEntries, []string{f.fileHash.String()}; !slices.Equal(got, want) {
				t.Fatalf("DanglingFileEntries = %v, want %v", got, want)
			}

			both, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep(AnchorBoth): %v", err)
			}
			if got, want := both.DanglingSHA256Entries, []string{f.sha256Hex}; !slices.Equal(got, want) {
				t.Fatalf("DanglingSHA256Entries = %v, want %v", got, want)
			}
		})
	}
}

// TestSweepAnchorSHA256AbortsOnRacingSHA256Entry: under AnchorSHA256 the two
// sha scans bound the file-entry deletions. A recommit visible to the
// pre-check's scan (get 1) aborts before anything is deleted; one landing
// only after it (get 2) still aborts the shard's deletion but has lost its
// file entries to the stale cleanup — the documented degraded outcome of
// accepted race (f), healed by an identical re-upload.
func TestSweepAnchorSHA256AbortsOnRacingSHA256Entry(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name+"/pre-check-abort", func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("sha recommit at pre-check")})
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			hooked := &hookedGCStore{GCStore: gcs}
			recommitted := false
			hooked.onSHA256EntryGet = func(n int) {
				if n != 1 {
					return
				}
				recommitted = true
				setIndexEntry(t, st, "index/sha256", f.sha256Hex, f.shardHash)
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace})
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
			if got, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != f.shardHash {
				t.Fatalf("file entry = %q, %v; want %q", got, err, f.shardHash)
			}
			assertChunkEntriesIntact(t, ctx, gcs, f, res)
			assertFileIntact(t, ctx, st, f)
		})
		t.Run(backend.name+"/post-files-loop-abort", func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts := [][]byte{[]byte("sha recommit after file deletes")}
			f := putGCFile(t, ctx, st, parts)
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			hooked := &hookedGCStore{GCStore: gcs}
			recommitted := false
			hooked.onSHA256EntryGet = func(n int) {
				if n != 2 {
					return
				}
				recommitted = true
				setIndexEntry(t, st, "index/sha256", f.sha256Hex, f.shardHash)
			}
			res, err := Sweep(ctx, hooked, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace})
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
			if got, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex); err != nil || got != f.shardHash {
				t.Fatalf("sha256 entry = %q, %v; want %q", got, err, f.shardHash)
			}
			if got, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || got != "" {
				t.Fatalf("file entry = %q, %v; want removed", got, err)
			}
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); err != nil {
				t.Fatalf("shard destroyed under the racing commit: %v", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
				t.Fatal("xorb destroyed under the racing commit")
			}
			assertChunkEntriesIntact(t, ctx, gcs, f, res)

			// An identical re-upload rewrites the lost entries (self-heal).
			if again := putGCFile(t, ctx, st, parts); again.shardHash != f.shardHash {
				t.Fatal("re-upload produced different hashes")
			}
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatalf("GetShard after re-upload: %v", err)
			}
		})
	}
}

// TestSweepUnknownAnchorFails: both entry points validate the anchor before
// touching the store, and a failed SweepStep releases the single-flight
// lock for the next call.
func TestSweepUnknownAnchorFails(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			hooked := &hookedGCStore{GCStore: backend.newStore(t).(GCStore)}
			g := NewGC(hooked)
			if _, err := Sweep(ctx, hooked, SweepOptions{Anchor: "bogus", Grace: noGrace}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
				t.Fatalf("Sweep = %v, want unknown-anchor error", err)
			}
			if _, err := g.SweepStep(ctx, SweepOptions{Anchor: "bogus", Grace: noGrace}); err == nil || !strings.Contains(err.Error(), "unknown sweep anchor") {
				t.Fatalf("SweepStep = %v, want unknown-anchor error", err)
			}
			if hooked.walkShardsCalls != 0 {
				t.Fatalf("walkShardsCalls = %d after failed sweeps, want 0 (fail before store access)", hooked.walkShardsCalls)
			}
			// The failed step must not leak the single-flight lock.
			if _, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace}); err != nil {
				t.Fatalf("SweepStep after failed anchor: %v", err)
			}
			if hooked.walkShardsCalls == 0 {
				t.Fatal("valid SweepStep did not reach the store")
			}
		})
	}
}

// TestSweepAnchorSHA256KeepsNilMetadataFileShards: the unanchorable-file
// exemption's other branch — a stored shard whose file block carries no
// MetadataExt at all. PutShard always installs metadata, so the shard
// object is written directly; file backend only, the sweep logic under
// test is backend-independent.
func TestSweepAnchorSHA256KeepsNilMetadataFileShards(t *testing.T) {
	ctx := context.Background()
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	shardObj := shard.NewShard()
	fileHash, xorbHashes, _ := addGCFileBlock(t, ctx, fs, shardObj, [][]byte{[]byte("no metadata ext")})
	r, err := shardObj.Encode(false)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	shardHash, err := computeShardHashFromReader(bytes.NewReader(raw))
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
	setIndexEntry(t, fs, "index/files", fileHash.String(), shardHash)

	// File-referenced and metadata-less: exempt under AnchorSHA256.
	res, err := Sweep(ctx, fs, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace})
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
	res, err = Sweep(ctx, fs, SweepOptions{Anchor: AnchorSHA256, Grace: noGrace})
	if err != nil {
		t.Fatalf("Sweep after unlink: %v", err)
	}
	if got, want := sweptHashes(res.SweptShards), []string{shardHash}; !slices.Equal(got, want) {
		t.Fatalf("SweptShards = %v, want %v", got, want)
	}
	if got, want := sweptHashes(res.SweptXorbs), []string{xorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("SweptXorbs = %v, want %v", got, want)
	}
}

// TestSweepAnchorBothUnchanged: passing AnchorBoth explicitly is the zero
// value — a shard stays live while either entry remains, exactly like the
// default-mode lifecycles in TestSweepNeedsBothUnlinks, and no file entries
// are ever deleted.
func TestSweepAnchorBothUnchanged(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)
			f := putGCFile(t, ctx, st, [][]byte{[]byte("explicit anchor both")})
			if _, err := NewGC(gcs).Unlink(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}
			res, err := Sweep(ctx, gcs, SweepOptions{Anchor: AnchorBoth, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if len(res.SweptShards) != 0 {
				t.Fatalf("sha-anchored shard swept under explicit AnchorBoth: %+v", res)
			}
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}
			res, err = Sweep(ctx, gcs, SweepOptions{Anchor: AnchorBoth, Grace: noGrace})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
				t.Fatalf("SweptShards = %v, want %v", got, want)
			}
			if res.DeletedFileEntries != 0 {
				t.Fatalf("DeletedFileEntries = %d, want 0 (only AnchorSHA256 produces it)", res.DeletedFileEntries)
			}
		})
	}
}

// TestSweepStepUnreadableShardCannotLivelock: unswept queue items charge no
// bound. With MaxDeletes=1 and an undecodable dead shard sorting first in
// the queue, the first step must still sweep the healthy dead shard behind
// it — per-item accounting would burn the only slot on the corrupt object
// on every stateless pass, forever — and repeated steps reach Done with the
// corrupt shard reported unreadable, nothing more to do.
func TestSweepStepUnreadableShardCannotLivelock(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("healthy dead shard")})
			unlinkGCFile(t, ctx, gcs, f)
			// The all-zero name sorts before any real hash on both backends.
			corrupt := strings.Repeat("0", 64)
			if corrupt >= f.shardHash {
				t.Fatalf("test setup: %s must sort before %s", corrupt, f.shardHash)
			}
			putRawShardObject(t, ctx, st, corrupt, []byte("not a decodable shard"))

			g := NewGC(gcs)
			opts := SweepOptions{Grace: noGrace, MaxDeletes: 1}
			res, err := g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
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
					t.Fatalf("later step swept %v/%v, want nothing left", sweptHashes(res.SweptShards), sweptHashes(res.SweptXorbs))
				}
			}
			if got, want := res.UnreadableShards, []string{corrupt}; !slices.Equal(got, want) {
				t.Fatalf("done step UnreadableShards = %v, want %v", got, want)
			}
			// The poisoned passes judge no xorb: the healthy shard's xorb
			// stays until the corrupt object is repaired or removed.
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); !ok {
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
		})
	}
}

// TestSweepStepSparedUnanchorableShardCannotLivelock: under AnchorSHA256 a
// file-referenced shard carrying an unanchorable file is re-queued dead on
// every pass — the mark cannot see file refs — and spared by sweepShard's
// pre-check. Spared items charge no bound, so with MaxDeletes=1 the first
// step must still sweep the sha-dead shard behind it and repeated steps
// reach Done with the spared shard intact.
func TestSweepStepSparedUnanchorableShardCannotLivelock(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("sha-dead healthy shard")})
			if _, err := NewGC(gcs).UnlinkSHA256(ctx, sha256Digest(f.sha256Hex)); err != nil {
				t.Fatal(err)
			}

			// A valid shard whose file block has no MetadataExt, stored
			// under the all-zero name so it sorts before the sha-dead
			// shard, with a file entry pointing at it: unanchorable and
			// spared, yet queued dead by every sha-mode mark.
			spared := strings.Repeat("0", 64)
			if spared >= f.shardHash {
				t.Fatalf("test setup: %s must sort before %s", spared, f.shardHash)
			}
			sparedShard := shard.NewShard()
			sparedFile, sparedXorbs, _ := addGCFileBlock(t, ctx, st, sparedShard, [][]byte{[]byte("unanchorable payload")})
			r, err := sparedShard.Encode(false)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(r)
			if err != nil {
				t.Fatal(err)
			}
			putRawShardObject(t, ctx, st, spared, raw)
			setIndexEntry(t, st, "index/files", sparedFile.String(), spared)

			g := NewGC(gcs)
			opts := SweepOptions{Anchor: AnchorSHA256, Grace: noGrace, MaxDeletes: 1}
			res, err := g.SweepStep(ctx, opts)
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{f.shardHash}; !slices.Equal(got, want) {
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
					t.Fatalf("later step swept shards %v, want none", sweptHashes(res.SweptShards))
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
			if _, err := gcs.GetShardByHash(ctx, f.shardHash); !errors.Is(err, iofs.ErrNotExist) {
				t.Fatalf("dead shard load = %v, want ErrNotExist", err)
			}
			if ok, _ := st.HasXorb(ctx, "default", f.xorbHashes[0]); ok {
				t.Fatal("dead shard's xorb still stored")
			}
		})
	}
}

// TestSweepStepExhaustedAtShardDrainSkipsXorbPhase: a step whose bounds run
// out exactly as the shard queue drains returns at once — no phase-2 walk,
// no shard loads beyond the dead shard's own — reporting Done false with
// nothing measured for xorbs.
func TestSweepStepExhaustedAtShardDrainSkipsXorbPhase(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			dead := putGCFile(t, ctx, st, [][]byte{[]byte("drained dead shard")})
			unlinkGCFile(t, ctx, gcs, dead)
			live := putGCFile(t, ctx, st, [][]byte{[]byte("live shard phase 2 would load")})

			hooked := &hookedGCStore{GCStore: gcs}
			g := NewGC(hooked)
			res, err := g.SweepStep(ctx, SweepOptions{Grace: noGrace, MaxDeletes: 1})
			if err != nil {
				t.Fatalf("SweepStep: %v", err)
			}
			if got, want := sweptHashes(res.SweptShards), []string{dead.shardHash}; !slices.Equal(got, want) {
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
			if ok, _ := st.HasXorb(ctx, "default", dead.xorbHashes[0]); !ok {
				t.Fatal("xorb swept by a step that skipped the xorb phase")
			}
			res, err = g.SweepStep(ctx, SweepOptions{Grace: noGrace})
			if err != nil {
				t.Fatalf("second SweepStep: %v", err)
			}
			if !res.Done || len(res.SweptXorbs) != 1 {
				t.Fatalf("second step = done %v, %d xorbs; want done with the xorb swept", res.Done, len(res.SweptXorbs))
			}
			assertFileIntact(t, ctx, st, live)
		})
	}
}

// TestSweepCanceledContextFailsBeforeWork: a pass entered with a dead
// context reports the cancellation, even over an empty store where no walk
// or load would ever notice it.
func TestSweepCanceledContextFailsBeforeWork(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			st := backend.newStore(t)
			gcs := st.(GCStore)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err := Sweep(ctx, gcs, SweepOptions{Grace: noGrace}); !errors.Is(err, context.Canceled) {
				t.Fatalf("Sweep = %v, want context.Canceled", err)
			}
			if _, err := NewGC(gcs).SweepStep(ctx, SweepOptions{Grace: noGrace}); !errors.Is(err, context.Canceled) {
				t.Fatalf("SweepStep = %v, want context.Canceled", err)
			}
		})
	}
}
