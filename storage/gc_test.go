package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// noGrace disables the sweep grace window so freshly written objects are
// collectable in tests.
var noGrace = SweepOptions{Grace: -1}

// gcBackends returns a fresh SweepStore for each storage backend.
func gcBackends(t *testing.T) map[string]SweepStore {
	t.Helper()
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	return map[string]SweepStore{
		"file": fs,
		"s3":   newTestS3Storage(t),
	}
}

// gcTerm is one reconstruction term: all chunks of one xorb, from index 0.
type gcTerm struct {
	xorbHash xet.XorbHash
	chunks   [][]byte
}

// putGCXorb encodes chunks into one xorb and stores it.
func putGCXorb(t *testing.T, st SweepStore, chunks ...[]byte) xet.XorbHash {
	t.Helper()
	encoded, xorbHash := encodeTestXorb(t, true, chunks...)
	if _, err := st.PutXorb(context.Background(), "default", xorbHash, bytes.NewReader(encoded)); err != nil {
		t.Fatal(err)
	}
	return xorbHash
}

func gcFileBlock(fileHash xet.FileHash, terms ...gcTerm) shard.FileBlock {
	fb := shard.FileBlock{FileHash: fileHash}
	for _, term := range terms {
		var total uint32
		for _, c := range term.chunks {
			total += uint32(len(c))
		}
		fb.Entries = append(fb.Entries, shard.FileDataSequenceEntry{
			CASHash:          term.xorbHash,
			ChunkIndexEnd:    uint32(len(term.chunks)),
			UnpackedSegBytes: total,
		})
	}
	return fb
}

func gcCASBlock(xorbHash xet.XorbHash, chunkHashes ...xet.ChunkHash) shard.CASBlock {
	cb := shard.CASBlock{CASHash: xorbHash}
	for _, ch := range chunkHashes {
		cb.Chunks = append(cb.Chunks, shard.CASChunkSequenceEntry{ChunkHash: ch})
	}
	return cb
}

func putGCShard(t *testing.T, st SweepStore, files []shard.FileBlock, cas []shard.CASBlock) {
	t.Helper()
	s := shard.NewShard()
	for _, fb := range files {
		s.AddFile(fb)
	}
	for _, cb := range cas {
		s.AddCASBlock(cb)
	}
	s.SetFooter()
	if inserted, err := st.PutShard(context.Background(), s); err != nil || !inserted {
		t.Fatalf("PutShard() = %v, %v", inserted, err)
	}
}

func gcSHA256(terms ...gcTerm) [32]byte {
	h := sha256.New()
	for _, term := range terms {
		for _, c := range term.chunks {
			h.Write(c)
		}
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return digest
}

func countWalk(t *testing.T, walk func(context.Context, func(string, int64, time.Time) error) error) int {
	t.Helper()
	n := 0
	if err := walk(context.Background(), func(string, int64, time.Time) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func countIndexWalk(t *testing.T, walk func(context.Context, func(string, string, time.Time) error) error) int {
	t.Helper()
	n := 0
	if err := walk(context.Background(), func(string, string, time.Time) error {
		n++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// gcCounts snapshots how many objects each kind holds.
type gcCounts struct{ files, sha256s, chunks, shards, xorbs int }

func snapshotCounts(t *testing.T, st SweepStore) gcCounts {
	t.Helper()
	return gcCounts{
		files:   countIndexWalk(t, st.WalkFileIndex),
		sha256s: countIndexWalk(t, st.WalkSHA256Index),
		chunks:  countIndexWalk(t, st.WalkChunkIndex),
		shards:  countWalk(t, st.WalkShards),
		xorbs:   countWalk(t, st.WalkXorbs),
	}
}

func TestGCUnlinkAndSweepSharedXorb(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunkA := []byte("file-a-only-chunk")
			chunkShared := bytes.Repeat([]byte("shared-chunk"), 10)
			chunkB := []byte("file-b-only-chunk")

			xorbA := putGCXorb(t, st, chunkA)
			xorbShared := putGCXorb(t, st, chunkShared)
			xorbB := putGCXorb(t, st, chunkB)

			fileHashA := xet.FileHash{0xA1}
			fileHashB := xet.FileHash{0xB1}
			termsA := []gcTerm{{xorbA, [][]byte{chunkA}}, {xorbShared, [][]byte{chunkShared}}}
			termsB := []gcTerm{{xorbB, [][]byte{chunkB}}, {xorbShared, [][]byte{chunkShared}}}

			// Shard A introduces xorbA and the shared xorb; shard B dedups the
			// shared xorb and introduces only xorbB.
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashA, termsA...)},
				[]shard.CASBlock{gcCASBlock(xorbA, xet.ChunkHash{1}), gcCASBlock(xorbShared, xet.ChunkHash{2})})
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashB, termsB...)},
				[]shard.CASBlock{gcCASBlock(xorbB, xet.ChunkHash{3})})

			sha256A := gcSHA256(termsA...)
			sha256B := gcSHA256(termsB...)

			// Unlink file A; its SHA-256 entry stays until the shard falls.
			res, err := Unlink(ctx, st, fileHashA.String())
			if err != nil {
				t.Fatalf("Unlink(A): %v", err)
			}
			if res.SHA256 != hex.EncodeToString(sha256A[:]) || !res.RemovedFileIndex {
				t.Fatalf("Unlink(A) = %+v", res)
			}
			if _, err := st.GetFileHashBySHA256(ctx, "default", sha256A); err != nil {
				t.Fatalf("SHA-256 A must resolve until its shard is swept: %v", err)
			}
			if _, err := Unlink(ctx, st, fileHashA.String()); !errors.Is(err, ErrFileNotFound) {
				t.Fatalf("second Unlink error = %v, want ErrFileNotFound", err)
			}

			report, err := Sweep(ctx, st, noGrace)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if report.LiveFiles != 1 || report.LiveShards != 1 || report.LiveXorbs != 2 {
				t.Fatalf("live counts = %+v", report)
			}
			if report.RemovedShards != 1 || report.RemovedXorbs != 1 || report.RemovedChunkIndexEntries != 2 || report.RemovedSHA256IndexEntries != 1 {
				t.Fatalf("removed counts = %+v", report)
			}
			if _, err := st.GetFileHashBySHA256(ctx, "default", sha256A); err == nil {
				t.Fatal("SHA-256 A still resolves after its shard was swept")
			}

			// The shared xorb survives because live file B references it.
			if ok, _ := st.HasXorb(ctx, "default", xorbA); ok {
				t.Fatal("xorbA still present after sweep")
			}
			if ok, _ := st.HasXorb(ctx, "default", xorbShared); !ok {
				t.Fatal("shared xorb was removed while file B is live")
			}
			content, err := st.GetReconstructedFile(ctx, "default", sha256B)
			if err != nil {
				t.Fatalf("GetReconstructedFile(B): %v", err)
			}
			got, err := io.ReadAll(content)
			content.Close()
			if err != nil || !bytes.Equal(got, append(append([]byte{}, chunkB...), chunkShared...)) {
				t.Fatalf("file B content mismatch after sweep: %v", err)
			}

			// Unlink file B by file hash, then sweep everything away.
			res, err = Unlink(ctx, st, fileHashB.String())
			if err != nil {
				t.Fatalf("Unlink(file B): %v", err)
			}
			if res.SHA256 != hex.EncodeToString(sha256B[:]) || !res.RemovedFileIndex {
				t.Fatalf("Unlink(file B) = %+v", res)
			}

			report, err = Sweep(ctx, st, noGrace)
			if err != nil {
				t.Fatalf("Sweep 2: %v", err)
			}
			if report.RemovedShards != 1 || report.RemovedXorbs != 2 || report.RemovedChunkIndexEntries != 1 || report.RemovedSHA256IndexEntries != 1 {
				t.Fatalf("second sweep counts = %+v", report)
			}
			if counts := snapshotCounts(t, st); counts != (gcCounts{}) {
				t.Fatalf("store not empty after final sweep: %+v", counts)
			}
		})
	}
}

func TestGCSweepKeepsMultiFileShardUntilAllFilesUnlinked(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunkC := []byte("file-c-chunk")
			chunkD := []byte("file-d-chunk")
			xorbC := putGCXorb(t, st, chunkC)
			xorbD := putGCXorb(t, st, chunkD)

			fileHashC := xet.FileHash{0xC1}
			fileHashD := xet.FileHash{0xD1}
			termC := gcTerm{xorbC, [][]byte{chunkC}}
			termD := gcTerm{xorbD, [][]byte{chunkD}}

			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHashC, termC), gcFileBlock(fileHashD, termD)},
				[]shard.CASBlock{gcCASBlock(xorbC, xet.ChunkHash{4}), gcCASBlock(xorbD, xet.ChunkHash{5})})

			sha256C := gcSHA256(termC)
			sha256D := gcSHA256(termD)

			if _, err := Unlink(ctx, st, fileHashC.String()); err != nil {
				t.Fatalf("Unlink(C): %v", err)
			}
			report, err := Sweep(ctx, st, noGrace)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			// The shard still holds live file D, so nothing is reclaimed yet.
			if report.RemovedShards != 0 || report.RemovedXorbs != 0 || report.RemovedChunkIndexEntries != 0 || report.RemovedSHA256IndexEntries != 0 {
				t.Fatalf("sweep reclaimed shard with live file: %+v", report)
			}
			if ok, _ := st.HasXorb(ctx, "default", xorbC); !ok {
				t.Fatal("xorbC removed while its shard is live")
			}
			// One SHA-256 can map to several xet hashes; the entry survives its
			// unlinked file until the whole shard is swept.
			if _, err := st.GetFileHashBySHA256(ctx, "default", sha256C); err != nil {
				t.Fatalf("SHA-256 C must survive while its shard is live: %v", err)
			}
			content, err := st.GetReconstructedFile(ctx, "default", sha256D)
			if err != nil {
				t.Fatalf("GetReconstructedFile(D): %v", err)
			}
			content.Close()

			if _, err := Unlink(ctx, st, fileHashD.String()); err != nil {
				t.Fatalf("Unlink(D): %v", err)
			}
			if _, err := Sweep(ctx, st, noGrace); err != nil {
				t.Fatalf("Sweep 2: %v", err)
			}
			if counts := snapshotCounts(t, st); counts != (gcCounts{}) {
				t.Fatalf("store not empty after final sweep: %+v", counts)
			}
		})
	}
}

func TestGCSweepGraceProtectsFreshObjects(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("grace-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xE1}
			term := gcTerm{xorbHash, [][]byte{chunk}}
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHash, term)},
				[]shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{6})})

			if _, err := Unlink(ctx, st, fileHash.String()); err != nil {
				t.Fatalf("Unlink: %v", err)
			}

			// Default grace keeps everything that was just written.
			report, err := Sweep(ctx, st, SweepOptions{})
			if err != nil {
				t.Fatalf("Sweep(default grace): %v", err)
			}
			if report.RemovedShards != 0 || report.RemovedXorbs != 0 || report.RemovedChunkIndexEntries != 0 {
				t.Fatalf("sweep removed fresh objects: %+v", report)
			}
			if report.SkippedGrace == 0 {
				t.Fatalf("SkippedGrace = 0, want > 0: %+v", report)
			}
			if ok, _ := st.HasXorb(ctx, "default", xorbHash); !ok {
				t.Fatal("fresh xorb removed inside grace window")
			}

			if _, err := Sweep(ctx, st, noGrace); err != nil {
				t.Fatalf("Sweep(no grace): %v", err)
			}
			if counts := snapshotCounts(t, st); counts != (gcCounts{}) {
				t.Fatalf("store not empty after no-grace sweep: %+v", counts)
			}
		})
	}
}

func TestGCSweepDryRunDeletesNothing(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("dry-run-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xF1}
			term := gcTerm{xorbHash, [][]byte{chunk}}
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHash, term)},
				[]shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{7})})

			if _, err := Unlink(ctx, st, fileHash.String()); err != nil {
				t.Fatalf("Unlink: %v", err)
			}
			before := snapshotCounts(t, st)

			report, err := Sweep(ctx, st, SweepOptions{DryRun: true, Grace: -1})
			if err != nil {
				t.Fatalf("Sweep(dry run): %v", err)
			}
			if !report.DryRun || report.RemovedShards != 1 || report.RemovedXorbs != 1 || report.RemovedChunkIndexEntries != 1 || report.RemovedSHA256IndexEntries != 1 {
				t.Fatalf("dry-run counts = %+v", report)
			}
			if after := snapshotCounts(t, st); after != before {
				t.Fatalf("dry run mutated store: before %+v, after %+v", before, after)
			}
		})
	}
}

func TestGCSweepRepairsDanglingEntries(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("dangling-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xA7}
			term := gcTerm{xorbHash, [][]byte{chunk}}
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHash, term)},
				[]shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{8})})

			// Simulate a manually removed shard object: every index entry now
			// points at nothing.
			var shardHash string
			if err := st.WalkShards(ctx, func(h string, _ int64, _ time.Time) error {
				shardHash = h
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := st.DeleteShard(ctx, shardHash); err != nil {
				t.Fatalf("DeleteShard() = %v", err)
			}

			report, err := Sweep(ctx, st, noGrace)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if report.RemovedFileIndexEntries != 1 || report.RemovedSHA256IndexEntries != 1 ||
				report.RemovedChunkIndexEntries != 1 || report.RemovedXorbs != 1 {
				t.Fatalf("repair counts = %+v", report)
			}
			if counts := snapshotCounts(t, st); counts != (gcCounts{}) {
				t.Fatalf("store not empty after repair sweep: %+v", counts)
			}
		})
	}
}

func TestGCReuploadAfterUnlinkRestoresFile(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("restore-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xB7}
			term := gcTerm{xorbHash, [][]byte{chunk}}
			files := []shard.FileBlock{gcFileBlock(fileHash, term)}
			cas := []shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{9})}
			putGCShard(t, st, files, cas)

			sha := gcSHA256(term)
			if _, err := Unlink(ctx, st, fileHash.String()); err != nil {
				t.Fatalf("Unlink: %v", err)
			}

			// Re-uploading the same shard restores the index entries.
			s := shard.NewShard()
			for _, fb := range files {
				s.AddFile(fb)
			}
			for _, cb := range cas {
				s.AddCASBlock(cb)
			}
			s.SetFooter()
			if _, err := st.PutShard(ctx, s); err != nil {
				t.Fatalf("PutShard(re-upload): %v", err)
			}
			if _, err := st.GetFileHashBySHA256(ctx, "default", sha); err != nil {
				t.Fatalf("SHA-256 does not resolve after re-upload: %v", err)
			}
			if _, err := st.GetShard(ctx, fileHash); err != nil {
				t.Fatalf("file does not resolve after re-upload: %v", err)
			}
		})
	}
}

// agedXorbWalk reports every xorb as ancient, simulating a long-lived xorb
// reused by a freshly landed shard.
type agedXorbWalk struct {
	SweepStore
}

func (a agedXorbWalk) WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error {
	return a.SweepStore.WalkXorbs(ctx, func(h string, size int64, _ time.Time) error {
		return fn(h, size, time.Time{})
	})
}

func TestGCSweepGraceKeepsXorbsOfFreshShard(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("reused-old-xorb-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xC7}
			term := gcTerm{xorbHash, [][]byte{chunk}}
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHash, term)},
				[]shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{10})})

			// Simulate a shard from another instance landing after the root
			// scan: its file index entry is invisible to this sweep, and the
			// xorb it reuses is far older than the grace window.
			if removed, err := st.DeleteFileIndexEntry(ctx, fileHash.String()); err != nil || !removed {
				t.Fatalf("DeleteFileIndexEntry() = %v, %v", removed, err)
			}

			report, err := Sweep(ctx, agedXorbWalk{st}, SweepOptions{})
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if report.RemovedShards != 0 || report.RemovedXorbs != 0 {
				t.Fatalf("sweep removed objects of a grace-kept shard: %+v", report)
			}
			if report.SkippedGrace == 0 {
				t.Fatalf("SkippedGrace = 0, want > 0: %+v", report)
			}
			if ok, _ := st.HasXorb(ctx, "default", xorbHash); !ok {
				t.Fatal("old xorb reused by fresh shard was removed")
			}
		})
	}
}

func TestShardLoadDoesNotRelinkUnlinkedFiles(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			chunk := []byte("relink-guard-chunk")
			xorbHash := putGCXorb(t, st, chunk)
			fileHash := xet.FileHash{0xF7}
			putGCShard(t, st,
				[]shard.FileBlock{gcFileBlock(fileHash, gcTerm{xorbHash, [][]byte{chunk}})},
				[]shard.CASBlock{gcCASBlock(xorbHash, xet.ChunkHash{13})})

			var shardHash string
			if err := st.WalkFileIndex(ctx, func(_, sh string, _ time.Time) error {
				shardHash = sh
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := Unlink(ctx, st, fileHash.String()); err != nil {
				t.Fatalf("Unlink: %v", err)
			}

			// Evict the parsed shard so the next load takes the miss path.
			switch b := st.(type) {
			case *FileStorage:
				b.shardMut.Lock()
				b.shardIndex.Remove(shardHash)
				b.shardMut.Unlock()
			case *S3Storage:
				b.shardMut.Lock()
				b.shardIndex.Remove(shardHash)
				b.shardMut.Unlock()
			}

			// A sweep's grace branch loads shards by hash; that load must not
			// re-link the shard's files through the in-memory cache.
			if _, err := st.GetShardByHash(ctx, shardHash); err != nil {
				t.Fatalf("GetShardByHash: %v", err)
			}
			if _, err := st.GetShard(ctx, fileHash); err == nil {
				t.Fatal("unlinked file resolvable after loading its shard by hash")
			}
		})
	}
}
