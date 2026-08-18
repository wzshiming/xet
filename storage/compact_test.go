package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// noCompactGrace disables the grace window so freshly written xorbs are
// compaction candidates.
var noCompactGrace = CompactOptions{Grace: -1}

// incompressible returns deterministic pseudo-random bytes, so a xorb's
// on-disk size reflects its payload size.
func incompressible(seed uint64, n int) []byte {
	out := make([]byte, n)
	state := seed*6364136223846793005 + 1442695040888963407
	for i := range out {
		state = state*6364136223846793005 + 1442695040888963407
		out[i] = byte(state >> 33)
	}
	return out
}

// partialTerm references a sub-range of a xorb's chunks.
func partialTerm(xorbHash xet.XorbHash, chunks [][]byte, start, end uint32) shard.FileDataSequenceEntry {
	var total uint32
	for _, c := range chunks[start:end] {
		total += uint32(len(c))
	}
	return shard.FileDataSequenceEntry{
		CASHash:          xorbHash,
		ChunkIndexStart:  start,
		ChunkIndexEnd:    end,
		UnpackedSegBytes: total,
	}
}

func TestCompactRepacksSparseXorb(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// One xorb where only the first chunk is still referenced.
			live := incompressible(1, 4096)
			chunks := [][]byte{live,
				incompressible(2, 65536),
				incompressible(3, 65536),
				incompressible(4, 65536),
			}
			sparse := putGCXorb(t, st, chunks...)

			fileHash := xet.FileHash{0xC1}
			fb := shard.FileBlock{FileHash: fileHash}
			fb.Entries = append(fb.Entries, partialTerm(sparse, chunks, 0, 1))
			putGCShard(t, st, []shard.FileBlock{fb},
				[]shard.CASBlock{gcCASBlock(sparse, xet.ComputeChunkHash(live))})

			digest := gcSHA256(gcTerm{sparse, [][]byte{live}})
			before := snapshotCounts(t, st)

			// A dry run reports the candidate without touching the store.
			dry := noCompactGrace
			dry.DryRun = true
			report, err := Compact(ctx, st, dry)
			if err != nil {
				t.Fatalf("Compact(dry run): %v", err)
			}
			if report.SparseXorbs != 1 || report.NewXorbs != 0 || report.RewrittenShards != 0 {
				t.Fatalf("dry-run report = %+v", report)
			}
			if got := snapshotCounts(t, st); got != before {
				t.Fatalf("dry run changed the store: %+v -> %+v", before, got)
			}

			report, err = Compact(ctx, st, noCompactGrace)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if report.SparseXorbs != 1 || report.NewXorbs != 1 || report.RewrittenShards != 1 || report.MovedChunks != 1 {
				t.Fatalf("compact report = %+v", report)
			}

			// The file keeps its hash and its content.
			gotHash, err := st.GetFileHashBySHA256(ctx, "default", digest)
			if err != nil {
				t.Fatalf("GetFileHashBySHA256 after compact: %v", err)
			}
			if gotHash != fileHash {
				t.Fatalf("file hash = %s, want %s", gotHash.String(), fileHash.String())
			}
			content, err := st.GetReconstructedFile(ctx, "default", digest)
			if err != nil {
				t.Fatalf("GetReconstructedFile after compact: %v", err)
			}
			got, err := io.ReadAll(content)
			content.Close()
			if err != nil || !bytes.Equal(got, live) {
				t.Fatalf("content mismatch after compact: %v", err)
			}

			// The superseded xorb and shard are collectable now that nothing
			// references them.
			sweep, err := Sweep(ctx, st, noGrace)
			if err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if sweep.RemovedXorbs != 1 || sweep.RemovedShards != 1 {
				t.Fatalf("sweep after compact = %+v", sweep)
			}
			if ok, _ := st.HasXorb(ctx, "default", sparse); ok {
				t.Fatal("sparse xorb survived the sweep")
			}
			if counts := snapshotCounts(t, st); counts.files != 1 || counts.shards != 1 || counts.xorbs != 1 {
				t.Fatalf("counts after compact+sweep = %+v", counts)
			}

			// Nothing left to repack.
			report, err = Compact(ctx, st, noCompactGrace)
			if err != nil {
				t.Fatalf("second Compact: %v", err)
			}
			if report.SparseXorbs != 0 || report.NewXorbs != 0 {
				t.Fatalf("second compact report = %+v", report)
			}
		})
	}
}

func TestCompactKeepsDenseXorbsAndSharedTerms(t *testing.T) {
	for name, st := range gcBackends(t) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// Every chunk is referenced, so the xorb is dense even though two
			// files share the same term.
			chunks := [][]byte{incompressible(5, 65536), incompressible(6, 65536)}
			dense := putGCXorb(t, st, chunks...)

			fileA := shard.FileBlock{FileHash: xet.FileHash{0xD1}}
			fileA.Entries = append(fileA.Entries, partialTerm(dense, chunks, 0, 2))
			fileB := shard.FileBlock{FileHash: xet.FileHash{0xD2}}
			fileB.Entries = append(fileB.Entries, partialTerm(dense, chunks, 0, 1))
			putGCShard(t, st, []shard.FileBlock{fileA, fileB},
				[]shard.CASBlock{gcCASBlock(dense, xet.ComputeChunkHash(chunks[0]), xet.ComputeChunkHash(chunks[1]))})

			before := snapshotCounts(t, st)
			report, err := Compact(ctx, st, noCompactGrace)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if report.ScannedXorbs != 1 || report.SparseXorbs != 0 || report.NewXorbs != 0 {
				t.Fatalf("compact report = %+v", report)
			}
			if got := snapshotCounts(t, st); got != before {
				t.Fatalf("dense store changed: %+v -> %+v", before, got)
			}
		})
	}
}

func TestCompactGraceProtectsFreshXorbs(t *testing.T) {
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	live := incompressible(7, 1024)
	chunks := [][]byte{live, incompressible(8, 65536)}
	sparse := putGCXorb(t, st, chunks...)

	fb := shard.FileBlock{FileHash: xet.FileHash{0xE1}}
	fb.Entries = append(fb.Entries, partialTerm(sparse, chunks, 0, 1))
	putGCShard(t, st, []shard.FileBlock{fb},
		[]shard.CASBlock{gcCASBlock(sparse, xet.ComputeChunkHash(live))})

	report, err := Compact(ctx, st, CompactOptions{})
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if report.SkippedGrace != 1 || report.SparseXorbs != 0 {
		t.Fatalf("report = %+v", report)
	}
}

func TestCompactRewritesMultiTermFile(t *testing.T) {
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// The file spans two xorbs: one sparse, one dense. Only the sparse one is
	// repacked, and the term order must survive.
	head := incompressible(9, 4096)
	sparseChunks := [][]byte{head, incompressible(10, 65536), incompressible(11, 65536)}
	sparse := putGCXorb(t, st, sparseChunks...)
	tail := incompressible(12, 8192)
	dense := putGCXorb(t, st, tail)

	fileHash := xet.FileHash{0xF1}
	fb := shard.FileBlock{FileHash: fileHash}
	fb.Entries = append(fb.Entries,
		partialTerm(sparse, sparseChunks, 0, 1),
		partialTerm(dense, [][]byte{tail}, 0, 1),
	)
	putGCShard(t, st, []shard.FileBlock{fb}, []shard.CASBlock{
		gcCASBlock(sparse, xet.ComputeChunkHash(head)),
		gcCASBlock(dense, xet.ComputeChunkHash(tail)),
	})

	want := append(append([]byte{}, head...), tail...)
	digest := gcSHA256(gcTerm{sparse, [][]byte{head}}, gcTerm{dense, [][]byte{tail}})

	report, err := Compact(ctx, st, noCompactGrace)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if report.SparseXorbs != 1 || report.NewXorbs != 1 || report.RewrittenShards != 1 {
		t.Fatalf("report = %+v", report)
	}

	sh, err := st.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatalf("GetShard: %v", err)
	}
	if len(sh.Files) != 1 || len(sh.Files[0].Entries) != 2 {
		t.Fatalf("rewritten shard has %d files", len(sh.Files))
	}
	if sh.Files[0].Entries[0].CASHash == sparse {
		t.Fatal("first term still points at the sparse xorb")
	}
	if sh.Files[0].Entries[1].CASHash != dense {
		t.Fatal("dense term was rewritten")
	}

	content, err := st.GetReconstructedFile(ctx, "default", digest)
	if err != nil {
		t.Fatalf("GetReconstructedFile: %v", err)
	}
	got, err := io.ReadAll(content)
	content.Close()
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("content mismatch: %v", err)
	}
	if _, err := Unlink(ctx, st, hex.EncodeToString(digest[:]), HashKindSHA256); err != nil {
		t.Fatalf("Unlink after compact: %v", err)
	}
}

func TestCompactTouchGivesSupersededXorbsGrace(t *testing.T) {
	ctx := context.Background()
	fs, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	live := incompressible(7, 4096)
	chunks := [][]byte{live, incompressible(8, 65536), incompressible(9, 65536)}
	sparse := putGCXorb(t, fs, chunks...)

	fileHash := xet.FileHash{0xD1}
	fb := shard.FileBlock{FileHash: fileHash}
	fb.Entries = append(fb.Entries, partialTerm(sparse, chunks, 0, 1))
	putGCShard(t, fs, []shard.FileBlock{fb},
		[]shard.CASBlock{gcCASBlock(sparse, xet.ComputeChunkHash(live))})

	// Age the xorb and its shard well past the grace window, as they would
	// be in production when a compaction picks them up.
	var shardHash string
	if err := fs.WalkShards(ctx, func(h string, _ int64, _ time.Time) error {
		shardHash = h
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * DefaultSweepGrace)
	for _, path := range []string{
		fs.objectPath("xorbs", sparse.String()),
		fs.objectPath("shards", shardHash),
	} {
		if err := os.Chtimes(path, old, old); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := Compact(ctx, fs, CompactOptions{}); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	// A default-grace sweep may take the superseded shard but must leave
	// the touched xorb for in-flight reconstruction responses.
	report, err := Sweep(ctx, fs, SweepOptions{})
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if report.RemovedXorbs != 0 {
		t.Fatalf("grace sweep removed a superseded xorb: %+v", report)
	}
	if ok, _ := fs.HasXorb(ctx, "default", sparse); !ok {
		t.Fatal("superseded xorb gone within its grace window")
	}

	// Once the grace window no longer applies, the xorb is reclaimed.
	report, err = Sweep(ctx, fs, noGrace)
	if err != nil {
		t.Fatalf("Sweep(no grace): %v", err)
	}
	if report.RemovedXorbs != 1 {
		t.Fatalf("no-grace sweep report = %+v", report)
	}
}
