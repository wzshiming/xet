package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

func TestCoalesceRuns(t *testing.T) {
	cases := []struct {
		name string
		in   []chunkRun
		want []chunkRun
	}{
		{"single", []chunkRun{{0, 5}}, []chunkRun{{0, 5}}},
		{"overlapping", []chunkRun{{0, 5}, {3, 8}}, []chunkRun{{0, 8}}},
		{"adjacent", []chunkRun{{0, 3}, {3, 6}}, []chunkRun{{0, 6}}},
		{"disjoint", []chunkRun{{0, 2}, {4, 6}}, []chunkRun{{0, 2}, {4, 6}}},
		{"unsorted", []chunkRun{{7, 9}, {0, 2}, {3, 5}}, []chunkRun{{0, 2}, {3, 5}, {7, 9}}},
		{"unsorted overlapping", []chunkRun{{4, 6}, {0, 3}, {2, 5}}, []chunkRun{{0, 6}}},
		{"duplicates", []chunkRun{{2, 4}, {2, 4}, {2, 4}}, []chunkRun{{2, 4}}},
		{"contained", []chunkRun{{0, 10}, {2, 4}}, []chunkRun{{0, 10}}},
		{"empty", nil, []chunkRun{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := slices.Clone(tc.in)
			if got := coalesceRuns(in); !slices.Equal(got, tc.want) {
				t.Fatalf("coalesceRuns(%v) = %v, want %v", tc.in, got, tc.want)
			}
			if !slices.Equal(in, tc.in) {
				t.Fatalf("coalesceRuns mutated its input: %v, want %v", in, tc.in)
			}
		})
	}
}

func TestPickOwner(t *testing.T) {
	minLiveFile := map[string]string{"s1": "fb", "s2": "fa", "s3": "fc"}
	perms := [][]string{
		{"s1", "s2", "s3"},
		{"s3", "s2", "s1"},
		{"s2", "s1", "s3"},
		{"s3", "s1", "s2"},
	}
	for _, perm := range perms {
		if got := pickOwner(perm, minLiveFile); got != "s2" {
			t.Fatalf("pickOwner(%v) = %q, want %q", perm, got, "s2")
		}
	}
	if got := pickOwner(nil, minLiveFile); got != "" {
		t.Fatalf("pickOwner(nil) = %q, want empty", got)
	}
	// Equal minLiveFile values fall back to the smaller shard hash.
	tied := map[string]string{"sb": "f", "sa": "f"}
	for _, perm := range [][]string{{"sa", "sb"}, {"sb", "sa"}} {
		if got := pickOwner(perm, tied); got != "sa" {
			t.Fatalf("pickOwner(%v) = %q, want %q", perm, got, "sa")
		}
	}
}

// compactTestXorb is one stored multi-chunk fixture xorb.
type compactTestXorb struct {
	hash        xet.XorbHash
	parts       [][]byte
	chunkHashes []xet.ChunkHash
}

// putCompactXorb stores one xorb with one chunk per part.
func putCompactXorb(t *testing.T, ctx context.Context, st Storage, parts [][]byte) compactTestXorb {
	t.Helper()
	var encoded bytes.Buffer
	encoder := xorb.NewEncoder(&encoded, true)
	cx := compactTestXorb{parts: parts}
	for _, part := range parts {
		if _, err := encoder.Write(part); err != nil {
			t.Fatal(err)
		}
		cx.chunkHashes = append(cx.chunkHashes, xet.ComputeChunkHash(part))
	}
	if err := encoder.Close(); err != nil {
		t.Fatal(err)
	}
	cx.hash = encoder.SummoryHash()
	if _, err := st.PutXorb(ctx, "default", cx.hash, bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	return cx
}

// compactTestTerm is one fixture term: chunks [start, end) of one xorb.
type compactTestTerm struct {
	x          compactTestXorb
	start, end uint32
}

// putCompactShard stores one shard with one file per term list and a CAS block per referenced xorb.
func putCompactShard(t *testing.T, ctx context.Context, st Storage, files ...[]compactTestTerm) (string, []xet.FileHash) {
	t.Helper()
	shardObj := shard.NewShard()
	casSeen := map[xet.XorbHash]bool{}
	var fileHashes []xet.FileHash
	for _, terms := range files {
		fb := shard.FileBlock{Flags: shard.FileWithVerification}
		var chunkHashes []xet.ChunkHash
		var chunkSizes []uint64
		for _, term := range terms {
			var segBytes uint32
			for _, part := range term.x.parts[term.start:term.end] {
				segBytes += uint32(len(part))
				chunkSizes = append(chunkSizes, uint64(len(part)))
			}
			chunkHashes = append(chunkHashes, term.x.chunkHashes[term.start:term.end]...)
			fb.Entries = append(fb.Entries, shard.FileDataSequenceEntry{
				CASHash: term.x.hash, UnpackedSegBytes: segBytes,
				ChunkIndexStart: term.start, ChunkIndexEnd: term.end,
			})
			fb.Verification = append(fb.Verification,
				xet.ComputeVerificationHash(term.x.chunkHashes[term.start:term.end]))
			if casSeen[term.x.hash] {
				continue
			}
			casSeen[term.x.hash] = true
			block := shard.CASBlock{CASHash: term.x.hash}
			var offset uint32
			for i, part := range term.x.parts {
				block.Chunks = append(block.Chunks, shard.CASChunkSequenceEntry{
					ChunkHash: term.x.chunkHashes[i], ByteRangeStart: offset, UnpackedSegBytes: uint32(len(part)),
				})
				offset += uint32(len(part))
			}
			block.NumBytesInCAS = offset
			shardObj.AddCASBlock(block)
		}
		fb.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
		fileHashes = append(fileHashes, fb.FileHash)
		shardObj.AddFile(fb)
	}
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	shardHash, err := st.(GCStore).GetFileIndexEntry(ctx, fileHashes[0])
	if err != nil || shardHash == "" {
		t.Fatalf("stored shard hash = %q, %v", shardHash, err)
	}
	return shardHash, fileHashes
}

// sourceHexes lists the planned source xorbs in plan order.
func sourceHexes(plan *compactPlan) []string {
	hexes := make([]string, 0, len(plan.sources))
	for _, src := range plan.sources {
		hexes = append(hexes, src.hash.String())
	}
	return hexes
}

func TestPlanCompactSharedAndPrivateXorbs(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := []byte("chunk shared by both compact files")
			fileA := putGCFile(t, ctx, st, [][]byte{shared, []byte("compact private to A")})
			fileB := putGCFile(t, ctx, st, [][]byte{shared, []byte("compact private to B")})
			if fileA.xorbHashes[0] != fileB.xorbHashes[0] {
				t.Fatal("test setup: shared part must map to one xorb")
			}

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}

			wantSources := []string{
				fileA.xorbHashes[0].String(),
				fileA.xorbHashes[1].String(),
				fileB.xorbHashes[1].String(),
			}
			slices.Sort(wantSources)
			if got := sourceHexes(plan); !slices.Equal(got, wantSources) {
				t.Fatalf("sources = %v, want %v", got, wantSources)
			}

			var wantLive int64
			for _, src := range plan.sources {
				if !slices.Equal(src.runs, []chunkRun{{0, 1}}) {
					t.Fatalf("runs of %s = %v, want [{0 1}]", src.hash.String(), src.runs)
				}
				offs, err := gcs.GetXorbChunkOffsets(ctx, src.hash)
				if err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(src.offsets, offs) {
					t.Fatalf("offsets of %s = %v, want %v", src.hash.String(), src.offsets, offs)
				}
				wantLive += int64(offs[len(offs)-1])
			}

			wantShards := []*gcFile{&fileA, &fileB}
			slices.SortFunc(wantShards, func(a, b *gcFile) int {
				return strings.Compare(a.fileHash.String(), b.fileHash.String())
			})
			if len(plan.shards) != 2 {
				t.Fatalf("planned shards = %d, want 2", len(plan.shards))
			}
			for i, want := range wantShards {
				got := plan.shards[i]
				if got.hash != want.shardHash {
					t.Fatalf("shard[%d].hash = %q, want %q", i, got.hash, want.shardHash)
				}
				if !slices.Equal(got.liveFiles, []string{want.fileHash.String()}) {
					t.Fatalf("shard[%d].liveFiles = %v, want [%s]", i, got.liveFiles, want.fileHash.String())
				}
				if got.minLiveFile != want.fileHash.String() {
					t.Fatalf("shard[%d].minLiveFile = %q, want %q", i, got.minLiveFile, want.fileHash.String())
				}
				if got.shard == nil {
					t.Fatalf("shard[%d].shard not loaded", i)
				}
			}

			// Every source's chunk metadata is known from the CAS blocks.
			wantChunks := map[string]xet.ChunkHash{
				fileA.xorbHashes[0].String(): fileA.chunkHashes[0],
				fileA.xorbHashes[1].String(): fileA.chunkHashes[1],
				fileB.xorbHashes[1].String(): fileB.chunkHashes[1],
			}
			for _, src := range plan.sources {
				known := plan.knownChunks[src.hash]
				if len(known) != 1 || known[0].ChunkHash != wantChunks[src.hash.String()] {
					t.Fatalf("knownChunks[%s] = %v, want one entry with hash %s",
						src.hash.String(), known, wantChunks[src.hash.String()].String())
				}
			}

			res := plan.dryRunResult()
			if !res.DryRun {
				t.Fatal("dryRunResult did not mark DryRun")
			}
			if res.SourceXorbs != 3 || res.LiveRuns != 3 {
				t.Fatalf("SourceXorbs, LiveRuns = %d, %d; want 3, 3", res.SourceXorbs, res.LiveRuns)
			}
			if res.LivePackedBytes != wantLive || res.DeadPackedBytes != 0 {
				t.Fatalf("LivePackedBytes, DeadPackedBytes = %d, %d; want %d, 0", res.LivePackedBytes, res.DeadPackedBytes, wantLive)
			}
			if res.EstimatedNewXorbs != 1 {
				t.Fatalf("EstimatedNewXorbs = %d, want 1", res.EstimatedNewXorbs)
			}
			if len(res.SkippedShards)+len(res.MissingXorbs)+len(res.DanglingFileEntries) != 0 {
				t.Fatalf("skipped/missing/dangling = %v, %v, %v; want all empty", res.SkippedShards, res.MissingXorbs, res.DanglingFileEntries)
			}
			if res.XorbsWritten != 0 || res.XorbBytesWritten != 0 || res.ShardsRewritten != 0 || res.FilesRepointed != 0 {
				t.Fatal("dry-run result has non-zero write-side fields")
			}
		})
	}
}

func TestPlanCompactReportsDanglingFileEntries(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("compact survives a dangling entry")})
			danglingFile := strings.Repeat("ab", 32)
			backend.writeDangling(t, st, danglingFile, strings.Repeat("cd", 32))

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}

			if got, want := plan.danglingFileEntries, []string{danglingFile}; !slices.Equal(got, want) {
				t.Fatalf("danglingFileEntries = %v, want %v", got, want)
			}
			if len(plan.shards) != 1 || plan.shards[0].hash != f.shardHash {
				t.Fatalf("planned shards = %+v, want just %s", plan.shards, f.shardHash)
			}
			if got, want := sourceHexes(plan), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("sources = %v, want %v", got, want)
			}
			res := plan.dryRunResult()
			if got, want := res.DanglingFileEntries, []string{danglingFile}; !slices.Equal(got, want) {
				t.Fatalf("result DanglingFileEntries = %v, want %v", got, want)
			}
			if len(res.SkippedShards) != 0 || len(res.MissingXorbs) != 0 {
				t.Fatalf("SkippedShards, MissingXorbs = %v, %v; want empty", res.SkippedShards, res.MissingXorbs)
			}
		})
	}
}

func TestPlanCompactMissingXorbSkipsShard(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := putCompactXorb(t, ctx, st, [][]byte{
				[]byte("shared chunk zero"), []byte("shared chunk one"), []byte("shared chunk two"),
			})
			privA := putCompactXorb(t, ctx, st, [][]byte{[]byte("private to shard A")})
			privB := putCompactXorb(t, ctx, st, [][]byte{[]byte("private to shard B")})
			shardA, _ := putCompactShard(t, ctx, st, []compactTestTerm{{shared, 0, 2}, {privA, 0, 1}})
			shardB, filesB := putCompactShard(t, ctx, st, []compactTestTerm{{shared, 1, 3}, {privB, 0, 1}})

			if err := gcs.DeleteXorb(ctx, privA.hash); err != nil {
				t.Fatalf("DeleteXorb: %v", err)
			}

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}

			if got, want := plan.missingXorbs, []string{privA.hash.String()}; !slices.Equal(got, want) {
				t.Fatalf("missingXorbs = %v, want %v", got, want)
			}
			if len(plan.skippedShards) != 1 || plan.skippedShards[0].Hash != shardA {
				t.Fatalf("skippedShards = %+v, want just %s", plan.skippedShards, shardA)
			}
			if !strings.Contains(plan.skippedShards[0].Reason, privA.hash.String()) {
				t.Fatalf("skip reason %q does not name the missing xorb", plan.skippedShards[0].Reason)
			}
			if len(plan.shards) != 1 || plan.shards[0].hash != shardB {
				t.Fatalf("planned shards = %+v, want just %s", plan.shards, shardB)
			}
			if got, want := plan.shards[0].minLiveFile, filesB[0].String(); got != want {
				t.Fatalf("minLiveFile = %q, want %q", got, want)
			}

			// Skipped shard A contributes no runs, even on the xorb it shares with B.
			wantSources := []string{shared.hash.String(), privB.hash.String()}
			slices.Sort(wantSources)
			if got := sourceHexes(plan); !slices.Equal(got, wantSources) {
				t.Fatalf("sources = %v, want %v", got, wantSources)
			}
			var sharedSrc, privBSrc *compactSource
			for _, src := range plan.sources {
				switch src.hash {
				case shared.hash:
					sharedSrc = src
				case privB.hash:
					privBSrc = src
				}
			}
			if !slices.Equal(sharedSrc.runs, []chunkRun{{1, 3}}) {
				t.Fatalf("shared runs = %v, want [{1 3}]", sharedSrc.runs)
			}
			if !slices.Equal(privBSrc.runs, []chunkRun{{0, 1}}) {
				t.Fatalf("privB runs = %v, want [{0 1}]", privBSrc.runs)
			}

			res := plan.dryRunResult()
			if len(res.SkippedShards) != 1 || res.SkippedShards[0].Hash != shardA || res.SkippedShards[0].Reason == "" {
				t.Fatalf("result SkippedShards = %+v, want %s with a reason", res.SkippedShards, shardA)
			}
			if got, want := res.MissingXorbs, []string{privA.hash.String()}; !slices.Equal(got, want) {
				t.Fatalf("result MissingXorbs = %v, want %v", got, want)
			}
			wantLive := int64(sharedSrc.offsets[2]-sharedSrc.offsets[0]) + int64(privBSrc.offsets[0])
			wantDead := int64(sharedSrc.offsets[0])
			if res.SourceXorbs != 2 || res.LiveRuns != 2 {
				t.Fatalf("SourceXorbs, LiveRuns = %d, %d; want 2, 2", res.SourceXorbs, res.LiveRuns)
			}
			if res.LivePackedBytes != wantLive || res.DeadPackedBytes != wantDead {
				t.Fatalf("LivePackedBytes, DeadPackedBytes = %d, %d; want %d, %d",
					res.LivePackedBytes, res.DeadPackedBytes, wantLive, wantDead)
			}
		})
	}
}

// compactStoreSnapshot is the stored state compared around a dry run.
type compactStoreSnapshot struct {
	shards map[string]string
	xorbs  map[string]string
	files  map[string]string
	chunks map[string]string
	shas   map[string]string
}

func snapshotCompactStore(t *testing.T, ctx context.Context, gcs GCStore, files []gcFile) compactStoreSnapshot {
	t.Helper()
	snap := compactStoreSnapshot{
		shards: map[string]string{},
		xorbs:  map[string]string{},
		files:  map[string]string{},
		chunks: map[string]string{},
		shas:   map[string]string{},
	}
	err := gcs.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		snap.shards[hash] = fmt.Sprintf("%d@%d", size, modTime.UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = gcs.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		snap.xorbs[hash] = fmt.Sprintf("%d@%d", size, modTime.UnixNano())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = gcs.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		snap.files[fileHash] = shardHash
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		for _, chunkHash := range f.chunkHashes {
			entry, err := gcs.GetChunkIndexEntry(ctx, chunkHash)
			if err != nil {
				t.Fatal(err)
			}
			snap.chunks[chunkHash.String()] = entry
		}
		entry, err := gcs.GetSHA256IndexEntry(ctx, f.sha256Hex)
		if err != nil {
			t.Fatal(err)
		}
		snap.shas[f.sha256Hex] = entry
	}
	return snap
}

func TestPlanCompactPerformsNoWrites(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := []byte("chunk shared for the purity check")
			fileA := putGCFile(t, ctx, st, [][]byte{shared, []byte("purity private to A")})
			fileB := putGCFile(t, ctx, st, [][]byte{shared, []byte("purity private to B")})

			before := snapshotCompactStore(t, ctx, gcs, []gcFile{fileA, fileB})

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}
			_ = plan.dryRunResult()

			after := snapshotCompactStore(t, ctx, gcs, []gcFile{fileA, fileB})
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("store changed across a dry run:\nbefore: %+v\nafter:  %+v", before, after)
			}
		})
	}
}

// Liveness is per shard: an unlinked file's terms still contribute runs until the whole shard dies.
func TestPlanCompactUnlinkedFileTermsStillContribute(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			x := putCompactXorb(t, ctx, st, [][]byte{
				[]byte("kept file chunk zero"), []byte("kept file chunk one"),
				[]byte("unlinked file chunk two"), []byte("unlinked file chunk three"),
			})
			shardHash, files := putCompactShard(t, ctx, st,
				[]compactTestTerm{{x, 0, 2}},
				[]compactTestTerm{{x, 2, 4}},
			)

			if removed, err := Unlink(ctx, gcs, files[1]); err != nil || !removed {
				t.Fatalf("Unlink = %v, %v", removed, err)
			}

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}

			if len(plan.shards) != 1 || plan.shards[0].hash != shardHash {
				t.Fatalf("planned shards = %+v, want just %s", plan.shards, shardHash)
			}
			if got, want := plan.shards[0].liveFiles, []string{files[0].String()}; !slices.Equal(got, want) {
				t.Fatalf("liveFiles = %v, want %v", got, want)
			}
			if got, want := plan.shards[0].minLiveFile, files[0].String(); got != want {
				t.Fatalf("minLiveFile = %q, want %q", got, want)
			}

			// Both files' adjacent terms coalesce into one full-xorb run.
			if len(plan.sources) != 1 || plan.sources[0].hash != x.hash {
				t.Fatalf("sources = %v, want just %s", sourceHexes(plan), x.hash.String())
			}
			if got := plan.sources[0].runs; !slices.Equal(got, []chunkRun{{0, 4}}) {
				t.Fatalf("runs = %v, want [{0 4}]", got)
			}

			res := plan.dryRunResult()
			offs := plan.sources[0].offsets
			if res.LiveRuns != 1 || res.LivePackedBytes != int64(offs[len(offs)-1]) || res.DeadPackedBytes != 0 {
				t.Fatalf("LiveRuns, LivePackedBytes, DeadPackedBytes = %d, %d, %d; want 1, %d, 0",
					res.LiveRuns, res.LivePackedBytes, res.DeadPackedBytes, offs[len(offs)-1])
			}
		})
	}
}

// reconstructCompactFile rebuilds a file's bytes by walking its current shard.
func reconstructCompactFile(t *testing.T, ctx context.Context, st Storage, fileHash xet.FileHash) []byte {
	t.Helper()
	gcs := st.(GCStore)
	sh, err := st.GetShard(ctx, fileHash)
	if err != nil {
		t.Fatalf("GetShard(%s): %v", fileHash.String(), err)
	}
	var fb *shard.FileBlock
	for i := range sh.Files {
		if sh.Files[i].FileHash == fileHash {
			fb = &sh.Files[i]
			break
		}
	}
	if fb == nil {
		t.Fatalf("file %s not in its shard", fileHash.String())
	}
	var content []byte
	readBuf := make([]byte, xet.MaxChunkSize)
	for _, entry := range fb.Entries {
		offsets, err := gcs.GetXorbChunkOffsets(ctx, entry.CASHash)
		if err != nil {
			t.Fatalf("offsets of xorb %s: %v", entry.CASHash.String(), err)
		}
		start, end, err := xorb.ChunkDataRangeFromOffsets(offsets, entry.ChunkIndexStart, entry.ChunkIndexEnd)
		if err != nil {
			t.Fatal(err)
		}
		rsc, err := gcs.GetXorbReadSeekCloser(ctx, "default", entry.CASHash)
		if err != nil {
			t.Fatalf("open xorb %s: %v", entry.CASHash.String(), err)
		}
		if _, err := rsc.Seek(start, io.SeekStart); err != nil {
			t.Fatal(err)
		}
		dec := xorb.NewDecoder(io.LimitReader(rsc, end-start+1), false)
		for idx := entry.ChunkIndexStart; idx < entry.ChunkIndexEnd; idx++ {
			n, err := dec.Read(readBuf)
			if err != nil {
				t.Fatalf("decode xorb %s chunk %d: %v", entry.CASHash.String(), idx, err)
			}
			content = append(content, readBuf[:n]...)
		}
		_ = rsc.Close()
	}
	return content
}

// termsContent concatenates the chunk bytes the terms cover.
func termsContent(terms []compactTestTerm) []byte {
	var content []byte
	for _, term := range terms {
		for _, part := range term.x.parts[term.start:term.end] {
			content = append(content, part...)
		}
	}
	return content
}

// walkXorbSet returns hash -> stored size for every xorb object.
func walkXorbSet(t *testing.T, ctx context.Context, gcs GCStore) map[string]int64 {
	t.Helper()
	xorbs := map[string]int64{}
	err := gcs.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		xorbs[hash] = size
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return xorbs
}

// walkFileEntries returns fileHash -> shardHash for every index/files entry.
func walkFileEntries(t *testing.T, ctx context.Context, gcs GCStore) map[string]string {
	t.Helper()
	entries := map[string]string{}
	err := gcs.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		entries[fileHash] = shardHash
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

// corruptStoredXorb overwrites the stored xorb object in place, bypassing PutXorb validation.
func corruptStoredXorb(t *testing.T, ctx context.Context, st Storage, xorbHash xet.XorbHash, raw []byte) {
	t.Helper()
	switch b := st.(type) {
	case *FileStorage:
		if err := os.WriteFile(b.objectPath("xorbs", xorbHash.String()), raw, 0644); err != nil {
			t.Fatal(err)
		}
	case *S3Storage:
		if err := b.putObject(ctx, b.objectKey("xorbs", xorbHash.String()), bytes.NewReader(raw)); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported backend %T", st)
	}
}

// putFlaggedCompactShard stores one single-file shard: one term over x plus a
// full CAS block carrying the given per-chunk flags.
func putFlaggedCompactShard(t *testing.T, ctx context.Context, st Storage, x compactTestXorb, start, end uint32, flags []shard.ChunkFlags) xet.FileHash {
	t.Helper()
	var segBytes uint32
	var sizes []uint64
	for _, part := range x.parts[start:end] {
		segBytes += uint32(len(part))
		sizes = append(sizes, uint64(len(part)))
	}
	shardObj := shard.NewShard()
	fb := shard.FileBlock{
		FileHash: xet.ComputeFileHash(x.chunkHashes[start:end], sizes),
		Entries: []shard.FileDataSequenceEntry{{
			CASHash: x.hash, UnpackedSegBytes: segBytes,
			ChunkIndexStart: start, ChunkIndexEnd: end,
		}},
	}
	shardObj.AddFile(fb)
	block := shard.CASBlock{CASHash: x.hash}
	var offset uint32
	for i, part := range x.parts {
		block.Chunks = append(block.Chunks, shard.CASChunkSequenceEntry{
			ChunkHash: x.chunkHashes[i], ByteRangeStart: offset,
			UnpackedSegBytes: uint32(len(part)), Flags: flags[i],
		})
		offset += uint32(len(part))
	}
	block.NumBytesInCAS = offset
	shardObj.AddCASBlock(block)
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	return fb.FileHash
}

func TestCompactMergesSmallXorbs(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			fileA := putGCFile(t, ctx, st, [][]byte{[]byte("merge a0"), []byte("merge a1"), []byte("merge a2")})
			fileB := putGCFile(t, ctx, st, [][]byte{[]byte("merge b0"), []byte("merge b1")})
			before := walkXorbSet(t, ctx, gcs)
			if len(before) != 5 {
				t.Fatalf("fixture xorbs = %d, want 5", len(before))
			}

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.DryRun {
				t.Fatal("Compact result marked DryRun")
			}
			if res.XorbsWritten != 1 || res.ShardsRewritten != 2 || res.FilesRepointed != 2 {
				t.Fatalf("XorbsWritten, ShardsRewritten, FilesRepointed = %d, %d, %d; want 1, 2, 2",
					res.XorbsWritten, res.ShardsRewritten, res.FilesRepointed)
			}
			if res.XorbBytesWritten <= 0 {
				t.Fatalf("XorbBytesWritten = %d, want > 0", res.XorbBytesWritten)
			}

			// Compaction only adds: every old xorb survives plus exactly one new one.
			after := walkXorbSet(t, ctx, gcs)
			newXorbs := []string{}
			for hash, size := range after {
				if _, ok := before[hash]; !ok {
					newXorbs = append(newXorbs, hash)
					if size != res.XorbBytesWritten {
						t.Fatalf("new xorb size = %d, want %d", size, res.XorbBytesWritten)
					}
				}
			}
			for hash := range before {
				if _, ok := after[hash]; !ok {
					t.Fatalf("compaction deleted xorb %s", hash)
				}
			}
			if len(newXorbs) != 1 {
				t.Fatalf("new xorbs = %v, want exactly one", newXorbs)
			}

			// All live terms now reference the single new xorb.
			for _, f := range []gcFile{fileA, fileB} {
				sh, err := st.GetShard(ctx, f.fileHash)
				if err != nil {
					t.Fatal(err)
				}
				for _, entry := range sh.Files[0].Entries {
					if entry.CASHash.String() != newXorbs[0] {
						t.Fatalf("term references %s, want %s", entry.CASHash.String(), newXorbs[0])
					}
				}
				if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
					t.Fatalf("file %s reconstructed %q, want %q", f.fileHash.String(), got, f.content)
				}
			}
		})
	}
}

func TestCompactThenSweepReclaimsUnlinkedBytes(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			x := putCompactXorb(t, ctx, st, [][]byte{
				bytes.Repeat([]byte("keep0 "), 200), bytes.Repeat([]byte("keep1 "), 200),
				bytes.Repeat([]byte("drop2 "), 200), bytes.Repeat([]byte("drop3 "), 200),
			})
			keptTerms := []compactTestTerm{{x, 0, 2}}
			_, filesA := putCompactShard(t, ctx, st, keptTerms)
			_, filesB := putCompactShard(t, ctx, st, []compactTestTerm{{x, 2, 4}})

			if removed, err := Unlink(ctx, gcs, filesB[0]); err != nil || !removed {
				t.Fatalf("Unlink = %v, %v", removed, err)
			}
			var beforeBytes int64
			for _, size := range walkXorbSet(t, ctx, gcs) {
				beforeBytes += size
			}

			g := NewGC(gcs)
			if _, err := g.Compact(ctx, false); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if _, err := g.Sweep(ctx, noGrace, false); err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			var afterBytes int64
			for _, size := range walkXorbSet(t, ctx, gcs) {
				afterBytes += size
			}
			if afterBytes >= beforeBytes {
				t.Fatalf("xorb bytes = %d after compact+sweep, want < %d", afterBytes, beforeBytes)
			}
			if got, want := reconstructCompactFile(t, ctx, st, filesA[0]), termsContent(keptTerms); !bytes.Equal(got, want) {
				t.Fatalf("kept file reconstructed %d bytes, want %d", len(got), len(want))
			}
			if entry, err := gcs.GetFileIndexEntry(ctx, filesB[0]); err != nil || entry != "" {
				t.Fatalf("unlinked file entry = %q, %v; want absent", entry, err)
			}
		})
	}
}

func TestCompactTwiceSecondPassIsNoOp(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := []byte("idempotent shared chunk")
			fileA := putGCFile(t, ctx, st, [][]byte{shared, []byte("idempotent a")})
			fileB := putGCFile(t, ctx, st, [][]byte{shared, []byte("idempotent b")})

			g := NewGC(gcs)
			first, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("first Compact: %v", err)
			}
			if first.XorbsWritten != 1 || first.ShardsRewritten != 2 {
				t.Fatalf("first pass XorbsWritten, ShardsRewritten = %d, %d; want 1, 2",
					first.XorbsWritten, first.ShardsRewritten)
			}

			second, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("second Compact: %v", err)
			}
			if second.XorbsWritten != 0 || second.XorbBytesWritten != 0 || second.ShardsRewritten != 0 || second.FilesRepointed != 0 {
				t.Fatalf("second pass wrote: XorbsWritten=%d XorbBytesWritten=%d ShardsRewritten=%d FilesRepointed=%d; want all 0",
					second.XorbsWritten, second.XorbBytesWritten, second.ShardsRewritten, second.FilesRepointed)
			}

			for _, f := range []gcFile{fileA, fileB} {
				if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
					t.Fatalf("file %s reconstructed %q, want %q", f.fileHash.String(), got, f.content)
				}
			}
		})
	}
}

// putBigCompactXorb stores one xorb of 3-byte chunks tagged by tag, optionally
// prefixed with one dead chunk the file term never covers.
func putBigCompactXorb(t *testing.T, ctx context.Context, st Storage, tag byte, liveChunks int, withDead bool) (compactTestXorb, uint32) {
	t.Helper()
	var parts [][]byte
	if withDead {
		parts = append(parts, []byte{tag, 0xff, 0xff})
	}
	for j := 0; j < liveChunks; j++ {
		parts = append(parts, []byte{tag, byte(j >> 8), byte(j)})
	}
	start := uint32(0)
	if withDead {
		start = 1
	}
	return putCompactXorb(t, ctx, st, parts), start
}

// Bin assignment must be order-independent: pass-1 outputs re-enter pass 2 as
// single whole runs in NEW hash order, and re-binning them is churn. The tag
// bytes are chosen so the 8192-chunk source hash sorts between the other two:
// old next-fit then splits the 5000/3000 runs (3 bins instead of 2) and pass 2
// merges them under the new order, so this fails against next-fit packing.
func TestCompactMultiBinSecondPassIsNoOp(t *testing.T) {
	// The pack logic is backend-independent; the ~16k index writes make this file-only.
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	gcs := GCStore(st)

	counts := []int{5000, 8192, 3000}
	dead := []bool{true, false, true}
	terms := make([][]compactTestTerm, 3)
	var files []xet.FileHash
	for i, n := range counts {
		x, start := putBigCompactXorb(t, ctx, st, byte(15+i), n, dead[i])
		terms[i] = []compactTestTerm{{x, start, start + uint32(n)}}
		_, fhs := putCompactShard(t, ctx, st, terms[i])
		files = append(files, fhs[0])
	}

	// The premise the whole test rests on: the un-mergeable 8192 source must
	// sort BETWEEN 3000 and 5000, so next-fit splits the mergeable pair while
	// first-fit reunites it; a payload change reordering the hashes would
	// silently disarm the test.
	h5000, h8192, h3000 := terms[0][0].x.hash.String(), terms[1][0].x.hash.String(), terms[2][0].x.hash.String()
	if !(h3000 < h8192 && h8192 < h5000) {
		t.Fatalf("fixture hash order changed:\n3000=%s\n8192=%s\n5000=%s\nwant 3000 < 8192 < 5000; pick new tags", h3000, h8192, h5000)
	}

	g := NewGC(gcs)
	first, err := g.Compact(ctx, false)
	if err != nil {
		t.Fatalf("first Compact: %v", err)
	}
	// First-fit merges the 5000/3000 runs into one 8000-chunk bin wherever
	// they sort; the fully-live 8192 source reproduces its own hash, so only
	// the merged bin is written (its shard is still rewritten: the fresh CAS
	// block normalizes fields like NumBytesOnDisk).
	if first.EstimatedNewXorbs != 2 {
		t.Fatalf("first pass EstimatedNewXorbs = %d, want 2", first.EstimatedNewXorbs)
	}
	if first.XorbsWritten != 1 || first.ShardsRewritten != 3 || first.FilesRepointed != 3 {
		t.Fatalf("first pass XorbsWritten, ShardsRewritten, FilesRepointed = %d, %d, %d; want 1, 3, 3",
			first.XorbsWritten, first.ShardsRewritten, first.FilesRepointed)
	}

	second, err := g.Compact(ctx, false)
	if err != nil {
		t.Fatalf("second Compact: %v", err)
	}
	if second.XorbsWritten != 0 || second.XorbBytesWritten != 0 || second.ShardsRewritten != 0 || second.FilesRepointed != 0 {
		t.Fatalf("second pass wrote: XorbsWritten=%d XorbBytesWritten=%d ShardsRewritten=%d FilesRepointed=%d; want all 0",
			second.XorbsWritten, second.XorbBytesWritten, second.ShardsRewritten, second.FilesRepointed)
	}
	if second.EstimatedNewXorbs != 2 {
		t.Fatalf("second pass EstimatedNewXorbs = %d, want 2", second.EstimatedNewXorbs)
	}

	for i, fh := range files {
		if got, want := reconstructCompactFile(t, ctx, st, fh), termsContent(terms[i]); !bytes.Equal(got, want) {
			t.Fatalf("file %d reconstructed %d bytes, want %d", i, len(got), len(want))
		}
	}
}

func TestCompactCutoverSkipsMidPassUnlink(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			x := putCompactXorb(t, ctx, st, [][]byte{
				[]byte("kept chunk zero"), []byte("kept chunk one"),
				[]byte("unlinked chunk two"), []byte("unlinked chunk three"),
			})
			keptTerms := []compactTestTerm{{x, 0, 2}}
			oldShard, files := putCompactShard(t, ctx, st, keptTerms, []compactTestTerm{{x, 2, 4}})

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}
			pack, err := packXorbs(ctx, gcs, plan)
			if err != nil {
				t.Fatalf("packXorbs: %v", err)
			}

			// The unlink lands between pack and cutover; the repoint must not resurrect it.
			if removed, err := gcs.DeleteFileIndexEntry(ctx, files[1]); err != nil || !removed {
				t.Fatalf("DeleteFileIndexEntry = %v, %v", removed, err)
			}

			result := &CompactResult{}
			var entryMu sync.Mutex
			if err := cutover(ctx, gcs, &entryMu, plan, pack, result); err != nil {
				t.Fatalf("cutover: %v", err)
			}

			if result.ShardsRewritten != 1 || result.FilesRepointed != 1 {
				t.Fatalf("ShardsRewritten, FilesRepointed = %d, %d; want 1, 1",
					result.ShardsRewritten, result.FilesRepointed)
			}
			if entry, err := gcs.GetFileIndexEntry(ctx, files[1]); err != nil || entry != "" {
				t.Fatalf("unlinked file entry = %q, %v; want still absent", entry, err)
			}
			entry, err := gcs.GetFileIndexEntry(ctx, files[0])
			if err != nil || entry == "" || entry == oldShard {
				t.Fatalf("kept file entry = %q, %v; want a new shard hash", entry, err)
			}
			if got, want := reconstructCompactFile(t, ctx, st, files[0]), termsContent(keptTerms); !bytes.Equal(got, want) {
				t.Fatalf("kept file reconstructed %q, want %q", got, want)
			}
		})
	}
}

func TestCompactSharedNewXorbSingleOwner(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			xa := putCompactXorb(t, ctx, st, [][]byte{[]byte("owner chunk a0"), []byte("owner chunk a1")})
			xb := putCompactXorb(t, ctx, st, [][]byte{[]byte("owner chunk b0")})
			termsA := []compactTestTerm{{xa, 0, 2}}
			termsB := []compactTestTerm{{xb, 0, 1}}
			_, filesA := putCompactShard(t, ctx, st, termsA)
			_, filesB := putCompactShard(t, ctx, st, termsB)

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.XorbsWritten != 1 || res.ShardsRewritten != 2 {
				t.Fatalf("XorbsWritten, ShardsRewritten = %d, %d; want 1, 2", res.XorbsWritten, res.ShardsRewritten)
			}

			shA, err := st.GetShard(ctx, filesA[0])
			if err != nil {
				t.Fatal(err)
			}
			shB, err := st.GetShard(ctx, filesB[0])
			if err != nil {
				t.Fatal(err)
			}
			if err := shA.Validate(); err != nil {
				t.Fatalf("rewritten shard A invalid: %v", err)
			}
			if err := shB.Validate(); err != nil {
				t.Fatalf("rewritten shard B invalid: %v", err)
			}

			// Both shards' terms land in the one shared new xorb.
			newXorb := shA.Files[0].Entries[0].CASHash
			if got := shB.Files[0].Entries[0].CASHash; got != newXorb {
				t.Fatalf("shard B references %s, shard A references %s; want shared", got.String(), newXorb.String())
			}

			// The shard with the smaller min live file owns the single CAS block.
			owner, other := shA, shB
			if filesB[0].String() < filesA[0].String() {
				owner, other = shB, shA
			}
			if len(owner.CASInfos) != 1 || owner.CASInfos[0].CASHash != newXorb {
				t.Fatalf("owner CASInfos = %+v, want one block for %s", owner.CASInfos, newXorb.String())
			}
			if len(owner.CASInfos[0].Chunks) != 3 {
				t.Fatalf("owner CAS block has %d chunks, want 3", len(owner.CASInfos[0].Chunks))
			}
			if len(other.CASInfos) != 0 {
				t.Fatalf("non-owner CASInfos = %+v, want none", other.CASInfos)
			}

			if got, want := reconstructCompactFile(t, ctx, st, filesA[0]), termsContent(termsA); !bytes.Equal(got, want) {
				t.Fatalf("file A reconstructed %q, want %q", got, want)
			}
			if got, want := reconstructCompactFile(t, ctx, st, filesB[0]), termsContent(termsB); !bytes.Equal(got, want) {
				t.Fatalf("file B reconstructed %q, want %q", got, want)
			}
		})
	}
}

func TestCompactPreservesFileMetadata(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts := [][]byte{[]byte("preserve zero"), []byte("preserve one"), []byte("preserve two")}
			x := putCompactXorb(t, ctx, st, parts)

			// Hand-built shard: two terms with verification, dedup flag on chunk 1.
			shardObj := shard.NewShard()
			fb := shard.FileBlock{Flags: shard.FileWithVerification}
			fb.Entries = append(fb.Entries,
				shard.FileDataSequenceEntry{
					CASHash: x.hash, UnpackedSegBytes: uint32(len(parts[0]) + len(parts[1])),
					ChunkIndexStart: 0, ChunkIndexEnd: 2,
				},
				shard.FileDataSequenceEntry{
					CASHash: x.hash, UnpackedSegBytes: uint32(len(parts[2])),
					ChunkIndexStart: 2, ChunkIndexEnd: 3,
				},
			)
			fb.Verification = append(fb.Verification,
				xet.ComputeVerificationHash(x.chunkHashes[0:2]),
				xet.ComputeVerificationHash(x.chunkHashes[2:3]),
			)
			var sizes []uint64
			for _, part := range parts {
				sizes = append(sizes, uint64(len(part)))
			}
			fb.FileHash = xet.ComputeFileHash(x.chunkHashes, sizes)
			shardObj.AddFile(fb)
			block := shard.CASBlock{CASHash: x.hash}
			var offset uint32
			for i, part := range parts {
				var flags shard.ChunkFlags
				if i == 1 {
					flags = shard.ChunkGlobalDedupEligible
				}
				block.Chunks = append(block.Chunks, shard.CASChunkSequenceEntry{
					ChunkHash: x.chunkHashes[i], ByteRangeStart: offset,
					UnpackedSegBytes: uint32(len(part)), Flags: flags,
				})
				offset += uint32(len(part))
			}
			block.NumBytesInCAS = offset
			shardObj.AddCASBlock(block)
			if _, err := st.PutShard(ctx, shardObj); err != nil {
				t.Fatal(err)
			}

			oldShardHash, err := gcs.GetFileIndexEntry(ctx, fb.FileHash)
			if err != nil || oldShardHash == "" {
				t.Fatalf("stored shard hash = %q, %v", oldShardHash, err)
			}
			oldShard, err := gcs.GetShardByHash(ctx, oldShardHash)
			if err != nil {
				t.Fatal(err)
			}

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.ShardsRewritten != 1 {
				t.Fatalf("ShardsRewritten = %d, want 1", res.ShardsRewritten)
			}

			newShard, err := st.GetShard(ctx, fb.FileHash)
			if err != nil {
				t.Fatal(err)
			}
			oldFb, newFb := &oldShard.Files[0], &newShard.Files[0]
			if newFb.Flags != oldFb.Flags {
				t.Fatalf("file flags = %v, want %v", newFb.Flags, oldFb.Flags)
			}
			if !slices.Equal(newFb.Verification, oldFb.Verification) {
				t.Fatalf("verification hashes changed: %v, want %v", newFb.Verification, oldFb.Verification)
			}
			if newFb.MetadataExt == nil || oldFb.MetadataExt == nil || *newFb.MetadataExt != *oldFb.MetadataExt {
				t.Fatalf("MetadataExt = %+v, want %+v", newFb.MetadataExt, oldFb.MetadataExt)
			}

			// The dedup flag follows the chunk to its index in the new owner block.
			if len(newShard.CASInfos) != 1 {
				t.Fatalf("rewritten CASInfos = %d blocks, want 1", len(newShard.CASInfos))
			}
			for i, chunk := range newShard.CASInfos[0].Chunks {
				if chunk.ChunkHash == x.chunkHashes[1] {
					if chunk.Flags&shard.ChunkGlobalDedupEligible == 0 {
						t.Fatalf("chunk %d lost ChunkGlobalDedupEligible", i)
					}
				}
			}
			if got, want := reconstructCompactFile(t, ctx, st, fb.FileHash), termsContent([]compactTestTerm{{x, 0, 3}}); !bytes.Equal(got, want) {
				t.Fatalf("reconstructed %q, want %q", got, want)
			}
		})
	}
}

func TestCompactCorruptChunkAborts(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// High-entropy chunks store uncompressed, so a payload flip decodes fine but hashes differently.
			rng := rand.New(rand.NewSource(1))
			parts := make([][]byte, 3)
			for i := range parts {
				parts[i] = make([]byte, 256)
				rng.Read(parts[i])
			}
			x := putCompactXorb(t, ctx, st, parts)
			putCompactShard(t, ctx, st, []compactTestTerm{{x, 0, 3}})

			rsc, err := gcs.GetXorbReadSeekCloser(ctx, "default", x.hash)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(rsc)
			if err != nil {
				t.Fatal(err)
			}
			_ = rsc.Close()
			if raw[4] != 0 {
				t.Fatalf("first fixture chunk stored with compression %d, want none", raw[4])
			}
			raw[8] ^= 0xff // first payload byte of chunk 0
			corruptStoredXorb(t, ctx, st, x.hash, raw)

			entriesBefore := walkFileEntries(t, ctx, gcs)

			g := NewGC(gcs)
			_, err = g.Compact(ctx, false)
			if err == nil {
				t.Fatal("Compact accepted a corrupted chunk")
			}
			if !strings.Contains(err.Error(), x.hash.String()) || !strings.Contains(err.Error(), "chunk 0") {
				t.Fatalf("error %q does not name xorb %s chunk 0", err, x.hash.String())
			}

			if entriesAfter := walkFileEntries(t, ctx, gcs); !reflect.DeepEqual(entriesBefore, entriesAfter) {
				t.Fatalf("file entries changed across aborted pass:\nbefore: %v\nafter:  %v", entriesBefore, entriesAfter)
			}
			for fileHex, shardHash := range entriesBefore {
				fileHash, err := xet.ParseFileHash(fileHex)
				if err != nil {
					t.Fatal(err)
				}
				sh, err := st.GetShard(ctx, fileHash)
				if err != nil {
					t.Fatalf("GetShard(%s): %v", fileHex, err)
				}
				for _, entry := range sh.Files[0].Entries {
					if entry.CASHash != x.hash {
						t.Fatalf("file %s term moved to %s; want original xorb (shard %s)", fileHex, entry.CASHash.String(), shardHash)
					}
				}
			}
		})
	}
}

func TestCompactMutualExclusionWithSweep(t *testing.T) {
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	putGCFile(t, ctx, st, [][]byte{[]byte("mutual exclusion")})

	g := NewGC(st)
	g.mu.Lock()
	if _, err := g.Compact(ctx, false); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("Compact while busy = %v, want ErrGCBusy", err)
	}
	if _, err := g.Compact(ctx, true); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("dry-run Compact while busy = %v, want ErrGCBusy", err)
	}
	if _, err := g.Sweep(ctx, noGrace, false); !errors.Is(err, ErrGCBusy) {
		t.Fatalf("Sweep while busy = %v, want ErrGCBusy", err)
	}
	g.mu.Unlock()

	if _, err := g.Compact(ctx, false); err != nil {
		t.Fatalf("Compact after unlock: %v", err)
	}
	if _, err := g.Sweep(ctx, noGrace, false); err != nil {
		t.Fatalf("Sweep after unlock: %v", err)
	}
}

func TestCompactGetShardReturnsRewrittenShard(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("evict cache 0"), []byte("evict cache 1")})

			// Warm the file index cache so the cutover must evict it.
			if _, err := st.GetShard(ctx, f.fileHash); err != nil {
				t.Fatal(err)
			}

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.FilesRepointed != 1 {
				t.Fatalf("FilesRepointed = %d, want 1", res.FilesRepointed)
			}

			entry, err := gcs.GetFileIndexEntry(ctx, f.fileHash)
			if err != nil || entry == "" || entry == f.shardHash {
				t.Fatalf("file entry = %q, %v; want a new shard hash", entry, err)
			}
			sh, err := st.GetShard(ctx, f.fileHash)
			if err != nil {
				t.Fatal(err)
			}
			oldXorbs := map[xet.XorbHash]bool{}
			for _, hash := range f.xorbHashes {
				oldXorbs[hash] = true
			}
			for _, term := range sh.Files[0].Entries {
				if oldXorbs[term.CASHash] {
					t.Fatalf("GetShard still returns a term on old xorb %s", term.CASHash.String())
				}
			}
			if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
				t.Fatalf("reconstructed %q, want %q", got, f.content)
			}
		})
	}
}

func TestRemapTermRequiresExactlyOneRun(t *testing.T) {
	src := xet.XorbHash{1}
	newXorb := xet.XorbHash{2}
	placements := map[placementKey]placement{
		{srcXorb: src, start: 2}: {newXorb: newXorb, baseIdx: 7},
	}
	term := shard.FileDataSequenceEntry{CASHash: src, UnpackedSegBytes: 9, ChunkIndexStart: 3, ChunkIndexEnd: 5}

	got, err := remapTerm(term, map[xet.XorbHash][]chunkRun{src: {{2, 6}}}, placements)
	if err != nil {
		t.Fatalf("remapTerm: %v", err)
	}
	want := shard.FileDataSequenceEntry{CASHash: newXorb, UnpackedSegBytes: 9, ChunkIndexStart: 8, ChunkIndexEnd: 10}
	if got != want {
		t.Fatalf("remapTerm = %+v, want %+v", got, want)
	}

	if _, err := remapTerm(term, map[xet.XorbHash][]chunkRun{src: {{0, 4}}}, placements); err == nil {
		t.Fatal("remapTerm accepted a term outside every run")
	}
	// Defensive: planning never produces overlapping runs.
	if _, err := remapTerm(term, map[xet.XorbHash][]chunkRun{src: {{2, 6}, {3, 7}}}, placements); err == nil {
		t.Fatal("remapTerm accepted a term covered by two runs")
	}
}

// Two sources with identical live chunks but different dead chunks repack into
// hash-identical bins; the carried dedup flag must survive the merge.
func TestCompactDuplicateOutputBinsKeepDedupFlags(t *testing.T) {
	// The pack logic is backend-independent; the ~12k index writes make this file-only.
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}

	const half = xet.MaxChunksPerXorb/2 + 1 // two such runs never share a bin
	cs := make([][]byte, half)
	for i := range cs {
		cs[i] = []byte{byte(i >> 8), byte(i)}
	}
	ps := make([][]byte, half-1)
	for j := range ps {
		ps[j] = []byte{byte(j>>8) + 0x20, byte(j)}
	}
	partsA := append([][]byte{{0xff, 0xff, 0x01}}, cs...)
	xa := putCompactXorb(t, ctx, st, partsA) // dead chunk 0, live [1, half+1)
	xb := putCompactXorb(t, ctx, st, cs)     // fully live, same content as xa's live run
	xp := putCompactXorb(t, ctx, st, ps)     // private filler keeping file hashes distinct

	// Shard A's CAS block flags live chunk 5 (xa index 6); shard B's copies carry none.
	flagsA := make([]shard.ChunkFlags, len(partsA))
	flagsA[6] = shard.ChunkGlobalDedupEligible
	fileA := putFlaggedCompactShard(t, ctx, st, xa, 1, uint32(len(partsA)), flagsA)

	termsB := []compactTestTerm{{xb, 0, uint32(len(cs))}, {xp, 0, uint32(len(ps))}}
	_, filesB := putCompactShard(t, ctx, st, termsB)

	g := NewGC(st)
	if _, err := g.Compact(ctx, false); err != nil {
		t.Fatalf("Compact: %v", err)
	}

	shA, err := st.GetShard(ctx, fileA)
	if err != nil {
		t.Fatal(err)
	}
	shB, err := st.GetShard(ctx, filesB[0])
	if err != nil {
		t.Fatal(err)
	}
	sharedXorb := shA.Files[0].Entries[0].CASHash
	if got := shB.Files[0].Entries[0].CASHash; got != sharedXorb {
		t.Fatalf("shards reference %s and %s; want the shared repacked xorb", got.String(), sharedXorb.String())
	}

	var ownerBlock *shard.CASBlock
	for _, sh := range []*shard.Shard{shA, shB} {
		for i := range sh.CASInfos {
			if sh.CASInfos[i].CASHash == sharedXorb {
				if ownerBlock != nil {
					t.Fatal("two CAS blocks describe the shared xorb")
				}
				ownerBlock = &sh.CASInfos[i]
			}
		}
	}
	if ownerBlock == nil {
		t.Fatal("no CAS block describes the shared xorb")
	}
	if got := ownerBlock.Chunks[5].ChunkHash; got != xa.chunkHashes[6] {
		t.Fatalf("owner block chunk 5 = %s, want %s", got.String(), xa.chunkHashes[6].String())
	}
	if ownerBlock.Chunks[5].Flags&shard.ChunkGlobalDedupEligible == 0 {
		t.Fatal("dedup flag lost when hash-identical bins merged")
	}

	if got, want := reconstructCompactFile(t, ctx, st, fileA), bytes.Join(cs, nil); !bytes.Equal(got, want) {
		t.Fatalf("file A reconstructed %d bytes, want %d", len(got), len(want))
	}
	if got, want := reconstructCompactFile(t, ctx, st, filesB[0]), termsContent(termsB); !bytes.Equal(got, want) {
		t.Fatalf("file B reconstructed %d bytes, want %d", len(got), len(want))
	}
}

// Two live shards carrying equal-length copies of one source block, each
// flagging a different chunk: the pass must union the flags, not pick a copy.
func TestCompactMergesDuplicateSourceBlockFlags(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts := [][]byte{[]byte("dup block zero"), []byte("dup block one")}
			x := putCompactXorb(t, ctx, st, parts)

			file1 := putFlaggedCompactShard(t, ctx, st, x, 0, 2, []shard.ChunkFlags{0, shard.ChunkGlobalDedupEligible})
			file2 := putFlaggedCompactShard(t, ctx, st, x, 0, 1, []shard.ChunkFlags{shard.ChunkGlobalDedupEligible, 0})

			g := NewGC(gcs)
			if _, err := g.Compact(ctx, false); err != nil {
				t.Fatalf("Compact: %v", err)
			}

			sh1, err := st.GetShard(ctx, file1)
			if err != nil {
				t.Fatal(err)
			}
			sh2, err := st.GetShard(ctx, file2)
			if err != nil {
				t.Fatal(err)
			}
			newXorb := sh1.Files[0].Entries[0].CASHash

			var ownerBlock *shard.CASBlock
			for _, sh := range []*shard.Shard{sh1, sh2} {
				for i := range sh.CASInfos {
					if sh.CASInfos[i].CASHash == newXorb {
						if ownerBlock != nil {
							t.Fatal("two CAS blocks describe the repacked xorb")
						}
						ownerBlock = &sh.CASInfos[i]
					}
				}
			}
			if ownerBlock == nil {
				t.Fatal("no CAS block describes the repacked xorb")
			}
			for i, chunkHash := range x.chunkHashes {
				flagged := false
				for _, entry := range ownerBlock.Chunks {
					if entry.ChunkHash == chunkHash && entry.Flags&shard.ChunkGlobalDedupEligible != 0 {
						flagged = true
					}
				}
				if !flagged {
					t.Fatalf("chunk %d lost its dedup flag in the merged block", i)
				}
			}
		})
	}
}

// A reader that loaded the old index entry right before the cutover repoint
// must not re-cache it afterwards: the fill is dropped by the generation bump.
func TestCompactStaleCacheFillCannotResurrectOldShard(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("stale fill 0"), []byte("stale fill 1")})

			var gen uint64
			var fill func(xet.FileHash, string, uint64)
			switch b := st.(type) {
			case *FileStorage:
				b.fileMut.Lock()
				gen = b.fileGen
				b.fileMut.Unlock()
				fill = b.fillFileIndex
			case *S3Storage:
				b.fileMut.Lock()
				gen = b.fileGen
				b.fileMut.Unlock()
				fill = b.fillFileIndex
			default:
				t.Fatalf("unsupported backend %T", st)
			}

			g := NewGC(gcs)
			if _, err := g.Compact(ctx, false); err != nil {
				t.Fatalf("Compact: %v", err)
			}
			fill(f.fileHash, f.shardHash, gen)
			if _, err := g.Sweep(ctx, noGrace, false); err != nil {
				t.Fatalf("Sweep: %v", err)
			}

			// A stale cached mapping would resolve to the swept shard and fail here.
			if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
				t.Fatalf("reconstructed %q, want %q", got, f.content)
			}
		})
	}
}

// putPartialCASCompactShard stores one single-file shard over terms, carrying
// CAS blocks only for the xorbs in withCAS (Validate allows CAS-less terms).
func putPartialCASCompactShard(t *testing.T, ctx context.Context, st Storage, terms []compactTestTerm, withCAS map[xet.XorbHash]bool) (string, xet.FileHash) {
	t.Helper()
	shardObj := shard.NewShard()
	fb := shard.FileBlock{Flags: shard.FileWithVerification}
	var chunkHashes []xet.ChunkHash
	var chunkSizes []uint64
	casSeen := map[xet.XorbHash]bool{}
	for _, term := range terms {
		var segBytes uint32
		for _, part := range term.x.parts[term.start:term.end] {
			segBytes += uint32(len(part))
			chunkSizes = append(chunkSizes, uint64(len(part)))
		}
		chunkHashes = append(chunkHashes, term.x.chunkHashes[term.start:term.end]...)
		fb.Entries = append(fb.Entries, shard.FileDataSequenceEntry{
			CASHash: term.x.hash, UnpackedSegBytes: segBytes,
			ChunkIndexStart: term.start, ChunkIndexEnd: term.end,
		})
		fb.Verification = append(fb.Verification,
			xet.ComputeVerificationHash(term.x.chunkHashes[term.start:term.end]))
		if !withCAS[term.x.hash] || casSeen[term.x.hash] {
			continue
		}
		casSeen[term.x.hash] = true
		block := shard.CASBlock{CASHash: term.x.hash}
		var offset uint32
		for i, part := range term.x.parts {
			block.Chunks = append(block.Chunks, shard.CASChunkSequenceEntry{
				ChunkHash: term.x.chunkHashes[i], ByteRangeStart: offset, UnpackedSegBytes: uint32(len(part)),
			})
			offset += uint32(len(part))
		}
		block.NumBytesInCAS = offset
		shardObj.AddCASBlock(block)
	}
	fb.FileHash = xet.ComputeFileHash(chunkHashes, chunkSizes)
	shardObj.AddFile(fb)
	if _, err := st.PutShard(ctx, shardObj); err != nil {
		t.Fatal(err)
	}
	shardHash, err := st.(GCStore).GetFileIndexEntry(ctx, fb.FileHash)
	if err != nil || shardHash == "" {
		t.Fatalf("stored shard hash = %q, %v", shardHash, err)
	}
	return shardHash, fb.FileHash
}

// A xorb whose only CAS-block owner was unlinked (term-only shard still live):
// chunks are verified against the xorb footer hashes and dedup flags recovered
// through the chunk index into the superseded owner shard.
func TestCompactDedupOnlyXorbVerifiesAndRecoversFlags(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			x := putCompactXorb(t, ctx, st, [][]byte{
				[]byte("dedup-only zero"), []byte("dedup-only one"), []byte("dedup-only two"),
			})
			if shard.IsChunkGlobalDedupEligible(x.chunkHashes[1], false, 0) {
				t.Fatal("fixture: chunk 1 must not be hash-eligible, or the test proves nothing")
			}
			p := putCompactXorb(t, ctx, st, [][]byte{[]byte("private to term-only shard")})

			// Owner shard A carries the CAS block flagging chunk 1 (first-chunk rule).
			fileA := putFlaggedCompactShard(t, ctx, st, x, 0, 3,
				[]shard.ChunkFlags{0, shard.ChunkGlobalDedupEligible, 0})
			// Shard B references x by terms only; its CAS block covers just p.
			termsB := []compactTestTerm{{x, 0, 3}, {p, 0, 1}}
			_, fileB := putPartialCASCompactShard(t, ctx, st, termsB, map[xet.XorbHash]bool{p.hash: true})

			// A's shard object outlives the unlink until a sweep.
			if removed, err := Unlink(ctx, gcs, fileA); err != nil || !removed {
				t.Fatalf("Unlink = %v, %v", removed, err)
			}

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.UnverifiedChunks != 0 {
				t.Fatalf("UnverifiedChunks = %d, want 0 (footer hashes cover the xorb)", res.UnverifiedChunks)
			}
			if res.XorbsWritten != 1 || res.ShardsRewritten != 1 {
				t.Fatalf("XorbsWritten, ShardsRewritten = %d, %d; want 1, 1", res.XorbsWritten, res.ShardsRewritten)
			}

			sh, err := st.GetShard(ctx, fileB)
			if err != nil {
				t.Fatal(err)
			}
			flagged := false
			for i := range sh.CASInfos {
				for _, chunk := range sh.CASInfos[i].Chunks {
					if chunk.ChunkHash != x.chunkHashes[1] {
						continue
					}
					flagged = chunk.Flags&shard.ChunkGlobalDedupEligible != 0
				}
			}
			if !flagged {
				t.Fatal("dedup flag not recovered from the superseded owner's CAS entry")
			}
			if got, want := reconstructCompactFile(t, ctx, st, fileB), termsContent(termsB); !bytes.Equal(got, want) {
				t.Fatalf("file B reconstructed %q, want %q", got, want)
			}
		})
	}
}

// A footer-less dedup-only xorb has no expected hashes anywhere: the pass
// reports the chunks as unverified instead of aborting.
func TestCompactFooterlessDedupOnlyXorbCountsUnverified(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			parts := [][]byte{[]byte("footer-less zero"), []byte("footer-less one")}
			var encoded bytes.Buffer
			encoder := xorb.NewEncoder(&encoded, false)
			w := compactTestXorb{parts: parts}
			for _, part := range parts {
				if _, err := encoder.Write(part); err != nil {
					t.Fatal(err)
				}
				w.chunkHashes = append(w.chunkHashes, xet.ComputeChunkHash(part))
			}
			if err := encoder.Close(); err != nil {
				t.Fatal(err)
			}
			w.hash = encoder.SummoryHash()
			if _, err := st.PutXorb(ctx, "default", w.hash, bytes.NewReader(encoded.Bytes())); err != nil {
				t.Fatal(err)
			}

			terms := []compactTestTerm{{w, 0, 2}}
			oldShard, fileC := putPartialCASCompactShard(t, ctx, st, terms, nil)

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if res.UnverifiedChunks != 2 {
				t.Fatalf("UnverifiedChunks = %d, want 2", res.UnverifiedChunks)
			}
			// The repack reproduces w's content-addressed hash (footer-independent),
			// but the rewritten shard gains the owner CAS block.
			if res.XorbsWritten != 0 || res.ShardsRewritten != 1 {
				t.Fatalf("XorbsWritten, ShardsRewritten = %d, %d; want 0, 1", res.XorbsWritten, res.ShardsRewritten)
			}
			if entry, err := gcs.GetFileIndexEntry(ctx, fileC); err != nil || entry == oldShard || entry == "" {
				t.Fatalf("file entry = %q, %v; want a new shard hash", entry, err)
			}
			if got, want := reconstructCompactFile(t, ctx, st, fileC), termsContent(terms); !bytes.Equal(got, want) {
				t.Fatalf("reconstructed %q, want %q", got, want)
			}
		})
	}
}

// A dedup-only xorb whose stored data disagrees with its own footer hashes
// still aborts the pass.
func TestCompactDedupOnlyFooterMismatchAborts(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			// High-entropy chunks store uncompressed, so a payload flip decodes fine but hashes differently.
			rng := rand.New(rand.NewSource(7))
			parts := make([][]byte, 2)
			for i := range parts {
				parts[i] = make([]byte, 256)
				rng.Read(parts[i])
			}
			y := putCompactXorb(t, ctx, st, parts)
			putPartialCASCompactShard(t, ctx, st, []compactTestTerm{{y, 0, 2}}, nil)

			rsc, err := gcs.GetXorbReadSeekCloser(ctx, "default", y.hash)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(rsc)
			if err != nil {
				t.Fatal(err)
			}
			_ = rsc.Close()
			if raw[4] != 0 {
				t.Fatalf("first fixture chunk stored with compression %d, want none", raw[4])
			}
			raw[8] ^= 0xff // first payload byte of chunk 0
			corruptStoredXorb(t, ctx, st, y.hash, raw)

			entriesBefore := walkFileEntries(t, ctx, gcs)
			g := NewGC(gcs)
			_, err = g.Compact(ctx, false)
			if err == nil {
				t.Fatal("Compact accepted a chunk disagreeing with the footer hashes")
			}
			if !strings.Contains(err.Error(), y.hash.String()) || !strings.Contains(err.Error(), "chunk 0") {
				t.Fatalf("error %q does not name xorb %s chunk 0", err, y.hash.String())
			}
			if entriesAfter := walkFileEntries(t, ctx, gcs); !reflect.DeepEqual(entriesBefore, entriesAfter) {
				t.Fatalf("file entries changed across aborted pass:\nbefore: %v\nafter:  %v", entriesBefore, entriesAfter)
			}
		})
	}
}

// A stored shard can carry a term no run can cover (PutShardObject skips
// Validate, and Validate skips terms without a local CAS block anyway): the
// plan must skip that shard with a reason, not fail the pass.
func TestCompactSkipsShardWithInfeasibleTerm(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			f := putGCFile(t, ctx, st, [][]byte{[]byte("feasible zero"), []byte("feasible one")})

			putBadShard := func(term shard.FileDataSequenceEntry, tag string) (string, xet.FileHash) {
				bad := shard.NewShard()
				fileHash := xet.ComputeFileHash(
					[]xet.ChunkHash{xet.ComputeChunkHash([]byte(tag))}, []uint64{9})
				bad.AddFile(shard.FileBlock{FileHash: fileHash, Entries: []shard.FileDataSequenceEntry{term}})
				badHash, err := gcs.PutShardObject(ctx, bad)
				if err != nil {
					t.Fatal(err)
				}
				if err := gcs.SetFileIndexEntry(ctx, fileHash, badHash); err != nil {
					t.Fatal(err)
				}
				return badHash, fileHash
			}
			overHash, overFile := putBadShard(shard.FileDataSequenceEntry{
				CASHash: f.xorbHashes[0], UnpackedSegBytes: 9, ChunkIndexStart: 0, ChunkIndexEnd: 99,
			}, "term beyond chunk count")
			emptyHash, _ := putBadShard(shard.FileDataSequenceEntry{
				CASHash: f.xorbHashes[1], UnpackedSegBytes: 0, ChunkIndexStart: 1, ChunkIndexEnd: 1,
			}, "empty term")

			plan, err := planCompact(ctx, gcs)
			if err != nil {
				t.Fatalf("planCompact: %v", err)
			}
			reasons := map[string]string{}
			for _, s := range plan.skippedShards {
				reasons[s.Hash] = s.Reason
			}
			if len(reasons) != 2 {
				t.Fatalf("skippedShards = %+v, want the two bad shards", plan.skippedShards)
			}
			if !strings.Contains(reasons[overHash], "exceeds its 1 stored chunks") {
				t.Fatalf("out-of-range skip reason = %q", reasons[overHash])
			}
			if !strings.Contains(reasons[emptyHash], "empty term") {
				t.Fatalf("empty-term skip reason = %q", reasons[emptyHash])
			}
			if len(plan.shards) != 1 || plan.shards[0].hash != f.shardHash {
				t.Fatalf("planned shards = %+v, want just %s", plan.shards, f.shardHash)
			}

			g := NewGC(gcs)
			res, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("Compact: %v", err)
			}
			if len(res.SkippedShards) != 2 {
				t.Fatalf("result SkippedShards = %+v, want 2 entries", res.SkippedShards)
			}
			if res.XorbsWritten != 1 || res.ShardsRewritten != 1 {
				t.Fatalf("XorbsWritten, ShardsRewritten = %d, %d; want 1, 1", res.XorbsWritten, res.ShardsRewritten)
			}
			// The skipped shards' file entries are untouched.
			if entry, err := gcs.GetFileIndexEntry(ctx, overFile); err != nil || entry != overHash {
				t.Fatalf("bad shard file entry = %q, %v; want %s", entry, err, overHash)
			}
			if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
				t.Fatalf("reconstructed %q, want %q", got, f.content)
			}
		})
	}
}

func TestOversizedRunReason(t *testing.T) {
	var h xet.XorbHash
	ones := make([]uint32, xet.MaxChunksPerXorb+1)
	for i := range ones {
		ones[i] = 1
	}
	if reason := oversizedRunReason(h, ones, []chunkRun{{0, uint32(len(ones))}}); reason == "" {
		t.Fatal("run over the chunk-count cap not reported")
	}
	big := []uint32{xet.MaxXorbSize / 2, xet.MaxXorbSize / 2, 2}
	if reason := oversizedRunReason(h, big, []chunkRun{{0, 3}}); reason == "" {
		t.Fatal("run over the byte cap not reported")
	}
	// Runs exactly at the caps still fit an empty bin.
	if reason := oversizedRunReason(h, big, []chunkRun{{0, 2}}); reason != "" {
		t.Fatalf("cap-sized run reported oversized: %s", reason)
	}
	if reason := oversizedRunReason(h, ones[:xet.MaxChunksPerXorb], []chunkRun{{0, xet.MaxChunksPerXorb}}); reason != "" {
		t.Fatalf("cap-count run reported oversized: %s", reason)
	}
}

// A concatenation of two footer-less xorbs scans as one object whose unpacked
// payload exceeds what any valid xorb can hold (the scan caps chunk count, not
// bytes); the shard needing that whole run is skipped at plan time instead of
// aborting the pack mid-pass.
func TestCompactSkipsShardNeedingOversizedRun(t *testing.T) {
	// The skip logic is backend-independent; file-only keeps the 64MiB fixture fast.
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	gcs := GCStore(st)

	f := putGCFile(t, ctx, st, [][]byte{[]byte("survives the oversized neighbor")})

	// One max-size chunk over the byte cap; zero chunks keep the stored object tiny.
	const n = xet.MaxXorbSize/xet.MaxChunkSize + 1
	zero := make([]byte, xet.MaxChunkSize)
	var raw bytes.Buffer
	var enc *xorb.Encoder
	for i := 0; i < n; i++ {
		if i%(n/2+1) == 0 {
			if enc != nil {
				if err := enc.Close(); err != nil {
					t.Fatal(err)
				}
			}
			enc = xorb.NewEncoder(&raw, false)
		}
		if _, err := enc.Write(zero); err != nil {
			t.Fatal(err)
		}
	}
	if err := enc.Close(); err != nil {
		t.Fatal(err)
	}
	// PutXorb validates content, so a tiny valid xorb claims the object slot
	// and the oversized raw bytes replace it in place.
	over := putCompactXorb(t, ctx, st, [][]byte{[]byte("oversized placeholder")})
	corruptStoredXorb(t, ctx, st, over.hash, raw.Bytes())

	bad := shard.NewShard()
	badFile := xet.ComputeFileHash(
		[]xet.ChunkHash{xet.ComputeChunkHash([]byte("oversized run"))}, []uint64{9})
	bad.AddFile(shard.FileBlock{FileHash: badFile, Entries: []shard.FileDataSequenceEntry{{
		CASHash: over.hash, UnpackedSegBytes: n * xet.MaxChunkSize, ChunkIndexStart: 0, ChunkIndexEnd: n,
	}}})
	badHash, err := gcs.PutShardObject(ctx, bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := gcs.SetFileIndexEntry(ctx, badFile, badHash); err != nil {
		t.Fatal(err)
	}

	plan, err := planCompact(ctx, gcs)
	if err != nil {
		t.Fatalf("planCompact: %v", err)
	}
	if len(plan.skippedShards) != 1 || plan.skippedShards[0].Hash != badHash ||
		!strings.Contains(plan.skippedShards[0].Reason, "exceeds new-xorb limits") {
		t.Fatalf("skippedShards = %+v, want %s skipped for an oversized run", plan.skippedShards, badHash)
	}
	if len(plan.shards) != 1 || plan.shards[0].hash != f.shardHash {
		t.Fatalf("planned shards = %+v, want just %s", plan.shards, f.shardHash)
	}
	if got, want := sourceHexes(plan), []string{f.xorbHashes[0].String()}; !slices.Equal(got, want) {
		t.Fatalf("sources = %v, want %v", got, want)
	}

	g := NewGC(gcs)
	res, err := g.Compact(ctx, false)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if len(res.SkippedShards) != 1 {
		t.Fatalf("result SkippedShards = %+v, want 1 entry", res.SkippedShards)
	}
	// The fully-live good xorb repacks to its own hash (no write), but its
	// shard gains the fresh owner CAS block and its file is repointed.
	if res.XorbsWritten != 0 || res.ShardsRewritten != 1 || res.FilesRepointed != 1 {
		t.Fatalf("XorbsWritten, ShardsRewritten, FilesRepointed = %d, %d, %d; want 0, 1, 1",
			res.XorbsWritten, res.ShardsRewritten, res.FilesRepointed)
	}
	if entry, err := gcs.GetFileIndexEntry(ctx, f.fileHash); err != nil || entry == f.shardHash || entry == "" {
		t.Fatalf("good file entry = %q, %v; want a new shard hash", entry, err)
	}
	// The skipped shard's file entry and the oversized object are untouched.
	if entry, err := gcs.GetFileIndexEntry(ctx, badFile); err != nil || entry != badHash {
		t.Fatalf("bad shard file entry = %q, %v; want %s", entry, err, badHash)
	}
	if got := reconstructCompactFile(t, ctx, st, f.fileHash); !bytes.Equal(got, f.content) {
		t.Fatalf("reconstructed %q, want %q", got, f.content)
	}
}

// Full lifecycle: dead chunks and the dead shard must strictly shrink stored bytes across compact+sweep, kept files stay byte-identical, and a second compact is a full no-op.
func TestCompactSweepCompactCycleConverges(t *testing.T) {
	for _, backend := range listBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := context.Background()
			st := backend.newStore(t)
			gcs := st.(GCStore)

			shared := putCompactXorb(t, ctx, st, [][]byte{
				bytes.Repeat([]byte("cycle s0 "), 100), bytes.Repeat([]byte("cycle s1 "), 100),
				bytes.Repeat([]byte("cycle s2 "), 100), bytes.Repeat([]byte("cycle s3 "), 100),
			})
			xa := putCompactXorb(t, ctx, st, [][]byte{
				bytes.Repeat([]byte("cycle a0 "), 100), bytes.Repeat([]byte("cycle a1 "), 100),
			})
			xb := putCompactXorb(t, ctx, st, [][]byte{
				bytes.Repeat([]byte("cycle b0 "), 100), bytes.Repeat([]byte("cycle b1 "), 100),
			})

			// Shards 1 and 3 stay fully live; shard 2 loses both files, so shared 2-4 and xb 0-1 go dead.
			keptTerms := [][]compactTestTerm{
				{{shared, 0, 2}},
				{{xa, 0, 2}},
				{{xa, 0, 1}, {xb, 1, 2}},
			}
			_, files1 := putCompactShard(t, ctx, st, keptTerms[0], keptTerms[1])
			_, files2 := putCompactShard(t, ctx, st, []compactTestTerm{{shared, 2, 4}}, []compactTestTerm{{xb, 0, 1}})
			_, files3 := putCompactShard(t, ctx, st, keptTerms[2])
			keptFiles := []xet.FileHash{files1[0], files1[1], files3[0]}

			g := NewGC(gcs)
			for _, fh := range files2 {
				if removed, err := g.Unlink(ctx, fh); err != nil || !removed {
					t.Fatalf("Unlink(%s) = %v, %v", fh.String(), removed, err)
				}
			}

			totalStoredBytes := func() int64 {
				var total int64
				for _, size := range walkXorbSet(t, ctx, gcs) {
					total += size
				}
				err := gcs.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
					total += size
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				return total
			}
			assertKeptFilesIntact := func(step string) {
				t.Helper()
				for i, fh := range keptFiles {
					if got, want := reconstructCompactFile(t, ctx, st, fh), termsContent(keptTerms[i]); !bytes.Equal(got, want) {
						t.Fatalf("%s: kept file %d reconstructed %d bytes, want %d", step, i, len(got), len(want))
					}
				}
			}
			preCompactBytes := totalStoredBytes()

			first, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("first Compact: %v", err)
			}
			if first.XorbsWritten != 1 || first.ShardsRewritten != 2 || first.FilesRepointed != 3 {
				t.Fatalf("first pass XorbsWritten, ShardsRewritten, FilesRepointed = %d, %d, %d; want 1, 2, 3",
					first.XorbsWritten, first.ShardsRewritten, first.FilesRepointed)
			}
			assertKeptFilesIntact("after compact")
			for _, fh := range files2 {
				if entry, err := gcs.GetFileIndexEntry(ctx, fh); err != nil || entry != "" {
					t.Fatalf("unlinked file entry = %q, %v; want absent", entry, err)
				}
			}

			if _, err := g.Sweep(ctx, noGrace, false); err != nil {
				t.Fatalf("Sweep: %v", err)
			}
			if afterBytes := totalStoredBytes(); afterBytes >= preCompactBytes {
				t.Fatalf("stored bytes = %d after compact+sweep, want < %d", afterBytes, preCompactBytes)
			}
			assertKeptFilesIntact("after sweep")

			second, err := g.Compact(ctx, false)
			if err != nil {
				t.Fatalf("second Compact: %v", err)
			}
			if second.XorbsWritten != 0 || second.XorbBytesWritten != 0 || second.ShardsRewritten != 0 || second.FilesRepointed != 0 {
				t.Fatalf("second pass wrote: XorbsWritten=%d XorbBytesWritten=%d ShardsRewritten=%d FilesRepointed=%d; want all 0",
					second.XorbsWritten, second.XorbBytesWritten, second.ShardsRewritten, second.FilesRepointed)
			}
			assertKeptFilesIntact("after second compact")
		})
	}
}

// A dedup-only xorb whose footer hash table was forged (payload intact) must fail the footer-to-xorb-hash binding and abort the pass untouched.
func TestCompactForgedFooterHashTableAborts(t *testing.T) {
	// The binding check is backend-independent; file-only keeps it simple.
	ctx := context.Background()
	st, err := NewFileStorage(WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	gcs := GCStore(st)

	y := putCompactXorb(t, ctx, st, [][]byte{[]byte("forged footer zero"), []byte("forged footer one")})
	putPartialCASCompactShard(t, ctx, st, []compactTestTerm{{y, 0, 2}}, nil)

	rsc, err := gcs.GetXorbReadSeekCloser(ctx, "default", y.hash)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(rsc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rsc.Close()
	// Flip a byte of chunk 0's hash in the footer's XBLBHSH section: footer start + main header (40) + section header (12).
	footerLen := int(binary.LittleEndian.Uint32(raw[len(raw)-4:]))
	footerStart := len(raw) - footerLen - 4
	raw[footerStart+52] ^= 0xff
	corruptStoredXorb(t, ctx, st, y.hash, raw)

	entriesBefore := walkFileEntries(t, ctx, gcs)
	g := NewGC(gcs)
	_, err = g.Compact(ctx, false)
	if err == nil {
		t.Fatal("Compact accepted a forged footer hash table")
	}
	if !strings.Contains(err.Error(), y.hash.String()) || !strings.Contains(err.Error(), "do not reproduce the xorb hash") {
		t.Fatalf("error %q does not report the footer binding failure for xorb %s", err, y.hash.String())
	}
	if entriesAfter := walkFileEntries(t, ctx, gcs); !reflect.DeepEqual(entriesBefore, entriesAfter) {
		t.Fatalf("file entries changed across aborted pass:\nbefore: %v\nafter:  %v", entriesBefore, entriesAfter)
	}
}
