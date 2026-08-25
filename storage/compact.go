package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"slices"
	"strings"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// CompactResult reports one Compact pass; write-side fields stay zero until the packing stage.
type CompactResult struct {
	DryRun bool `json:"dry_run"`

	SourceXorbs     int   `json:"source_xorbs"`
	LiveRuns        int   `json:"live_runs"`
	LivePackedBytes int64 `json:"live_packed_bytes"`
	DeadPackedBytes int64 `json:"dead_packed_bytes"`
	// EstimatedNewXorbs is distinct packed xorbs; dry runs report the planned bin count, an upper bound (duplicate-content bins collapse).
	EstimatedNewXorbs int `json:"estimated_new_xorbs"`

	// SkippedShards are live shards left untouched, each with the reason.
	SkippedShards []SkippedShard `json:"skipped_shards"`
	// MissingXorbs are term-referenced xorbs whose object is gone.
	MissingXorbs []string `json:"missing_xorbs"`
	// DanglingFileEntries are index/files entries whose shard object is missing; reported, never deleted.
	DanglingFileEntries []string `json:"dangling_file_entries"`

	XorbsWritten     int   `json:"xorbs_written"`
	XorbBytesWritten int64 `json:"xorb_bytes_written"`
	ShardsRewritten  int   `json:"shards_rewritten"`
	FilesRepointed   int   `json:"files_repointed"`
	// UnverifiedChunks counts repacked chunks with no recorded CAS entry or
	// footer hash to verify against.
	UnverifiedChunks int `json:"unverified_chunks"`
}

// SkippedShard is one live shard the pass left untouched.
type SkippedShard struct {
	Hash   string `json:"hash"`
	Reason string `json:"reason"`
}

// chunkRun is a run of live chunks [start, end) within one source xorb.
type chunkRun struct {
	start, end uint32
}

// compactSource is one stored xorb live chunks will be compacted out of.
type compactSource struct {
	hash    xet.XorbHash
	offsets []uint64   // cumulative packed end offset per chunk
	sizes   []uint32   // unpacked byte size per chunk
	runs    []chunkRun // maximal disjoint runs, sorted by start
}

// compactShard is one live shard the pass will rewrite.
type compactShard struct {
	hash        string // stored shard hash
	shard       *shard.Shard
	liveFiles   []string // hex file hashes pointing at the shard, sorted
	minLiveFile string   // smallest live file hash; stable across passes
}

// binRun is one planned run placed in an output bin.
type binRun struct {
	src *compactSource
	run chunkRun
}

// packBin is one planned output xorb: its runs in placement order.
type packBin struct {
	runs     []binRun
	unpacked uint64
	chunks   uint32
}

// compactStats are the dry-run size estimates derived from the sources.
type compactStats struct {
	liveRuns          int
	livePackedBytes   int64
	deadPackedBytes   int64
	estimatedNewXorbs int
}

// compactPlan is the read-only planning output the packing stage consumes.
type compactPlan struct {
	sources []*compactSource // sorted by xorb hash
	shards  []*compactShard  // sorted by minLiveFile
	bins    []*packBin       // complete arithmetic bin assignment of all runs

	// knownChunks is chunk metadata per (xorb, chunk index) for hash checks and dedup-flag carryover.
	knownChunks map[xet.XorbHash][]shard.CASChunkSequenceEntry

	danglingFileEntries []string
	missingXorbs        []string
	skippedShards       []SkippedShard

	stats compactStats
}

// planCompact builds the read-only pass plan: live shards, per-xorb runs, dry-run stats.
func planCompact(ctx context.Context, st GCStore) (*compactPlan, error) {
	plan := &compactPlan{
		knownChunks:         map[xet.XorbHash][]shard.CASChunkSequenceEntry{},
		danglingFileEntries: []string{},
		missingXorbs:        []string{},
		skippedShards:       []SkippedShard{},
	}

	liveFiles := map[string][]string{} // shard hash -> file hashes
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		liveFiles[shardHash] = append(liveFiles[shardHash], fileHash)
		return nil
	})
	if err != nil {
		return nil, err
	}

	shardHashes := make([]string, 0, len(liveFiles))
	for shardHash := range liveFiles {
		shardHashes = append(shardHashes, shardHash)
	}
	slices.Sort(shardHashes)

	loaded := make([]*compactShard, 0, len(shardHashes))
	for _, shardHash := range shardHashes {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				plan.danglingFileEntries = append(plan.danglingFileEntries, liveFiles[shardHash]...)
				continue
			}
			return nil, fmt.Errorf("load live shard %s: %w", shardHash, err)
		}
		files := slices.Clone(liveFiles[shardHash])
		slices.Sort(files)
		loaded = append(loaded, &compactShard{hash: shardHash, shard: sh, liveFiles: files, minLiveFile: files[0]})
	}
	slices.Sort(plan.danglingFileEntries)

	// The offsets fetch doubles as the existence check for source xorbs.
	termXorbs := sortedTermXorbs(loaded)
	offsets := map[xet.XorbHash][]uint64{}
	missing := map[xet.XorbHash]bool{}
	for _, hash := range termXorbs {
		offs, err := st.GetXorbChunkOffsets(ctx, hash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				missing[hash] = true
				plan.missingXorbs = append(plan.missingXorbs, hash.String())
				continue
			}
			return nil, fmt.Errorf("chunk offsets of xorb %s: %w", hash.String(), err)
		}
		offsets[hash] = offs
	}

	// Liveness is per shard: ALL file blocks' terms count, even ones whose file was unlinked.
	feasible := make([]*compactShard, 0, len(loaded))
	for _, cs := range loaded {
		if hash, ok := termReferencesMissing(cs.shard, missing); ok {
			plan.skippedShards = append(plan.skippedShards, SkippedShard{
				Hash:   cs.hash,
				Reason: "references missing xorb " + hash.String(),
			})
			continue
		}
		// A stored shard can carry an un-remappable term (Validate skips terms
		// without a local CAS block); skip it instead of failing the pass.
		if reason := infeasibleTerm(cs.shard, offsets); reason != "" {
			plan.skippedShards = append(plan.skippedShards, SkippedShard{Hash: cs.hash, Reason: reason})
			continue
		}
		feasible = append(feasible, cs)
	}

	// infeasibleTerm bounded every term, so coalesced runs end within the stored chunks.
	ranges := termRanges(feasible)
	sizes := map[xet.XorbHash][]uint32{}
	runsByXorb := map[xet.XorbHash][]chunkRun{}
	oversized := map[xet.XorbHash]string{}
	for _, hash := range termXorbs {
		runs := coalesceRuns(ranges[hash])
		if len(runs) == 0 {
			continue
		}
		sz, err := st.GetXorbChunkUnpackedSizes(ctx, hash)
		if err != nil {
			return nil, fmt.Errorf("chunk sizes of xorb %s: %w", hash.String(), err)
		}
		if len(sz) != len(offsets[hash]) {
			return nil, fmt.Errorf("xorb %s: %d chunk sizes but %d offsets", hash.String(), len(sz), len(offsets[hash]))
		}
		sizes[hash] = sz
		runsByXorb[hash] = runs
		if reason := oversizedRunReason(hash, sz, runs); reason != "" {
			oversized[hash] = reason
		}
	}
	// An over-cap run cannot come from a valid stored xorb and could never be
	// packed whole: shards whose terms need such a source are skipped like any
	// other infeasibility. Dropping shards only shrinks runs, so once is enough.
	if len(oversized) > 0 {
		kept := feasible[:0]
		for _, cs := range feasible {
			if reason := termReferencesOversized(cs.shard, oversized); reason != "" {
				plan.skippedShards = append(plan.skippedShards, SkippedShard{Hash: cs.hash, Reason: reason})
				continue
			}
			kept = append(kept, cs)
		}
		feasible = kept
		runsByXorb = map[xet.XorbHash][]chunkRun{}
		for hash, rs := range termRanges(feasible) {
			if runs := coalesceRuns(rs); len(runs) > 0 {
				runsByXorb[hash] = runs
			}
		}
	}
	plan.shards = feasible
	slices.SortFunc(plan.shards, func(a, b *compactShard) int {
		return strings.Compare(a.minLiveFile, b.minLiveFile)
	})

	// Xorbs referenced only by skipped shards get no runs and stay as-is.
	for _, hash := range termXorbs {
		runs := runsByXorb[hash]
		if len(runs) == 0 {
			continue
		}
		plan.sources = append(plan.sources, &compactSource{hash: hash, offsets: offsets[hash], sizes: sizes[hash], runs: runs})
	}
	plan.bins = assignBins(plan.sources)

	// Chunk metadata comes only from participating shards, and only for the
	// xorbs this pass repacks; duplicate CAS blocks merge (longest chunk list
	// wins, dedup flags accumulate).
	isSource := make(map[xet.XorbHash]bool, len(plan.sources))
	for _, src := range plan.sources {
		isSource[src.hash] = true
	}
	for _, cs := range plan.shards {
		for i := range cs.shard.CASInfos {
			block := &cs.shard.CASInfos[i]
			if !isSource[block.CASHash] {
				continue
			}
			merged, err := mergeKnownChunks(block.CASHash, plan.knownChunks[block.CASHash], block.Chunks)
			if err != nil {
				return nil, fmt.Errorf("shard %s: %w", cs.hash, err)
			}
			plan.knownChunks[block.CASHash] = merged
		}
	}

	plan.stats = computeCompactStats(plan.sources)
	plan.stats.estimatedNewXorbs = len(plan.bins)
	return plan, nil
}

// dryRunResult reports the plan as a dry-run CompactResult.
func (p *compactPlan) dryRunResult() *CompactResult {
	res := &CompactResult{
		DryRun:              true,
		SourceXorbs:         len(p.sources),
		LiveRuns:            p.stats.liveRuns,
		LivePackedBytes:     p.stats.livePackedBytes,
		DeadPackedBytes:     p.stats.deadPackedBytes,
		EstimatedNewXorbs:   p.stats.estimatedNewXorbs,
		SkippedShards:       append(make([]SkippedShard, 0, len(p.skippedShards)), p.skippedShards...),
		MissingXorbs:        append(make([]string, 0, len(p.missingXorbs)), p.missingXorbs...),
		DanglingFileEntries: append(make([]string, 0, len(p.danglingFileEntries)), p.danglingFileEntries...),
	}
	return res
}

// sortedTermXorbs returns every xorb referenced by the shards' terms once, sorted by hash.
func sortedTermXorbs(shards []*compactShard) []xet.XorbHash {
	seen := map[xet.XorbHash]bool{}
	hashes := []xet.XorbHash{}
	for _, cs := range shards {
		for i := range cs.shard.Files {
			for _, entry := range cs.shard.Files[i].Entries {
				if !seen[entry.CASHash] {
					seen[entry.CASHash] = true
					hashes = append(hashes, entry.CASHash)
				}
			}
		}
	}
	slices.SortFunc(hashes, func(a, b xet.XorbHash) int {
		return strings.Compare(a.String(), b.String())
	})
	return hashes
}

// termReferencesMissing reports the first missing xorb among the shard's terms.
func termReferencesMissing(sh *shard.Shard, missing map[xet.XorbHash]bool) (xet.XorbHash, bool) {
	for i := range sh.Files {
		for _, entry := range sh.Files[i].Entries {
			if missing[entry.CASHash] {
				return entry.CASHash, true
			}
		}
	}
	return xet.XorbHash{}, false
}

// termRanges collects every term's [start, end) chunk range per referenced xorb.
func termRanges(shards []*compactShard) map[xet.XorbHash][]chunkRun {
	ranges := map[xet.XorbHash][]chunkRun{}
	for _, cs := range shards {
		for i := range cs.shard.Files {
			for _, entry := range cs.shard.Files[i].Entries {
				ranges[entry.CASHash] = append(ranges[entry.CASHash], chunkRun{start: entry.ChunkIndexStart, end: entry.ChunkIndexEnd})
			}
		}
	}
	return ranges
}

// oversizedRunReason returns why some run of the xorb can never fit an empty
// new xorb, or "" when every run is within the caps.
func oversizedRunReason(hash xet.XorbHash, sizes []uint32, runs []chunkRun) string {
	for _, r := range runs {
		if r.end-r.start > xet.MaxChunksPerXorb || runUnpackedBytes(sizes, r) > xet.MaxXorbSize {
			return fmt.Sprintf("live run [%d, %d) of xorb %s exceeds new-xorb limits", r.start, r.end, hash.String())
		}
	}
	return ""
}

// termReferencesOversized returns the skip reason when some term of the shard
// needs a xorb with an over-cap run.
func termReferencesOversized(sh *shard.Shard, oversized map[xet.XorbHash]string) string {
	for i := range sh.Files {
		for _, entry := range sh.Files[i].Entries {
			if reason, ok := oversized[entry.CASHash]; ok {
				return reason
			}
		}
	}
	return ""
}

// infeasibleTerm returns why some term of the shard cannot be remapped: empty
// or reaching past its xorb's stored chunks. Every surviving term then sits
// inside exactly one coalesced run by construction.
func infeasibleTerm(sh *shard.Shard, offsets map[xet.XorbHash][]uint64) string {
	for i := range sh.Files {
		for _, entry := range sh.Files[i].Entries {
			if entry.ChunkIndexEnd <= entry.ChunkIndexStart {
				return fmt.Sprintf("empty term [%d, %d) of xorb %s",
					entry.ChunkIndexStart, entry.ChunkIndexEnd, entry.CASHash.String())
			}
			if n := len(offsets[entry.CASHash]); int(entry.ChunkIndexEnd) > n {
				return fmt.Sprintf("term [%d, %d) of xorb %s exceeds its %d stored chunks",
					entry.ChunkIndexStart, entry.ChunkIndexEnd, entry.CASHash.String(), n)
			}
		}
	}
	return ""
}

// coalesceRuns merges overlapping and adjacent [start, end) ranges into sorted maximal disjoint runs.
func coalesceRuns(ranges []chunkRun) []chunkRun {
	sorted := make([]chunkRun, 0, len(ranges))
	for _, r := range ranges {
		if r.end > r.start {
			sorted = append(sorted, r)
		}
	}
	slices.SortFunc(sorted, func(a, b chunkRun) int {
		if a.start != b.start {
			return cmp.Compare(a.start, b.start)
		}
		return cmp.Compare(a.end, b.end)
	})
	runs := make([]chunkRun, 0, len(sorted))
	for _, r := range sorted {
		if n := len(runs); n > 0 && r.start <= runs[n-1].end {
			runs[n-1].end = max(runs[n-1].end, r.end)
			continue
		}
		runs = append(runs, r)
	}
	return runs
}

// pickOwner returns the candidate shard with the smallest minLiveFile: the pass-stable CAS-block owner.
func pickOwner(candidates []string, minLiveFile map[string]string) string {
	owner := ""
	for _, hash := range candidates {
		if owner == "" {
			owner = hash
			continue
		}
		a, b := minLiveFile[hash], minLiveFile[owner]
		if a < b || (a == b && hash < owner) {
			owner = hash
		}
	}
	return owner
}

// mergeKnownChunks merges one CAS block into a xorb's known chunk metadata:
// the longer list wins, flags accumulate, hash or size disagreement is corruption.
func mergeKnownChunks(hash xet.XorbHash, known, incoming []shard.CASChunkSequenceEntry) ([]shard.CASChunkSequenceEntry, error) {
	longer, shorter := known, incoming
	if len(incoming) > len(known) {
		longer, shorter = incoming, known
	}
	merged := slices.Clone(longer)
	for i := range shorter {
		if merged[i].ChunkHash != shorter[i].ChunkHash || merged[i].UnpackedSegBytes != shorter[i].UnpackedSegBytes {
			return nil, fmt.Errorf("xorb %s chunk %d: live shards disagree on chunk metadata", hash.String(), i)
		}
		merged[i].Flags |= shorter[i].Flags
	}
	return merged, nil
}

// runPackedBytes returns the stored bytes of chunks [start, end) from cumulative packed end offsets.
func runPackedBytes(offsets []uint64, r chunkRun) uint64 {
	var startOff uint64
	if r.start > 0 {
		startOff = offsets[r.start-1]
	}
	return offsets[r.end-1] - startOff
}

// runUnpackedBytes returns the uncompressed bytes of chunks [start, end).
func runUnpackedBytes(sizes []uint32, r chunkRun) uint64 {
	var total uint64
	for _, s := range sizes[r.start:r.end] {
		total += uint64(s)
	}
	return total
}

// assignBins places every run into output bins by pure arithmetic, first-fit
// over all open bins in canonical plan order (source hash asc, run start asc).
// First-fit final bins are pairwise unmergeable in unpacked bytes or chunk
// count, so re-binning a pass's own outputs (each one whole run) reproduces
// every bin verbatim: a second pass is a no-op.
// Planning skips over-cap runs, so every run here fits an empty bin.
func assignBins(sources []*compactSource) []*packBin {
	bins := []*packBin{}
	for _, src := range sources {
		for _, r := range src.runs {
			runBytes := runUnpackedBytes(src.sizes, r)
			runChunks := r.end - r.start
			var bin *packBin
			for _, b := range bins {
				if b.unpacked+runBytes <= xet.MaxXorbSize && b.chunks+runChunks <= xet.MaxChunksPerXorb {
					bin = b
					break
				}
			}
			if bin == nil {
				bin = &packBin{}
				bins = append(bins, bin)
			}
			bin.runs = append(bin.runs, binRun{src: src, run: r})
			bin.unpacked += runBytes
			bin.chunks += runChunks
		}
	}
	return bins
}

// computeCompactStats aggregates the dry-run stats over the planned sources.
func computeCompactStats(sources []*compactSource) compactStats {
	var stats compactStats
	for _, src := range sources {
		total := int64(src.offsets[len(src.offsets)-1])
		var live int64
		for _, r := range src.runs {
			live += int64(runPackedBytes(src.offsets, r))
		}
		stats.liveRuns += len(src.runs)
		stats.livePackedBytes += live
		stats.deadPackedBytes += total - live
	}
	return stats
}
