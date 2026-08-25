package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// placementKey identifies one planned run; runs are unique per source by start.
type placementKey struct {
	srcXorb xet.XorbHash
	start   uint32
}

// placement locates a packed run inside the new xorb it landed in.
type placement struct {
	newXorb xet.XorbHash
	baseIdx uint32 // index of the run's first chunk in the new xorb
}

// packedXorb is the chunk metadata of one xorb produced by the pack phase.
type packedXorb struct {
	chunks   []shard.CASChunkSequenceEntry // ordered, cumulative unpacked ByteRangeStart
	unpacked uint32
	packed   uint32 // stored object size, footer included
}

// packResult is what the pack phase hands to the cutover phase. The per-xorb
// chunk metadata (~40B per live chunk, ~320KiB per full 8192-chunk xorb) is
// held until the owning shard's CAS block is durably stored.
type packResult struct {
	placements map[placementKey]placement
	xorbs      map[xet.XorbHash]*packedXorb

	xorbsWritten     int
	xorbBytesWritten int64
	unverifiedChunks int
}

// sourceState is the pack-phase verification state of one source xorb,
// released once its last run is packed.
type sourceState struct {
	known         []shard.CASChunkSequenceEntry
	footerHashes  []xet.ChunkHash
	footerMissing bool
	remainingRuns int
}

// packXorbs is the pack phase: it executes the plan's arithmetic bin
// assignment, decoding and verifying every planned run and storing one fresh
// dense xorb per bin; nothing is repointed until cutover.
func packXorbs(ctx context.Context, st GCStore, plan *compactPlan) (*packResult, error) {
	res := &packResult{
		placements: map[placementKey]placement{},
		xorbs:      map[xet.XorbHash]*packedXorb{},
	}
	states := make(map[xet.XorbHash]*sourceState, len(plan.sources))
	for _, src := range plan.sources {
		states[src.hash] = &sourceState{known: plan.knownChunks[src.hash], remainingRuns: len(src.runs)}
	}
	readBuf := make([]byte, xet.MaxChunkSize)
	for _, bin := range plan.bins {
		if err := packOneBin(ctx, st, plan, bin, states, res, readBuf); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// packOneBin decodes the bin's runs in placement order through one reader per
// source, verifies every chunk, and stores the encoded xorb; the buffered
// payload is bounded by the bin's MaxXorbSize cap.
func packOneBin(ctx context.Context, st GCStore, plan *compactPlan, bin *packBin, states map[xet.XorbHash]*sourceState, res *packResult, readBuf []byte) error {
	var buf bytes.Buffer
	enc := xorb.NewEncoder(&buf, true)
	flags := make([]shard.ChunkFlags, 0, bin.chunks)
	pending := make([]pendingPlacement, 0, len(bin.runs))

	var rsc io.ReadSeekCloser
	var open xet.XorbHash
	closeSource := func() {
		if rsc != nil {
			_ = rsc.Close()
			rsc = nil
		}
	}
	defer closeSource()

	baseIdx := uint32(0)
	for _, br := range bin.runs {
		state := states[br.src.hash]
		if rsc == nil || open != br.src.hash {
			closeSource()
			var err error
			rsc, err = st.GetXorbReadSeekCloser(ctx, "default", br.src.hash)
			if err != nil {
				return fmt.Errorf("open source xorb %s: %w", br.src.hash.String(), err)
			}
			open = br.src.hash
			if err := loadFooterHashes(rsc, br.src, state); err != nil {
				return err
			}
		}
		runFlags, err := decodeRunInto(ctx, st, enc, rsc, br.src, br.run, state, res, readBuf)
		if err != nil {
			return err
		}
		flags = append(flags, runFlags...)
		pending = append(pending, pendingPlacement{
			key:     placementKey{srcXorb: br.src.hash, start: br.run.start},
			baseIdx: baseIdx,
		})
		baseIdx += br.run.end - br.run.start
		state.remainingRuns--
		if state.remainingRuns == 0 {
			// The source is fully consumed: release its verification metadata.
			delete(plan.knownChunks, br.src.hash)
			delete(states, br.src.hash)
		}
	}
	closeSource()

	if err := enc.Close(); err != nil {
		return fmt.Errorf("close packed xorb: %w", err)
	}
	hash := enc.SummoryHash()
	hashes, sizes := enc.ChunkHashes(), enc.ChunkSizes()
	entries := make([]shard.CASChunkSequenceEntry, len(hashes))
	var offset uint32
	for i := range hashes {
		entries[i] = shard.CASChunkSequenceEntry{
			ChunkHash:        hashes[i],
			ByteRangeStart:   offset,
			UnpackedSegBytes: uint32(sizes[i]),
			Flags:            flags[i],
		}
		offset += uint32(sizes[i])
	}
	inserted, err := st.PutXorb(ctx, "default", hash, bytes.NewReader(buf.Bytes()))
	if err != nil {
		return fmt.Errorf("store packed xorb %s: %w", hash.String(), err)
	}
	// Arithmetic bin assignment makes the pass idempotent: an identical
	// re-pack reproduces this content-addressed hash and inserted stays false.
	if inserted {
		res.xorbsWritten++
		res.xorbBytesWritten += int64(buf.Len())
	}
	if prev, ok := res.xorbs[hash]; ok {
		// Two bins with identical content (hence identical chunk lists):
		// keep every carried dedup flag.
		for i := range entries {
			prev.chunks[i].Flags |= entries[i].Flags
		}
	} else {
		res.xorbs[hash] = &packedXorb{chunks: entries, unpacked: offset, packed: uint32(buf.Len())}
	}
	for _, p := range pending {
		res.placements[p.key] = placement{newXorb: hash, baseIdx: p.baseIdx}
	}
	return nil
}

// pendingPlacement is a run already encoded into the open bin, waiting for
// the xorb hash computed on close.
type pendingPlacement struct {
	key     placementKey
	baseIdx uint32
}

// loadFooterHashes loads the source's footer chunk hashes once, only when
// some planned chunk has no recorded CAS entry to verify against.
func loadFooterHashes(rsc io.ReadSeeker, src *compactSource, state *sourceState) error {
	if state.footerHashes != nil || state.footerMissing {
		return nil
	}
	if int(src.runs[len(src.runs)-1].end) <= len(state.known) {
		return nil
	}
	hashes, err := xorb.ReadChunkHashes(rsc)
	if errors.Is(err, xorb.ErrNoFooter) {
		state.footerMissing = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("read footer hashes of xorb %s: %w", src.hash.String(), err)
	}
	if len(hashes) != len(src.sizes) {
		return fmt.Errorf("xorb %s footer: %d chunk hashes, %d stored chunks", src.hash.String(), len(hashes), len(src.sizes))
	}
	// Bind the untrusted footer to the source: its hashes must reproduce the xorb hash.
	sizes64 := make([]uint64, len(src.sizes))
	for i, s := range src.sizes {
		sizes64[i] = uint64(s)
	}
	if xet.ComputeXorbHash(hashes, sizes64) != src.hash {
		return fmt.Errorf("xorb %s: footer chunk hashes do not reproduce the xorb hash", src.hash.String())
	}
	state.footerHashes = hashes
	return nil
}

// decodeRunInto reads chunks [run.start, run.end) of src straight into the
// bin encoder, verifying each against the recorded CAS entry when one is
// known and against the source footer hash otherwise; chunks with neither
// are counted, not rejected.
func decodeRunInto(ctx context.Context, st GCStore, enc *xorb.Encoder, rsc io.ReadSeeker, src *compactSource, run chunkRun, state *sourceState, res *packResult, readBuf []byte) ([]shard.ChunkFlags, error) {
	startByte, endByte, err := xorb.ChunkDataRangeFromOffsets(src.offsets, run.start, run.end)
	if err != nil {
		return nil, fmt.Errorf("xorb %s: %w", src.hash.String(), err)
	}
	if _, err := rsc.Seek(startByte, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek xorb %s: %w", src.hash.String(), err)
	}
	dec := xorb.NewDecoder(io.LimitReader(rsc, endByte-startByte+1), false)
	flags := make([]shard.ChunkFlags, 0, run.end-run.start)
	for idx := run.start; idx < run.end; idx++ {
		n, err := dec.Read(readBuf)
		if err != nil {
			return nil, fmt.Errorf("decode xorb %s chunk %d: %w", src.hash.String(), idx, err)
		}
		chunk := readBuf[:n]
		if uint32(n) != src.sizes[idx] {
			return nil, fmt.Errorf("xorb %s chunk %d: decoded %d bytes, planned %d",
				src.hash.String(), idx, n, src.sizes[idx])
		}
		hash := xet.ComputeChunkHash(chunk)
		var chunkFlags shard.ChunkFlags
		if int(idx) < len(state.known) {
			k := state.known[idx]
			if hash != k.ChunkHash {
				// Pre-cutover abort: the store stays untouched, orphan new xorbs are Sweep's.
				return nil, fmt.Errorf("xorb %s chunk %d: stored data hashes to %s, shard records %s",
					src.hash.String(), idx, hash.String(), k.ChunkHash.String())
			}
			if uint32(n) != k.UnpackedSegBytes {
				return nil, fmt.Errorf("xorb %s chunk %d: decoded %d bytes, shard records %d",
					src.hash.String(), idx, n, k.UnpackedSegBytes)
			}
			chunkFlags = k.Flags
		} else {
			// No live shard describes this chunk (dedup-only xorb).
			if int(idx) < len(state.footerHashes) {
				if hash != state.footerHashes[idx] {
					return nil, fmt.Errorf("xorb %s chunk %d: stored data hashes to %s, footer records %s",
						src.hash.String(), idx, hash.String(), state.footerHashes[idx].String())
				}
			} else {
				res.unverifiedChunks++
			}
			chunkFlags = recoverChunkFlags(ctx, st, hash)
		}
		if _, err := enc.Write(chunk); err != nil {
			return nil, fmt.Errorf("pack chunk %d of xorb %s: %w", idx, src.hash.String(), err)
		}
		flags = append(flags, chunkFlags)
	}
	return flags, nil
}

// recoverChunkFlags best-effort recovers dedup flags for a chunk no live
// shard describes: the chunk index may still name a superseded shard whose
// CAS entry carries them. Every lookup miss or error falls back to the
// hash-based eligibility rule.
func recoverChunkFlags(ctx context.Context, st GCStore, hash xet.ChunkHash) shard.ChunkFlags {
	if shardHash, err := st.GetChunkIndexEntry(ctx, hash); err == nil && shardHash != "" {
		if sh, err := st.GetShardByHash(ctx, shardHash); err == nil {
			for i := range sh.CASInfos {
				for _, c := range sh.CASInfos[i].Chunks {
					if c.ChunkHash == hash {
						return c.Flags
					}
				}
			}
		}
	}
	if shard.IsChunkGlobalDedupEligible(hash, false, 0) {
		return shard.ChunkGlobalDedupEligible
	}
	return 0
}

// cutover is the rewrite phase: every planned shard is rebuilt with terms
// remapped onto the packed xorbs, old CAS blocks dropped, one fresh CAS block
// per owned new xorb attached, then stored and its live files repointed under
// entryMu (one short critical section per file, never resurrecting an entry
// that moved on). Shards are rewritten one at a time; each one's pack
// metadata is released as soon as its CAS blocks are durable.
func cutover(ctx context.Context, st GCStore, entryMu *sync.Mutex, plan *compactPlan, pack *packResult, result *CompactResult) error {
	runsByXorb := make(map[xet.XorbHash][]chunkRun, len(plan.sources))
	for _, src := range plan.sources {
		runsByXorb[src.hash] = src.runs
	}

	// An arithmetic-only remap pass first, so each new xorb's referencing
	// shards are known before CAS-block owners are chosen.
	minLiveFile := make(map[string]string, len(plan.shards))
	refs := map[xet.XorbHash][]string{} // new xorb -> referencing old shard hashes
	for _, cs := range plan.shards {
		minLiveFile[cs.hash] = cs.minLiveFile
		seen := map[xet.XorbHash]bool{}
		for j := range cs.shard.Files {
			fb := &cs.shard.Files[j]
			for _, entry := range fb.Entries {
				ne, err := remapTerm(entry, runsByXorb, pack.placements)
				if err != nil {
					return fmt.Errorf("shard %s file %s: %w", cs.hash, fb.FileHash.String(), err)
				}
				if !seen[ne.CASHash] {
					seen[ne.CASHash] = true
					refs[ne.CASHash] = append(refs[ne.CASHash], cs.hash)
				}
			}
		}
	}
	owned := map[string][]xet.XorbHash{} // owner shard hash -> new xorbs
	for hash, candidates := range refs {
		owner := pickOwner(candidates, minLiveFile)
		owned[owner] = append(owned[owner], hash)
	}

	for _, cs := range plan.shards {
		ns := shard.NewShard()
		for j := range cs.shard.Files {
			fb := &cs.shard.Files[j]
			nfb := shard.FileBlock{
				FileHash:     fb.FileHash,
				Flags:        fb.Flags,
				Entries:      make([]shard.FileDataSequenceEntry, 0, len(fb.Entries)),
				Verification: slices.Clone(fb.Verification),
				MetadataExt:  fb.MetadataExt,
			}
			for _, entry := range fb.Entries {
				ne, err := remapTerm(entry, runsByXorb, pack.placements)
				if err != nil {
					return fmt.Errorf("shard %s file %s: %w", cs.hash, fb.FileHash.String(), err)
				}
				nfb.Entries = append(nfb.Entries, ne)
			}
			ns.AddFile(nfb)
		}
		hashes := owned[cs.hash]
		slices.SortFunc(hashes, func(a, b xet.XorbHash) int {
			return strings.Compare(a.String(), b.String())
		})
		for _, hash := range hashes {
			px := pack.xorbs[hash]
			ns.AddCASBlock(shard.CASBlock{
				CASHash:        hash,
				Chunks:         px.chunks,
				NumBytesInCAS:  px.unpacked,
				NumBytesOnDisk: px.packed,
			})
		}
		if err := ns.Validate(); err != nil {
			return fmt.Errorf("rewritten shard for %s: %w", cs.hash, err)
		}
		newHash, err := st.PutShardObject(ctx, ns)
		if err != nil {
			return fmt.Errorf("store rewritten shard for %s: %w", cs.hash, err)
		}
		// The owned CAS blocks are durable: release their chunk metadata.
		for _, hash := range hashes {
			delete(pack.xorbs, hash)
		}
		if newHash == cs.hash {
			continue
		}
		result.ShardsRewritten++
		for _, fileHex := range cs.liveFiles {
			fileHash, err := xet.ParseFileHash(fileHex)
			if err != nil {
				return fmt.Errorf("live file %q of shard %s: %w", fileHex, cs.hash, err)
			}
			repointed, err := repointFile(ctx, st, entryMu, fileHash, cs.hash, newHash)
			if err != nil {
				return err
			}
			if repointed {
				result.FilesRepointed++
			}
		}
	}
	return nil
}

// remapTerm rewrites one reconstruction term onto the packed xorb holding
// its run; the term must sit fully inside exactly one planned run.
func remapTerm(entry shard.FileDataSequenceEntry, runsByXorb map[xet.XorbHash][]chunkRun, placements map[placementKey]placement) (shard.FileDataSequenceEntry, error) {
	var match chunkRun
	matches := 0
	for _, run := range runsByXorb[entry.CASHash] {
		if entry.ChunkIndexStart >= run.start && entry.ChunkIndexEnd <= run.end {
			match = run
			matches++
		}
	}
	if matches != 1 {
		return entry, fmt.Errorf("term [%d, %d) of xorb %s is covered by %d planned runs, want exactly one",
			entry.ChunkIndexStart, entry.ChunkIndexEnd, entry.CASHash.String(), matches)
	}
	pl, ok := placements[placementKey{srcXorb: entry.CASHash, start: match.start}]
	if !ok {
		return entry, fmt.Errorf("term [%d, %d) of xorb %s: run [%d, %d) was never packed",
			entry.ChunkIndexStart, entry.ChunkIndexEnd, entry.CASHash.String(), match.start, match.end)
	}
	ne := entry
	ne.CASHash = pl.newXorb
	ne.ChunkIndexStart = pl.baseIdx + (entry.ChunkIndexStart - match.start)
	ne.ChunkIndexEnd = ne.ChunkIndexStart + (entry.ChunkIndexEnd - entry.ChunkIndexStart)
	return ne, nil
}

// repointFile flips one index/files entry to the rewritten shard, unless the
// entry moved on (unlinked or re-uploaded mid-pass) — never resurrect.
func repointFile(ctx context.Context, st GCStore, entryMu *sync.Mutex, fileHash xet.FileHash, oldShard, newShard string) (bool, error) {
	entryMu.Lock()
	defer entryMu.Unlock()
	cur, err := st.GetFileIndexEntry(ctx, fileHash)
	if err != nil {
		return false, fmt.Errorf("read file index %s: %w", fileHash.String(), err)
	}
	if cur != oldShard {
		return false, nil
	}
	if err := st.SetFileIndexEntry(ctx, fileHash, newShard); err != nil {
		return false, fmt.Errorf("repoint file %s: %w", fileHash.String(), err)
	}
	return true, nil
}
