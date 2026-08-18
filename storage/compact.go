package storage

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
	"github.com/wzshiming/xet/xorb"
)

// DefaultCompactUtilization is the live-byte ratio below which a xorb is
// repacked when no threshold is given.
const DefaultCompactUtilization = 0.5

// defaultNamespace is the only namespace this storage addresses xorbs in.
const defaultNamespace = "default"

// CompactStore is the storage surface Compact operates on: enumerate files
// and xorbs, read chunks, and swap in rewritten shards and repacked xorbs.
// It deliberately grants no delete operations and no shard-write lock:
// compaction only adds objects and repoints indexes, leaving removal to
// Sweep.
type CompactStore interface {
	Storage

	// GetShardByHash loads a shard by the hash of its serialized bytes.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	// ReplaceShard stores a shard whose files may already be indexed,
	// repointing them at it. Files whose index entries were unlinked since
	// the shard was loaded are left unlinked.
	ReplaceShard(ctx context.Context, s *shard.Shard) (string, error)

	// XorbChunkCount reports how many chunks a stored xorb holds.
	XorbChunkCount(ctx context.Context, xorbHash xet.XorbHash) (uint32, error)

	// TouchXorb bumps the xorb's modification time so grace windows measure
	// from now. Compaction touches superseded xorbs to keep a following
	// sweep from deleting them while reconstruction responses issued before
	// the rewrite still reference them. Missing xorbs are ignored.
	TouchXorb(ctx context.Context, xorbHash string) error

	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string, modTime time.Time) error) error
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error
}

// CompactOptions configures a compaction pass.
type CompactOptions struct {
	// DryRun reports the candidates without writing anything.
	DryRun bool
	// MinUtilization is the ratio of live packed bytes to total packed bytes
	// below which a xorb is repacked. Zero means DefaultCompactUtilization.
	MinUtilization float64
	// Grace keeps xorbs modified within the window untouched, so in-flight
	// uploads are not repacked out from under their pending shard. Zero means
	// DefaultSweepGrace; negative disables the grace window.
	Grace time.Duration
	// MaxXorbs caps how many sparse xorbs one pass repacks; once reached the
	// remaining xorbs are skipped unmeasured. Zero means no cap.
	MaxXorbs int
}

// CompactReport summarizes one compaction pass.
type CompactReport struct {
	DryRun bool `json:"dry_run"`

	ScannedXorbs int   `json:"scanned_xorbs"`
	SparseXorbs  int   `json:"sparse_xorbs"`
	SparseBytes  int64 `json:"sparse_bytes"`
	LiveBytes    int64 `json:"live_bytes"`

	RewrittenShards int   `json:"rewritten_shards"`
	NewXorbs        int   `json:"new_xorbs"`
	NewXorbBytes    int64 `json:"new_xorb_bytes"`
	MovedChunks     int   `json:"moved_chunks"`

	SkippedGrace  int `json:"skipped_grace"`
	SkippedKeyed  int `json:"skipped_keyed_shards"`
	SkippedCapped int `json:"skipped_capped"`
}

// chunkSpan is a half-open chunk index range within one xorb.
type chunkSpan struct {
	start, end uint32
}

// xorbUsage tracks which chunk ranges of a xorb are still reachable.
type xorbUsage struct {
	spans  []chunkSpan
	shards map[string]struct{}
}

// Compact repacks the live chunks of sparsely referenced xorbs into fresh
// dense xorbs and rewrites the shards pointing at them. Chunk hashes, file
// hashes and file SHA-256 digests are unchanged, so clients keep resolving the
// same file ids; the xorb and shard hashes do change, and the superseded
// objects are left for a later Sweep to delete. Superseded xorbs are touched
// so that sweep leaves them one grace window for reconstruction responses
// issued before the rewrite.
//
// Unlike Sweep this never deletes, so it runs without blocking uploads. A
// shard uploaded concurrently may still reference a superseded xorb, which
// only means that xorb stays alive until it too becomes unreferenced. Do not
// run it concurrently with a no-grace Sweep, which could delete freshly
// packed xorbs before their rewritten shard lands; GC serializes the two for
// callers that route through it.
//
// Repacking is per shard: chunks shared by two rewritten shards are copied
// into each shard's new xorbs, so a heavily shared sparse xorb can cost more
// space than it reclaims.
func Compact(ctx context.Context, st CompactStore, opts CompactOptions) (*CompactReport, error) {
	minUtilization := opts.MinUtilization
	if minUtilization == 0 {
		minUtilization = DefaultCompactUtilization
	}
	grace := opts.Grace
	if grace == 0 {
		grace = DefaultSweepGrace
	} else if grace < 0 {
		grace = 0
	}
	cutoff := time.Now().Add(-grace)

	report := &CompactReport{DryRun: opts.DryRun}

	// Every file index entry is a root; its shard names the chunk ranges that
	// must survive. Only span usage is retained; the shards selected for
	// rewriting are reloaded through the backend's bounded cache later.
	liveShards := map[string]struct{}{}
	usage := map[string]*xorbUsage{}
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string, _ time.Time) error {
		if _, ok := liveShards[shardHash]; ok {
			return nil
		}
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if isNotExist(err) {
				return nil // dangling root; Sweep repairs it
			}
			return fmt.Errorf("load shard %s: %w", shardHash, err)
		}
		liveShards[shardHash] = struct{}{}
		for i := range sh.Files {
			for _, entry := range sh.Files[i].Entries {
				key := entry.CASHash.String()
				u := usage[key]
				if u == nil {
					u = &xorbUsage{shards: map[string]struct{}{}}
					usage[key] = u
				}
				u.spans = append(u.spans, chunkSpan{entry.ChunkIndexStart, entry.ChunkIndexEnd})
				u.shards[shardHash] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	// Pick the xorbs whose live bytes no longer justify their size.
	candidates := map[string]struct{}{}
	rewrite := map[string]struct{}{}
	err = st.WalkXorbs(ctx, func(xorbHash string, size int64, modTime time.Time) error {
		u := usage[xorbHash]
		if u == nil || size <= 0 {
			return nil // unreferenced xorbs are Sweep's job
		}
		report.ScannedXorbs++
		if modTime.After(cutoff) {
			report.SkippedGrace++
			return nil
		}
		if opts.MaxXorbs > 0 && len(candidates) >= opts.MaxXorbs {
			report.SkippedCapped++ // skipped unmeasured once the cap is reached
			return nil
		}
		hash, err := xet.ParseXorbHash(xorbHash)
		if err != nil {
			return nil
		}
		chunkCount, err := st.XorbChunkCount(ctx, hash)
		if err != nil {
			return fmt.Errorf("count chunks of xorb %s: %w", xorbHash, err)
		}
		live := mergeSpans(u.spans)
		if len(live) == 1 && live[0].start == 0 && live[0].end >= chunkCount {
			return nil // every chunk is referenced; utilization is 1
		}
		// Utilization is measured against the packed chunk bytes: the footer is
		// part of the object but is rewritten with the xorb, not reclaimed.
		dataBytes, err := spanBytes(ctx, st, hash, []chunkSpan{{0, chunkCount}})
		if err != nil {
			return fmt.Errorf("measure xorb %s: %w", xorbHash, err)
		}
		liveBytes, err := spanBytes(ctx, st, hash, live)
		if err != nil {
			return fmt.Errorf("measure xorb %s: %w", xorbHash, err)
		}
		if dataBytes <= 0 || float64(liveBytes) >= minUtilization*float64(dataBytes) {
			return nil
		}
		report.SparseXorbs++
		report.SparseBytes += size
		report.LiveBytes += liveBytes
		candidates[xorbHash] = struct{}{}
		for shardHash := range u.shards {
			rewrite[shardHash] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	if opts.DryRun || len(candidates) == 0 {
		return report, nil
	}

	for shardHash := range rewrite {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if isNotExist(err) {
				continue // unlinked since the mark phase
			}
			return report, fmt.Errorf("load shard %s: %w", shardHash, err)
		}
		// Keyed shards carry HMAC'd chunk hashes, so their CAS blocks cannot
		// be rebuilt from chunk data.
		if sh.Footer != nil && sh.Footer.IsKeyed() {
			report.SkippedKeyed++
			continue
		}
		if err := rewriteShard(ctx, st, sh, candidates, report); err != nil {
			return report, fmt.Errorf("rewrite shard %s: %w", shardHash, err)
		}
		report.RewrittenShards++
	}

	// The superseded xorbs lost their references just now, but their
	// modification times are old, so a sweep would take them immediately.
	// Touch them so reconstruction responses issued before the rewrite
	// (e.g. presigned URLs) get one grace window to drain.
	for xorbHash := range candidates {
		if err := st.TouchXorb(ctx, xorbHash); err != nil {
			return report, fmt.Errorf("touch superseded xorb %s: %w", xorbHash, err)
		}
	}

	return report, nil
}

// spanBytes sums the on-disk bytes of the given disjoint chunk ranges.
func spanBytes(ctx context.Context, st Storage, xorbHash xet.XorbHash, spans []chunkSpan) (int64, error) {
	var total int64
	for _, span := range spans {
		start, end, err := st.GetXorbDataRange(ctx, defaultNamespace, xorbHash, span.start, span.end)
		if err != nil {
			return 0, err
		}
		total += end - start + 1
	}
	return total, nil
}

// mergeSpans merges overlapping and touching chunk ranges.
func mergeSpans(spans []chunkSpan) []chunkSpan {
	if len(spans) < 2 {
		return spans
	}
	sorted := slices.Clone(spans)
	slices.SortFunc(sorted, func(a, b chunkSpan) int { return cmp.Compare(a.start, b.start) })
	merged := sorted[:1]
	for _, span := range sorted[1:] {
		last := &merged[len(merged)-1]
		if span.start <= last.end {
			if span.end > last.end {
				last.end = span.end
			}
			continue
		}
		merged = append(merged, span)
	}
	return merged
}

// rewriteShard copies the terms that point at candidate xorbs into fresh
// xorbs and stores the resulting shard, repointing the files at it.
func rewriteShard(ctx context.Context, st CompactStore, sh *shard.Shard, candidates map[string]struct{}, report *CompactReport) error {
	packer := &xorbPacker{st: st, report: report}
	defer packer.closeSources()

	rebuilt := shard.NewShard()
	for i := range sh.Files {
		file := sh.Files[i]
		// Fixed capacity: terms are never split, so entry pointers handed to
		// the packer stay valid while the rest of the file is rebuilt.
		entries := make([]shard.FileDataSequenceEntry, 0, len(file.Entries))
		for _, entry := range file.Entries {
			if _, ok := candidates[entry.CASHash.String()]; !ok {
				entries = append(entries, entry)
				continue
			}
			entries = append(entries, entry)
			if err := packer.copyTerm(ctx, entry, &entries[len(entries)-1], len(entries) == 1); err != nil {
				return err
			}
		}
		file.Entries = entries
		rebuilt.AddFile(file)
	}

	if err := packer.flush(ctx); err != nil {
		return err
	}

	for _, casBlock := range sh.CASInfos {
		if _, ok := candidates[casBlock.CASHash.String()]; ok {
			continue
		}
		rebuilt.AddCASBlock(casBlock)
	}
	for _, casBlock := range packer.blocks {
		rebuilt.AddCASBlock(casBlock)
	}

	if _, err := st.ReplaceShard(ctx, rebuilt); err != nil {
		return err
	}
	return nil
}

// xorbPacker appends terms into fresh xorbs, flushing whenever the next term
// would not fit. Terms are never split, so each entry is patched with the
// hash and chunk range of the single xorb that received it.
type xorbPacker struct {
	st     CompactStore
	report *CompactReport

	buf     bytes.Buffer
	encoder *xorb.Encoder
	chunks  []shard.CASChunkSequenceEntry
	pending []*shard.FileDataSequenceEntry
	packed  uint32 // unpacked bytes written to the open xorb

	sources map[string]io.ReadSeekCloser // open source xorbs, reused across terms
	readBuf []byte

	blocks []shard.CASBlock
}

func (p *xorbPacker) copyTerm(ctx context.Context, src shard.FileDataSequenceEntry, dst *shard.FileDataSequenceEntry, firstOfFile bool) error {
	count := src.ChunkIndexEnd - src.ChunkIndexStart
	if p.encoder != nil && (uint64(p.packed)+uint64(src.UnpackedSegBytes) > xet.MaxXorbSize ||
		len(p.chunks)+int(count) > xet.MaxChunksPerXorb) {
		if err := p.flush(ctx); err != nil {
			return err
		}
	}
	if p.encoder == nil {
		p.buf.Reset()
		p.encoder = xorb.NewEncoder(&p.buf, true)
	}

	dst.ChunkIndexStart = uint32(len(p.chunks))
	firstChunk := firstOfFile
	err := p.readChunks(ctx, src.CASHash, src.ChunkIndexStart, src.ChunkIndexEnd, func(data []byte) error {
		if _, err := p.encoder.Write(data); err != nil {
			return err
		}
		chunkHash := xet.ComputeChunkHash(data)
		var flags shard.ChunkFlags
		if shard.IsChunkGlobalDedupEligible(chunkHash, firstChunk, 0) {
			flags = shard.ChunkGlobalDedupEligible
		}
		firstChunk = false
		p.chunks = append(p.chunks, shard.CASChunkSequenceEntry{
			ChunkHash:        chunkHash,
			ByteRangeStart:   p.packed,
			UnpackedSegBytes: uint32(len(data)),
			Flags:            flags,
		})
		p.packed += uint32(len(data))
		p.report.MovedChunks++
		return nil
	})
	if err != nil {
		return err
	}
	dst.ChunkIndexEnd = uint32(len(p.chunks))
	if dst.ChunkIndexEnd-dst.ChunkIndexStart != count {
		return fmt.Errorf("xorb %s term [%d,%d): copied %d chunks", src.CASHash.String(),
			src.ChunkIndexStart, src.ChunkIndexEnd, dst.ChunkIndexEnd-dst.ChunkIndexStart)
	}
	p.pending = append(p.pending, dst)
	return nil
}

// flush finalizes the open xorb, stores it and patches the terms it received.
func (p *xorbPacker) flush(ctx context.Context) error {
	if p.encoder == nil || len(p.chunks) == 0 {
		p.encoder = nil
		return nil
	}
	if err := p.encoder.Close(); err != nil {
		return fmt.Errorf("encode xorb: %w", err)
	}
	xorbHash := p.encoder.SummoryHash()
	if _, err := p.st.PutXorb(ctx, defaultNamespace, xorbHash, bytes.NewReader(p.buf.Bytes())); err != nil {
		return fmt.Errorf("store xorb %s: %w", xorbHash.String(), err)
	}

	p.blocks = append(p.blocks, shard.CASBlock{
		CASHash:        xorbHash,
		Chunks:         p.chunks,
		NumBytesInCAS:  p.packed,
		NumBytesOnDisk: uint32(p.buf.Len()),
	})
	for _, entry := range p.pending {
		entry.CASHash = xorbHash
	}
	p.report.NewXorbs++
	p.report.NewXorbBytes += int64(p.buf.Len())

	p.encoder = nil
	p.chunks = nil
	p.pending = nil
	p.packed = 0
	return nil
}

// source returns an open handle for the given source xorb, reused across
// terms until closeSources.
func (p *xorbPacker) source(ctx context.Context, xorbHash xet.XorbHash) (io.ReadSeekCloser, error) {
	key := xorbHash.String()
	if rsc, ok := p.sources[key]; ok {
		return rsc, nil
	}
	rsc, err := p.st.GetXorbReadSeekCloser(ctx, defaultNamespace, xorbHash)
	if err != nil {
		return nil, err
	}
	if p.sources == nil {
		p.sources = map[string]io.ReadSeekCloser{}
	}
	p.sources[key] = rsc
	return rsc, nil
}

func (p *xorbPacker) closeSources() {
	for _, rsc := range p.sources {
		_ = rsc.Close()
	}
	p.sources = nil
}

// readChunks decodes the chunks of [chunkStart, chunkEnd) one at a time.
func (p *xorbPacker) readChunks(ctx context.Context, xorbHash xet.XorbHash, chunkStart, chunkEnd uint32, fn func(data []byte) error) error {
	start, end, err := p.st.GetXorbDataRange(ctx, defaultNamespace, xorbHash, chunkStart, chunkEnd)
	if err != nil {
		return fmt.Errorf("locate xorb chunks: %w", err)
	}
	rsc, err := p.source(ctx, xorbHash)
	if err != nil {
		return err
	}
	if _, err := rsc.Seek(start, io.SeekStart); err != nil {
		return err
	}
	if p.readBuf == nil {
		p.readBuf = make([]byte, xet.MaxChunkSize)
	}

	decoder := xorb.NewDecoder(io.LimitReader(rsc, end-start+1), false)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := decoder.Read(p.readBuf)
		if n > 0 {
			if err := fn(p.readBuf[:n]); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("decode xorb chunks: %w", err)
		}
	}
}
