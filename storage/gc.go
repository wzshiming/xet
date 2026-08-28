package storage

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"slices"
	"sync"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// DefaultSweepGrace is how recently an object must have been written for
// Sweep to leave it alone even when unreferenced. PutShard commits the
// index/files entry last, so an upload that has stored its shard and xorbs
// but not yet committed looks unreferenced; the grace window keeps such
// in-flight uploads out of a concurrent sweep's reach. Dedup hits do not
// refresh object timestamps, so reused old xorbs are protected by the
// re-checks against the file index and stored shards, not by the window;
// reused shards by the per-shard file-entry re-read right before deletion.
const DefaultSweepGrace = time.Hour

// ErrGCBusy is returned when a sweep is already running on the same GC.
var ErrGCBusy = errors.New("gc already running")

// GCStore is the bare per-backend surface needed to unlink files and sweep
// unreferenced shards and xorbs; the orchestration lives in Unlink and Sweep.
// The bare backends (*FileStorage, *S3Storage) implement it directly.
type GCStore interface {
	ListStore

	// WalkShards calls fn for every stored shard object.
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error
	// WalkXorbs calls fn for every stored xorb object.
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error

	// GetFileIndexEntry returns the shard hash recorded for fileHash, or
	// "" when the entry is absent, bypassing caches.
	GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error)
	// DeleteFileIndexEntry removes the index/files entry for fileHash,
	// reporting whether it existed.
	DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error)
	// DeleteShard removes a stored shard object.
	DeleteShard(ctx context.Context, shardHash string) error
	// DeleteXorb removes a stored xorb object.
	DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) error

	// GetChunkIndexEntry returns the shard hash recorded for chunkHash, or
	// "" when the entry is absent, bypassing caches.
	GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error)
	// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
	DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) error

	// GetSHA256IndexEntry returns the shard hash recorded for the hex
	// SHA-256 digest, or "" when the entry is absent, bypassing caches.
	GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error)
	// DeleteSHA256IndexEntry removes the index/sha256 entry.
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) error

	// SetChunkIndexEntry force-writes the index/chunks entry for chunkHash.
	SetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) error
	// SetSHA256IndexEntry force-writes the index/sha256 entry.
	SetSHA256IndexEntry(ctx context.Context, sha256Hex string, shardHash string) error
}

// Unlink removes the file-index entry for fileHash, reporting whether it
// existed. Only the index/files entry is touched: the shard, its xorbs, and
// the chunk/sha256 index entries may all serve other live files, so they
// stay until a Sweep proves the shard unreferenced. Until then the content
// remains reconstructable through its SHA-256 whenever another file keeps
// the shard alive.
func Unlink(ctx context.Context, st GCStore, fileHash xet.FileHash) (bool, error) {
	return st.DeleteFileIndexEntry(ctx, fileHash)
}

// SweptObject is one removed (or, in a dry run, removable) stored object.
type SweptObject struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// SweepResult reports what one Sweep pass removed or would remove.
type SweepResult struct {
	DryRun bool `json:"dry_run"`

	SweptShards []SweptObject `json:"swept_shards"`
	SweptXorbs  []SweptObject `json:"swept_xorbs"`

	DeletedChunkEntries  int `json:"deleted_chunk_entries"`
	DeletedSHA256Entries int `json:"deleted_sha256_entries"`

	// Repointed*Entries count dead-shard index entries redirected to a live
	// shard that carries the same chunk or SHA-256.
	RepointedChunkEntries  int `json:"repointed_chunk_entries"`
	RepointedSHA256Entries int `json:"repointed_sha256_entries"`

	// DanglingFileEntries are index/files entries whose shard object is
	// missing; they are reported for repair, never deleted.
	DanglingFileEntries []string `json:"dangling_file_entries"`

	// SkippedInGrace counts unreferenced objects left alone because they
	// were written within the grace window.
	SkippedInGrace int `json:"skipped_in_grace"`
	// ReclaimedBytes sums the sizes of swept shards and xorbs.
	ReclaimedBytes int64 `json:"reclaimed_bytes"`

	// Done reports a finished pass; Remaining* are the unconsumed dead-queue lengths, best-effort estimates.
	Done            bool `json:"done"`
	RemainingShards int  `json:"remaining_shards"`
	RemainingXorbs  int  `json:"remaining_xorbs"`
}

// GC serializes sweeps over one store within a single process: concurrent
// sweeps fail fast with ErrGCBusy instead of queueing. Nothing
// serializes sweepers across processes; deployments sharing a store must
// run at most one sweeper.
type GC struct {
	st    GCStore
	mu    sync.Mutex
	cycle *sweepCycle // in-progress stepped sweep, guarded by mu
}

// NewGC creates a GC coordinator over st.
func NewGC(st GCStore) *GC {
	return &GC{st: st}
}

// Unlink removes the file-index entry for fileHash; see the package Unlink.
func (g *GC) Unlink(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	return Unlink(ctx, g.st, fileHash)
}

// SweepStep consumes one bounded slice of a sweep cycle, failing with
// ErrGCBusy when a sweep is already running. The first call marks a fresh
// cycle, paying the full mark cost whatever the limits say, and the step
// that drains the shard queue pays the full re-shield walk the same way;
// later calls resume the cycle until the returned result reports Done.
// Results accumulate, so the Done step reads like one full Sweep pass.
// A parked cycle expires one grace window after its mark and is re-marked
// on the next step with a fresh accumulation — consuming a cycle within
// one window bounds the staleness of its shield state; a non-positive
// grace never expires. A cycle marked with a different Grace value is
// discarded and re-marked; dry runs report a full stateless pass and
// leave the cycle alone; any non-dry-run error discards the cycle.
func (g *GC) SweepStep(ctx context.Context, opts SweepOptions) (*SweepResult, error) {
	if !g.mu.TryLock() {
		return nil, ErrGCBusy
	}
	defer g.mu.Unlock()
	// Discarded before the dry-run return too, so even dry-run-only callers
	// release an expired cycle's shield maps.
	if g.cycle != nil && g.cycle.expired() {
		g.cycle = nil
	}
	if opts.DryRun {
		return Sweep(ctx, g.st, opts)
	}
	c := g.cycle
	g.cycle = nil
	if c != nil && !c.matches(opts) {
		c = nil
	}
	if c == nil {
		var err error
		c, err = sweepMark(ctx, g.st, opts)
		if err != nil {
			return nil, err
		}
	}
	if err := c.run(ctx, g.st, opts.MaxDeletes, opts.Budget); err != nil {
		return nil, err
	}
	if c.phase != sweepPhaseDone {
		g.cycle = c
	}
	return c.snapshot(), nil
}

// SweepOptions configures one Sweep pass; a zero Grace means DefaultSweepGrace, a negative one disables the window.
type SweepOptions struct {
	Grace      time.Duration // shields objects recently written (mtime)
	DryRun     bool          // report removable objects without deleting
	MaxDeletes int           // max dead-queue items one GC.SweepStep consumes, swept or skipped; 0 = unlimited, ignored by Sweep
	Budget     time.Duration // wall-clock cap per GC.SweepStep, checked between dead-queue items only (mark and re-shield always run whole); 0 = unlimited, ignored by Sweep
}

// window returns the grace window with zero defaulted.
func (o SweepOptions) window() time.Duration {
	if o.Grace == 0 {
		return DefaultSweepGrace
	}
	return o.Grace
}

// Sweep is a mark-and-sweep pass at shard granularity: a shard is live while
// any index/files entry points at it, and a xorb is live while any live
// shard references it through reconstruction terms or CAS blocks. Dead
// shards take their chunk and sha256 index entries with them; an entry is
// only touched when it still points at the dead shard, and when some live
// shard carries the same chunk or SHA-256 the entry is repointed to that
// shard instead of deleted, so global dedup and SHA-256 lookups keep
// working for the surviving content.
//
// Sweep does not lock out writers: work owned by someone else is skipped
// instead. Writers are protected by the mtime grace window and the
// re-checks: unreferenced shards inside the window are treated as uploads
// mid-commit and shield every xorb they reference (dedup may have reused
// xorbs far older than the window), the dead sets are re-checked against
// the file index once the walks finish, each dead shard's own file entries
// are read once more right before it is deleted, and once the shard
// deletes finish every shard object still stored re-shields its xorbs,
// covering commits that landed during that phase. A commit landing between
// that final re-shield and an individual xorb delete can still lose that
// xorb; an upload committed after the mark may see the chunk/sha256
// entries it reused from a dead shard removed or repointed (a dedup miss
// or a broken SHA-256 lookup, never lost file data); and a dedup re-upload
// of an aged unreferenced shard can race its delete (dedup hits do not
// refresh mtimes). Those residual races are accepted.
//
// Sweep always runs to completion: MaxDeletes and Budget only bound
// GC.SweepStep, through which a GC can consume one pass in bounded steps;
// the grace anchor of such a cycle stays fixed at its mark time.
func Sweep(ctx context.Context, st GCStore, opts SweepOptions) (*SweepResult, error) {
	c, err := sweepMark(ctx, st, opts)
	if err != nil {
		return nil, err
	}
	if err := c.run(ctx, st, 0, 0); err != nil {
		return nil, err
	}
	c.res.Done = true
	return c.res, nil
}

// One cycle's phases: consume dead shards, then — after the final
// re-shield — dead xorbs.
const (
	sweepPhaseShards = iota
	sweepPhaseXorbs
	sweepPhaseDone
)

// sweepCycle is one mark's worth of sweep work, consumable in bounded steps:
// the dead queues plus the shield state and grace anchor fixed at mark
// time. res accumulates across steps, so once both queues drain it reads
// like the result of a single full pass.
type sweepCycle struct {
	grace  time.Duration // normalized window the mark ran with
	marked time.Time     // mark anchor; a parked cycle expires one grace window after it

	cutoff time.Time

	res *SweepResult

	deadShards []SweptObject
	deadXorbs  []SweptObject

	liveShards  map[string]bool
	liveXorbs   map[string]bool
	chunkOwners map[string]string
	shaOwners   map[string]string

	phase int
}

// matches reports whether the cycle's mark used the same effective window.
func (c *sweepCycle) matches(opts SweepOptions) bool {
	return c.grace == opts.window()
}

// expired reports whether the cycle has outlived one grace window since its
// mark; a non-positive grace (the explicit no-safety-window mode) never
// expires.
func (c *sweepCycle) expired() bool {
	return c.grace > 0 && time.Since(c.marked) >= c.grace
}

// snapshot clones the accumulated result with the progress fields filled in.
func (c *sweepCycle) snapshot() *SweepResult {
	res := *c.res
	res.SweptShards = slices.Clone(res.SweptShards)
	res.SweptXorbs = slices.Clone(res.SweptXorbs)
	res.DanglingFileEntries = slices.Clone(res.DanglingFileEntries)
	res.Done = c.phase == sweepPhaseDone
	res.RemainingShards = len(c.deadShards)
	res.RemainingXorbs = len(c.deadXorbs)
	return &res
}

// sweepMark runs the mark phase: one time anchor, the live and dead walks,
// and the second file-index look pruning both dead queues.
func sweepMark(ctx context.Context, st GCStore, opts SweepOptions) (*sweepCycle, error) {
	grace := opts.window()
	now := time.Now()
	// A negative grace pushes the cutoff into the future: nothing is exempt.
	cutoff := now.Add(-grace)

	c := &sweepCycle{
		grace:  grace,
		marked: now,
		cutoff: cutoff,
		res: &SweepResult{
			DryRun:              opts.DryRun,
			SweptShards:         []SweptObject{},
			SweptXorbs:          []SweptObject{},
			DanglingFileEntries: []string{},
		},
		liveShards:  map[string]bool{},
		liveXorbs:   map[string]bool{},
		chunkOwners: map[string]string{},
		shaOwners:   map[string]string{},
	}

	// Mark: group file-index entries by shard, then load each referenced
	// shard once to collect the xorbs and SHA-256 digests it keeps alive.
	liveFiles := map[string][]string{} // shard hash -> file hashes
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		liveFiles[shardHash] = append(liveFiles[shardHash], fileHash)
		return nil
	})
	if err != nil {
		return nil, err
	}

	graceXorbs := map[string]bool{} // shielded by in-grace uncommitted shards
	for shardHash, fileHashes := range liveFiles {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				c.res.DanglingFileEntries = append(c.res.DanglingFileEntries, fileHashes...)
				continue
			}
			return nil, fmt.Errorf("load live shard %s: %w", shardHash, err)
		}
		c.liveShards[shardHash] = true
		markShardXorbs(sh, c.liveXorbs)
		markShardOwners(sh, shardHash, c.chunkOwners, c.shaOwners)
	}
	slices.Sort(c.res.DanglingFileEntries)

	// Collect dead objects first so nothing is deleted mid-walk.
	err = st.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		if c.liveShards[hash] {
			return nil
		}
		if !modTime.Before(cutoff) {
			c.res.SkippedInGrace++
			// Likely an upload that has not committed its file entry yet:
			// shield the xorbs it references, dedup may have reused old ones.
			sh, err := st.GetShardByHash(ctx, hash)
			if err != nil {
				if errors.Is(err, iofs.ErrNotExist) {
					return nil
				}
				return fmt.Errorf("load in-grace shard %s: %w", hash, err)
			}
			markShardXorbs(sh, graceXorbs)
			return nil
		}
		c.deadShards = append(c.deadShards, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = st.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		if c.liveXorbs[hash] {
			return nil
		}
		if graceXorbs[hash] || !modTime.Before(cutoff) {
			c.res.SkippedInGrace++
			return nil
		}
		c.deadXorbs = append(c.deadXorbs, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Second look before deleting: entries committed since the mark belong
	// to in-flight uploads; their shards and xorbs are skipped, not raced.
	var recommitted []string
	err = st.WalkFileIndex(ctx, func(_, shardHash string) error {
		if !c.liveShards[shardHash] {
			c.liveShards[shardHash] = true
			recommitted = append(recommitted, shardHash)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, shardHash := range recommitted {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				continue // dangling entry; left for the next sweep to report
			}
			return nil, fmt.Errorf("load live shard %s: %w", shardHash, err)
		}
		markShardXorbs(sh, c.liveXorbs)
		markShardOwners(sh, shardHash, c.chunkOwners, c.shaOwners)
	}
	c.deadShards = slices.DeleteFunc(c.deadShards, func(obj SweptObject) bool {
		return c.liveShards[obj.Hash]
	})
	c.deadXorbs = slices.DeleteFunc(c.deadXorbs, func(obj SweptObject) bool {
		return c.liveXorbs[obj.Hash]
	})
	return c, nil
}

// run consumes the dead queues through the per-item delete logic. maxDeletes
// caps consumed items (swept or skipped) and budget the wall clock, both
// checked between items with at least one item consumed per call; zero means
// unlimited. The final re-shield runs once at the shard-to-xorb transition,
// not counted as consumption.
func (c *sweepCycle) run(ctx context.Context, st GCStore, maxDeletes int, budget time.Duration) error {
	start := time.Now()
	consumed := 0
	exhausted := func() bool {
		if consumed == 0 {
			return false
		}
		return (maxDeletes > 0 && consumed >= maxDeletes) || (budget > 0 && time.Since(start) >= budget)
	}
	dryRun := c.res.DryRun

	for c.phase == sweepPhaseShards {
		if len(c.deadShards) == 0 {
			// Final re-shield: the shard-delete phase can take long, so any
			// shard object stored at this point (late commits, in-grace
			// uploads, skipped shards) shields its xorbs from the deletes
			// below. Skipped in dry runs, where the undeleted dead shards
			// would shield their own xorbs and empty the report.
			if !dryRun {
				err := st.WalkShards(ctx, func(hash string, _ int64, _ time.Time) error {
					if c.liveShards[hash] {
						return nil
					}
					sh, err := st.GetShardByHash(ctx, hash)
					if err != nil {
						if errors.Is(err, iofs.ErrNotExist) {
							return nil
						}
						return fmt.Errorf("load shard %s: %w", hash, err)
					}
					markShardXorbs(sh, c.liveXorbs)
					return nil
				})
				if err != nil {
					return err
				}
			}
			c.phase = sweepPhaseXorbs
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if exhausted() {
			return nil
		}
		obj := c.deadShards[0]
		c.deadShards = c.deadShards[1:]
		consumed++
		if dryRun {
			c.res.SweptShards = append(c.res.SweptShards, obj)
			c.res.ReclaimedBytes += obj.Size
			continue
		}
		swept, err := sweepShard(ctx, st, obj.Hash, c.liveXorbs, c.chunkOwners, c.shaOwners, c.res)
		if err != nil {
			return err
		}
		if !swept {
			continue
		}
		c.res.SweptShards = append(c.res.SweptShards, obj)
		c.res.ReclaimedBytes += obj.Size
	}

	for c.phase == sweepPhaseXorbs {
		if len(c.deadXorbs) == 0 {
			c.phase = sweepPhaseDone
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if exhausted() {
			return nil
		}
		obj := c.deadXorbs[0]
		c.deadXorbs = c.deadXorbs[1:]
		consumed++
		// A shard skipped as re-committed shields its xorbs.
		if c.liveXorbs[obj.Hash] {
			continue
		}
		c.res.SweptXorbs = append(c.res.SweptXorbs, obj)
		c.res.ReclaimedBytes += obj.Size
		if dryRun {
			continue
		}
		xorbHash, err := xet.ParseXorbHash(obj.Hash)
		if err != nil {
			return fmt.Errorf("parse xorb hash %s: %w", obj.Hash, err)
		}
		if err := st.DeleteXorb(ctx, xorbHash); err != nil {
			return err
		}
	}
	return nil
}

// sweepShard removes one dead shard together with the chunk and sha256 index
// entries it owns, reporting whether it did. A file entry pointing back at
// the shard means a re-upload committed it after the mark: it is skipped and
// its xorbs are shielded through liveXorbs. An entry is only touched while
// it still points at the dead shard: when a live shard carries the same
// chunk or SHA-256 the entry is repointed to it, keeping dedup and SHA-256
// lookups alive on backends whose index keeps the first writer; otherwise
// it is deleted. Entries go first: a crash mid-way leaves a re-sweepable
// shard, never entries pointing into nothing. A re-upload committing after
// the file-entry re-read can lose the shard and leave dangling file
// entries, reported by the next sweep's DanglingFileEntries for repair —
// an accepted race.
func sweepShard(ctx context.Context, st GCStore, shardHash string, liveXorbs map[string]bool, chunkOwners, shaOwners map[string]string, res *SweepResult) (bool, error) {
	sh, err := st.GetShardByHash(ctx, shardHash)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			// Deleted by someone else after the walk: nothing was reclaimed
			// here, and without the shard its entries cannot be enumerated.
			return false, nil
		}
		return false, fmt.Errorf("load dead shard %s: %w", shardHash, err)
	}

	for i := range sh.Files {
		current, err := st.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
		if err != nil {
			return false, err
		}
		if current == shardHash {
			markShardXorbs(sh, liveXorbs)
			markShardOwners(sh, shardHash, chunkOwners, shaOwners)
			return false, nil
		}
	}

	for i := range sh.CASInfos {
		for _, chunk := range sh.CASInfos[i].Chunks {
			current, err := st.GetChunkIndexEntry(ctx, chunk.ChunkHash)
			if err != nil {
				return false, err
			}
			if current != shardHash {
				continue
			}
			if owner, ok := chunkOwners[chunk.ChunkHash.String()]; ok {
				if err := st.SetChunkIndexEntry(ctx, chunk.ChunkHash, owner); err != nil {
					return false, err
				}
				res.RepointedChunkEntries++
				continue
			}
			if err := st.DeleteChunkIndexEntry(ctx, chunk.ChunkHash); err != nil {
				return false, err
			}
			res.DeletedChunkEntries++
		}
	}

	for i := range sh.Files {
		if sh.Files[i].MetadataExt == nil {
			continue
		}
		key := sh.Files[i].MetadataExt.SHA256Hash.String()
		current, err := st.GetSHA256IndexEntry(ctx, key)
		if err != nil {
			return false, err
		}
		if current != shardHash {
			continue
		}
		if owner, ok := shaOwners[key]; ok {
			if err := st.SetSHA256IndexEntry(ctx, key, owner); err != nil {
				return false, err
			}
			res.RepointedSHA256Entries++
			continue
		}
		if err := st.DeleteSHA256IndexEntry(ctx, key); err != nil {
			return false, err
		}
		res.DeletedSHA256Entries++
	}

	return true, st.DeleteShard(ctx, shardHash)
}

// markShardXorbs records every xorb the shard references as live.
func markShardXorbs(sh *shard.Shard, xorbs map[string]bool) {
	for i := range sh.Files {
		for _, entry := range sh.Files[i].Entries {
			xorbs[entry.CASHash.String()] = true
		}
	}
	for i := range sh.CASInfos {
		xorbs[sh.CASInfos[i].CASHash.String()] = true
	}
}

// markShardOwners records the live shard as a repoint target for every chunk
// and SHA-256 it carries.
func markShardOwners(sh *shard.Shard, shardHash string, chunkOwners, shaOwners map[string]string) {
	for i := range sh.CASInfos {
		for _, chunk := range sh.CASInfos[i].Chunks {
			chunkOwners[chunk.ChunkHash.String()] = shardHash
		}
	}
	for i := range sh.Files {
		if sh.Files[i].MetadataExt == nil {
			continue
		}
		shaOwners[sh.Files[i].MetadataExt.SHA256Hash.String()] = shardHash
	}
}
