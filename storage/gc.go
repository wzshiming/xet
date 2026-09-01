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

// GCStore is the bare per-backend surface needed to unlink file and SHA-256
// index entries and sweep unreferenced shards and xorbs; the orchestration
// lives in GC.Unlink, GC.UnlinkSHA256, and Sweep. The bare backends
// (*FileStorage, *S3Storage) implement it directly.
type GCStore interface {
	ListStore

	// WalkShards calls fn for every stored shard object.
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error
	// WalkXorbs calls fn for every stored xorb object.
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error
	// WalkSHA256Index calls fn for every index/sha256 entry.
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error

	// LoadShard reads a stored shard object without consulting or
	// populating any read cache: sweeps load whole stores, and routing
	// them through the serving cache would evict every hot entry.
	LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error)

	// GetFileIndexEntry returns the shard hash recorded for fileHash, or
	// "" when the entry is absent, bypassing caches, together with the
	// entry's modification time (the zero time when absent).
	GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, time.Time, error)
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
	// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting
	// whether it existed.
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) (bool, error)

	// SetChunkIndexEntry force-writes the index/chunks entry for chunkHash.
	SetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash, shardHash string) error
	// SetSHA256IndexEntry force-writes the index/sha256 entry.
	SetSHA256IndexEntry(ctx context.Context, sha256Hex string, shardHash string) error
	// SetFileIndexEntry force-writes the index/files entry for fileHash.
	SetFileIndexEntry(ctx context.Context, fileHash xet.FileHash, shardHash string) error
}

// SweptObject is one removed (or, in a dry run, removable) stored object.
type SweptObject struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// SweepResult reports what one Sweep pass removed or would remove.
type SweepResult struct {
	// DryRun marks a report produced without deleting anything. It is an
	// upper bound on the next real pass, which re-validates every dead
	// shard against the live index right before deletion and may spare
	// more than the dry run predicts.
	DryRun bool `json:"dry_run"`

	SweptShards []SweptObject `json:"swept_shards"`
	SweptXorbs  []SweptObject `json:"swept_xorbs"`

	DeletedChunkEntries  int `json:"deleted_chunk_entries"`
	DeletedSHA256Entries int `json:"deleted_sha256_entries"`
	// DeletedFileEntries counts file entries of dead shards removed by the
	// reverse-clean — AnchorSHA256's stale-entry cleanup only; under anchors
	// where file entries confer liveness the abort guard keeps it at zero.
	DeletedFileEntries int `json:"deleted_file_entries"`

	// Repointed*Entries count dead-shard index entries redirected to a live
	// shard that carries the same chunk, SHA-256, or file.
	RepointedChunkEntries  int `json:"repointed_chunk_entries"`
	RepointedSHA256Entries int `json:"repointed_sha256_entries"`
	RepointedFileEntries   int `json:"repointed_file_entries"`

	// DanglingFileEntries are index/files entries whose shard object is
	// missing; they are reported for repair, never deleted.
	DanglingFileEntries []string `json:"dangling_file_entries"`
	// DanglingSHA256Entries are index/sha256 entries whose shard object is
	// missing; they are reported for repair, never deleted. Always empty
	// under AnchorFiles, whose sweeps do not walk the sha256 index, and
	// never includes the all-zero empty-file entry.
	DanglingSHA256Entries []string `json:"dangling_sha256_entries"`

	// UnreadableShards are stored shard objects that could not be loaded —
	// decode failures or backend errors. Each is treated as live, nothing
	// of it is deleted, and while any is present the pass sweeps no xorbs:
	// an unreadable shard's references are unknown, so every queued xorb
	// might be one of them. Dead-shard cleanup still proceeds.
	UnreadableShards []string `json:"unreadable_shards"`

	// SkippedInGrace counts unreferenced objects left alone because they
	// were written within the grace window or shielded by an in-grace
	// shard's references.
	SkippedInGrace int `json:"skipped_in_grace"`
	// SkippedRevived counts queued dead xorbs consumed without deletion
	// because a later look — a re-shield walk or an aborted shard delete —
	// marked them live; like sweeps, they count against MaxDeletes.
	SkippedRevived int `json:"skipped_revived"`
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

// Unlink removes the file-index entry for fileHash, reporting whether it
// existed. Only the index/files entry is touched: the shard, its xorbs, and
// the chunk/sha256 index entries may all serve other live files, so they
// stay until a Sweep proves the shard unreferenced. Under the default sweep
// anchor the content also stays reconstructable through its SHA-256 entry,
// which itself keeps the shard alive: full removal takes Unlink plus
// UnlinkSHA256, or an AnchorFiles sweep. Empty files are the exception —
// their all-zero sha256 entry never anchors, so Unlink alone suffices.
// An unlink racing a concurrent sweep's repoint of the same entry can be
// overwritten by it (the entry resurfaces pointing at a live shard);
// re-issue the unlink if it must win against a running sweep.
func (g *GC) Unlink(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	return g.st.DeleteFileIndexEntry(ctx, fileHash)
}

// UnlinkSHA256 removes the index/sha256 entry for digest, reporting whether
// it existed. Only that entry is removed — it is never resolved to file
// hashes first: SHA-256 lookups for the digest stop resolving immediately,
// while file entries, the shard, and its data are untouched and stay
// reachable by file hash. Space is reclaimed by a later Sweep once, per its
// anchor mode, nothing references the shard; a dedup re-upload of the same
// content rewrites the entry. The all-zero digest is rejected: it is the
// empty-file marker shared by every empty file. An unlink racing a
// concurrent sweep's repoint of the same entry can be overwritten by it
// (the entry resurfaces pointing at a live shard); re-issue the unlink if
// it must win against a running sweep.
func (g *GC) UnlinkSHA256(ctx context.Context, digest [32]byte) (bool, error) {
	if digest == [32]byte{} {
		return false, errors.New("all-zero SHA-256 digest is the shared empty-file marker")
	}
	return g.st.DeleteSHA256IndexEntry(ctx, shard.NewSHA256Hash(digest).String())
}

// SweepStep consumes one bounded slice of a sweep cycle, failing with
// ErrGCBusy when a sweep is already running. The first call marks a fresh
// cycle, paying the full mark cost whatever the limits say, and the step
// that moves into the xorb queue pays the full re-shield walk the same way;
// every step that consumes from the xorb queue re-runs the re-shield first,
// so the late-commit loss window is bounded by one step, not by the parked
// lifetime of the cycle, and steps with an empty xorb queue skip the walk.
// Later calls resume the cycle until the returned result reports Done.
// Results accumulate, so the Done step reads like one full Sweep pass.
// A parked cycle expires one grace window after its mark and is re-marked
// on the next step with a fresh accumulation — consuming a cycle within
// one window bounds the staleness of its shield state; a non-positive
// grace expires after DefaultSweepGrace instead, so an abandoned cycle's
// queues and shield maps are never pinned for good. A cycle marked with a
// different Grace or Anchor is discarded and re-marked; dry runs report a
// full stateless pass and leave the cycle alone; any non-dry-run error
// discards the cycle, except a canceled or timed-out context, which parks
// it like an exhausted budget — a disconnected client's progress survives,
// and the resuming step's result still accumulates everything consumed.
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
		// A canceled or timed-out context parks the cycle instead of
		// discarding it: the caller vanished, the shield state did not go
		// bad, and the next step must not re-pay the mark.
		if c.phase != sweepPhaseDone && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			g.cycle = c
		}
		return nil, err
	}
	if c.phase != sweepPhaseDone {
		g.cycle = c
	}
	return c.snapshot(), nil
}

// SweepAnchor selects which index entries anchor shard liveness during Sweep.
type SweepAnchor string

const (
	// AnchorBoth (default): a shard is live while any file entry OR any
	// sha256 entry references it; dead only when both kinds are gone.
	AnchorBoth SweepAnchor = ""
	// AnchorFiles ignores sha256 entries: live while any file entry
	// references it; sha256 entries of dead shards are removed with them.
	AnchorFiles SweepAnchor = "files"
	// AnchorSHA256 ignores file entries: live while any sha256 entry
	// references it; file entries of dead shards are removed with them.
	AnchorSHA256 SweepAnchor = "sha256"
)

// zeroSHA256Hex is the all-zero digest empty files store as their SHA-256
// metadata. Shared by every empty file across unrelated shards, it never
// anchors liveness in any mode.
const zeroSHA256Hex = "0000000000000000000000000000000000000000000000000000000000000000"

// SweepOptions configures one Sweep pass; a zero Grace means DefaultSweepGrace, a negative one disables the window.
type SweepOptions struct {
	Grace      time.Duration // shields objects recently written (mtime)
	DryRun     bool          // report removable objects without deleting
	Anchor     SweepAnchor   // which index entries anchor shard liveness
	MaxDeletes int           // max dead-queue items one GC.SweepStep consumes, swept or skipped; 0 = unlimited, ignored by Sweep
	Budget     time.Duration // wall-clock cap per GC.SweepStep, checked between dead-queue items only (mark and re-shield always run whole, their time uncharged); 0 = unlimited, ignored by Sweep
}

// window returns the grace window with zero defaulted.
func (o SweepOptions) window() time.Duration {
	if o.Grace == 0 {
		return DefaultSweepGrace
	}
	return o.Grace
}

// Sweep is a mark-and-sweep pass at shard granularity. Which index entries
// anchor a shard's liveness is picked by SweepOptions.Anchor:
//
// Under AnchorBoth, the default, a shard is live while any index/files or
// index/sha256 entry points at it, so content stays resolvable by file hash
// and by SHA-256 until both kinds of reference are unlinked. Under
// AnchorFiles only file entries anchor — the historical behavior, where
// GC.Unlink alone lets the next sweep reclaim. Under AnchorSHA256 only sha256
// entries anchor, collapsing shards that store duplicate content under a
// different chunking, except that a shard carrying a file that can never be
// sha-anchored (no SHA-256 metadata, or the all-zero digest every empty
// file stores) is exempt while a file entry points at it. The all-zero
// sha256 entry, shared by every empty file, never anchors in any mode.
//
// A xorb is live while any live shard references it through reconstruction
// terms or CAS blocks. Dead shards take their index entries with them; an
// entry is only touched when it still points at the dead shard, and when
// some live shard carries the same chunk, SHA-256, or file the entry is
// repointed to that shard instead of deleted, so dedup and lookups keep
// working for the surviving content. File entries of a dead shard with no
// live owner are deleted with it — only under AnchorSHA256, whose sweeps
// clean up the stale entries of sha-dead shards; under the other anchors an
// entry found still pointing at the shard aborts its deletion instead.
//
// Sweep does not lock out writers: work owned by someone else is skipped
// instead. Writers are protected by the mtime grace window and the
// re-checks: unreferenced shards inside the window are treated as uploads
// mid-commit and shield every xorb they reference (dedup may have reused
// xorbs far older than the window), the dead sets are re-checked once the
// walks finish against file and sha256 entries that appeared since the mark
// (PutShard commits chunk entries, then sha256, then files last, so a fresh
// file ref implies a completed commit), each dead shard's own entries are
// re-read per the anchor right before it is deleted, and once the shard
// deletes finish every shard object stored since the cycle last looked
// re-shields its xorbs — older unreferenced shards were already judged at
// the mark — covering commits that landed during that phase, and a step
// resuming a parked cycle in the xorb phase repeats that walk before
// consuming more.
// A commit landing behind the re-shield walk's cursor during the walk, or
// after it within the same step, can still lose the xorb it reuses; an
// upload committed after the mark may see the chunk/sha256 entries
// it reused from a dead shard removed or repointed (a dedup miss or a
// broken SHA-256 lookup, never lost file data); and a dedup re-upload of an
// aged unreferenced shard can race its delete (dedup hits do not refresh
// mtimes). Those residual races are accepted.
//
// Any shard object that exists but cannot be loaded — a decode failure or
// a backend error — is reported in UnreadableShards and treated as live:
// nothing of it is deleted, dead-shard cleanup elsewhere proceeds, and the
// pass deletes no xorbs at all, since the unreadable shard's references are
// unknown and any queued xorb might be one of them. Transient errors heal
// on a later sweep; decode failures keep the shard reported until it is
// repaired or removed. Context cancellation still aborts the sweep instead.
//
// Under AnchorSHA256 one more race matters: a dead shard's file entries can
// be unlinked and the identical shard recommitted while the sweep runs,
// recreating byte-identical entry→shard pairs that neither the second look
// nor plain sha-mode liveness would count (both backends keep a sha256
// entry's live first writer, so the recommit does not win back sha entries
// owned by other shards — the file-entry mtimes are its only trace). With a
// positive grace a fresh file entry, one modified at or after the mark's
// cutoff, therefore shields the shard it points at, judged at the mark, at
// the per-shard re-read, and again at each file-entry read of the delete
// loop — against the cutoff truncated to whole seconds, since S3 reports
// entry times at second precision. What stays exposed is a commit landing
// between an entry's final read and that entry's delete: read-then-delete
// is not atomic, neither backend has a compare-and-delete, and the
// recommit's index writes recreate the same entry→shard mapping the delete
// is about to erase, so such a commit can still be lost undetectably; with
// the window disabled
// the whole delete-and-recreate race is accepted like the others above.
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
	anchor SweepAnchor   // liveness anchor the mark ran with
	marked time.Time     // mark anchor; a parked cycle expires one grace window after it

	cutoff time.Time
	// freshCutoff is cutoff truncated to whole seconds. File-entry freshness
	// is judged against it because S3 reports LastModified at second
	// precision; truncating the cutoff the same way can only widen the
	// shield by under a second, never miss a genuinely fresh entry.
	freshCutoff time.Time
	// lastLook is the instant before the cycle's latest full look at the
	// stored shards — the mark or the newest re-shield walk. Only shards
	// written at or after it can carry commits the cycle has not judged.
	lastLook time.Time

	res *SweepResult

	deadShards []SweptObject
	deadXorbs  []SweptObject

	liveShards  map[string]bool
	liveXorbs   map[string]bool
	chunkOwners map[string]string
	shaOwners   map[string]string
	fileOwners  map[string]string

	phase int
}

// markLive records a live shard: its xorbs and its chunk, SHA-256, and file
// repoint ownerships.
func (c *sweepCycle) markLive(sh *shard.Shard, shardHash string) {
	c.liveShards[shardHash] = true
	markShardXorbs(sh, c.liveXorbs)
	markShardOwners(sh, shardHash, c.chunkOwners, c.shaOwners, c.fileOwners)
}

// markUnreadable records a shard whose stored object cannot be loaded:
// treated as live so nothing of it is swept, reported for repair, and — its
// references being unknown — poisoning the cycle's xorb phase.
func (c *sweepCycle) markUnreadable(shardHash string) {
	c.liveShards[shardHash] = true
	c.res.UnreadableShards = append(c.res.UnreadableShards, shardHash)
	slices.Sort(c.res.UnreadableShards)
}

// loadAborts reports a shard-load failure the sweep must stop for — the
// step's own dying context — rather than brand the shard unreadable.
func loadAborts(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// matches reports whether the cycle's mark used the same effective window
// and anchor.
func (c *sweepCycle) matches(opts SweepOptions) bool {
	return c.grace == opts.window() && c.anchor == opts.Anchor
}

// expired reports whether the cycle has outlived its parked lifetime: one
// grace window since its mark, or DefaultSweepGrace under a non-positive
// grace (the explicit no-safety-window mode), so an abandoned cycle's
// queues and shield maps are reclaimed instead of pinned until restart.
func (c *sweepCycle) expired() bool {
	ttl := c.grace
	if ttl <= 0 {
		ttl = DefaultSweepGrace
	}
	return time.Since(c.marked) >= ttl
}

// snapshot clones the accumulated result with the progress fields filled in.
func (c *sweepCycle) snapshot() *SweepResult {
	res := *c.res
	res.SweptShards = slices.Clone(res.SweptShards)
	res.SweptXorbs = slices.Clone(res.SweptXorbs)
	res.DanglingFileEntries = slices.Clone(res.DanglingFileEntries)
	res.DanglingSHA256Entries = slices.Clone(res.DanglingSHA256Entries)
	res.UnreadableShards = slices.Clone(res.UnreadableShards)
	res.Done = c.phase == sweepPhaseDone
	res.RemainingShards = len(c.deadShards)
	res.RemainingXorbs = len(c.deadXorbs)
	return &res
}

// sweepMark runs the mark phase: one time anchor, the live and dead walks,
// and the second index look pruning both dead queues.
func sweepMark(ctx context.Context, st GCStore, opts SweepOptions) (*sweepCycle, error) {
	switch opts.Anchor {
	case AnchorBoth, AnchorFiles, AnchorSHA256:
	default:
		return nil, fmt.Errorf("unknown sweep anchor %q", opts.Anchor)
	}
	grace := opts.window()
	now := time.Now()
	// A negative grace pushes the cutoff into the future: nothing is exempt.
	cutoff := now.Add(-grace)

	c := &sweepCycle{
		grace:       grace,
		anchor:      opts.Anchor,
		marked:      now,
		cutoff:      cutoff,
		freshCutoff: cutoff.Truncate(time.Second),
		lastLook:    now,
		res: &SweepResult{
			DryRun:                opts.DryRun,
			SweptShards:           []SweptObject{},
			SweptXorbs:            []SweptObject{},
			DanglingFileEntries:   []string{},
			DanglingSHA256Entries: []string{},
			UnreadableShards:      []string{},
		},
		liveShards:  map[string]bool{},
		liveXorbs:   map[string]bool{},
		chunkOwners: map[string]string{},
		shaOwners:   map[string]string{},
		fileOwners:  map[string]string{},
	}

	// Mark: group file-index entries by shard and — when sha256 entries
	// anchor — collect the shards they reference, then load each candidate
	// once to collect the xorbs and repoint owners it keeps alive. The seen
	// sets remember every entry→shard pair so the second look can tell
	// fresh commits from entries already judged.
	liveFiles := map[string][]string{} // shard hash -> file hashes
	seenFileRefs := map[string]bool{}  // fileHash+shardHash pairs of the first walk
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		liveFiles[shardHash] = append(liveFiles[shardHash], fileHash)
		seenFileRefs[fileHash+shardHash] = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	shaRefs := map[string]bool{}         // shards referenced by non-zero sha256 entries
	shaRefHexes := map[string][]string{} // shard hash -> referencing sha256 hexes
	seenShaRefs := map[string]bool{}     // sha256Hex+shardHash pairs of the first walk
	if opts.Anchor != AnchorFiles {
		err := st.WalkSHA256Index(ctx, func(sha256Hex, shardHash string) error {
			if sha256Hex != zeroSHA256Hex {
				shaRefs[shardHash] = true
				shaRefHexes[shardHash] = append(shaRefHexes[shardHash], sha256Hex)
				seenShaRefs[sha256Hex+shardHash] = true
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	graceXorbs := map[string]bool{} // shielded by in-grace uncommitted shards
	for shardHash, fileHashes := range liveFiles {
		sh, err := st.LoadShard(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				c.res.DanglingFileEntries = append(c.res.DanglingFileEntries, fileHashes...)
				continue
			}
			if loadAborts(err) {
				return nil, err
			}
			c.markUnreadable(shardHash)
			continue
		}
		// Under AnchorSHA256 a file entry alone does not anchor, except for
		// shards carrying files that can never be sha-anchored: a live file
		// entry must keep those reachable. A fresh file entry — modified
		// inside the grace window — shields the shard too (see the Sweep
		// doc), so dry runs report what a real sweep would spare.
		if opts.Anchor == AnchorSHA256 && !shaRefs[shardHash] && !hasUnanchorableFile(sh) {
			fresh := false
			if grace > 0 {
				fresh, err = freshFileRef(ctx, st, sh, shardHash, c.freshCutoff)
				if err != nil {
					return nil, err
				}
			}
			if !fresh {
				continue
			}
		}
		c.markLive(sh, shardHash)
	}
	slices.Sort(c.res.DanglingFileEntries)
	// Shards anchored only by their sha256 entries (unlinked but resolvable).
	for shardHash := range shaRefs {
		if c.liveShards[shardHash] {
			continue
		}
		sh, err := st.LoadShard(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				// Dangling sha256 entries: reported for repair, never deleted.
				c.res.DanglingSHA256Entries = append(c.res.DanglingSHA256Entries, shaRefHexes[shardHash]...)
				continue
			}
			if loadAborts(err) {
				return nil, err
			}
			c.markUnreadable(shardHash)
			continue
		}
		c.markLive(sh, shardHash)
	}
	slices.Sort(c.res.DanglingSHA256Entries)

	// Objects are judged against the cutoff truncated like freshCutoff: S3
	// reports LastModified at second precision, so the flooring could cost an
	// in-flight upload up to a second of shield. A non-positive grace puts
	// the cutoff at or past now, and truncating that down would wrongly
	// shield objects written in the current second, so it keeps the raw one.
	objCutoff := cutoff
	if grace > 0 {
		objCutoff = c.freshCutoff
	}

	// Collect dead objects first so nothing is deleted mid-walk.
	err = st.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		if c.liveShards[hash] {
			return nil
		}
		if !modTime.Before(objCutoff) {
			c.res.SkippedInGrace++
			// Likely an upload that has not committed its file entry yet:
			// shield the xorbs it references, dedup may have reused old ones.
			sh, err := st.LoadShard(ctx, hash)
			if err != nil {
				if errors.Is(err, iofs.ErrNotExist) {
					return nil
				}
				if loadAborts(err) {
					return err
				}
				c.markUnreadable(hash)
				return nil
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
		if graceXorbs[hash] || !modTime.Before(objCutoff) {
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
	// Only entry→shard pairs absent from the first walks are fresh — under
	// AnchorSHA256 the file entries of sha-dead shards were seen and
	// deliberately not anchored. An entry deleted and identically recommitted
	// between the walks is indistinguishable from its old self; such a shard
	// is shielded by the per-shard re-read in sweepShard where its anchor
	// mode counts file entries at all.
	var recommitted []string
	revive := func(shardHash string) {
		if !c.liveShards[shardHash] {
			c.liveShards[shardHash] = true
			recommitted = append(recommitted, shardHash)
		}
	}
	err = st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		if !seenFileRefs[fileHash+shardHash] {
			revive(shardHash)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if opts.Anchor != AnchorFiles {
		err = st.WalkSHA256Index(ctx, func(sha256Hex, shardHash string) error {
			if sha256Hex != zeroSHA256Hex && !seenShaRefs[sha256Hex+shardHash] {
				revive(shardHash)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for _, shardHash := range recommitted {
		sh, err := st.LoadShard(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				continue // dangling entry; left for the next sweep to report
			}
			if loadAborts(err) {
				return nil, err
			}
			c.markUnreadable(shardHash)
			continue
		}
		c.markLive(sh, shardHash)
	}
	c.deadShards = slices.DeleteFunc(c.deadShards, func(obj SweptObject) bool {
		return c.liveShards[obj.Hash]
	})
	c.deadXorbs = slices.DeleteFunc(c.deadXorbs, func(obj SweptObject) bool {
		return c.liveXorbs[obj.Hash]
	})
	return c, nil
}

// reshieldXorbs walks the stored shards and marks the xorbs of every shard
// object not already live and written since the cycle's last full look,
// shielding commits whose shards appeared since the cycle last looked from
// the queued xorb deletes; a shard vanishing mid-walk is skipped. Older
// non-live shards are not reloaded: the mark already judged them — their
// xorbs were queued, or kept off the queue by the grace shield — and a
// previous walk covered anything since. The threshold is truncated to whole
// seconds for S3's second-precision timestamps, which can only widen the
// reload set; like the grace comparisons it assumes backend and sweeper
// clocks agree to about a second.
func (c *sweepCycle) reshieldXorbs(ctx context.Context, st GCStore) error {
	threshold := c.lastLook.Truncate(time.Second)
	// Advanced only when the walk completes: an aborted walk must stay
	// re-coverable by the next one.
	walkStart := time.Now()
	err := st.WalkShards(ctx, func(hash string, _ int64, modTime time.Time) error {
		if c.liveShards[hash] || modTime.Before(threshold) {
			return nil
		}
		sh, err := st.LoadShard(ctx, hash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil
			}
			if loadAborts(err) {
				return err
			}
			c.markUnreadable(hash)
			return nil
		}
		markShardXorbs(sh, c.liveXorbs)
		return nil
	})
	if err != nil {
		return err
	}
	c.lastLook = walkStart
	return nil
}

// run consumes the dead queues through the per-item delete logic. maxDeletes
// caps consumed items (swept or skipped) and budget the wall clock, both
// checked between items with at least one item consumed per call; zero means
// unlimited. The re-shield runs at the shard-to-xorb transition and again
// whenever a call starts on a cycle already in the xorb phase, skipped when
// no dead xorbs are queued; it is never counted as consumption nor charged
// against the budget.
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
	// An unreadable shard poisons the xorb phase: its references are
	// unknown, so no queued xorb is provably dead. Dropping the queue here
	// also keeps the re-shield walks from running for nothing.
	if len(c.res.UnreadableShards) > 0 {
		c.deadXorbs = nil
	}
	reshield := func() error {
		walkStart := time.Now()
		err := c.reshieldXorbs(ctx, st)
		// The walk runs whole; shifting start keeps it off the budget clock.
		start = start.Add(time.Since(walkStart))
		return err
	}

	// A step resuming a cycle parked in the xorb phase re-runs the re-shield
	// first: commits that landed during the park must shield their xorbs
	// before any more deletes, and queue entries revived live are pruned so
	// RemainingXorbs stays honest.
	if c.phase == sweepPhaseXorbs && !dryRun && len(c.deadXorbs) > 0 {
		if err := reshield(); err != nil {
			return err
		}
		c.deadXorbs = slices.DeleteFunc(c.deadXorbs, func(obj SweptObject) bool {
			return c.liveXorbs[obj.Hash]
		})
	}

	for c.phase == sweepPhaseShards {
		if len(c.deadShards) == 0 {
			// A step spent exactly at the queue's end parks here: the next
			// step's transition walk covers the gap, one walk instead of two.
			if exhausted() {
				return nil
			}
			// Re-shield at the transition: the shard-delete phase can take
			// long, so any shard object stored at this point (late commits,
			// in-grace uploads, skipped shards) shields its xorbs from the
			// deletes below. Skipped in dry runs, where the undeleted dead
			// shards would shield their own xorbs and empty the report, and
			// when no dead xorbs are queued — nothing to shield.
			if !dryRun && len(c.deadXorbs) > 0 {
				if err := reshield(); err != nil {
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
		swept, err := sweepShard(ctx, st, c, obj.Hash)
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
		if len(c.res.UnreadableShards) > 0 {
			c.deadXorbs = nil
		}
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
			c.res.SkippedRevived++
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

// sweepShard removes one dead shard together with the index entries it owns,
// reporting whether it did. The shard's liveness is re-read per the cycle's
// anchor first: entries committed back after the mark (a re-upload) make it
// live again — it is skipped and its xorbs and owners are marked. An entry
// is only touched while it still points at the dead shard: when a live
// shard carries the same file, chunk, or SHA-256 the entry is repointed to
// it, keeping lookups alive on backends whose index keeps the first writer;
// otherwise it is deleted. Entries go first — files, then sha256, then
// chunks, the exact reverse of PutShard's commit order, then the shard
// object — so a crash mid-way leaves a re-sweepable shard, never entries
// pointing into nothing, and both abort guards fire before any chunk entry
// a racing commit has reused is destroyed. A racing commit whose entries
// become visible to the delete loops aborts the deletion: an anchoring
// entry found pointing back at the shard contradicts the re-read, so the
// shard is re-marked live instead. A commit whose file entry lands only
// after the file loop's reads can still lose entries and the shard; the
// entry then dangles and is reported by the next sweep's
// DanglingFileEntries for repair — an accepted race.
func sweepShard(ctx context.Context, st GCStore, c *sweepCycle, shardHash string) (bool, error) {
	sh, err := st.LoadShard(ctx, shardHash)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			// Deleted by someone else after the walk: nothing was reclaimed
			// here, and without the shard its entries cannot be enumerated.
			return false, nil
		}
		if loadAborts(err) {
			return false, err
		}
		c.markUnreadable(shardHash)
		return false, nil
	}

	// Under AnchorSHA256 with a positive grace a fresh file entry shields
	// the shard (see the Sweep doc); other anchors stop at the first match.
	freshGuard := c.anchor == AnchorSHA256 && c.grace > 0
	fileRefd := false
	freshRefd := false
	for i := range sh.Files {
		current, modTime, err := st.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
		if err != nil {
			return false, err
		}
		if current != shardHash {
			continue
		}
		fileRefd = true
		if !freshGuard {
			break
		}
		if !modTime.Before(c.freshCutoff) {
			freshRefd = true
			break
		}
	}
	live := fileRefd
	if c.anchor != AnchorFiles {
		shaRefd := false
		for i := range sh.Files {
			ext := sh.Files[i].MetadataExt
			if ext == nil || ext.SHA256Hash == (shard.SHA256Hash{}) {
				continue
			}
			current, err := st.GetSHA256IndexEntry(ctx, ext.SHA256Hash.String())
			if err != nil {
				return false, err
			}
			if current == shardHash {
				shaRefd = true
				break
			}
		}
		if c.anchor == AnchorSHA256 {
			live = shaRefd || freshRefd || (fileRefd && hasUnanchorableFile(sh))
		} else {
			live = fileRefd || shaRefd
		}
	}
	if live {
		c.markLive(sh, shardHash)
		return false, nil
	}

	for i := range sh.Files {
		fileHash := sh.Files[i].FileHash
		current, modTime, err := st.GetFileIndexEntry(ctx, fileHash)
		if err != nil {
			return false, err
		}
		if current != shardHash {
			continue
		}
		// Under anchors where file entries confer liveness the re-read above
		// saw none pointing here, so this entry belongs to a racing commit:
		// abort the shard's deletion instead of destroying the commit.
		if c.anchor != AnchorSHA256 {
			c.markLive(sh, shardHash)
			return false, nil
		}
		// Under AnchorSHA256 stale entries of sha-dead shards are the designed
		// cleanup, but with a positive grace a fresh entry is a completed
		// recommit: abort like the mark and re-read freshness shields do.
		if c.grace > 0 && !modTime.Before(c.freshCutoff) {
			c.markLive(sh, shardHash)
			return false, nil
		}
		if owner, ok := c.fileOwners[fileHash.String()]; ok {
			if err := st.SetFileIndexEntry(ctx, fileHash, owner); err != nil {
				return false, err
			}
			c.res.RepointedFileEntries++
			continue
		}
		if _, err := st.DeleteFileIndexEntry(ctx, fileHash); err != nil {
			return false, err
		}
		c.res.DeletedFileEntries++
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
		// Under anchors where sha256 entries confer liveness the re-read saw
		// no non-zero entry pointing here, and PutShard commits sha256
		// entries before file entries: this entry is a commit in flight —
		// abort the shard's deletion. The zero entry never anchors and is
		// cleaned up with its shard as usual.
		if c.anchor != AnchorFiles && key != zeroSHA256Hex {
			c.markLive(sh, shardHash)
			return false, nil
		}
		if owner, ok := c.shaOwners[key]; ok {
			if err := st.SetSHA256IndexEntry(ctx, key, owner); err != nil {
				return false, err
			}
			c.res.RepointedSHA256Entries++
			continue
		}
		if _, err := st.DeleteSHA256IndexEntry(ctx, key); err != nil {
			return false, err
		}
		c.res.DeletedSHA256Entries++
	}

	// Chunk entries go last: they never abort, so a racing commit caught by
	// the file or sha256 guard above keeps them — deleting them earlier left
	// the revived shard with a permanent dedup gap nothing rewrites.
	for i := range sh.CASInfos {
		for _, chunk := range sh.CASInfos[i].Chunks {
			current, err := st.GetChunkIndexEntry(ctx, chunk.ChunkHash)
			if err != nil {
				return false, err
			}
			if current != shardHash {
				continue
			}
			if owner, ok := c.chunkOwners[chunk.ChunkHash.String()]; ok {
				if err := st.SetChunkIndexEntry(ctx, chunk.ChunkHash, owner); err != nil {
					return false, err
				}
				c.res.RepointedChunkEntries++
				continue
			}
			if err := st.DeleteChunkIndexEntry(ctx, chunk.ChunkHash); err != nil {
				return false, err
			}
			c.res.DeletedChunkEntries++
		}
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

// markShardOwners records the live shard as a repoint target for every
// chunk, SHA-256, and file it carries.
func markShardOwners(sh *shard.Shard, shardHash string, chunkOwners, shaOwners, fileOwners map[string]string) {
	for i := range sh.CASInfos {
		for _, chunk := range sh.CASInfos[i].Chunks {
			chunkOwners[chunk.ChunkHash.String()] = shardHash
		}
	}
	for i := range sh.Files {
		fileOwners[sh.Files[i].FileHash.String()] = shardHash
		if sh.Files[i].MetadataExt == nil {
			continue
		}
		shaOwners[sh.Files[i].MetadataExt.SHA256Hash.String()] = shardHash
	}
}

// freshFileRef reports whether one of sh's file entries still points at
// shardHash with a modification time not before cutoff. PutShard commits
// file entries last, so such an entry is evidence of a commit completed
// after roughly the cutoff; under AnchorSHA256 with a positive grace it
// shields the shard even though plain file refs do not anchor there.
func freshFileRef(ctx context.Context, st GCStore, sh *shard.Shard, shardHash string, cutoff time.Time) (bool, error) {
	for i := range sh.Files {
		current, modTime, err := st.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
		if err != nil {
			return false, err
		}
		if current == shardHash && !modTime.Before(cutoff) {
			return true, nil
		}
	}
	return false, nil
}

// hasUnanchorableFile reports whether the shard carries a file that can
// never be sha-anchored: one without SHA-256 metadata or with the all-zero
// digest empty files store.
func hasUnanchorableFile(sh *shard.Shard) bool {
	for i := range sh.Files {
		ext := sh.Files[i].MetadataExt
		if ext == nil || ext.SHA256Hash == (shard.SHA256Hash{}) {
			return true
		}
	}
	return false
}
