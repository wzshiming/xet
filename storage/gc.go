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

// DefaultSweepGrace is the default window during which unreferenced objects are not swept.
const DefaultSweepGrace = time.Hour

// ErrGCBusy is returned when a sweep is already running on the same GC.
var ErrGCBusy = errors.New("gc already running")

// GCStore is the per-backend surface behind GC.Unlink, GC.UnlinkSHA256, and Sweep.
type GCStore interface {
	ListStore

	// WalkShards calls fn for every stored shard object.
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error
	// WalkXorbs calls fn for every stored xorb object.
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error
	// WalkSHA256Index calls fn for every index/sha256 entry.
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, shardHash string) error) error
	// LoadShard reads a stored shard bypassing the read cache, which a whole-store sweep would evict.
	LoadShard(ctx context.Context, shardHash string) (*shard.Shard, error)
	// GetFileIndexEntry returns the shard hash recorded for fileHash, "" when absent, bypassing caches.
	GetFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (string, error)
	// DeleteFileIndexEntry removes the index/files entry for fileHash, reporting whether it existed.
	DeleteFileIndexEntry(ctx context.Context, fileHash xet.FileHash) (bool, error)
	// DeleteShard removes a stored shard object.
	DeleteShard(ctx context.Context, shardHash string) error
	// DeleteXorb removes a stored xorb object.
	DeleteXorb(ctx context.Context, xorbHash xet.XorbHash) error
	// GetChunkIndexEntry returns the shard hash recorded for chunkHash, "" when absent, bypassing caches.
	GetChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) (string, error)
	// DeleteChunkIndexEntry removes the index/chunks entry for chunkHash.
	DeleteChunkIndexEntry(ctx context.Context, chunkHash xet.ChunkHash) error
	// GetSHA256IndexEntry returns the shard hash recorded for sha256Hex, "" when absent, bypassing caches.
	GetSHA256IndexEntry(ctx context.Context, sha256Hex string) (string, error)
	// DeleteSHA256IndexEntry removes the index/sha256 entry, reporting whether it existed.
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) (bool, error)
}

// SweptObject is one removed (or, in a dry run, removable) stored object.
type SweptObject struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// SweepResult reports what one sweep pass removed or would remove.
type SweepResult struct {
	// DryRun marks a report that deleted nothing; an upper bound on a real pass, with no entry counts.
	DryRun bool `json:"dry_run"`

	SweptShards []SweptObject `json:"swept_shards"`
	SweptXorbs  []SweptObject `json:"swept_xorbs"`

	DeletedChunkEntries int `json:"deleted_chunk_entries"`
	// DeletedFileEntries counts the stale index/files entries of sha-dead shards (AnchorSHA256 only).
	DeletedFileEntries int `json:"deleted_file_entries"`
	// DeletedSHA256Entries counts zero-digest entries of swept shards plus, under AnchorFiles, non-zero ones.
	DeletedSHA256Entries int `json:"deleted_sha256_entries"`

	// DanglingFileEntries are index/files entries whose shard object is missing; reported, never deleted.
	DanglingFileEntries []string `json:"dangling_file_entries"`
	// DanglingSHA256Entries are non-zero index/sha256 entries whose shard is missing; empty under AnchorFiles.
	DanglingSHA256Entries []string `json:"dangling_sha256_entries"`

	// UnreadableShards are stored shards that failed to load; each is treated as live and the pass deletes no xorbs.
	UnreadableShards []string `json:"unreadable_shards"`

	// SkippedInGrace counts unreferenced objects spared as written within the grace window.
	SkippedInGrace int `json:"skipped_in_grace"`
	// ReclaimedBytes sums the sizes of swept shards and xorbs.
	ReclaimedBytes int64 `json:"reclaimed_bytes"`

	// Done reports a finished pass. Remaining* are best-effort; RemainingXorbs is 0
	// whenever the pass ended before judging xorbs.
	Done            bool `json:"done"`
	RemainingShards int  `json:"remaining_shards"`
	RemainingXorbs  int  `json:"remaining_xorbs"`
}

// GC serializes sweeps over one store within a process; concurrent sweeps
// fail fast with ErrGCBusy. Nothing serializes sweepers across processes.
type GC struct {
	st GCStore
	mu sync.Mutex
}

// NewGC creates a GC coordinator over st.
func NewGC(st GCStore) *GC {
	return &GC{st: st}
}

// Unlink removes the index/files entry for fileHash, reporting whether it
// existed. The shard, its xorbs, and its other entries stay until a sweep
// proves the shard unreferenced; see Sweep for which entries anchor it.
func (g *GC) Unlink(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	return g.st.DeleteFileIndexEntry(ctx, fileHash)
}

// UnlinkSHA256 removes the index/sha256 entry for digest, reporting whether
// it existed; file entries, shard, and data stay until a sweep reclaims them.
// The all-zero digest is rejected: it is the shared empty-file marker.
func (g *GC) UnlinkSHA256(ctx context.Context, digest [32]byte) (bool, error) {
	if digest == [32]byte{} {
		return false, errors.New("all-zero SHA-256 digest is the shared empty-file marker")
	}
	return g.st.DeleteSHA256IndexEntry(ctx, shard.NewSHA256Hash(digest).String())
}

// SweepStep runs one independent, stateless pass bounded by MaxDeletes and
// Budget, failing with ErrGCBusy while another sweep runs; repeat until Done.
// Only actual deletions count toward the bounds, so unsweepable items never
// stall progress. Dry runs ignore the bounds.
func (g *GC) SweepStep(ctx context.Context, opts SweepOptions) (*SweepResult, error) {
	if !g.mu.TryLock() {
		return nil, ErrGCBusy
	}
	defer g.mu.Unlock()
	if opts.DryRun {
		return sweepPass(ctx, g.st, opts, 0, 0)
	}
	return sweepPass(ctx, g.st, opts, opts.MaxDeletes, opts.Budget)
}

// zeroSHA256Hex is the all-zero digest empty files store as their SHA-256; it never anchors liveness.
const zeroSHA256Hex = "0000000000000000000000000000000000000000000000000000000000000000"

// SweepAnchor selects which index entries anchor shard liveness during a sweep; see Sweep.
type SweepAnchor string

const (
	// AnchorBoth (default): any files entry or non-zero sha256 entry anchors a shard.
	AnchorBoth SweepAnchor = ""
	// AnchorFiles: only files entries anchor.
	AnchorFiles SweepAnchor = "files"
	// AnchorSHA256: only non-zero sha256 entries anchor (stores managed purely by SHA-256, e.g. Git LFS).
	AnchorSHA256 SweepAnchor = "sha256"
)

// SweepOptions configures one sweep pass; a zero Grace means
// DefaultSweepGrace, a negative one disables the window.
type SweepOptions struct {
	Anchor     SweepAnchor   // which index entries anchor shard liveness; see SweepAnchor
	Grace      time.Duration // shields objects written (mtime) within the window; negative disables it — quiescent stores only
	DryRun     bool          // report removable objects without deleting
	MaxDeletes int           // max actual deletions per GC.SweepStep; 0 = unlimited, ignored by Sweep and dry runs
	Budget     time.Duration // wall-clock cap per GC.SweepStep, checked between dead-queue items once anything was swept; 0 = unlimited, ignored by Sweep and dry runs
}

// Sweep runs one full mark-and-sweep pass. GC never rewrites: it only deletes whole objects and
// entries it can prove dead from fresh reads. MaxDeletes/Budget bound GC.SweepStep only.
//
// A shard is live under AnchorBoth while any files or non-zero sha256 entry points at it; under
// AnchorFiles while any files entry does (the sha256 index is not walked; a dead shard's sha256
// entries go with it); under AnchorSHA256 while any non-zero sha256 entry does (a dead shard's
// stale files entries go with it, but a shard carrying a file with no or zero SHA-256 stays while
// file-referenced). Zero sha256 never anchors. A xorb is live while any stored shard references it.
// Phase 1 re-checks and deletes dead shards; phase 2 re-walks the shards and deletes unreferenced
// xorbs. An unreadable shard is treated as live, reported, and no xorb is deleted that pass.
//
// The grace window shields uploads mid-commit (PutShard: xorbs, shard, chunks, sha256, files entry
// last); negative Grace disables it — quiescent stores only. Guards are read-then-delete without
// compare-and-delete: a commit landing between a final read and the delete can lose that shard or
// its entries, and a shard object landing after phase 2's walk can lose a dedup-reused xorb. Run
// one sweeper per store.
func Sweep(ctx context.Context, st GCStore, opts SweepOptions) (*SweepResult, error) {
	return sweepPass(ctx, st, opts, 0, 0)
}

// loadAborts reports a shard-load failure caused by the pass's own dying context.
func loadAborts(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// markUnreadable records a shard that could not be loaded; it is treated as live.
func markUnreadable(res *SweepResult, shardHash string) {
	if slices.Contains(res.UnreadableShards, shardHash) {
		return
	}
	res.UnreadableShards = append(res.UnreadableShards, shardHash)
	slices.Sort(res.UnreadableShards)
}

// sweepPass is the stateless pass behind Sweep and GC.SweepStep: maxDeletes caps
// actual deletions and budget the wall clock spent deleting; zero means unlimited.
func sweepPass(ctx context.Context, st GCStore, opts SweepOptions, maxDeletes int, budget time.Duration) (*SweepResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch opts.Anchor {
	case AnchorBoth, AnchorFiles, AnchorSHA256:
	default:
		return nil, fmt.Errorf("unknown sweep anchor %q", opts.Anchor)
	}
	grace := opts.Grace
	if grace == 0 {
		grace = DefaultSweepGrace
	}
	objCutoff := time.Now().Add(-grace)
	// S3 LastModified is second-precision; truncating a positive cutoff only widens the shield.
	if grace > 0 {
		objCutoff = objCutoff.Truncate(time.Second)
	}

	res := &SweepResult{
		DryRun:                opts.DryRun,
		SweptShards:           []SweptObject{},
		SweptXorbs:            []SweptObject{},
		DanglingFileEntries:   []string{},
		DanglingSHA256Entries: []string{},
		UnreadableShards:      []string{},
	}

	fileRefs := map[string][]string{} // shard hash -> file hashes
	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string) error {
		fileRefs[shardHash] = append(fileRefs[shardHash], fileHash)
		return nil
	})
	if err != nil {
		return nil, err
	}
	shaRefs := map[string][]string{} // shard hash -> non-zero sha256 hexes
	if opts.Anchor != AnchorFiles {
		err = st.WalkSHA256Index(ctx, func(sha256Hex, shardHash string) error {
			if sha256Hex != zeroSHA256Hex {
				shaRefs[shardHash] = append(shaRefs[shardHash], sha256Hex)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	present := map[string]bool{}
	var deadShards []SweptObject
	err = st.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		present[hash] = true
		anchored := (opts.Anchor != AnchorSHA256 && len(fileRefs[hash]) > 0) || len(shaRefs[hash]) > 0
		if anchored {
			return nil
		}
		if !modTime.Before(objCutoff) {
			res.SkippedInGrace++
			return nil
		}
		deadShards = append(deadShards, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}

	for shardHash, fileHashes := range fileRefs {
		if !present[shardHash] {
			res.DanglingFileEntries = append(res.DanglingFileEntries, fileHashes...)
		}
	}
	slices.Sort(res.DanglingFileEntries)
	for shardHash, hexes := range shaRefs {
		if !present[shardHash] {
			res.DanglingSHA256Entries = append(res.DanglingSHA256Entries, hexes...)
		}
	}
	slices.Sort(res.DanglingSHA256Entries)

	start := time.Now()
	sweptCount := 0
	exhausted := func() bool {
		return (maxDeletes > 0 && sweptCount >= maxDeletes) ||
			(budget > 0 && sweptCount > 0 && time.Since(start) >= budget)
	}

	drySwept := map[string]bool{}
	for len(deadShards) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if exhausted() {
			res.RemainingShards = len(deadShards)
			return res, nil
		}
		obj := deadShards[0]
		deadShards = deadShards[1:]
		if opts.DryRun {
			res.SweptShards = append(res.SweptShards, obj)
			res.ReclaimedBytes += obj.Size
			drySwept[obj.Hash] = true
			continue
		}
		swept, err := sweepShard(ctx, st, res, opts.Anchor, obj.Hash)
		if err != nil {
			return nil, err
		}
		if swept {
			res.SweptShards = append(res.SweptShards, obj)
			res.ReclaimedBytes += obj.Size
			sweptCount++
		}
	}

	// Bounds spent as the queue drained: skip phase 2's loads.
	if exhausted() {
		return res, nil
	}

	// Phase 2: every present shard shields its xorbs; dry runs pretend their reported shards are gone.
	walkStart := time.Now()
	refXorbs := map[string]bool{}
	err = st.WalkShards(ctx, func(hash string, _ int64, _ time.Time) error {
		if drySwept[hash] || slices.Contains(res.UnreadableShards, hash) {
			return nil
		}
		sh, err := st.LoadShard(ctx, hash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				return nil // vanished mid-walk
			}
			if loadAborts(err) {
				return err
			}
			markUnreadable(res, hash)
			return nil
		}
		markShardXorbs(sh, refXorbs)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(res.UnreadableShards) > 0 {
		res.Done = true
		return res, nil
	}

	var deadXorbs []SweptObject
	err = st.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		if refXorbs[hash] {
			return nil
		}
		if !modTime.Before(objCutoff) {
			res.SkippedInGrace++
			return nil
		}
		deadXorbs = append(deadXorbs, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Walks are mark work, uncharged to Budget.
	start = start.Add(time.Since(walkStart))

	for len(deadXorbs) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if exhausted() {
			res.RemainingXorbs = len(deadXorbs)
			return res, nil
		}
		obj := deadXorbs[0]
		deadXorbs = deadXorbs[1:]
		if !opts.DryRun {
			xorbHash, err := xet.ParseXorbHash(obj.Hash)
			if err != nil {
				return nil, fmt.Errorf("parse xorb hash %s: %w", obj.Hash, err)
			}
			if err := st.DeleteXorb(ctx, xorbHash); err != nil {
				return nil, err
			}
			sweptCount++
		}
		res.SweptXorbs = append(res.SweptXorbs, obj)
		res.ReclaimedBytes += obj.Size
	}
	res.Done = true
	return res, nil
}

// sweepShard deletes one dead shard with the entries it owns, in reverse of PutShard's commit
// order (files, sha256, chunks, shard), reporting whether it did. An anchoring entry still
// pointing at the shard aborts before anything is deleted. AnchorSHA256 pre-checks first: it
// deletes files entries before its sha guard would otherwise run, and spares a file-referenced
// shard carrying an unanchorable file.
func sweepShard(ctx context.Context, st GCStore, res *SweepResult, anchor SweepAnchor, shardHash string) (bool, error) {
	sh, err := st.LoadShard(ctx, shardHash)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return false, nil
		}
		if loadAborts(err) {
			return false, err
		}
		markUnreadable(res, shardHash)
		return false, nil
	}
	// SHA256 mode deletes files entries below, ahead of its sha guard, so both checks run first.
	if anchor == AnchorSHA256 {
		if hasUnanchorableFile(sh) {
			if refd, err := fileEntryPointsAt(ctx, st, sh, shardHash); err != nil || refd {
				return false, err
			}
		}
		if refd, err := sha256EntryPointsAt(ctx, st, sh, shardHash); err != nil || refd {
			return false, err
		}
	}
	if anchor != AnchorSHA256 {
		if refd, err := fileEntryPointsAt(ctx, st, sh, shardHash); err != nil || refd {
			return false, err
		}
	} else {
		for i := range sh.Files {
			current, err := st.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
			if err != nil {
				return false, err
			}
			if current != shardHash {
				continue
			}
			removed, err := st.DeleteFileIndexEntry(ctx, sh.Files[i].FileHash)
			if err != nil {
				return false, err
			}
			if removed {
				res.DeletedFileEntries++
			}
		}
	}
	if anchor != AnchorFiles {
		if refd, err := sha256EntryPointsAt(ctx, st, sh, shardHash); err != nil || refd {
			return false, err
		}
	} else {
		for i := range sh.Files {
			if ext := sh.Files[i].MetadataExt; ext != nil && ext.SHA256Hash != (shard.SHA256Hash{}) {
				if err := deleteOwnedSHA256(ctx, st, res, ext.SHA256Hash.String(), shardHash); err != nil {
					return false, err
				}
			}
		}
	}
	// Metadata-less files never own the zero entry, so this check is a no-op for them.
	if hasUnanchorableFile(sh) {
		if err := deleteOwnedSHA256(ctx, st, res, zeroSHA256Hex, shardHash); err != nil {
			return false, err
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
			if err := st.DeleteChunkIndexEntry(ctx, chunk.ChunkHash); err != nil {
				return false, err
			}
			res.DeletedChunkEntries++
		}
	}
	return true, st.DeleteShard(ctx, shardHash)
}

// fileEntryPointsAt reports whether any files entry of the shard still points at shardHash.
func fileEntryPointsAt(ctx context.Context, st GCStore, sh *shard.Shard, shardHash string) (bool, error) {
	for i := range sh.Files {
		current, err := st.GetFileIndexEntry(ctx, sh.Files[i].FileHash)
		if err != nil {
			return false, err
		}
		if current == shardHash {
			return true, nil
		}
	}
	return false, nil
}

// sha256EntryPointsAt reports whether any non-zero sha256 entry of the shard still points at shardHash.
func sha256EntryPointsAt(ctx context.Context, st GCStore, sh *shard.Shard, shardHash string) (bool, error) {
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
			return true, nil
		}
	}
	return false, nil
}

// deleteOwnedSHA256 deletes the sha256 entry hex if it still points at shardHash, counting real removals.
func deleteOwnedSHA256(ctx context.Context, st GCStore, res *SweepResult, hex, shardHash string) error {
	current, err := st.GetSHA256IndexEntry(ctx, hex)
	if err != nil {
		return err
	}
	if current != shardHash {
		return nil
	}
	removed, err := st.DeleteSHA256IndexEntry(ctx, hex)
	if err != nil {
		return err
	}
	if removed {
		res.DeletedSHA256Entries++
	}
	return nil
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

// hasUnanchorableFile reports whether any file of the shard has no SHA-256 metadata or the zero digest.
func hasUnanchorableFile(sh *shard.Shard) bool {
	for i := range sh.Files {
		ext := sh.Files[i].MetadataExt
		if ext == nil || ext.SHA256Hash == (shard.SHA256Hash{}) {
			return true
		}
	}
	return false
}
