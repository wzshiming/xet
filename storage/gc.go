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

// GCStore is the per-backend surface needed to unlink files and sweep
// unreferenced shards and xorbs; the orchestration lives in Unlink and Sweep.
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
	// were modified within the grace window.
	SkippedInGrace int `json:"skipped_in_grace"`
	// ReclaimedBytes sums the sizes of swept shards and xorbs.
	ReclaimedBytes int64 `json:"reclaimed_bytes"`
}

// GC serializes sweeps over one store within a single process: concurrent
// Sweep calls fail fast with ErrGCBusy instead of queueing. Nothing
// serializes sweepers across processes; deployments sharing a store must
// run at most one sweeper.
type GC struct {
	st GCStore
	mu sync.Mutex
}

// NewGC creates a GC coordinator over st.
func NewGC(st GCStore) *GC {
	return &GC{st: st}
}

// Unlink removes the file-index entry for fileHash; see the package Unlink.
func (g *GC) Unlink(ctx context.Context, fileHash xet.FileHash) (bool, error) {
	return Unlink(ctx, g.st, fileHash)
}

// Sweep runs one sweep pass, failing with ErrGCBusy when one is already
// running.
func (g *GC) Sweep(ctx context.Context, grace time.Duration, dryRun bool) (*SweepResult, error) {
	if !g.mu.TryLock() {
		return nil, ErrGCBusy
	}
	defer g.mu.Unlock()
	return Sweep(ctx, g.st, grace, dryRun)
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
// instead. Unreferenced shards inside the grace window are treated as
// uploads mid-commit and shield every xorb they reference (dedup may have
// reused xorbs far older than the window), the dead sets are re-checked
// against the file index once the walks finish, each dead shard's own file
// entries are read once more right before it is deleted, and once the
// shard deletes finish every shard object still stored re-shields its
// xorbs, covering commits that landed during that phase. A commit landing
// between the final re-shield and an individual xorb delete can still lose
// that xorb, and an upload committed after the mark may see the
// chunk/sha256 entries it reused from a dead shard removed or repointed (a
// dedup miss or a broken SHA-256 lookup, never lost file data); those
// residual races are accepted.
func Sweep(ctx context.Context, st GCStore, grace time.Duration, dryRun bool) (*SweepResult, error) {

	if grace == 0 {
		grace = DefaultSweepGrace
	}
	// A negative grace pushes the cutoff into the future: nothing is exempt.
	cutoff := time.Now().Add(-grace)

	res := &SweepResult{
		DryRun:              dryRun,
		SweptShards:         []SweptObject{},
		SweptXorbs:          []SweptObject{},
		DanglingFileEntries: []string{},
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

	liveShards := map[string]bool{}
	liveXorbs := map[string]bool{}
	graceXorbs := map[string]bool{}    // shielded by in-grace uncommitted shards
	chunkOwners := map[string]string{} // chunk hex -> live shard hash
	shaOwners := map[string]string{}   // sha256 hex -> live shard hash
	for shardHash, fileHashes := range liveFiles {
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if errors.Is(err, iofs.ErrNotExist) {
				res.DanglingFileEntries = append(res.DanglingFileEntries, fileHashes...)
				continue
			}
			return nil, fmt.Errorf("load live shard %s: %w", shardHash, err)
		}
		liveShards[shardHash] = true
		markShardXorbs(sh, liveXorbs)
		markShardOwners(sh, shardHash, chunkOwners, shaOwners)
	}
	slices.Sort(res.DanglingFileEntries)

	// Collect dead objects first so nothing is deleted mid-walk.
	var deadShards, deadXorbs []SweptObject
	err = st.WalkShards(ctx, func(hash string, size int64, modTime time.Time) error {
		if liveShards[hash] {
			return nil
		}
		if !modTime.Before(cutoff) {
			res.SkippedInGrace++
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
		deadShards = append(deadShards, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}
	err = st.WalkXorbs(ctx, func(hash string, size int64, modTime time.Time) error {
		if liveXorbs[hash] {
			return nil
		}
		if graceXorbs[hash] || !modTime.Before(cutoff) {
			res.SkippedInGrace++
			return nil
		}
		deadXorbs = append(deadXorbs, SweptObject{Hash: hash, Size: size})
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Second look before deleting: entries committed since the mark belong
	// to in-flight uploads; their shards and xorbs are skipped, not raced.
	var recommitted []string
	err = st.WalkFileIndex(ctx, func(_, shardHash string) error {
		if !liveShards[shardHash] {
			liveShards[shardHash] = true
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
		markShardXorbs(sh, liveXorbs)
		markShardOwners(sh, shardHash, chunkOwners, shaOwners)
	}
	deadShards = slices.DeleteFunc(deadShards, func(obj SweptObject) bool {
		return liveShards[obj.Hash]
	})
	deadXorbs = slices.DeleteFunc(deadXorbs, func(obj SweptObject) bool {
		return liveXorbs[obj.Hash]
	})

	for _, obj := range deadShards {
		if dryRun {
			res.SweptShards = append(res.SweptShards, obj)
			res.ReclaimedBytes += obj.Size
			continue
		}
		swept, err := sweepShard(ctx, st, obj.Hash, liveXorbs, chunkOwners, shaOwners, res)
		if err != nil {
			return nil, err
		}
		if !swept {
			continue
		}
		res.SweptShards = append(res.SweptShards, obj)
		res.ReclaimedBytes += obj.Size
	}
	// Final re-shield: the shard-delete phase above can take long, so any
	// shard object stored at this point (late commits, in-grace uploads,
	// skipped shards) shields its xorbs from the delete loop below. Skipped
	// in dry runs, where the undeleted dead shards would shield their own
	// xorbs and empty the report.
	if !dryRun {
		err = st.WalkShards(ctx, func(hash string, _ int64, _ time.Time) error {
			if liveShards[hash] {
				return nil
			}
			sh, err := st.GetShardByHash(ctx, hash)
			if err != nil {
				if errors.Is(err, iofs.ErrNotExist) {
					return nil
				}
				return fmt.Errorf("load shard %s: %w", hash, err)
			}
			markShardXorbs(sh, liveXorbs)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	for _, obj := range deadXorbs {
		// A shard skipped as re-committed shields its xorbs.
		if liveXorbs[obj.Hash] {
			continue
		}
		res.SweptXorbs = append(res.SweptXorbs, obj)
		res.ReclaimedBytes += obj.Size
		if dryRun {
			continue
		}
		xorbHash, err := xet.ParseXorbHash(obj.Hash)
		if err != nil {
			return nil, fmt.Errorf("parse xorb hash %s: %w", obj.Hash, err)
		}
		if err := st.DeleteXorb(ctx, xorbHash); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// sweepShard removes one dead shard together with the chunk and sha256 index
// entries it owns, reporting whether it did. A file entry pointing back at
// the shard means a re-upload committed it after the mark: it is skipped and
// its xorbs are shielded through liveXorbs. An entry is only touched while
// it still points at the dead shard: when a live shard carries the same
// chunk or SHA-256 the entry is repointed to it, keeping dedup and SHA-256
// lookups alive on backends whose index keeps the first writer; otherwise
// it is deleted. Entries go first: a crash mid-way leaves a re-sweepable
// shard, never entries pointing into nothing.
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
