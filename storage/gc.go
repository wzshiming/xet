package storage

import (
	"context"
	"errors"
	"fmt"
	iofs "io/fs"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// SweepStore is the storage surface Unlink and Sweep operate on: enumerate
// everything, delete anything, and exclude shard writes for the duration of
// a sweep. The unexported lock methods keep that contract internal to this
// package: shard writes hold the read side, Sweep the write side. The guard
// is process-local: with multiple instances sharing a backend, the grace
// window is the only cross-instance protection.
type SweepStore interface {
	Storage

	// GetShardByHash loads a shard by the hash of its serialized bytes.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string, modTime time.Time) error) error
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, fileHash string, modTime time.Time) error) error
	WalkChunkIndex(ctx context.Context, fn func(chunkHash, shardHash string, modTime time.Time) error) error
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error

	// The index-entry deletes report whether the entry existed, which Unlink
	// needs for its not-found result.
	DeleteFileIndexEntry(ctx context.Context, fileHash string) (bool, error)

	// The object deletes are idempotent and report no prior existence, so S3
	// can issue blind deletes without a HEAD per object. The SHA-256 delete is
	// only called by Sweep: one SHA-256 can map to several xet hashes, so the
	// entry falls with its shard rather than with any single unlink.
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) error
	DeleteChunkIndexEntry(ctx context.Context, chunkHash string) error
	DeleteShard(ctx context.Context, shardHash string) error
	DeleteXorb(ctx context.Context, xorbHash string) error

	// gcLock blocks shard writes for the duration of a sweep.
	gcLock()
	gcUnlock()
}

// ErrFileNotFound reports that no file index entry matched the given hash.
var ErrFileNotFound = errors.New("file not found in storage")

// UnlinkResult reports what a file removal dropped and resolved.
type UnlinkResult struct {
	FileHash string `json:"file_hash"`
	// SHA256 is the digest recorded in the shard's metadata, reported for the
	// caller's bookkeeping; the SHA-256 index entry itself is never unlinked.
	SHA256           string `json:"sha256,omitempty"`
	RemovedFileIndex bool   `json:"removed_file_index"`
}

// isNotExist recognizes missing-object errors from both backends.
func isNotExist(err error) bool {
	return errors.Is(err, iofs.ErrNotExist) || isS3NotFound(err)
}

// Unlink removes the file index entry that makes the file reachable; shards,
// xorbs and the SHA-256 index are left to Sweep. Unlink takes a xet file hash
// only: one SHA-256 can map to several xet hashes (same bytes, different
// chunking), so deleting by SHA-256 could unlink the wrong file, and the
// SHA-256 entry itself falls only with its shard.
func Unlink(ctx context.Context, st SweepStore, fileHashStr string) (*UnlinkResult, error) {
	fileHash, err := xet.ParseFileHash(fileHashStr)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid hash %q", ErrFileNotFound, fileHashStr)
	}

	res := &UnlinkResult{FileHash: fileHash.String()}
	sh, err := st.GetShard(ctx, fileHash)
	switch {
	case err == nil:
		for i := range sh.Files {
			if sh.Files[i].FileHash == fileHash && sh.Files[i].MetadataExt != nil {
				res.SHA256 = sh.Files[i].MetadataExt.SHA256Hash.String()
				break
			}
		}
	case isNotExist(err):
		// Index or shard already gone; still drop whatever entries remain.
	default:
		return res, err
	}

	if res.RemovedFileIndex, err = st.DeleteFileIndexEntry(ctx, res.FileHash); err != nil {
		return res, err
	}
	if !res.RemovedFileIndex {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, fileHashStr)
	}
	return res, nil
}

// DefaultSweepGrace is the sweep grace window applied when none is given.
const DefaultSweepGrace = time.Hour

// SweepOptions configures a mark-and-sweep pass.
type SweepOptions struct {
	// DryRun counts what would be removed without deleting anything.
	DryRun bool
	// Grace keeps objects modified within the window, protecting in-flight
	// uploads whose xorbs precede their shard. Zero means DefaultSweepGrace;
	// negative disables the grace window. Sweeping without grace while
	// uploads are running can delete a xorb whose shard arrives later,
	// leaving that file permanently unreconstructable. Uploads that take
	// longer than the window are equally unprotected, so size Grace above
	// the longest expected upload.
	Grace time.Duration
}

// SweepReport summarizes one mark-and-sweep pass.
type SweepReport struct {
	DryRun bool `json:"dry_run"`

	LiveFiles  int `json:"live_files"`
	LiveShards int `json:"live_shards"`
	LiveXorbs  int `json:"live_xorbs"`

	RemovedFileIndexEntries   int   `json:"removed_file_index_entries"`
	RemovedSHA256IndexEntries int   `json:"removed_sha256_index_entries"`
	RemovedChunkIndexEntries  int   `json:"removed_chunk_index_entries"`
	RemovedShards             int   `json:"removed_shards"`
	RemovedShardBytes         int64 `json:"removed_shard_bytes"`
	RemovedXorbs              int   `json:"removed_xorbs"`
	RemovedXorbBytes          int64 `json:"removed_xorb_bytes"`
	SkippedGrace              int   `json:"skipped_grace"`
}

// markXorbs records every xorb the shard references as live.
func markXorbs(sh *shard.Shard, liveXorbs map[string]struct{}) {
	for i := range sh.Files {
		for _, entry := range sh.Files[i].Entries {
			liveXorbs[entry.CASHash.String()] = struct{}{}
		}
	}
	for i := range sh.CASInfos {
		liveXorbs[sh.CASInfos[i].CASHash.String()] = struct{}{}
	}
}

// Sweep removes everything unreachable from the file index: shards no live
// file maps to, xorbs no live shard references, and index entries pointing at
// dead targets. Liveness is per shard, so a shard (and its xorbs) survives
// until every file in it has been unlinked. It holds the write side of the
// gc guard, so in-process shard writes wait for the duration while reads
// proceed concurrently under Grace protection.
func Sweep(ctx context.Context, st SweepStore, opts SweepOptions) (*SweepReport, error) {
	grace := opts.Grace
	if grace == 0 {
		grace = DefaultSweepGrace
	} else if grace < 0 {
		grace = 0
	}
	cutoff := time.Now().Add(-grace)

	st.gcLock()
	defer st.gcUnlock()

	report := &SweepReport{DryRun: opts.DryRun}

	// Mark: every file index entry is a root; loading its shard yields the
	// xorbs that must stay.
	liveFiles := map[string]struct{}{}
	liveShards := map[string]struct{}{}
	liveXorbs := map[string]struct{}{}
	// liveShardFiles holds every file carried by a live shard, including
	// unlinked ones: their SHA-256 entries stay until the shard falls.
	liveShardFiles := map[string]struct{}{}
	var danglingRoots []string

	err := st.WalkFileIndex(ctx, func(fileHash, shardHash string, modTime time.Time) error {
		liveFiles[fileHash] = struct{}{}
		if _, ok := liveShards[shardHash]; ok {
			return nil
		}
		sh, err := st.GetShardByHash(ctx, shardHash)
		if err != nil {
			if isNotExist(err) {
				if modTime.After(cutoff) {
					report.SkippedGrace++
					return nil
				}
				delete(liveFiles, fileHash)
				danglingRoots = append(danglingRoots, fileHash)
				return nil
			}
			return fmt.Errorf("load shard %s: %w", shardHash, err)
		}
		liveShards[shardHash] = struct{}{}
		markXorbs(sh, liveXorbs)
		for i := range sh.Files {
			liveShardFiles[sh.Files[i].FileHash.String()] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return report, err
	}
	report.LiveFiles = len(liveFiles)
	report.LiveShards = len(liveShards)
	report.LiveXorbs = len(liveXorbs)

	// Repair roots whose shard object is gone.
	for _, fileHash := range danglingRoots {
		report.RemovedFileIndexEntries++
		if opts.DryRun {
			continue
		}
		if _, err := st.DeleteFileIndexEntry(ctx, fileHash); err != nil {
			return report, err
		}
	}

	// Sweep index entries before objects so a crash cannot leave an index
	// pointing at nothing. A SHA-256 entry is kept while any live shard still
	// carries its file, even an unlinked one: it falls with the shard, once
	// every file index entry into that shard is gone.
	err = st.WalkSHA256Index(ctx, func(sha256Hex, fileHash string, modTime time.Time) error {
		if _, ok := liveShardFiles[fileHash]; ok {
			return nil
		}
		if modTime.After(cutoff) {
			report.SkippedGrace++
			return nil
		}
		report.RemovedSHA256IndexEntries++
		if opts.DryRun {
			return nil
		}
		return st.DeleteSHA256IndexEntry(ctx, sha256Hex)
	})
	if err != nil {
		return report, err
	}

	// A chunk entry pointing at a dead shard is dropped even when its xorb
	// survives via another live shard; the chunk merely stops deduplicating.
	err = st.WalkChunkIndex(ctx, func(chunkHash, shardHash string, modTime time.Time) error {
		if _, ok := liveShards[shardHash]; ok {
			return nil
		}
		if modTime.After(cutoff) {
			report.SkippedGrace++
			return nil
		}
		report.RemovedChunkIndexEntries++
		if opts.DryRun {
			return nil
		}
		return st.DeleteChunkIndexEntry(ctx, chunkHash)
	})
	if err != nil {
		return report, err
	}

	err = st.WalkShards(ctx, func(shardHash string, size int64, modTime time.Time) error {
		if _, ok := liveShards[shardHash]; ok {
			return nil
		}
		if modTime.After(cutoff) {
			report.SkippedGrace++
			// A fresh shard may have landed after the root scan (e.g. on
			// another instance) reusing old xorbs; pin those xorbs too.
			sh, err := st.GetShardByHash(ctx, shardHash)
			if err != nil {
				if isNotExist(err) {
					return nil
				}
				return fmt.Errorf("load shard %s: %w", shardHash, err)
			}
			markXorbs(sh, liveXorbs)
			return nil
		}
		report.RemovedShards++
		report.RemovedShardBytes += size
		if opts.DryRun {
			return nil
		}
		return st.DeleteShard(ctx, shardHash)
	})
	if err != nil {
		return report, err
	}

	err = st.WalkXorbs(ctx, func(xorbHash string, size int64, modTime time.Time) error {
		if _, ok := liveXorbs[xorbHash]; ok {
			return nil
		}
		if modTime.After(cutoff) {
			report.SkippedGrace++
			return nil
		}
		report.RemovedXorbs++
		report.RemovedXorbBytes += size
		if opts.DryRun {
			return nil
		}
		return st.DeleteXorb(ctx, xorbHash)
	})
	if err != nil {
		return report, err
	}

	return report, nil
}
