package storage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	iofs "io/fs"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// Collector extends Storage with the enumeration and deletion primitives GC
// needs. Both FileStorage and S3Storage implement it; the unexported lock
// methods keep the contract internal to this package.
type Collector interface {
	Storage

	// GetShardByHash loads a shard by the hash of its serialized bytes.
	GetShardByHash(ctx context.Context, shardHash string) (*shard.Shard, error)

	// ReplaceShard stores a shard whose files may already be indexed,
	// repointing them at it. Compaction uses it to swap in rewritten shards.
	ReplaceShard(ctx context.Context, s *shard.Shard) (string, error)

	// XorbChunkCount reports how many chunks a stored xorb holds.
	XorbChunkCount(ctx context.Context, xorbHash xet.XorbHash) (uint32, error)

	WalkFileIndex(ctx context.Context, fn func(fileHash, shardHash string, modTime time.Time) error) error
	WalkSHA256Index(ctx context.Context, fn func(sha256Hex, fileHash string, modTime time.Time) error) error
	WalkChunkIndex(ctx context.Context, fn func(chunkHash, shardHash string, modTime time.Time) error) error
	WalkShards(ctx context.Context, fn func(shardHash string, size int64, modTime time.Time) error) error
	WalkXorbs(ctx context.Context, fn func(xorbHash string, size int64, modTime time.Time) error) error

	DeleteFileIndexEntry(ctx context.Context, fileHash string) (bool, error)
	DeleteSHA256IndexEntry(ctx context.Context, sha256Hex string) (bool, error)
	DeleteChunkIndexEntry(ctx context.Context, chunkHash string) (bool, error)
	DeleteShard(ctx context.Context, shardHash string) (bool, error)
	DeleteXorb(ctx context.Context, xorbHash string) (bool, error)

	// gcLock blocks new shard writes for the duration of a sweep, so a
	// concurrent upload cannot persist references to objects being deleted.
	// The lock is process-local: with multiple instances sharing a backend,
	// the grace window is the only cross-instance protection.
	gcLock()
	gcUnlock()
}

var (
	_ Collector = (*FileStorage)(nil)
	_ Collector = (*S3Storage)(nil)
)

// ErrFileNotFound reports that neither hash interpretation matched an entry.
var ErrFileNotFound = errors.New("file not found in storage")

// HashKind selects how Unlink interprets the given hash string. SHA-256 and
// xet hashes are both 64 hex characters, so callers must state which one
// they hold.
type HashKind string

const (
	HashKindSHA256 HashKind = "sha256"
	HashKindFile   HashKind = "file"
)

// UnlinkResult reports which references a file removal dropped.
type UnlinkResult struct {
	FileHash           string `json:"file_hash"`
	SHA256             string `json:"sha256,omitempty"`
	RemovedFileIndex   bool   `json:"removed_file_index"`
	RemovedSHA256Index bool   `json:"removed_sha256_index"`
}

// isNotExist recognizes missing-object errors from both backends.
func isNotExist(err error) bool {
	return errors.Is(err, iofs.ErrNotExist) || isS3NotFound(err)
}

// Unlink removes the index entries that make a file reachable: its SHA-256
// mapping and its file-hash mapping. The shard and xorbs stay in place until
// a Sweep finds them unreferenced.
func Unlink(ctx context.Context, st Collector, hashStr string, kind HashKind) (*UnlinkResult, error) {
	switch kind {
	case HashKindSHA256:
		return unlinkBySHA256(ctx, st, hashStr)
	case HashKindFile:
		return unlinkByFileHash(ctx, st, hashStr)
	default:
		return nil, fmt.Errorf("unknown hash kind %q", kind)
	}
}

func unlinkBySHA256(ctx context.Context, st Collector, sha256Hex string) (*UnlinkResult, error) {
	raw, err := hex.DecodeString(sha256Hex)
	if err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("%w: invalid SHA-256 %q", ErrFileNotFound, sha256Hex)
	}
	var digest [32]byte
	copy(digest[:], raw)

	fileHash, err := st.GetFileHashBySHA256(ctx, "default", digest)
	if err != nil {
		if isNotExist(err) {
			return nil, fmt.Errorf("%w: SHA-256 %s", ErrFileNotFound, sha256Hex)
		}
		return nil, err
	}

	res := &UnlinkResult{FileHash: fileHash.String(), SHA256: hex.EncodeToString(digest[:])}
	if res.RemovedSHA256Index, err = st.DeleteSHA256IndexEntry(ctx, res.SHA256); err != nil {
		return res, err
	}
	if res.RemovedFileIndex, err = st.DeleteFileIndexEntry(ctx, res.FileHash); err != nil {
		return res, err
	}
	return res, nil
}

func unlinkByFileHash(ctx context.Context, st Collector, fileHashStr string) (*UnlinkResult, error) {
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

	if res.SHA256 != "" {
		if res.RemovedSHA256Index, err = st.DeleteSHA256IndexEntry(ctx, res.SHA256); err != nil {
			return res, err
		}
	}
	if res.RemovedFileIndex, err = st.DeleteFileIndexEntry(ctx, res.FileHash); err != nil {
		return res, err
	}
	if !res.RemovedFileIndex && !res.RemovedSHA256Index {
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
	// leaving that file permanently unreconstructable.
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
// until every file in it has been unlinked. Shard writes in this process are
// blocked for the duration (see Collector.gcLock for the multi-instance
// caveat); other operations proceed concurrently under Grace protection.
func Sweep(ctx context.Context, st Collector, opts SweepOptions) (*SweepReport, error) {
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
	// pointing at nothing.
	err = st.WalkSHA256Index(ctx, func(sha256Hex, fileHash string, modTime time.Time) error {
		if _, ok := liveFiles[fileHash]; ok {
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
		_, err := st.DeleteSHA256IndexEntry(ctx, sha256Hex)
		return err
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
		_, err := st.DeleteChunkIndexEntry(ctx, chunkHash)
		return err
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
		_, err := st.DeleteShard(ctx, shardHash)
		return err
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
		_, err := st.DeleteXorb(ctx, xorbHash)
		return err
	})
	if err != nil {
		return report, err
	}

	return report, nil
}
