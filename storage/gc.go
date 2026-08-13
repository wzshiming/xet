package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/shard"
)

// DefaultGCGracePeriod is the default modification-time window inside which
// GC never removes anything, protecting in-flight uploads and ingests whose
// objects are not yet referenced. It must exceed the longest expected upload.
const DefaultGCGracePeriod = 24 * time.Hour

// GCResult summarizes one mark-and-sweep collection.
type GCResult struct {
	// LiveFiles, LiveShards and LiveXorbs count the objects reachable from
	// the roots (including grace-pinned in-flight files).
	LiveFiles  int
	LiveShards int
	LiveXorbs  int

	// BrokenRoots counts roots that no longer resolve to a stored shard;
	// their leftover index entries are swept like any other garbage.
	BrokenRoots int

	RemovedFiles   int
	RemovedShards  int
	RemovedXorbs   int
	RemovedChunks  int
	RemovedSHA256s int
	RemovedTemps   int

	ReclaimedBytes int64
}

type gcConfig struct {
	roots    []xet.FileHash
	hasRoots bool
	grace    time.Duration
	dryRun   bool
}

// GCOption configures a garbage collection run.
type GCOption func(*gcConfig)

// WithGCRoots restricts the live set to the given root file hashes, e.g. the
// mirror's ready index entries. Without this option every stored file index
// entry is a root, which is the model for a standalone CAS server: uploaded
// files stay forever and only orphaned shards, xorbs and dangling index
// entries are collected.
func WithGCRoots(roots []xet.FileHash) GCOption {
	return func(c *gcConfig) {
		c.roots = roots
		c.hasRoots = true
	}
}

// WithGCGracePeriod sets the window inside which recently modified objects
// are never removed. Defaults to DefaultGCGracePeriod.
func WithGCGracePeriod(d time.Duration) GCOption {
	return func(c *gcConfig) { c.grace = d }
}

// WithGCDryRun reports what would be removed without deleting anything.
func WithGCDryRun(dryRun bool) GCOption {
	return func(c *gcConfig) { c.dryRun = dryRun }
}

// objectNameRe matches on-disk object names: both xet hashes and SHA-256
// digests render as 64 lowercase hex characters.
var objectNameRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// GC performs a mark-and-sweep collection over the storage directory. Roots
// are file hashes (see WithGCRoots); from each root the files index resolves
// to a shard, and the shard to every xorb it references. Everything else -
// file and sha256 index entries of unreachable files, shards nobody
// references, xorbs no live shard uses, chunk index entries pointing at dead
// shards, and stale temporary files - is removed, oldest-reference first so a
// crash mid-sweep never leaves an index entry pointing at a removed object
// that a live one referenced.
//
// Objects modified within the grace period are neither collected nor treated
// as broken; file index entries younger than the grace period additionally
// pin their shard and xorbs even when they are not roots, so a just-finished
// ingest whose root registration has not landed yet keeps its storage. The
// grace period must exceed the longest expected upload or ingest.
//
// GC may run in-process while the server is serving: the sweep order and the
// final cache flush keep dedup answers consistent, and PutShard verifies
// every referenced xorb by reconstructing the file, so an upload that raced a
// collection fails and retries instead of storing a broken reference.
// Collecting a live server's directory from a separate process is not
// supported, because that server's in-memory state would go stale.
func (fs *FileStorage) GC(ctx context.Context, opts ...GCOption) (*GCResult, error) {
	cfg := gcConfig{grace: DefaultGCGracePeriod}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.grace < 0 {
		return nil, fmt.Errorf("gc: grace period must not be negative")
	}
	cutoff := time.Now().Add(-cfg.grace)
	res := &GCResult{}

	// Mark phase: gather candidate roots. With an explicit root set, file
	// index entries still inside the grace window are pinned as well.
	candidates := map[string]struct{}{}
	for _, h := range cfg.roots {
		candidates[h.String()] = struct{}{}
	}
	err := fs.gcWalk("files", func(name, _ string, info os.FileInfo, isObject bool) error {
		if !isObject {
			return nil
		}
		if !cfg.hasRoots || info.ModTime().After(cutoff) {
			candidates[name] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	liveFiles := make(map[string]struct{}, len(candidates))
	liveShards := map[string]struct{}{}
	liveXorbs := map[string]struct{}{}
	for name := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := os.ReadFile(fs.objectPath("files", name))
		if err != nil {
			res.BrokenRoots++
			continue
		}
		shardHash := strings.TrimSpace(string(b))
		if _, ok := liveShards[shardHash]; ok {
			liveFiles[name] = struct{}{}
			continue
		}
		s, err := fs.readShardForGC(shardHash)
		if err != nil {
			res.BrokenRoots++
			continue
		}
		liveFiles[name] = struct{}{}
		liveShards[shardHash] = struct{}{}
		// A shard references xorbs in two places: CASInfos for the xorbs it
		// uploaded itself, and file term entries for xorbs deduplicated from
		// other shards. Both must stay for its files to reconstruct.
		for _, casBlock := range s.CASInfos {
			liveXorbs[casBlock.CASHash.String()] = struct{}{}
		}
		for _, fileBlock := range s.Files {
			for _, entry := range fileBlock.Entries {
				liveXorbs[entry.CASHash.String()] = struct{}{}
			}
		}
	}
	res.LiveFiles = len(liveFiles)
	res.LiveShards = len(liveShards)
	res.LiveXorbs = len(liveXorbs)

	// Sweep phase: index entries first, then the objects they point at, so
	// an interrupted run leaves unreferenced objects (collected by the next
	// run) rather than dangling references.
	sweeps := []struct {
		kind    string
		removed *int
		dead    func(name, path string) bool
	}{
		{"files", &res.RemovedFiles, func(name, _ string) bool {
			_, live := liveFiles[name]
			return !live
		}},
		{"sha256", &res.RemovedSHA256s, func(_, path string) bool {
			target, err := os.ReadFile(path)
			if err != nil {
				return false
			}
			_, live := liveFiles[strings.TrimSpace(string(target))]
			return !live
		}},
		{"chunks", &res.RemovedChunks, func(_, path string) bool {
			target, err := os.ReadFile(path)
			if err != nil {
				return false
			}
			_, live := liveShards[strings.TrimSpace(string(target))]
			return !live
		}},
		{"shards", &res.RemovedShards, func(name, _ string) bool {
			_, live := liveShards[name]
			return !live
		}},
		{"xorbs", &res.RemovedXorbs, func(name, _ string) bool {
			_, live := liveXorbs[name]
			return !live
		}},
	}
	for _, sw := range sweeps {
		if err := fs.gcSweep(ctx, sw.kind, cutoff, &cfg, res, sw.removed, sw.dead); err != nil {
			return nil, err
		}
	}

	// Drop the in-memory caches: they may hold mappings and open handles for
	// objects that no longer exist.
	if !cfg.dryRun {
		fs.fileMut.Lock()
		fs.fileIndex.Clear()
		fs.fileMut.Unlock()
		fs.shardMut.Lock()
		fs.shardIndex.Clear()
		fs.shardMut.Unlock()
		fs.chunkMut.Lock()
		fs.chunkIndex.Clear()
		fs.chunkMut.Unlock()
		fs.sha256Mut.Lock()
		fs.sha256Index.Clear()
		fs.sha256Mut.Unlock()
		fs.xorbMut.Lock()
		fs.xorbIndex.Clear() // OnEvicted closes the cached handles
		fs.xorbMut.Unlock()
	}

	return res, nil
}

// readShardForGC loads a shard from disk without touching the in-memory
// caches, which are cleared wholesale at the end of a collection.
func (fs *FileStorage) readShardForGC(shardHash string) (*shard.Shard, error) {
	f, err := os.Open(fs.objectPath("shards", shardHash))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = f.Close()
	}()
	s := shard.NewShard()
	if err := s.Decode(f, true); err != nil {
		return nil, err
	}
	return s, nil
}

// gcWalk visits every regular file under basePath/kind, reassembling fanout
// names (bucket prefix + file name). Entries whose name is not a well-formed
// object name (temp files, strays) are reported with isObject=false.
func (fs *FileStorage) gcWalk(kind string, fn func(name, path string, info os.FileInfo, isObject bool) error) error {
	root := filepath.Join(fs.basePath, kind)
	ents, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read %s directory: %w", kind, err)
	}
	visit := func(name, path string, de os.DirEntry) error {
		info, err := de.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		return fn(name, path, info, objectNameRe.MatchString(name))
	}
	for _, ent := range ents {
		if !ent.IsDir() {
			if err := visit(ent.Name(), filepath.Join(root, ent.Name()), ent); err != nil {
				return err
			}
			continue
		}
		bucket := ent.Name()
		subEnts, err := os.ReadDir(filepath.Join(root, bucket))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read %s bucket %s: %w", kind, bucket, err)
		}
		for _, se := range subEnts {
			if se.IsDir() {
				continue
			}
			if err := visit(bucket+se.Name(), filepath.Join(root, bucket, se.Name()), se); err != nil {
				return err
			}
		}
	}
	return nil
}

// isTempName reports whether name is a leftover from an interrupted atomic
// write: the ".tmp" rename staging files and CreateTemp shard files.
func isTempName(name string) bool {
	return strings.HasSuffix(name, ".tmp") || strings.HasPrefix(filepath.Base(name), ".shard-")
}

// gcSweep removes the entries under kind for which dead returns true, plus
// stale temporary files, honoring the grace cutoff and dry-run mode. Emptied
// fanout buckets are pruned afterwards.
func (fs *FileStorage) gcSweep(ctx context.Context, kind string, cutoff time.Time, cfg *gcConfig, res *GCResult, removed *int, dead func(name, path string) bool) error {
	err := fs.gcWalk(kind, func(name, path string, info os.FileInfo, isObject bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if info.ModTime().After(cutoff) {
			return nil
		}
		counter := removed
		if !isObject {
			if !isTempName(name) {
				return nil // stray file: leave it for the operator
			}
			counter = &res.RemovedTemps
		} else if !dead(name, path) {
			return nil
		}
		if !cfg.dryRun {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove %s: %w", path, err)
			}
		}
		*counter++
		res.ReclaimedBytes += info.Size()
		return nil
	})
	if err != nil {
		return err
	}
	if !cfg.dryRun {
		fs.pruneEmptyBuckets(kind)
	}
	return nil
}

// pruneEmptyBuckets removes fanout directories emptied by a sweep. Removal
// of a non-empty directory fails, which is exactly the keep signal.
func (fs *FileStorage) pruneEmptyBuckets(kind string) {
	root := filepath.Join(fs.basePath, kind)
	ents, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, ent := range ents {
		if ent.IsDir() {
			_ = os.Remove(filepath.Join(root, ent.Name()))
		}
	}
}
