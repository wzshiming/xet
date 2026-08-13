package mirror

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/wzshiming/xet"
)

// GCRoots returns the file hashes of the handler's current ready entries:
// the live root set for an in-process storage collection on a running
// mirror. In-flight ingests are not listed; their freshly written storage is
// protected by the GC grace period.
func (h *Handler) GCRoots() []xet.FileHash {
	h.mu.Lock()
	defer h.mu.Unlock()
	seen := make(map[xet.FileHash]struct{}, len(h.entries))
	roots := make([]xet.FileHash, 0, len(h.entries))
	for _, e := range h.entries {
		if e.State != stateReady || e.FileHash == "" {
			continue
		}
		fh, err := xet.ParseFileHash(e.FileHash)
		if err != nil {
			continue
		}
		if _, ok := seen[fh]; ok {
			continue
		}
		seen[fh] = struct{}{}
		roots = append(roots, fh)
	}
	return roots
}

// PruneIndex applies age-based retention to the mirror index. Branch
// mappings not re-checked within maxAge are dropped first; then every ready
// entry whose commit is not pinned by a surviving branch mapping and whose
// CheckedAt is older than maxAge is removed. Entries under pinned commits
// are kept regardless of age: they are the current content of a live branch,
// and commit-keyed entries are never revalidated so their CheckedAt stays at
// ingest time.
//
// Entries and branch pins leave memory and disk together, so pruned content
// is re-ingested on the next request instead of being served against storage
// a following GC removed. A subsequent storage GC rooted in GCRoots reclaims
// the shards and xorbs only the pruned entries referenced.
func (h *Handler) PruneIndex(maxAge time.Duration) (*PruneResult, error) {
	if maxAge <= 0 {
		return nil, fmt.Errorf("mirror: retention age must be positive")
	}
	cutoff := time.Now().Add(-maxAge)
	res := &PruneResult{}
	branchDir := h.branchDir()
	var paths []string

	h.mu.Lock()
	pinned := map[string]struct{}{}
	for key, b := range h.branches {
		if b.CheckedAt.Before(cutoff) {
			delete(h.branches, key)
			paths = append(paths, branchEntryPath(branchDir, b.Repo, b.Rev))
			res.RemovedBranches++
			continue
		}
		if b.Commit != "" {
			pinned[b.Commit] = struct{}{}
		}
	}
	for key, e := range h.entries {
		if e.State != stateReady {
			continue // failure records are transient and memory-only
		}
		if _, ok := pinned[e.Commit]; ok && e.Commit != "" {
			continue
		}
		if !e.CheckedAt.Before(cutoff) {
			continue
		}
		delete(h.entries, key)
		paths = append(paths, indexEntryPath(h.indexDir, e.Commit, e.Key))
		res.RemovedEntries++
	}
	h.mu.Unlock()

	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return res, fmt.Errorf("remove pruned index file: %w", err)
		}
	}
	removeEmptyCommitDirs(h.indexDir)
	removeEmptyDirs(branchDir)
	return res, nil
}

// PruneResult summarizes one retention pass over the mirror index.
type PruneResult struct {
	RemovedEntries  int
	RemovedBranches int
}

// removeEmptyCommitDirs removes commit directories left empty by pruning.
func removeEmptyCommitDirs(indexDir string) {
	ents, err := os.ReadDir(indexDir)
	if err != nil {
		return
	}
	for _, de := range ents {
		if de.IsDir() && commitRevRe.MatchString(de.Name()) {
			_ = os.Remove(filepath.Join(indexDir, de.Name())) // fails while non-empty
		}
	}
}

// removeEmptyDirs removes directories under root left empty by pruning,
// children before parents; root itself is kept.
func removeEmptyDirs(root string) {
	var dirs []string
	_ = filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if de.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})
	// WalkDir is pre-order, so the reverse visits children before parents.
	for i := len(dirs) - 1; i >= 0; i-- {
		_ = os.Remove(dirs[i]) // fails while non-empty, which keeps it
	}
}
