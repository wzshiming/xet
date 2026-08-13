package mirror

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/xet"
	"github.com/wzshiming/xet/storage"
)

// newIndexHandler creates a handler over an existing cache dir, loading the
// persisted index like a server restart would.
func newIndexHandler(t *testing.T, cacheDir string) *Handler {
	t.Helper()
	stor, err := storage.NewFileStorage(storage.WithBasePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(
		WithStorage(stor),
		WithUpstream("http://upstream.invalid"),
		WithCacheDir(cacheDir),
	)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func TestHandlerGCRoots(t *testing.T) {
	cacheDir := t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	commit := strings.Repeat("a", 40)
	hashA := xet.FileHash{1, 2, 3}.String()
	hashB := xet.FileHash{4, 5, 6}.String()

	entries := []*fileEntry{
		{Key: "/org/repo/resolve/" + commit + "/a.bin", State: stateReady, FileHash: hashA, Commit: commit, CheckedAt: time.Now()},
		{Key: "/org/repo/resolve/" + commit + "/b.bin", State: stateReady, FileHash: hashB, Commit: commit, CheckedAt: time.Now()},
		// Duplicate hash under another key: must be returned once.
		{Key: "/org/other/resolve/main/a.bin", State: stateReady, FileHash: hashA, CheckedAt: time.Now()},
		// Empty file: no storage behind it.
		{Key: "/org/repo/resolve/" + commit + "/empty.bin", State: stateReady, Commit: commit, CheckedAt: time.Now()},
		// Corrupt hash: cannot address storage.
		{Key: "/org/repo/resolve/" + commit + "/broken.bin", State: stateReady, FileHash: "not-a-hash", Commit: commit, CheckedAt: time.Now()},
	}
	for _, e := range entries {
		if err := persistEntry(indexDir, e); err != nil {
			t.Fatal(err)
		}
	}

	roots := newIndexHandler(t, cacheDir).GCRoots()
	got := map[string]bool{}
	for _, h := range roots {
		got[h.String()] = true
	}
	if len(roots) != 2 || !got[hashA] || !got[hashB] {
		t.Fatalf("roots = %v, want exactly {%s, %s}", got, hashA, hashB)
	}
}

func TestHandlerPruneIndex(t *testing.T) {
	cacheDir := t.TempDir()
	indexDir := filepath.Join(cacheDir, "index")
	branchDir := filepath.Join(indexDir, "branches")
	now := time.Now()
	old := now.Add(-48 * time.Hour)
	pinnedCommit := strings.Repeat("1", 40)
	staleCommit := strings.Repeat("2", 40)
	abandonedCommit := strings.Repeat("3", 40)

	// A live branch pins pinnedCommit; a branch not checked for two days
	// pinned staleCommit and must lose its pin.
	branches := []*branchEntry{
		{Repo: "org/live", Rev: "main", Commit: pinnedCommit, CheckedAt: now},
		{Repo: "org/stale", Rev: "main", Commit: staleCommit, CheckedAt: old},
	}
	for _, b := range branches {
		if err := persistBranch(branchDir, b); err != nil {
			t.Fatal(err)
		}
	}

	entries := []*fileEntry{
		// Old but pinned by the live branch: kept.
		{Key: "/org/live/resolve/" + pinnedCommit + "/model.bin", State: stateReady, FileHash: xet.FileHash{1}.String(), Commit: pinnedCommit, CheckedAt: old},
		// Old under the stale branch's commit: pin gone, pruned.
		{Key: "/org/stale/resolve/" + staleCommit + "/model.bin", State: stateReady, FileHash: xet.FileHash{2}.String(), Commit: staleCommit, CheckedAt: old},
		// Old under a commit no branch ever pinned: pruned.
		{Key: "/org/live/resolve/" + abandonedCommit + "/model.bin", State: stateReady, FileHash: xet.FileHash{3}.String(), Commit: abandonedCommit, CheckedAt: old},
		// Recently checked flat entry (pseudo-commit upstream): kept.
		{Key: "/org/flat/resolve/main/fresh.bin", State: stateReady, FileHash: xet.FileHash{4}.String(), CheckedAt: now},
		// Flat entry not revalidated for two days: pruned.
		{Key: "/org/flat/resolve/main/stale.bin", State: stateReady, FileHash: xet.FileHash{5}.String(), CheckedAt: old},
	}
	for _, e := range entries {
		if err := persistEntry(indexDir, e); err != nil {
			t.Fatal(err)
		}
	}

	h := newIndexHandler(t, cacheDir)
	if _, err := h.PruneIndex(0); err == nil {
		t.Fatal("PruneIndex(0) should fail")
	}
	if roots := h.GCRoots(); len(roots) != 5 {
		t.Fatalf("GCRoots before prune = %d, want 5", len(roots))
	}

	res, err := h.PruneIndex(24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if res.RemovedEntries != 3 || res.RemovedBranches != 1 {
		t.Fatalf("prune = %+v, want 3 entries, 1 branch", res)
	}

	// Memory and disk agree: pruned entries are gone from both, so a new
	// request re-ingests instead of serving storage a later GC removes.
	assertEntry := func(e *fileEntry, want bool) {
		t.Helper()
		h.mu.Lock()
		_, inMemory := h.entries[e.Key]
		h.mu.Unlock()
		if inMemory != want {
			t.Fatalf("entry %s in memory = %v, want %v", e.Key, inMemory, want)
		}
		_, err := os.Stat(indexEntryPath(indexDir, e.Commit, e.Key))
		if want && err != nil {
			t.Fatalf("entry %s should remain on disk: %v", e.Key, err)
		}
		if !want && !os.IsNotExist(err) {
			t.Fatalf("entry %s should be pruned from disk (err=%v)", e.Key, err)
		}
	}
	assertEntry(entries[0], true)
	assertEntry(entries[1], false)
	assertEntry(entries[2], false)
	assertEntry(entries[3], true)
	assertEntry(entries[4], false)

	h.mu.Lock()
	_, staleBranchInMemory := h.branches["org/stale\x00main"]
	_, liveBranchInMemory := h.branches["org/live\x00main"]
	h.mu.Unlock()
	if staleBranchInMemory || !liveBranchInMemory {
		t.Fatalf("branches in memory: stale=%v live=%v", staleBranchInMemory, liveBranchInMemory)
	}
	if _, err := os.Stat(branchEntryPath(branchDir, "org/live", "main")); err != nil {
		t.Fatalf("live branch mapping should remain: %v", err)
	}
	if _, err := os.Stat(branchEntryPath(branchDir, "org/stale", "main")); !os.IsNotExist(err) {
		t.Fatalf("stale branch mapping should be pruned (err=%v)", err)
	}
	// Emptied commit directories and branch repo directories are cleaned up.
	if _, err := os.Stat(filepath.Join(indexDir, staleCommit)); !os.IsNotExist(err) {
		t.Fatalf("empty commit dir should be pruned (err=%v)", err)
	}
	if _, err := os.Stat(filepath.Join(branchDir, "org", "stale")); !os.IsNotExist(err) {
		t.Fatalf("empty branch repo dir should be pruned (err=%v)", err)
	}

	// The surviving entries are exactly the storage GC roots.
	roots := h.GCRoots()
	got := map[string]bool{}
	for _, r := range roots {
		got[r.String()] = true
	}
	if len(roots) != 2 || !got[entries[0].FileHash] || !got[entries[3].FileHash] {
		t.Fatalf("GCRoots after prune = %v, want pinned and fresh hashes", got)
	}
}
