package mirror

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestRemoveBySHA256(t *testing.T) {
	dir := t.TempDir()
	commit := strings.Repeat("ab", 20)
	shaTarget := strings.Repeat("11", 32)
	shaOther := strings.Repeat("22", 32)

	entries := []*fileEntry{
		{Key: "/org/repo/resolve/" + commit + "/a.bin", State: stateReady, SHA256: shaTarget, Commit: commit, CheckedAt: time.Now()},
		{Key: "/org/repo/resolve/" + commit + "/copy-of-a.bin", State: stateReady, SHA256: shaTarget, Commit: commit, CheckedAt: time.Now()},
		{Key: "/org/repo/resolve/" + commit + "/b.bin", State: stateReady, SHA256: shaOther, Commit: commit, CheckedAt: time.Now()},
	}

	h := &Handler{indexDir: dir, entries: map[resolveKey]*fileEntry{}}
	for _, e := range entries {
		if err := persistEntry(dir, e); err != nil {
			t.Fatal(err)
		}
		k, ok := parseResolveKey(e.Key)
		if !ok {
			t.Fatalf("parseResolveKey(%q)", e.Key)
		}
		h.entries[k] = e
	}

	removed, err := h.RemoveBySHA256(shaTarget)
	if err != nil {
		t.Fatalf("RemoveBySHA256: %v", err)
	}
	if removed != 2 {
		t.Fatalf("removed = %d, want 2", removed)
	}
	if len(h.entries) != 1 {
		t.Fatalf("entries left = %d, want 1", len(h.entries))
	}
	for i, e := range entries[:2] {
		if _, err := os.Stat(indexEntryPath(dir, e.Commit, e.Key)); !os.IsNotExist(err) {
			t.Fatalf("entry %d still persisted: %v", i, err)
		}
	}
	if _, err := os.Stat(indexEntryPath(dir, entries[2].Commit, entries[2].Key)); err != nil {
		t.Fatalf("unrelated entry removed: %v", err)
	}

	// Removing again or with an empty hash is a no-op.
	if removed, err := h.RemoveBySHA256(shaTarget); err != nil || removed != 0 {
		t.Fatalf("second RemoveBySHA256 = %d, %v", removed, err)
	}
	if removed, err := h.RemoveBySHA256(""); err != nil || removed != 0 {
		t.Fatalf("RemoveBySHA256(empty) = %d, %v", removed, err)
	}
}
