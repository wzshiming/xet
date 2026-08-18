package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/wzshiming/xet"
)

type entryState string

const (
	stateReady  entryState = "ready"
	stateFailed entryState = "failed"
)

// fileEntry is the per-(repo, rev, path) state record. Only ready entries are
// persisted; failures and in-flight ingests are process-local.
type fileEntry struct {
	Key   string     `json:"key"`
	State entryState `json:"state"`

	FileHash string `json:"file_hash,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Size     int64  `json:"size"`
	ETag     string `json:"etag,omitempty"`
	Commit   string `json:"commit,omitempty"`

	CheckedAt time.Time `json:"checked_at"`

	// in-memory only
	failures  int
	nextRetry time.Time
	lastErr   error
	notFound  bool
}

// indexEntryPath returns the on-disk location of the entry for key: grouped
// under the revision directory when the key pins a 40-hex commit, flat
// otherwise. The location is a pure function of the key so lookups need no
// in-memory state.
func indexEntryPath(dir, key string) string {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:]) + ".json"
	if k, ok := parseResolveKey(key); ok && commitRevRe.MatchString(k.rev) {
		return filepath.Join(dir, k.rev, name)
	}
	return filepath.Join(dir, name)
}

// persistEntry writes a ready entry to disk so it survives restarts.
func persistEntry(dir string, e *fileEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal index entry: %w", err)
	}
	path := indexEntryPath(dir, e.Key)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write index entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize index entry: %w", err)
	}
	return nil
}

// readEntry loads one persisted entry, returning nil for anything unreadable
// or not ready.
func readEntry(path string) *fileEntry {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var e fileEntry
	if err := json.Unmarshal(data, &e); err != nil || e.Key == "" || e.State != stateReady {
		return nil
	}
	return &e
}

// entryForKey returns the terminal state for key: the process-local failure
// record, or the ready entry read from the persisted index file. Nothing is
// cached in memory and a ready entry is served only while its file is still
// in storage, so entries dropped or files unlinked behind the handler's back
// (GC, manual cleanup) are honored on the next request.
func (h *Handler) entryForKey(ctx context.Context, key resolveKey) *fileEntry {
	h.mu.Lock()
	e := h.failed[key]
	h.mu.Unlock()
	if e != nil {
		return e
	}
	path := indexEntryPath(h.indexDir, key.String())
	e = readEntry(path)
	if e == nil {
		// Legacy flat entry from before commit grouping: move it to its
		// canonical location as it is seen.
		if !commitRevRe.MatchString(key.rev) {
			return nil
		}
		sum := sha256.Sum256([]byte(key.String()))
		flat := filepath.Join(h.indexDir, hex.EncodeToString(sum[:])+".json")
		if e = readEntry(flat); e == nil {
			return nil
		}
		if persistEntry(h.indexDir, e) == nil {
			_ = os.Remove(flat)
		}
	}
	// Storage is the source of truth: an entry whose file left the CAS is
	// dropped so the next request re-ingests instead of redirecting to a
	// dead bridge.
	if !h.entryLive(ctx, e) {
		_ = os.Remove(path)
		return nil
	}
	return e
}

// entryLive reports whether the entry's content is still reachable in
// storage. Empty files hold no CAS content and are always live. Transient
// storage errors keep the entry: only a definite not-found drops it.
func (h *Handler) entryLive(ctx context.Context, e *fileEntry) bool {
	if e.FileHash == "" {
		return true
	}
	fileHash, err := xet.ParseFileHash(e.FileHash)
	if err != nil {
		return false
	}
	if _, err := h.storage.GetShard(ctx, fileHash); err != nil {
		return !errors.Is(err, fs.ErrNotExist)
	}
	return true
}
