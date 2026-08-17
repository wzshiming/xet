package mirror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
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

// indexEntryPath returns the on-disk location of an entry: grouped under its
// commit directory when the commit is a 40-hex id, flat otherwise.
func indexEntryPath(dir, commit, key string) string {
	sum := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(sum[:]) + ".json"
	if commitRevRe.MatchString(commit) {
		return filepath.Join(dir, commit, name)
	}
	return filepath.Join(dir, name)
}

// persistEntry writes a ready entry to disk so it survives restarts.
func persistEntry(dir string, e *fileEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal index entry: %w", err)
	}
	path := indexEntryPath(dir, e.Commit, e.Key)
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

// loadIndex reads all persisted entries from dir: entries grouped under
// per-commit directories plus legacy flat files, which are moved under their
// commit directory as they are seen.
func loadIndex(dir string) (map[resolveKey]*fileEntry, error) {
	files := make(map[resolveKey]*fileEntry)
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, fmt.Errorf("read index dir: %w", err)
	}
	for _, de := range dirEntries {
		if de.IsDir() {
			// Commit directories only; skips branches/ and strays.
			if !commitRevRe.MatchString(de.Name()) {
				continue
			}
			subEntries, err := os.ReadDir(filepath.Join(dir, de.Name()))
			if err != nil {
				continue
			}
			for _, se := range subEntries {
				if se.IsDir() || filepath.Ext(se.Name()) != ".json" {
					continue
				}
				if e := readEntry(filepath.Join(dir, de.Name(), se.Name())); e != nil {
					if k, ok := parseResolveKey(e.Key); ok {
						files[k] = e
					}
				}
			}
			continue
		}
		if filepath.Ext(de.Name()) != ".json" {
			continue
		}
		path := filepath.Join(dir, de.Name())
		e := readEntry(path)
		if e == nil {
			continue
		}
		if k, ok := parseResolveKey(e.Key); ok {
			files[k] = e
		}
		// Legacy flat entry: move it under its commit directory.
		if indexEntryPath(dir, e.Commit, e.Key) != path && persistEntry(dir, e) == nil {
			_ = os.Remove(path)
		}
	}
	return files, nil
}

// RemoveBySHA256 drops every ready entry whose content matches the given
// SHA-256 hex, from memory and from the persisted index, so a file removed
// from the CAS is no longer served from the mirror. It returns the number of
// entries removed.
func (h *Handler) RemoveBySHA256(sha256Hex string) (int, error) {
	if sha256Hex == "" {
		return 0, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	removed := 0
	var firstErr error
	for k, e := range h.entries {
		if e.SHA256 != sha256Hex {
			continue
		}
		delete(h.entries, k)
		removed++
		path := indexEntryPath(h.indexDir, e.Commit, e.Key)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) && firstErr == nil {
			firstErr = err
		}
	}
	return removed, firstErr
}
