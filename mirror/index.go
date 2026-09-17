package mirror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
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

// indexEntryPath returns the on-disk location of an entry, fanned out as
// <hash[:2]>/<hash[2:4]>/<hash[4:]>.json of the key hash and grouped under
// the commit directory when the commit is a 40-hex id.
func indexEntryPath(dir, commit, key string) string {
	sum := sha256.Sum256([]byte(key))
	h := hex.EncodeToString(sum[:])
	if commitRevRe.MatchString(commit) {
		dir = filepath.Join(dir, commit)
	}
	return filepath.Join(dir, h[:2], h[2:4], h[4:]+".json")
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

// loadIndex reads all persisted entries under dir, skipping the branches
// subtree. Only entries found at their canonical fanout path are loaded, so
// later removal always targets the file that was read.
func loadIndex(dir string) (map[resolveKey]*fileEntry, error) {
	files := make(map[resolveKey]*fileEntry)
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, fmt.Errorf("read index dir: %w", err)
	}
	dir = resolvedDir
	branches := filepath.Join(dir, "branches")
	err = filepath.WalkDir(dir, func(path string, de fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if de.IsDir() {
			if path == branches {
				return fs.SkipDir
			}
			return nil
		}
		if filepath.Ext(de.Name()) != ".json" {
			return nil
		}
		e := readEntry(path)
		if e == nil || indexEntryPath(dir, e.Commit, e.Key) != path {
			return nil
		}
		if k, ok := parseResolveKey(e.Key); ok {
			files[k] = e
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read index dir: %w", err)
	}
	return files, nil
}
