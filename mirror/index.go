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

func indexEntryPath(dir, key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
}

// persistEntry writes a ready entry to disk so it survives restarts.
func persistEntry(dir string, e *fileEntry) error {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal index entry: %w", err)
	}
	path := indexEntryPath(dir, e.Key)
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

// loadIndex reads all persisted entries from dir.
func loadIndex(dir string) (map[string]*fileEntry, error) {
	files := make(map[string]*fileEntry)
	dirEntries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return files, nil
		}
		return nil, fmt.Errorf("read index dir: %w", err)
	}
	for _, de := range dirEntries {
		if de.IsDir() || filepath.Ext(de.Name()) != ".json" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, de.Name()))
		if err != nil {
			continue
		}
		var e fileEntry
		if err := json.Unmarshal(data, &e); err != nil || e.Key == "" || e.State != stateReady {
			continue
		}
		files[e.Key] = &e
	}
	return files, nil
}
