package mirror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
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
					files[e.Key] = e
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
		files[e.Key] = e
		// Legacy flat entry: move it under its commit directory.
		if indexEntryPath(dir, e.Commit, e.Key) != path && persistEntry(dir, e) == nil {
			_ = os.Remove(path)
		}
	}
	return files, nil
}

// branchEntry pins a mutable branch revision to the upstream commit it
// pointed at when last checked. Commit is empty for upstreams without a real
// commit concept, recording that canonicalization must be skipped.
type branchEntry struct {
	Repo      string    `json:"repo"` // resolve repo prefix, no leading slash
	Rev       string    `json:"rev"`
	Commit    string    `json:"commit,omitempty"`
	CheckedAt time.Time `json:"checked_at"`
}

// key returns the in-memory map key. NUL separates repo from rev so a repo
// containing the separator cannot alias another (repo, rev) pair.
func (b *branchEntry) key() string {
	return b.Repo + "\x00" + b.Rev
}

// escapeSegment percent-encodes the bytes of one request-derived path segment
// that could escape its directory or produce hostile file names: the escape
// byte itself, separators, control bytes, Windows-reserved punctuation, and a
// leading dot (which covers ".", "..", and dotfiles). Typical hub names pass
// through unchanged, keeping the tree readable.
func escapeSegment(s string) string {
	if s == "" {
		return "%00" // impossible in real segments: '%' is always encoded
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '%' || c == '/' || c == '\\' || c < 0x20 || c == 0x7f,
			c == ':' || c == '*' || c == '?' || c == '"' || c == '<' || c == '>' || c == '|':
			fmt.Fprintf(&b, "%%%02X", c)
		case c == '.' && i == 0:
			b.WriteString("%2E")
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// escapeDirSegment escapes a repo segment used as a directory name. A
// trailing ".json" is additionally encoded so directories can never collide
// with the "<rev>.json" mapping files living beside them.
func escapeDirSegment(s string) string {
	s = escapeSegment(s)
	if strings.HasSuffix(s, ".json") {
		s = strings.TrimSuffix(s, ".json") + "%2Ejson"
	}
	return s
}

// branchEntryPath maps a branch mapping to its human-readable location,
// nested by repo path: <dir>/<repo...>/<rev>.json. Every segment is escaped
// so request-derived names cannot traverse outside dir; pathological segments
// that would exceed the file name limit fall back to the legacy hashed
// location.
func branchEntryPath(dir, repo, rev string) string {
	segs := strings.Split(repo, "/")
	parts := make([]string, 0, len(segs)+1)
	for _, seg := range segs {
		parts = append(parts, escapeDirSegment(seg))
	}
	parts = append(parts, escapeSegment(rev)+".json")
	for _, p := range parts {
		if len(p) > 255 {
			sum := sha256.Sum256([]byte(repo + "@" + rev))
			return filepath.Join(dir, hex.EncodeToString(sum[:])+".json")
		}
	}
	return filepath.Join(dir, filepath.Join(parts...))
}

// persistBranch writes a branch mapping to disk so it survives restarts.
func persistBranch(dir string, b *branchEntry) error {
	data, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal branch entry: %w", err)
	}
	path := branchEntryPath(dir, b.Repo, b.Rev)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create branch dir: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return fmt.Errorf("write branch entry: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("finalize branch entry: %w", err)
	}
	return nil
}

// loadBranches reads all persisted branch mappings under dir. Mappings found
// away from their canonical human-readable path (legacy hashed names) are
// moved as they are seen; the JSON body carries repo and rev, so no name
// needs decoding.
func loadBranches(dir string) (map[string]*branchEntry, error) {
	branches := make(map[string]*branchEntry)
	err := filepath.WalkDir(dir, func(path string, de fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		if de.IsDir() || filepath.Ext(de.Name()) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var b branchEntry
		if err := json.Unmarshal(data, &b); err != nil || b.Repo == "" || b.Rev == "" {
			return nil
		}
		branches[b.key()] = &b
		if canonical := branchEntryPath(dir, b.Repo, b.Rev); canonical != path {
			if persistBranch(dir, &b) == nil {
				_ = os.Remove(path)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("read branch dir: %w", err)
	}
	return branches, nil
}
