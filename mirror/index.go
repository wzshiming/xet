package mirror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type entryState string

const (
	stateReady  entryState = "ready"
	stateFailed entryState = "failed"
)

// fileEntry is the per-(repo, commit, path) record; only ready entries persist, inside their commit manifest.
type fileEntry struct {
	FileHash string `json:"file_hash,omitempty"`
	SHA256   string `json:"sha256,omitempty"`
	Size     int64  `json:"size"`
	ETag     string `json:"etag,omitempty"`

	CheckedAt time.Time `json:"checked_at"`

	// in-memory only
	State     entryState `json:"-"`
	failures  int
	nextRetry time.Time
	lastErr   error
	notFound  bool
	source    string
}

// commitManifest is the persisted record of every ready file of one commit.
type commitManifest struct {
	Repo   string                `json:"repo"`
	Commit string                `json:"commit"`
	Source string                `json:"source,omitempty"` // branch rev a pseudo commit stands in for
	Files  map[string]*fileEntry `json:"files"`
}

// commitState is the in-memory view of one (repo, commit) manifest; source is set once and never cleared.
type commitState struct {
	source string
	files  map[string]*fileEntry // shares ready entries with m.entries
	loaded bool                  // read or confirmed absent
}

// The caller holds m.mu.
func (cs *commitState) publish(path string, e *fileEntry) {
	if cs.files == nil {
		cs.files = map[string]*fileEntry{}
	}
	cs.files[path] = e
}

var errManifestUnread = errors.New("mirror: commit manifest unreadable")

// Hashing keeps request-derived names out of the filesystem namespace.
func repoDir(dir, repo string) string {
	sum := sha256.Sum256([]byte(repo))
	return filepath.Join(dir, hex.EncodeToString(sum[:]))
}

// commitPath is the manifest location; commit must be a validated 40-hex.
func commitPath(dir, repo, commit string) string {
	return filepath.Join(repoDir(dir, repo), "commits", commit+".json")
}

// The caller holds m.mu; failed loads remain retryable.
func (m *Mirror) loadCommit(repo, commit string) *commitState {
	name := repo + "\x00" + commit
	cs := m.commits[name]
	if cs == nil {
		cs = &commitState{}
		m.commits[name] = cs
	}
	if cs.loaded {
		return cs
	}
	data, err := os.ReadFile(commitPath(m.indexDir, repo, commit))
	if err != nil {
		cs.loaded = errors.Is(err, os.ErrNotExist)
		return cs
	}
	var man commitManifest
	if err := json.Unmarshal(data, &man); err != nil || man.Repo != repo || man.Commit != commit ||
		(man.Source != "" && pseudoCommit(repo, man.Source) != commit) {
		return cs
	}
	cs.loaded = true
	if cs.source == "" {
		cs.source = man.Source
	}
	for path, e := range man.Files {
		k := resolveKey{repo: repo, rev: commit, path: path}
		if e == nil || m.entries[k] != nil || m.tasks[k] != nil {
			continue // memory, including an in-flight task, outranks a late load
		}
		e.State = stateReady
		m.entries[k] = e
		cs.publish(path, e)
	}
	return cs
}

// Source must reach disk before the branch pointer.
func (m *Mirror) ensureSource(repo, commit, source string) {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()

	m.mu.Lock()
	cs := m.loadCommit(repo, commit)
	changed := cs.source == ""
	if changed {
		cs.source = source
	}
	m.mu.Unlock()
	if changed {
		_ = m.persistCommitLocked(repo, commit)
	}
}

// persistCommit rewrites one manifest from the ready entries in memory; persistMu serializes snapshots and writes.
func (m *Mirror) persistCommit(repo, commit string) error {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	return m.persistCommitLocked(repo, commit)
}

func (m *Mirror) persistCommitLocked(repo, commit string) error {
	m.mu.Lock()
	cs := m.loadCommit(repo, commit)
	if !cs.loaded {
		m.mu.Unlock()
		return errManifestUnread
	}
	man := commitManifest{Repo: repo, Commit: commit, Source: cs.source, Files: make(map[string]*fileEntry, len(cs.files))}
	for path, e := range cs.files {
		c := *e
		man.Files[path] = &c
	}
	m.mu.Unlock()

	path := commitPath(m.indexDir, repo, commit)
	if len(man.Files) == 0 && man.Source == "" {
		err := os.Remove(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return writeJSON(path, man)
}

// writeJSON replaces path atomically through a same-directory temp file.
func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	tmp := filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp")
	err = os.WriteFile(tmp, data, 0644)
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}
