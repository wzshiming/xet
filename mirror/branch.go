package mirror

import (
	"context"
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

func (m *Mirror) branchDir() string {
	return filepath.Join(m.indexDir, "branches")
}

// branchStale reports whether a branch mapping must be re-checked, following
// the revalidation cadence: zero interval re-checks every request, negative
// never does.
func (m *Mirror) branchStale(b *branchEntry) bool {
	if m.revalidateInterval < 0 {
		return false
	}
	return time.Since(b.CheckedAt) >= m.revalidateInterval
}

// branchProbe is the singleflight result of a branch mapping refresh. The
// probe result is handed back to the caller whose key was probed so its
// ingest task can reuse it instead of probing again.
type branchProbe struct {
	b   *branchEntry
	key resolveKey
	pr  *probeResult
}

// branchCommit resolves a branch revision to the upstream commit it points
// at, probing the upstream when the cached mapping is stale. It reports
// ok=false when no real commit is available (upstreams that synthesize
// pseudo-commits keep the branch-keyed flow with per-entry revalidation);
// probe failures fall back to the last known commit so cached content stays
// served while the upstream is unreachable. The returned probe result is
// non-nil when this call probed the upstream for exactly this key.
func (m *Mirror) branchCommit(key resolveKey) (string, *probeResult, bool) {
	name := key.repo + "\x00" + key.rev
	m.mu.Lock()
	b := m.branches[name]
	m.mu.Unlock()
	if b != nil && !m.branchStale(b) {
		return b.Commit, nil, b.Commit != ""
	}

	v, _, _ := m.flight.Do("branch\x00"+name, func() (any, error) {
		m.mu.Lock()
		b := m.branches[name]
		m.mu.Unlock()
		if b != nil && !m.branchStale(b) {
			return &branchProbe{b: b}, nil
		}
		// Background context: the probe result is shared by every request of
		// the repo branch, so one disconnecting client must not fail it.
		pr, err := m.probe(context.Background(), key.String())
		if err != nil {
			return &branchProbe{b: b}, nil // unreachable upstream: serve the last known commit
		}
		nb := &branchEntry{Repo: key.repo, Rev: key.rev, CheckedAt: time.Now()}
		if pr.realCommit && commitRevRe.MatchString(pr.commit) {
			nb.Commit = pr.commit
		}
		m.mu.Lock()
		m.branches[name] = nb
		m.mu.Unlock()
		_ = persistBranch(m.branchDir(), nb)
		return &branchProbe{b: nb, key: key, pr: pr}, nil
	})
	bp := v.(*branchProbe)
	var pr *probeResult
	if bp.key == key {
		pr = bp.pr
	}
	if bp.b != nil {
		return bp.b.Commit, pr, bp.b.Commit != ""
	}
	return "", pr, false
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
	if before, ok := strings.CutSuffix(s, ".json"); ok {
		s = before + "%2Ejson"
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
