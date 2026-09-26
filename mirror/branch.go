package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// branchEntry pins a branch rev to a 40-hex commit: the upstream's own or the synthesized pseudo commit.
type branchEntry struct {
	Commit    string    `json:"commit"`
	CheckedAt time.Time `json:"checked_at"`

	failures  int
	nextRetry time.Time
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

// The caller holds m.mu; transient read faults remain retryable.
func (m *Mirror) loadBranch(repo, rev string) *branchEntry {
	name := repo + "\x00" + rev
	if b, ok := m.branches[name]; ok {
		return b
	}
	data, err := os.ReadFile(branchEntryPath(m.indexDir, repo, rev))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			m.branches[name] = nil
		}
		return nil
	}
	var b branchEntry
	if err := json.Unmarshal(data, &b); err != nil || !commitRevRe.MatchString(b.Commit) {
		m.branches[name] = nil
		return nil
	}
	m.branches[name] = &b
	return &b
}

// branchProbe is the singleflight result of a branch refresh: the mapping, or the failure when none could be pinned.
type branchProbe struct {
	b  *branchEntry
	pr *probeResult // handed to the ingest task so the upstream is probed once
	fe *fileEntry
}

// The caller holds m.mu; two nil results mean a probe is due.
func (m *Mirror) branchState(key resolveKey) (*branchEntry, *fileEntry) {
	b := m.loadBranch(key.repo, key.rev)
	fe := m.entries[key]
	if fe.inBackoff() {
		return nil, fe
	}
	if b != nil && (!m.branchStale(b) || (fe == nil && time.Now().Before(b.nextRetry))) {
		return b, nil
	}
	return nil, nil
}

// The caller holds m.mu.
func (m *Mirror) branchBackoff(key resolveKey) bool {
	b := m.branches[key.repo+"\x00"+key.rev]
	return m.entries[key].inBackoff() || (b != nil && time.Now().Before(b.nextRetry))
}

// Rate limiting is transient, unlike other 4xx responses.
func authoritative(pr *probeResult) bool {
	return pr != nil && pr.status >= 400 && pr.status < 500 && pr.status != http.StatusTooManyRequests
}

// branchCommit resolves a branch rev to its pinned commit, probing origin with token when stale; only a 2xx probe pins.
func (m *Mirror) branchCommit(origin, token string, key resolveKey) (string, *probeResult, *fileEntry) {
	name := key.repo + "\x00" + key.rev
	m.mu.Lock()
	b, fe := m.branchState(key)
	m.mu.Unlock()
	if fe != nil {
		return "", nil, fe
	}
	if b != nil {
		return b.Commit, nil, nil
	}

	v, _, _ := m.flight.Do("branch\x00"+key.String(), func() (any, error) {
		m.mu.Lock()
		b, fe := m.branchState(key)
		prev := m.entries[key] // expired failure this probe retries
		m.mu.Unlock()
		if fe != nil {
			return &branchProbe{fe: fe}, nil // recorded between the caller's check and this flight
		}
		if b != nil {
			return &branchProbe{b: b}, nil
		}
		// Background context: the probe is shared by every requester of the file, so one disconnect must not fail it.
		pr, err := m.probe(withUpstreamAuth(context.Background(), origin, token), origin, key)
		if err == nil {
			err = probeErr(pr)
		}
		if err != nil {
			m.mu.Lock()
			defer m.mu.Unlock()
			// A transport fault, 5xx or 429 keeps an existing pin serving between bounded retries.
			if b := m.branches[name]; b != nil && !authoritative(pr) && prev == nil {
				b.failures++
				b.nextRetry = time.Now().Add(retryBackoff(b.failures))
				return &branchProbe{b: b}, nil
			}
			return &branchProbe{fe: m.recordFailure(key, err)}, nil
		}
		if pr.synthetic {
			m.ensureSource(key.repo, pr.commit, key.rev)
		}
		nb := &branchEntry{Commit: pr.commit, CheckedAt: time.Now()}
		m.mu.Lock()
		if m.entries[key] == prev {
			delete(m.entries, key)
		}
		m.branches[name] = nb
		m.mu.Unlock()
		_ = m.persistBranch(key.repo, key.rev)
		return &branchProbe{b: nb, pr: pr}, nil
	})
	bp := v.(*branchProbe)
	if bp.b == nil {
		return "", nil, bp.fe
	}
	return bp.b.Commit, bp.pr, nil
}

func branchEntryPath(dir, repo, rev string) string {
	sum := sha256.Sum256([]byte(rev))
	return filepath.Join(repoDir(dir, repo), "branches", hex.EncodeToString(sum[:])+".json")
}

// persistBranch writes one branch pointer; persistMu keeps disk in step with memory.
func (m *Mirror) persistBranch(repo, rev string) error {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	m.mu.Lock()
	b := *m.branches[repo+"\x00"+rev]
	m.mu.Unlock()
	return writeJSON(branchEntryPath(m.indexDir, repo, rev), b)
}
