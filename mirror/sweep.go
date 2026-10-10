package mirror

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/wzshiming/xet/storage"
)

// SweepResult reports one SweepIndex pass; in a dry run the counts are what a real pass would remove.
type SweepResult struct {
	DryRun           bool `json:"dry_run"`
	DroppedEntries   int  `json:"dropped_entries"`
	RemovedManifests int  `json:"removed_manifests"`
	RemovedTempFiles int  `json:"removed_temp_files"`
}

// Sweep drops index entries whose file is gone from storage, removes
// the manifests that leaves empty, and deletes the temp files interrupted
// index writes left behind before opts.Grace (zero: storage.DefaultSweepGrace;
// negative: any age); opts.DryRun only reports. Manifests no memory state
// holds are judged against storage on disk and never loaded. Branch pointers
// are never removed: a stale pin keeps serving while the upstream is down.
func (m *Mirror) Sweep(ctx context.Context, opts storage.SweepOptions) (SweepResult, error) {
	res := SweepResult{DryRun: opts.DryRun}
	grace := opts.Grace
	if grace == 0 {
		grace = storage.DefaultSweepGrace
	}
	err := m.walkIndex(ctx, func(sub, p string, ent fs.DirEntry) error {
		switch {
		case strings.HasPrefix(ent.Name(), ".") && strings.HasSuffix(ent.Name(), ".tmp"):
			removed, err := m.sweepTempFile(p, grace, opts.DryRun)
			if err != nil {
				return err
			}
			if removed {
				res.RemovedTempFiles++
			}
		case sub == "commits" && strings.HasSuffix(ent.Name(), ".json"):
			return m.sweepManifest(ctx, p, opts.DryRun, &res)
		}
		return nil
	})
	return res, err
}

// walkIndex calls fn for each regular file of a repo's commits or branches directory, named by that directory; a missing index holds none.
func (m *Mirror) walkIndex(ctx context.Context, fn func(sub, path string, ent fs.DirEntry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	repos, err := os.ReadDir(m.indexDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, repo := range repos {
		if !repo.IsDir() {
			continue
		}
		for _, sub := range []string{"commits", "branches"} {
			dir := filepath.Join(m.indexDir, repo.Name(), sub)
			ents, err := os.ReadDir(dir)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return err
			}
			for _, ent := range ents {
				if err := ctx.Err(); err != nil {
					return err
				}
				if !ent.Type().IsRegular() {
					continue
				}
				if err := fn(sub, filepath.Join(dir, ent.Name()), ent); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// sweepTempFile removes (or, dry, reports) a temp file older than grace. Removal runs under persistMu, where every writeJSON runs: a temp file that exists then is a leftover, not a write in flight.
func (m *Mirror) sweepTempFile(p string, grace time.Duration, dryRun bool) (bool, error) {
	if !dryRun {
		m.persistMu.Lock()
		defer m.persistMu.Unlock()
	}
	info, err := os.Stat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if grace > 0 && time.Since(info.ModTime()) < grace {
		return false, nil
	}
	if dryRun {
		return true, nil
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	return true, nil
}

// sweepManifest drops the dead entries of one manifest into res; GC does not judge corruption, so unparsable or misplaced manifests are left alone.
func (m *Mirror) sweepManifest(ctx context.Context, p string, dryRun bool, res *SweepResult) error {
	data, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var man commitManifest
	if json.Unmarshal(data, &man) != nil || !isCommit(man.Commit) || commitPath(m.indexDir, man.Repo, man.Commit) != p {
		return nil
	}
	name := revKey{repo: man.Repo, rev: man.Commit}
	m.mu.Lock()
	cs := m.commits[name]
	if cs == nil {
		m.mu.Unlock()
		return m.sweepDiskManifest(ctx, p, man, dryRun, res)
	}
	files, source, loaded := maps.Clone(cs.files), cs.source, cs.loaded
	// Disk entries memory did not hold before this pass are what an earlier failed rewrite left; a pending ingest is not one of them. The caller holds m.mu.
	stale := func(disk commitManifest, cs *commitState) int {
		n := 0
		for path := range disk.Files {
			key := resolveKey{repo: man.Repo, rev: man.Commit, path: path}
			_, snapped := files[path]
			_, held := cs.files[path]
			if !snapped && !held && m.entries[key] == nil && m.tasks[key] == nil {
				n++
			}
		}
		return n
	}
	wasStale := stale(man, cs)
	m.mu.Unlock()
	if !loaded {
		return nil
	}
	var dead []string
	for path, e := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !m.entryLive(ctx, e) {
			dead = append(dead, path)
		}
	}
	if dryRun {
		res.DroppedEntries += len(dead) + wasStale
		if len(dead) == len(files) && source == "" {
			res.RemovedManifests++
		}
		return nil
	}

	// Under persistMu the manifest written is exactly the snapshot taken here, so the counts describe that write.
	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	// Another writer may have rewritten or removed the manifest meanwhile; stale entries are judged against disk as it is now.
	data, err = os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var disk commitManifest
	if json.Unmarshal(data, &disk) != nil || disk.Repo != man.Repo || disk.Commit != man.Commit {
		return nil
	}
	m.mu.Lock()
	// A state gone meanwhile left nothing resident to judge; the next pass sees the disk copy.
	if cs = m.commits[name]; cs == nil {
		m.mu.Unlock()
		return nil
	}
	dropped := 0
	for _, path := range dead {
		key := resolveKey{repo: man.Repo, rev: man.Commit, path: path}
		// A re-ingest since the liveness check replaced the entry; only the one checked is dropped.
		if m.entries[key] == files[path] {
			delete(m.entries, key)
			delete(cs.files, path)
			dropped++
		}
	}
	nowStale := stale(disk, cs)
	cs, snap, ok := m.snapshotCommit(man.Repo, man.Commit)
	m.mu.Unlock()
	if !ok || (dropped == 0 && nowStale == 0 && (len(snap.Files) > 0 || snap.Source != "")) {
		return nil
	}
	removed, err := m.writeCommit(cs, snap)
	if err != nil {
		return err
	}
	res.DroppedEntries += dropped + nowStale
	if removed {
		res.RemovedManifests++
	}
	return nil
}

// sweepDiskManifest judges a manifest no memory state holds from its disk entries, loading nothing; a commit a writer opens meanwhile is left to the next pass, which judges it through memory.
func (m *Mirror) sweepDiskManifest(ctx context.Context, p string, man commitManifest, dryRun bool, res *SweepResult) error {
	var dead []string
	for path, e := range man.Files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e == nil || !m.entryLive(ctx, e) {
			dead = append(dead, path)
		}
	}
	if len(dead) == 0 && (len(man.Files) > 0 || man.Source != "") {
		return nil
	}
	if dryRun {
		res.DroppedEntries += len(dead)
		if len(dead) == len(man.Files) && man.Source == "" {
			res.RemovedManifests++
		}
		return nil
	}

	m.persistMu.Lock()
	defer m.persistMu.Unlock()
	name := revKey{repo: man.Repo, rev: man.Commit}
	m.mu.Lock()
	_, resident := m.commits[name]
	m.mu.Unlock()
	if resident {
		return nil
	}
	// Accepted window: a loadCommit racing between that check and the write below holds the dead entry until the next pass, which judges it resident.
	data, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var disk commitManifest
	if json.Unmarshal(data, &disk) != nil || disk.Repo != man.Repo || disk.Commit != man.Commit {
		return nil
	}
	dropped := 0
	for _, path := range dead {
		cur, ok := disk.Files[path]
		judged := man.Files[path]
		// A rewrite since the liveness check replaced the entry; only the one checked is dropped.
		if !ok || (cur == nil) != (judged == nil) || (cur != nil && cur.FileHash != judged.FileHash) {
			continue
		}
		delete(disk.Files, path)
		dropped++
	}
	if dropped == 0 && (len(disk.Files) > 0 || disk.Source != "") {
		return nil
	}
	if len(disk.Files) == 0 && disk.Source == "" {
		err := os.Remove(p)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err == nil {
			res.RemovedManifests++
		}
	} else if err := writeJSON(p, disk); err != nil {
		return err
	}
	res.DroppedEntries += dropped
	return nil
}
