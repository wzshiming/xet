package mirror

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/wzshiming/xet/mirror/spool"
	"github.com/wzshiming/xet/storage"
)

// task tracks one in-flight ingestion. All concurrent requests for the same
// key attach to it, so the upstream is downloaded exactly once per file. A
// task is created already holding its spool Item — the writer of its own
// download, or a follower of the flight already downloading the same
// content — whose result it publishes.
type task struct {
	key           resolveKey    // pinned (repo, commit, path) the entry publishes under
	src           resolveKey    // upstream resolve key: the source branch for pseudo commits
	origin, token string        // upstream credential of the resolver that started the task; later joiners never replace it
	prev          *fileEntry    // src failure this task retries, retired on success
	probe         *probeResult  // upstream metadata, taken before the task is handed out
	item          *spool.Item   // the task's handle on its flight; set before the task is handed out, never nil
	done          chan struct{} // closed once the task finished and its entry is published
	entry         *fileEntry    // the published entry, set before done closes
	err           error         // the terminal failure, set before done closes
}

// startTask returns the in-flight ingestion task for key, or the entry when
// the ingest already completed (or failed and is backing off). Everything
// that decides what the task holds — the upstream probe, the held-content
// check and the spool's Accept — runs inside the singleflight, so the task
// is registered exactly once per key and is always handed out with its
// Item; ctx bounds only the caller's wait for that flight. A pre-probe
// result from the branch mapping refresh stands in for the probe so the
// upstream is not probed twice; origin and token are the starter's upstream
// credential.
func (m *Mirror) startTask(ctx context.Context, key, src resolveKey, pre *probeResult, origin, token string) (*task, *fileEntry, error) {
	ch := m.flight.DoChan(key.String(), func() (any, error) {
		// A previous flight may have registered a task, or finished the whole
		// ingest, between the caller's check and this one.
		m.mu.Lock()
		t := m.tasks[key]
		e := m.lookup(key, src)
		prev := m.entries[src]
		m.mu.Unlock()
		if t != nil {
			return t, nil
		}
		if e != nil {
			if e.State == stateReady {
				return e, nil
			}
			if e.State == stateFailed && time.Now().Before(e.nextRetry) {
				return e, nil
			}
		}
		nt := &task{key: key, src: src, origin: origin, token: token, prev: prev, done: make(chan struct{})}

		// Background context: the probe is shared by every requester of the file, so one disconnect must not fail it.
		bg := withUpstreamAuth(context.Background(), origin, token)
		pr, err := pre, error(nil)
		if pr == nil {
			pr, err = m.probe(bg, origin, src)
		}
		if err == nil {
			err = probeErr(pr)
		}
		if err != nil {
			return m.failTask(nt, err), nil
		}
		nt.probe = pr

		if pr.sha256 != "" {
			if entry := m.heldEntry(bg, pr); entry != nil {
				m.publish(nt, entry)
				return entry, nil
			}
		}

		it, err := m.spool.Accept(bg, spool.Source{Origin: origin, Key: src.String(), ETag: pr.etag, Size: pr.size, SHA256: pr.sha256})
		if err != nil {
			return m.failTask(nt, err), nil
		}
		nt.item = it
		if pr.xet && pr.size < 0 {
			it.SetSize(-1) // xet provides no earlier size source when the probe omits it
		}
		m.mu.Lock()
		m.tasks[key] = nt
		m.mu.Unlock()
		go m.runTask(nt)
		return nt, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, nil, r.Err
		}
		if t, ok := r.Val.(*task); ok {
			return t, nil, nil
		}
		return nil, r.Val.(*fileEntry), nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// acquire is the shared resolution flow behind both Resolve and Ingest: it
// pins branch revisions to their upstream commit and returns what the request
// attaches to — the in-flight task, or the terminal entry (ready,
// revalidated on the usual cadence, or failed and still inside its retry
// backoff) — starting a new ingest task when neither exists. The upstream
// probe, the held-content publish and the task start run inside the per-key
// flight that start shares. Exactly one of the returned task and entry is
// non-nil; the returned key carries the branch pin. origin and token name the
// caller's upstream; ctx bounds only the revalidation probe and the caller's
// wait for the flight.
func (m *Mirror) acquire(ctx context.Context, origin, token string, key resolveKey) (resolveKey, *task, *fileEntry, error) {
	ctx = withUpstreamAuth(ctx, origin, token)
	var pre *probeResult
	if !isCommit(key.rev) {
		commit, pr, fe := m.branchCommit(origin, token, key)
		if fe != nil {
			return key, nil, fe, nil
		}
		key.rev, pre = commit, pr
	}

	m.mu.Lock()
	src := key
	if cs := m.loadCommit(key.repo, key.rev); cs.source != "" {
		src.rev = cs.source
	}
	t := m.tasks[key]
	e := m.lookup(key, src)
	stale := e != nil && e.State == stateReady && src != key && m.revalidateInterval >= 0 &&
		time.Since(e.CheckedAt) >= m.revalidateInterval && !m.branchBackoff(src)
	m.mu.Unlock()

	if t != nil {
		return key, t, nil, nil
	}
	if e != nil {
		switch e.State {
		case stateReady:
			if stale && !m.revalidate(ctx, origin, key, src, e, pre) {
				e = nil
			}
			if e != nil && !m.entryLive(ctx, e) {
				m.dropEntry(key, e)
				e = nil
			}
			if e != nil {
				return key, nil, e, nil
			}
			// stale or unlinked: fall through and re-ingest
		case stateFailed:
			if time.Now().Before(e.nextRetry) {
				return key, nil, e, nil
			}
		}
	}

	t, e, err := m.startTask(ctx, key, src, pre, origin, token)
	return key, t, e, err
}

// probeErr maps a non-2xx probe status to the ingest error; not-found matches ErrUpstreamNotFound.
func probeErr(pr *probeResult) error {
	switch {
	case pr.status == http.StatusNotFound:
		return ErrUpstreamNotFound
	case pr.status < 200 || pr.status >= 300:
		return fmt.Errorf("upstream status %d", pr.status)
	}
	return nil
}

// runTask executes one ingestion end to end on a background context carrying
// the task's pinned credential; client disconnects never cancel it. The
// writer fetches into its Item and finishes it; every task then waits for
// the flight's result and publishes it under its key.
func (m *Mirror) runTask(t *task) {
	ctx := withUpstreamAuth(context.Background(), t.origin, t.token)
	defer close(t.done) // the entry is published by then, on every path
	defer t.item.Release()

	pr := t.probe
	if t.item.Writer() {
		m.ingestSlots <- struct{}{}
		defer func() { <-m.ingestSlots }()

		var err error
		switch {
		case t.item.Size() >= 0 && t.item.Written() == t.item.Size():
			// A crashed process already spooled the whole file; skip the refetch.
		case pr.xet:
			err = m.fetchXet(ctx, t, t.src)
		default:
			err = m.fetchPlain(ctx, t, t.src)
		}
		t.item.Finish(ctx, err)
	}

	res, err := m.spool.Wait(ctx, t.item)
	if err != nil {
		m.failTask(t, err)
		return
	}
	m.publish(t, &fileEntry{State: stateReady, FileHash: res.FileHash, SHA256: res.SHA256, Size: res.Size, ETag: pr.etag, CheckedAt: time.Now()})
}

// heldEntry returns the ready entry for content local storage already holds
// under the probed sha256, or nil when the file must be downloaded: nothing
// is held, the held file is empty, or the upstream size disagrees with it.
func (m *Mirror) heldEntry(ctx context.Context, pr *probeResult) *fileEntry {
	fileHash, digest, ok := m.fileHashBySHA256(ctx, pr.sha256)
	if !ok {
		return nil
	}
	sh, err := m.storage.GetShard(ctx, fileHash)
	if err != nil {
		return nil
	}
	file := storage.FindFileBySHA256(sh, digest)
	if file == nil {
		return nil
	}
	var size int64
	for i := range file.Entries {
		size += int64(file.Entries[i].UnpackedSegBytes)
	}
	if size == 0 || (pr.size >= 0 && pr.size != size) {
		return nil
	}
	return &fileEntry{State: stateReady, FileHash: fileHash.String(), SHA256: pr.sha256, Size: size, ETag: pr.etag, CheckedAt: time.Now()}
}

// publish installs entry as the ready record of t's key, retiring the
// source failure the task retried, and persists the manifest.
func (m *Mirror) publish(t *task, entry *fileEntry) {
	m.mu.Lock()
	if m.entries[t.src] == t.prev {
		delete(m.entries, t.src)
		m.forgetFailed(t.src)
	}
	m.entries[t.key] = entry
	m.forgetFailed(t.key)
	m.openCommit(t.key.repo, t.key.rev).publish(t.key.path, entry)
	delete(m.tasks, t.key)
	t.entry = entry
	m.mu.Unlock()
	_ = m.persistCommit(t.key.repo, t.key.rev) // memory is authoritative; disk is best effort
}

// The caller holds m.mu; a newly discovered source invalidates source-less failures, and reading a failure counts as recent use.
func (m *Mirror) lookup(key, src resolveKey) *fileEntry {
	e := m.entries[key]
	if src != key && e != nil && e.State == stateFailed && e.source != src.rev {
		key, e = src, m.entries[src]
	}
	if e != nil && e.State == stateFailed {
		m.touchFailed(key)
	}
	return e
}

// failTask records err as the failure of t's key and returns that entry; a
// late failure must not replace the failure state of a newer branch pin.
func (m *Mirror) failTask(t *task, err error) *fileEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.err = err
	delete(m.tasks, t.key)
	if t.src != t.key {
		if b := m.loadBranch(t.src.repo, t.src.rev); b == nil || b.Commit == t.key.rev {
			failure := m.recordFailure(t.src, err)
			failure.source = t.src.rev
			m.entries[t.key] = failure
			m.touchFailed(t.key)
			return failure
		}
	}
	failure := m.recordFailure(t.key, err)
	failure.source = t.src.rev
	return failure
}

// recordFailure stores a process-local failed entry under key with exponential backoff. The caller holds m.mu.
func (m *Mirror) recordFailure(key resolveKey, err error) *fileEntry {
	failures := 1
	if prev := m.entries[key]; prev != nil && prev.State == stateFailed {
		failures = prev.failures + 1
	}
	e := &fileEntry{
		State:     stateFailed,
		failures:  failures,
		nextRetry: time.Now().Add(retryBackoff(failures)),
		lastErr:   err,
		notFound:  errors.Is(err, ErrUpstreamNotFound),
	}
	m.entries[key] = e
	m.touchFailed(key)
	return e
}

// touchFailed marks key's failure as recently used, evicting the least recently used ones past the cap. The caller holds m.mu.
func (m *Mirror) touchFailed(key resolveKey) {
	m.failed.Add(key, struct{}{})
}

// forgetFailed drops key from the failure LRU once a ready entry or a pin retired it; the eviction callback fires but finds no failed entry, since callers delete or replace it first. The caller holds m.mu.
func (m *Mirror) forgetFailed(key resolveKey) {
	m.failed.Remove(key)
}

func retryBackoff(failures int) time.Duration {
	shift := min(failures-1, maxFailureShift)
	return min(failureBackoffBase<<shift, failureBackoffCap)
}

func (e *fileEntry) inBackoff() bool {
	return e != nil && e.State == stateFailed && time.Now().Before(e.nextRetry)
}
