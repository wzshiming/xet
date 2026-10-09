package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wzshiming/xet/storage"
	"github.com/wzshiming/xet/upload"
)

// task tracks one in-flight ingestion. All concurrent requests for the same
// key attach to it, so the upstream is downloaded exactly once per file. A
// task is created already holding its spool: its own, which it downloads
// into, or that of the leader downloading the same content of its origin,
// whose result it then takes instead of downloading again.
type task struct {
	key           resolveKey    // pinned (repo, commit, path) the entry publishes under
	src           resolveKey    // upstream resolve key: the source branch for pseudo commits
	origin, token string        // upstream credential of the resolver that started the task; later joiners never replace it
	prev          *fileEntry    // src failure this task retries, retired on success
	probe         *probeResult  // upstream metadata, taken before the task is handed out
	spool         *spool        // the spool this task writes, or the leader's it reads; set before the task is handed out, never nil
	leader        *task         // the task whose spool and result this one takes; nil for a downloader
	lead          string        // the inflight id this task holds for its followers, "" otherwise
	sized         chan struct{} // closed once size is known or no early source remains
	done          chan struct{} // closed once the task finished and its entry is published
	sizeOnce      sync.Once
	size          atomic.Int64 // final content length, -1 until known
	entry         *fileEntry   // the published entry, set before done closes
	err           error        // the terminal failure, set before done closes
}

// setSize records the content length once known (first value wins) and
// unblocks resolve replies waiting on it; n < 0 only signals.
func (t *task) setSize(n int64) {
	if n >= 0 {
		t.size.CompareAndSwap(-1, n)
	}
	t.sizeOnce.Do(func() { close(t.sized) })
}

// startTask returns the in-flight ingestion task for key, or the entry when
// the ingest already completed (or failed and is backing off). Everything
// that decides what the task holds — the upstream probe, the held-content
// check, the follow decision and the spool open — runs inside the
// singleflight, so the task is registered exactly once per key and is always
// handed out with its spool; ctx bounds only the caller's wait for that
// flight. A pre-probe result from the branch mapping refresh stands in for
// the probe so the upstream is not probed twice; origin and token are the
// starter's upstream credential.
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
		nt := &task{key: key, src: src, origin: origin, token: token, prev: prev, sized: make(chan struct{}), done: make(chan struct{})}
		nt.size.Store(-1)

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

		var id string    // origin and content this task shares a download by
		var leader *task // the task downloading that content now, whether or not this one can follow it
		if pr.content != "" {
			id = origin + "\x00" + pr.content
			m.mu.Lock()
			leader = m.inflight[id]
			m.mu.Unlock()
			follow := leader != nil
			if follow && pr.size >= 0 {
				<-leader.sized // bounded by the leader's first response
				switch ls := leader.size.Load(); {
				case ls < 0:
					follow = false // unknown until the leader finishes: a body of a possibly wrong length is not served
				case ls != pr.size:
					// One content has one size: a probe disagreeing with the leader's is an upstream inconsistency, not something to serve.
					return m.failTask(nt, fmt.Errorf("upstream size %d disagrees with the shared download's %d", pr.size, ls)), nil
				}
			}
			if follow && leader.spool.acquire() {
				nt.spool, nt.leader = leader.spool, leader
			}
		}

		switch {
		case pr.size >= 0:
			nt.setSize(pr.size)
		case nt.leader != nil:
			select {
			case <-nt.leader.sized:
				nt.setSize(nt.leader.size.Load())
			default:
			}
		case pr.xet:
			// Xet provides no earlier size source when the probe omits it.
			nt.setSize(-1)
		}

		if nt.leader != nil {
			m.mu.Lock()
			m.tasks[key] = nt
			m.mu.Unlock()
			go m.runTask(nt)
			return nt, nil
		}
		// Registered under spoolMu, so SweepSpools never sees the spool file without the task holding it, and leaders register one at a time.
		m.spoolMu.Lock()
		// One writer per content spool, its leader: a task downloading beside a leader it cannot follow, or one registered since the lookup, keeps its spool private.
		m.mu.Lock()
		shared := id != "" && leader == nil && m.inflight[id] == nil
		m.mu.Unlock()
		sp, err := openSpool(m.spoolDir, origin, src.String(), pr.etag, pr.size, shared)
		if err != nil {
			m.spoolMu.Unlock()
			return m.failTask(nt, err), nil
		}
		sp.acquire() // held until runTask ends
		nt.spool = sp
		m.mu.Lock()
		m.tasks[key] = nt
		if shared {
			m.inflight[id] = nt
			nt.lead = id
		}
		m.mu.Unlock()
		m.spoolMu.Unlock()
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
	if !commitRevRe.MatchString(key.rev) {
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
// the task's pinned credential; client disconnects never cancel it. A
// follower waits for its leader and settles on its result; a downloader
// fetches into its spool — resuming the partial bytes a previous failed task
// (or a previous process) left under the same origin and etag — and ingests
// it into storage.
func (m *Mirror) runTask(t *task) {
	ctx := withUpstreamAuth(context.Background(), t.origin, t.token)
	defer func() {
		// Runs after done closes, so a task found in inflight always has a result to wait for.
		if t.lead == "" {
			return
		}
		m.mu.Lock()
		if m.inflight[t.lead] == t {
			delete(m.inflight, t.lead)
		}
		m.mu.Unlock()
	}()
	defer close(t.done) // the entry is published by then, on every path
	defer t.setSize(-1) // unblock size waiters at the latest when the task ends
	defer t.spool.release()

	pr := t.probe
	if leader := t.leader; leader != nil {
		if pr.size < 0 {
			<-leader.sized
			t.setSize(leader.size.Load())
		}
		<-leader.done
		m.settle(t, pr, leader)
		return
	}

	m.ingestSlots <- struct{}{}
	defer func() { <-m.ingestSlots }()

	var err error
	switch {
	case pr.size >= 0 && t.spool.size() == pr.size:
		// A previous task already spooled the whole file (e.g. it failed
		// between fetch and ingest); skip the refetch.
	case pr.xet:
		err = m.fetchXet(ctx, t, t.src)
	default:
		err = m.fetchPlain(ctx, t, t.src)
	}
	if err == nil {
		if want := t.size.Load(); want >= 0 && t.spool.size() != want {
			err = fmt.Errorf("upstream size mismatch: got %d bytes, want %d", t.spool.size(), want)
			if t.spool.size() > want {
				t.spool.markRemove()
			}
		}
	}
	if err != nil {
		if pr.etag == "" {
			t.spool.markRemove() // etag-less spools are truncated on reopen: nothing to resume
		}
		t.spool.finish(err)
		m.failTask(t, err)
		return
	}
	t.size.Store(t.spool.size())
	t.setSize(-1) // definitive size stored above; signal any waiters
	t.spool.finish(nil)

	entry, err := m.ingestSpool(ctx, t)
	if err != nil {
		if errors.Is(err, errSpoolCorrupt) || pr.etag == "" {
			t.spool.markRemove()
		}
		m.failTask(t, err)
		return
	}
	m.publish(t, entry)
	t.spool.markRemove() // bytes now live in storage; drop the spool when drained
}

// heldEntry returns the ready entry for content local storage already holds
// under the probed sha256, or nil when the file must be downloaded: nothing
// is held, the held file is empty, or the upstream size disagrees with it.
func (m *Mirror) heldEntry(ctx context.Context, pr *probeResult) *fileEntry {
	digest, err := hex.DecodeString(pr.sha256)
	if err != nil || len(digest) != sha256.Size {
		return nil
	}
	fileHash, err := m.storage.GetFileHashBySHA256(ctx, "default", [sha256.Size]byte(digest))
	if err != nil {
		return nil
	}
	sh, err := m.storage.GetShard(ctx, fileHash)
	if err != nil {
		return nil
	}
	file := storage.FindFileBySHA256(sh, [sha256.Size]byte(digest))
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

// settle ends a follower with the outcome of its finished leader: the
// leader's failure, under the follower's own backoff, or the leader's entry
// when it matches what the follower's upstream advertised.
func (m *Mirror) settle(t *task, pr *probeResult, leader *task) {
	err := leader.err
	if err == nil && pr.size >= 0 && pr.size != leader.entry.Size {
		err = fmt.Errorf("upstream size mismatch: got %d bytes, want %d", leader.entry.Size, pr.size)
	}
	if err == nil && pr.sha256 != "" && pr.sha256 != leader.entry.SHA256 {
		err = fmt.Errorf("%w: sha256 mismatch: shared download is %s, upstream advertised %s", errSpoolCorrupt, leader.entry.SHA256, pr.sha256)
	}
	if err != nil {
		m.failTask(t, err)
		return
	}
	le := leader.entry
	m.publish(t, &fileEntry{State: stateReady, FileHash: le.FileHash, SHA256: le.SHA256, Size: le.Size, ETag: pr.etag, CheckedAt: time.Now()})
	t.size.Store(le.Size)
	t.setSize(-1)
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

// ingestSpool verifies the spooled bytes and runs the standard upload pipeline
// against local storage, then returns the ready entry.
func (m *Mirror) ingestSpool(ctx context.Context, t *task) (*fileEntry, error) {
	f, err := os.Open(t.spool.f.Name())
	if err != nil {
		return nil, fmt.Errorf("open spool: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	hasher := sha256.New()
	size, err := io.Copy(hasher, f)
	if err != nil {
		return nil, fmt.Errorf("hash spool: %w", err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if t.probe.sha256 != "" && digest != t.probe.sha256 {
		return nil, fmt.Errorf("%w: sha256 mismatch: got %s, upstream advertised %s", errSpoolCorrupt, digest, t.probe.sha256)
	}

	entry := &fileEntry{
		State:     stateReady,
		SHA256:    digest,
		Size:      size,
		ETag:      t.probe.etag,
		CheckedAt: time.Now(),
	}

	if size > 0 {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind spool: %w", err)
		}
		fileHash, err := upload.UploadFile(ctx, m.localAdapter, f,
			upload.WithEnableSHA256(true),
			upload.WithConcurrency(4),
			upload.WithCacheManager(m.cache.Upload),
		)
		if err != nil {
			return nil, fmt.Errorf("ingest into storage: %w", err)
		}
		entry.FileHash = fileHash.String()
	}
	return entry, nil
}
