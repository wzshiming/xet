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

	"github.com/wzshiming/xet/upload"
)

// task tracks one in-flight ingestion. All concurrent requests for the same
// key attach to it, so the upstream is downloaded exactly once per file.
type task struct {
	key           resolveKey    // pinned (repo, commit, path) the entry publishes under
	src           resolveKey    // upstream resolve key: the source branch for pseudo commits
	origin, token string        // upstream credential of the resolver that started the task; later joiners never replace it
	prev          *fileEntry    // src failure this task retries, retired on success
	spool         *spool        // set by runTask before probed closes (when the probe succeeded)
	probed        chan struct{} // closed once probe metadata (or probeErr) is set
	sized         chan struct{} // closed once size is known or no early source remains
	done          chan struct{} // closed once the task finished and its entry is published
	sizeOnce      sync.Once
	probeErr      error
	probe         *probeResult
	size          atomic.Int64 // final content length, -1 until known
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
// the ingest already completed (or failed and is backing off). Only the task
// startup itself runs inside the singleflight, so the task is registered
// exactly once per key. A pre-probe result from the branch mapping refresh is
// handed to the task so the upstream is not probed twice; origin and token
// are the starter's upstream credential.
func (m *Mirror) startTask(key, src resolveKey, pre *probeResult, origin, token string) (*task, *fileEntry, error) {
	v, err, _ := m.flight.Do(key.String(), func() (any, error) {
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
		nt := &task{key: key, src: src, origin: origin, token: token, prev: prev, probed: make(chan struct{}), sized: make(chan struct{}), done: make(chan struct{})}
		nt.size.Store(-1)
		m.mu.Lock()
		m.tasks[key] = nt
		m.mu.Unlock()
		go m.runTask(nt, pre)
		return nt, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if t, ok := v.(*task); ok {
		return t, nil, nil
	}
	return nil, v.(*fileEntry), nil
}

// acquire is the shared resolution flow behind both Resolve and Ingest: it
// pins branch revisions to their upstream commit and returns what the request
// attaches to — the in-flight task, or the terminal entry (ready,
// revalidated on the usual cadence, or failed and still inside its retry
// backoff) — starting a new ingest task when neither exists. Exactly one of
// the returned task and entry is non-nil; the returned key carries the branch
// pin. origin and token name the caller's upstream; ctx bounds only the
// revalidation probe.
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

	t, e, err := m.startTask(key, src, pre, origin, token)
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
// the task's pinned credential; client disconnects never cancel it. The spool
// opens after the probe so partial bytes from a previous failed task (or a
// previous process) are resumed when the upstream etag still matches. A
// non-nil pre stands in for the probe.
func (m *Mirror) runTask(t *task, pre *probeResult) {
	ctx := withUpstreamAuth(context.Background(), t.origin, t.token)
	upath := t.src.String()
	defer close(t.done) // the entry is published by then, on every path

	pr, err := pre, error(nil)
	if pr == nil {
		pr, err = m.probe(ctx, t.origin, t.src)
	}
	if err == nil {
		err = probeErr(pr)
	}
	if err != nil {
		t.probeErr = err
		close(t.probed)
		m.failTask(t, err)
		return
	}

	sp, err := openSpool(m.spoolDir, upath, pr.etag, pr.size)
	if err != nil {
		t.probeErr = err
		close(t.probed)
		m.failTask(t, err)
		return
	}
	t.spool = sp
	sp.acquire() // held by runTask until ingest completes
	defer sp.release()

	t.probe = pr
	if pr.size >= 0 {
		t.setSize(pr.size)
	} else if pr.xet {
		// Xet provides no earlier size source when the probe omits it.
		t.setSize(-1)
	}
	close(t.probed)
	defer t.setSize(-1) // unblock size waiters at the latest when the task ends

	m.ingestSlots <- struct{}{}
	defer func() { <-m.ingestSlots }()

	switch {
	case pr.size >= 0 && sp.size() == pr.size:
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
		t.spool.finish(err)
		m.failTask(t, err)
		return
	}
	t.size.Store(t.spool.size())
	t.setSize(-1) // definitive size stored above; signal any waiters
	t.spool.finish(nil)

	entry, err := m.ingestSpool(ctx, t)
	if err != nil {
		if errors.Is(err, errSpoolCorrupt) {
			t.spool.markRemove()
		}
		m.failTask(t, err)
		return
	}

	m.mu.Lock()
	if m.entries[t.src] == t.prev {
		delete(m.entries, t.src)
	}
	m.entries[t.key] = entry
	m.loadCommit(t.key.repo, t.key.rev).publish(t.key.path, entry)
	delete(m.tasks, t.key)
	m.mu.Unlock()
	_ = m.persistCommit(t.key.repo, t.key.rev) // memory is authoritative; disk is best effort
	t.spool.markRemove()                       // bytes now live in storage; drop the spool when drained
}

// The caller holds m.mu; a newly discovered source invalidates source-less failures.
func (m *Mirror) lookup(key, src resolveKey) *fileEntry {
	e := m.entries[key]
	if src != key && e != nil && e.State == stateFailed && e.source != src.rev {
		return m.entries[src]
	}
	return e
}

// A late failure must not replace the failure state of a newer branch pin.
func (m *Mirror) failTask(t *task, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tasks, t.key)
	if t.src != t.key {
		if b := m.loadBranch(t.src.repo, t.src.rev); b == nil || b.Commit == t.key.rev {
			failure := m.recordFailure(t.src, err)
			failure.source = t.src.rev
			m.entries[t.key] = failure
			return
		}
	}
	m.recordFailure(t.key, err).source = t.src.rev
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
	return e
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
		)
		if err != nil {
			return nil, fmt.Errorf("ingest into storage: %w", err)
		}
		entry.FileHash = fileHash.String()
	}
	return entry, nil
}
