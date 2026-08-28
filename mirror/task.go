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
	spool    *spool        // set by runTask before probed closes (when the probe succeeded)
	probed   chan struct{} // closed once probe metadata (or probeErr) is set
	sized    chan struct{} // closed once size is known or no early source remains
	done     chan struct{} // closed once the task finished and its entry is published
	sizeOnce sync.Once
	probeErr error
	notFound bool
	probe    *probeResult
	size     atomic.Int64 // final content length, -1 until known
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
// handed to the task so the upstream is not probed twice.
func (m *Mirror) startTask(key resolveKey, pre *probeResult) (*task, *fileEntry, error) {
	v, err, _ := m.flight.Do(key.String(), func() (any, error) {
		// A previous flight may have registered a task, or finished the whole
		// ingest, between the caller's check and this one.
		m.mu.Lock()
		t := m.tasks[key]
		e := m.entries[key]
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
		nt := &task{probed: make(chan struct{}), sized: make(chan struct{}), done: make(chan struct{})}
		nt.size.Store(-1)
		m.mu.Lock()
		m.tasks[key] = nt
		m.mu.Unlock()
		go m.runTask(key, nt, pre)
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
// pin. ctx bounds only the revalidation probe.
func (m *Mirror) acquire(ctx context.Context, key resolveKey) (resolveKey, *task, *fileEntry, error) {
	var preProbe *probeResult
	if !commitRevRe.MatchString(key.rev) {
		commit, pr, ok := m.branchCommit(key)
		preProbe = pr
		if ok {
			key.rev = commit
		}
	}

	m.mu.Lock()
	t := m.tasks[key]
	e := m.entries[key]
	m.mu.Unlock()

	if t != nil {
		return key, t, nil, nil
	}
	if e != nil {
		switch e.State {
		case stateReady:
			if m.needsRevalidate(e, key.rev) {
				e = m.revalidate(ctx, key, e)
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

	t, e, err := m.startTask(key, preProbe)
	return key, t, e, err
}

// runTask executes one ingestion end to end on a background context; client
// disconnects never cancel it. The spool opens after the probe so partial
// bytes from a previous failed task (or a previous process) are resumed when
// the upstream etag still matches. A non-nil pre stands in for the probe.
func (m *Mirror) runTask(key resolveKey, t *task, pre *probeResult) {
	ctx := context.Background()
	upath := key.String()
	defer close(t.done) // the entry is published by then, on every path

	pr, err := pre, error(nil)
	if pr == nil {
		pr, err = m.probe(ctx, upath)
	}
	if err == nil {
		switch {
		case pr.status == http.StatusNotFound:
			err = ErrUpstreamNotFound
		case pr.status < 200 || pr.status >= 300:
			err = fmt.Errorf("upstream status %d", pr.status)
		}
	}
	if err != nil {
		t.probeErr = err
		t.notFound = errors.Is(err, ErrUpstreamNotFound)
		close(t.probed)
		m.failTask(key, t, err)
		return
	}

	sp, err := openSpool(m.spoolDir, upath, pr.etag, pr.size)
	if err != nil {
		t.probeErr = err
		close(t.probed)
		m.failTask(key, t, err)
		return
	}
	t.spool = sp
	sp.acquire() // held by runTask until ingest completes
	defer sp.release()

	t.probe = pr
	if pr.size >= 0 {
		t.setSize(pr.size)
	}
	close(t.probed)
	defer t.setSize(-1) // unblock size waiters at the latest when the task ends

	switch {
	case pr.size >= 0 && sp.size() == pr.size:
		// A previous task already spooled the whole file (e.g. it failed
		// between fetch and ingest); skip the refetch.
	case pr.xet:
		// The xet ingest reports no size before completion; when the probe
		// found none there is no early source, so stop replies waiting for one.
		t.setSize(-1)
		err = m.fetchXet(ctx, t, upath)
	default:
		err = m.fetchPlain(ctx, t, upath)
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
		m.failTask(key, t, err)
		return
	}
	t.size.Store(t.spool.size())
	t.setSize(-1) // definitive size stored above; signal any waiters
	t.spool.finish(nil)

	entry, err := m.ingestSpool(ctx, t, key)
	if err != nil {
		if errors.Is(err, errSpoolCorrupt) {
			t.spool.markRemove()
		}
		m.failTask(key, t, err)
		return
	}

	m.mu.Lock()
	m.entries[key] = entry
	delete(m.tasks, key)
	m.mu.Unlock()
	t.spool.markRemove() // bytes now live in storage; drop the spool when drained
}

// failTask records a failure with exponential backoff and clears the task.
func (m *Mirror) failTask(key resolveKey, t *task, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	failures := 1
	if prev := m.entries[key]; prev != nil && prev.State == stateFailed {
		failures = prev.failures + 1
	}
	shift := min(failures-1, maxFailureShift)
	backoff := min(failureBackoffBase<<shift, failureBackoffCap)
	m.entries[key] = &fileEntry{
		Key:       key.String(),
		State:     stateFailed,
		failures:  failures,
		nextRetry: time.Now().Add(backoff),
		lastErr:   err,
		notFound:  t.notFound,
	}
	delete(m.tasks, key)
}

// ingestSpool verifies the spooled bytes and runs the standard upload pipeline
// against local storage, then returns the ready index entry.
func (m *Mirror) ingestSpool(ctx context.Context, t *task, key resolveKey) (*fileEntry, error) {
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
		Key:       key.String(),
		State:     stateReady,
		SHA256:    digest,
		Size:      size,
		ETag:      t.probe.etag,
		Commit:    t.probe.commit,
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

	if err := persistEntry(m.indexDir, entry); err != nil {
		return nil, err
	}
	return entry, nil
}
