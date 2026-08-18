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

	// upstreamXetHash is the xet file hash the upstream resolve advertised;
	// empty when the file was fetched plain or resumed from a full spool.
	upstreamXetHash string
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
func (h *Handler) startTask(key resolveKey, pre *probeResult) (*task, *fileEntry, error) {
	v, err, _ := h.flight.Do(key.String(), func() (any, error) {
		// A previous flight may have registered a task, or finished the whole
		// ingest, between the caller's check and this one.
		h.mu.Lock()
		t := h.tasks[key]
		h.mu.Unlock()
		if t != nil {
			return t, nil
		}
		if e := h.entryForKey(context.Background(), key); e != nil {
			if e.State == stateReady {
				return e, nil
			}
			if e.State == stateFailed && time.Now().Before(e.nextRetry) {
				return e, nil
			}
		}
		nt := &task{probed: make(chan struct{}), sized: make(chan struct{}), done: make(chan struct{})}
		nt.size.Store(-1)
		h.mu.Lock()
		h.tasks[key] = nt
		h.mu.Unlock()
		go h.runTask(key, nt, pre)
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

// acquire is the shared resolution flow behind both the HTTP resolve path and
// Ingest: it pins branch revisions to their upstream commit and returns what
// the request attaches to — the in-flight task, or the terminal entry (ready,
// revalidated on the usual cadence, or failed and still inside its retry
// backoff) — starting a new ingest task when neither exists. Exactly one of
// the returned task and entry is non-nil; the returned key carries the branch
// pin. ctx bounds only the revalidation probe.
func (h *Handler) acquire(ctx context.Context, key resolveKey) (resolveKey, *task, *fileEntry, error) {
	var preProbe *probeResult
	if !commitRevRe.MatchString(key.rev) {
		commit, pr, ok := h.branchCommit(key)
		preProbe = pr
		if ok {
			key.rev = commit
		}
	}

	h.mu.Lock()
	t := h.tasks[key]
	h.mu.Unlock()
	if t != nil {
		return key, t, nil, nil
	}
	if e := h.entryForKey(ctx, key); e != nil {
		switch e.State {
		case stateReady:
			if h.needsRevalidate(e, key.rev) {
				e = h.revalidate(ctx, key, e)
			}
			if e != nil {
				return key, nil, e, nil
			}
			// stale: fall through and re-ingest
		case stateFailed:
			if time.Now().Before(e.nextRetry) {
				return key, nil, e, nil
			}
		}
	}

	t, e, err := h.startTask(key, preProbe)
	return key, t, e, err
}

// runTask executes one ingestion end to end on a background context; client
// disconnects never cancel it. The spool opens after the probe so partial
// bytes from a previous failed task (or a previous process) are resumed when
// the upstream etag still matches. A non-nil pre stands in for the probe.
func (h *Handler) runTask(key resolveKey, t *task, pre *probeResult) {
	ctx := context.Background()
	upath := key.String()
	defer close(t.done) // the entry is published by then, on every path

	pr, err := pre, error(nil)
	if pr == nil {
		pr, err = h.probe(ctx, upath)
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
		h.failTask(key, t, err)
		return
	}

	sp, err := openSpool(h.spoolDir, upath, pr.etag, pr.size)
	if err != nil {
		t.probeErr = err
		close(t.probed)
		h.failTask(key, t, err)
		return
	}
	t.spool = sp
	sp.acquire() // held by runTask until ingest completes
	defer sp.release()

	t.probe = pr
	if pr.size >= 0 {
		t.setSize(pr.size)
	}
	if pr.xetHash != "" {
		// Ingest verifies the local hash against this, so by-hash requests
		// parked on the task can be redirected to storage once it lands.
		t.upstreamXetHash = pr.xetHash
		h.mu.Lock()
		h.hashTasks[pr.xetHash] = key
		h.mu.Unlock()
		defer func() {
			h.mu.Lock()
			// Another key with the same content may have re-registered the
			// hash; only remove the mapping while it is still ours.
			if h.hashTasks[pr.xetHash] == key {
				delete(h.hashTasks, pr.xetHash)
			}
			h.mu.Unlock()
		}()
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
		err = h.fetchXet(ctx, t, upath)
	default:
		err = h.fetchPlain(ctx, t, upath)
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
		h.failTask(key, t, err)
		return
	}
	t.size.Store(t.spool.size())
	t.setSize(-1) // definitive size stored above; signal any waiters
	t.spool.finish(nil)

	if _, err := h.ingestSpool(ctx, t, key); err != nil {
		if errors.Is(err, errSpoolCorrupt) {
			t.spool.markRemove()
		}
		h.failTask(key, t, err)
		return
	}

	// The ready entry was persisted by ingestSpool; drop the failure record
	// so lookups stop hitting the backoff.
	h.mu.Lock()
	delete(h.failed, key)
	delete(h.tasks, key)
	h.mu.Unlock()
	t.spool.markRemove() // bytes now live in storage; drop the spool when drained
}

// failTask records a failure with exponential backoff and clears the task.
func (h *Handler) failTask(key resolveKey, t *task, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	failures := 1
	if prev := h.failed[key]; prev != nil {
		failures = prev.failures + 1
	}
	shift := min(failures-1, maxFailureShift)
	backoff := min(failureBackoffBase<<shift, failureBackoffCap)
	h.failed[key] = &fileEntry{
		Key:       key.String(),
		State:     stateFailed,
		failures:  failures,
		nextRetry: time.Now().Add(backoff),
		lastErr:   err,
		notFound:  t.notFound,
	}
	delete(h.tasks, key)
}

// ingestSpool verifies the spooled bytes and runs the standard upload pipeline
// against local storage, then returns the ready index entry.
func (h *Handler) ingestSpool(ctx context.Context, t *task, key resolveKey) (*fileEntry, error) {
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
		fileHash, err := upload.UploadFile(ctx, h.localAdapter, f,
			upload.WithEnableSHA256(true),
			upload.WithConcurrency(4),
		)
		if err != nil {
			return nil, fmt.Errorf("ingest into storage: %w", err)
		}
		entry.FileHash = fileHash.String()
		// Re-chunking must reproduce the upstream's hash, so a hash learned
		// from upstream metadata keeps resolving on this mirror; a divergence
		// is a chunking conformance bug and must not be served silently.
		if t.upstreamXetHash != "" && entry.FileHash != t.upstreamXetHash {
			return nil, fmt.Errorf("xet hash mismatch: local chunking produced %s, upstream advertised %s", entry.FileHash, t.upstreamXetHash)
		}
	}

	if err := persistEntry(h.indexDir, entry); err != nil {
		return nil, err
	}
	return entry, nil
}
