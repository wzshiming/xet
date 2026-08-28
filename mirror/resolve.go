package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/wzshiming/xet"
)

// Resolution is the outcome of a Resolve call. Exactly one field is set:
// Entry when the file is fully ingested, Stream while an ingest is in
// flight.
type Resolution struct {
	// Entry describes the ready file.
	Entry *Entry
	// Stream is the handle to the in-flight ingest.
	Stream *Stream
}

// Resolve resolves the file at /{repo}/resolve/{rev}/{path} through the
// shared acquire flow: branch revisions are pinned to their upstream commit,
// entries and tasks are keyed by immutable content, ready entries are
// revalidated on the usual cadence, and a new ingest task is started when no
// terminal entry or in-flight task exists. It returns the ready entry, the
// stream of the in-flight ingest, or the terminal ingest failure as an error
// (not-found matching ErrUpstreamNotFound). The components must be given in
// escaped URL path form, so Resolve and Ingest share tasks, entries, and
// spools.
//
// In one rare interleaving a returned Stream is already useless: its ingest
// finished and its spool was fully drained before the caller attached, so
// NewReader and NewSeekReader return nil. Resolve again for the published
// terminal entry. ctx bounds only the resolution itself, never the
// background ingest.
func (m *Mirror) Resolve(ctx context.Context, repo, rev, path string) (*Resolution, error) {
	if repo == "" || rev == "" || path == "" || strings.Contains(rev, "/") {
		return nil, fmt.Errorf("mirror: invalid resolve components repo=%q rev=%q path=%q", repo, rev, path)
	}
	key := resolveKey{repo: repo, rev: rev, path: path}
	_, t, e, err := m.acquire(ctx, key)
	if err != nil {
		return nil, err
	}
	if t != nil {
		return &Resolution{Stream: &Stream{t: t}}, nil
	}
	if e.State == stateReady {
		return &Resolution{Entry: exportEntry(e)}, nil
	}
	return nil, entryErr(e)
}

// Stream is the handle to one in-flight ingest. It never owns the ingest:
// abandoning the handle, or canceling the contexts passed to its methods,
// leaves the background download running, the same contract as a client
// disconnect on the HTTP path.
type Stream struct {
	t *task
}

// WaitMeta blocks until the upstream probe settled or ctx is done, and
// returns the upstream metadata the ingest runs against. Probe failures are
// returned as errors, with not-found matching ErrUpstreamNotFound. The
// remaining Stream methods may only be used after WaitMeta returned nil.
func (st *Stream) WaitMeta(ctx context.Context) (etag, commit string, err error) {
	select {
	case <-st.t.probed:
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
	if st.t.probeErr != nil {
		return "", "", st.t.probeErr
	}
	return st.t.probe.etag, st.t.probe.commit, nil
}

// WaitSize blocks until the content length is known — some hubs carry no
// size on probes (e.g. modelscope.cn sends none on HEAD), so it may only be
// learned from the ingest download's first response headers — or until no
// early size source remains, or ctx is done. It returns the final size, or
// -1 when the size stays unknown until the ingest completes; ok is false
// only when ctx was done first.
func (st *Stream) WaitSize(ctx context.Context) (size int64, ok bool) {
	select {
	case <-st.t.sized:
	case <-ctx.Done():
		return -1, false
	}
	return st.t.size.Load(), true
}

// NewReader returns a reader over the file bytes starting at offset off,
// tailing the growing spool until the ingest finishes. It returns nil when
// the ingest finished and the spool was already drained. ctx interrupts
// blocked reads, never the ingest.
func (st *Stream) NewReader(ctx context.Context, off int64) io.ReadCloser {
	return st.t.spool.newReader(ctx, off)
}

// NewSeekReader returns a ReadSeekCloser over the final size of the file,
// fit for http.ServeContent: reads of regions not yet spooled block until
// the data lands. It returns nil when the ingest finished and the spool was
// already drained. ctx interrupts blocked reads, never the ingest.
func (st *Stream) NewSeekReader(ctx context.Context, size int64) io.ReadSeekCloser {
	return st.t.spool.newSeekReader(ctx, size)
}

// LookupXetHash resolves an lfs sha256 oid to the local xet file hash
// through the storage sha256 index; ok is false when the content is not held
// locally. The server/hf package rewrites hub tree listings with it.
func (m *Mirror) LookupXetHash(ctx context.Context, oid string) (string, bool) {
	digest, err := hex.DecodeString(oid)
	if err != nil || len(digest) != sha256.Size {
		return "", false
	}
	fileHash, err := m.storage.GetFileHashBySHA256(ctx, "default", [32]byte(digest))
	if err != nil {
		return "", false
	}
	return fileHash.String(), true
}

// FetchUpstream issues an authenticated GET for the given escaped path (with
// optional query) against the upstream hub, following redirects and resuming
// dropped bodies. Downstream credentials must never be forwarded; the
// mirror's own upstream token is injected instead. The caller owns the
// response body.
func (m *Mirror) FetchUpstream(ctx context.Context, pathAndQuery string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, m.upstreamURL(pathAndQuery), nil)
	if err != nil {
		return nil, err
	}
	return m.fetchClient.Do(req)
}

// needsRevalidate reports whether a ready entry must be re-checked upstream.
func (m *Mirror) needsRevalidate(e *fileEntry, rev string) bool {
	if m.revalidateInterval < 0 || commitRevRe.MatchString(rev) {
		return false
	}
	return time.Since(e.CheckedAt) >= m.revalidateInterval
}

// revalidate re-probes the upstream. It returns the entry to serve, or nil
// when the entry went stale and must be re-ingested. Upstream errors keep
// serving the cached copy.
func (m *Mirror) revalidate(ctx context.Context, key resolveKey, e *fileEntry) *fileEntry {
	pr, err := m.probe(ctx, key.String())
	if err != nil || pr.status < 200 || pr.status >= 300 {
		return e
	}
	if pr.etag == e.ETag {
		e.CheckedAt = time.Now()
		_ = persistEntry(m.indexDir, e)
		return e
	}

	m.mu.Lock()
	if m.entries[key] == e {
		delete(m.entries, key)
	}
	m.mu.Unlock()
	_ = os.Remove(indexEntryPath(m.indexDir, e.Commit, e.Key))
	return nil
}

// entryLive reports whether a ready entry's file is still in storage; an
// unlinked file must be re-ingested. Empty files store no file hash and are
// always live, and transient storage errors keep serving the cached copy.
func (m *Mirror) entryLive(ctx context.Context, e *fileEntry) bool {
	if e.FileHash == "" {
		return true
	}
	fileHash, err := xet.ParseFileHash(e.FileHash)
	if err != nil {
		return false
	}
	if _, err := m.storage.GetShard(ctx, fileHash); err != nil {
		return !errors.Is(err, iofs.ErrNotExist)
	}
	return true
}

// dropEntry forgets a dead entry in memory and on disk.
func (m *Mirror) dropEntry(key resolveKey, e *fileEntry) {
	m.mu.Lock()
	if m.entries[key] == e {
		delete(m.entries, key)
	}
	m.mu.Unlock()
	_ = os.Remove(indexEntryPath(m.indexDir, e.Commit, e.Key))
}
