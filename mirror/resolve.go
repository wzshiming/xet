package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	iofs "io/fs"
	"net/http"
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

// Resolve resolves the file at upstreamURL, a hub download URL of the form
// {origin}/{repo}/resolve/{rev}/{path}, through the shared acquire flow:
// branch revisions are pinned to their upstream commit, entries and tasks are
// keyed by immutable content, ready entries are revalidated on the usual
// cadence, and a new ingest task is started when no terminal entry or
// in-flight task exists. token is the hub bearer token for that origin (empty
// for anonymous access); the resolver that starts an ingest pins its origin
// and token on it, and later resolvers of the same file join it. It returns
// the ready entry, the stream of the in-flight ingest, or the terminal
// ingest failure as an error (not-found matching ErrUpstreamNotFound). The
// URL path is taken in its escaped form, so Resolve and Ingest share tasks,
// entries, and spools.
//
// In one rare interleaving a returned Stream is already useless: its ingest
// finished and its spool was fully drained before the caller attached, so
// NewReader and NewSeekReader return nil. Resolve again for the published
// terminal entry. ctx bounds only the resolution itself, never the
// background ingest.
func (m *Mirror) Resolve(ctx context.Context, upstreamURL, token string) (*Resolution, error) {
	origin, key, err := parseUpstreamURL(upstreamURL)
	if err != nil {
		return nil, err
	}
	key, t, e, err := m.acquire(ctx, origin, token, key)
	if err != nil {
		return nil, err
	}
	if t != nil {
		return &Resolution{Stream: &Stream{t: t}}, nil
	}
	if e.State == stateReady {
		return &Resolution{Entry: exportEntry(key, e)}, nil
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

// WaitMeta must succeed before calling the remaining Stream methods.
func (st *Stream) WaitMeta(ctx context.Context) (etag, commit string, err error) {
	select {
	case <-st.t.probed:
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
	if st.t.probeErr != nil {
		return "", "", st.t.probeErr
	}
	return st.t.probe.etag, st.t.key.rev, nil
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

// FetchUpstream GETs rawURL with token as the bearer credential for its origin, following redirects and resuming body reads; the caller owns the response body.
func (m *Mirror) FetchUpstream(ctx context.Context, rawURL, token string) (*http.Response, error) {
	_, origin, err := upstreamOrigin(rawURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(withUpstreamAuth(ctx, origin, token), http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	return m.fetchClient.Do(req)
}

// revalidate re-probes a source-backed entry's branch on origin: a changed etag drops it for re-ingest, upstream errors keep it.
func (m *Mirror) revalidate(ctx context.Context, origin string, key, src resolveKey, e *fileEntry, pr *probeResult) bool {
	if pr == nil {
		var err error
		if pr, err = m.probe(ctx, origin, src); err != nil || probeErr(pr) != nil {
			return true
		}
	}
	if pr.etag != e.ETag {
		m.dropEntry(key, e)
		return false
	}
	m.mu.Lock()
	e.CheckedAt = time.Now()
	m.mu.Unlock()
	_ = m.persistCommit(key.repo, key.rev)
	return true
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

// dropEntry forgets a dead entry in memory and rewrites its commit manifest without it.
func (m *Mirror) dropEntry(key resolveKey, e *fileEntry) {
	m.mu.Lock()
	if m.entries[key] == e {
		cs := m.loadCommit(key.repo, key.rev)
		delete(m.entries, key)
		delete(cs.files, key.path)
	}
	m.mu.Unlock()
	_ = m.persistCommit(key.repo, key.rev)
}
