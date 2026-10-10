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

// Entry describes a fully ingested file, exporting what the index persists.
type Entry struct {
	// SHA256 is the hex digest of the file bytes, also the key of the plain
	// download bridge (/xet-bridge/{sha256}).
	SHA256 string
	// FileHash is the xet file hash in local storage; empty for empty files.
	FileHash string
	// Size is the file length in bytes.
	Size int64
	// ETag is the upstream entity tag the cached bytes were validated against.
	ETag string
	// Commit is the upstream revision the file was resolved at.
	Commit string
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
// URL path is taken in its escaped form, matching the HTTP route: tasks,
// entries, and spools are keyed by it.
//
// The upstream probe runs before anything is returned: content local storage
// already holds comes back as an Entry, and a probe failure as the error. ctx
// bounds only the resolution itself, including the wait for the probe and
// task start that resolvers of one file share, never the background ingest.
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

// exportEntry copies the persisted fields of a ready entry into the exported
// form.
func exportEntry(key resolveKey, e *fileEntry) *Entry {
	return &Entry{
		SHA256:   e.SHA256,
		FileHash: e.FileHash,
		Size:     e.Size,
		ETag:     e.ETag,
		Commit:   key.rev,
	}
}

// entryErr maps a failed entry to the error Resolve reports; not-found
// failures match ErrUpstreamNotFound.
func entryErr(e *fileEntry) error {
	if e.lastErr != nil {
		return e.lastErr
	}
	if e.notFound {
		return ErrUpstreamNotFound
	}
	return errors.New("upstream fetch failed")
}

// Stream is the handle to one in-flight ingest. It never owns the ingest:
// abandoning the handle, or canceling the contexts passed to its methods,
// leaves the background download running, the same contract as a client
// disconnect on the HTTP path.
type Stream struct {
	t *task
}

// Meta reports the upstream etag and the pinned commit, both known before the Stream is handed out.
func (st *Stream) Meta() (etag, commit string) {
	return st.t.probe.etag, st.t.key.rev
}

// WaitSize blocks until the content length is known — some hubs carry no
// size on probes (e.g. modelscope.cn sends none on HEAD), so it may only be
// learned from the ingest download's first response headers — or until no
// early size source remains, or ctx is done. It returns the final size, or
// -1 when the size stays unknown until the ingest completes; ok is false
// only when ctx was done first.
func (st *Stream) WaitSize(ctx context.Context) (size int64, ok bool) {
	select {
	case <-st.t.item.Sized():
	case <-ctx.Done():
		return -1, false
	}
	return st.t.item.Size(), true
}

// NewReader returns a reader over the file bytes starting at offset,
// tailing the growing spool until the ingest finishes. It returns an error
// when the spool was already retired — the ingest finished and every reader
// detached before this caller attached: Resolve again for the entry. Close
// unblocks a waiting read, never the ingest.
func (st *Stream) NewReader(offset int) (io.ReadCloser, error) {
	return st.t.item.NewReader(offset)
}

// NewSeekReader returns a ReadSeekCloser over the final size of the file
// positioned at offset, fit for http.ServeContent: reads of regions not yet
// spooled block until the data lands. It returns an error when the spool was
// already retired — the ingest finished and every reader detached before this
// caller attached: Resolve again for the entry.
func (st *Stream) NewSeekReader(offset, size int) (io.ReadSeekCloser, error) {
	return st.t.item.NewSeekReader(offset, size)
}

// Done is closed once the ingest finished and released its spool — the entry
// published and its manifest written (best effort), or the ingest failed;
// Resolve again for the outcome.
func (st *Stream) Done() <-chan struct{} { return st.t.done }

// LookupXetHash resolves an lfs sha256 oid to the local xet file hash
// through the storage sha256 index; ok is false when the content is not held
// locally. The server/hf package rewrites hub tree listings with it.
func (m *Mirror) LookupXetHash(ctx context.Context, oid string) (string, bool) {
	fileHash, _, ok := m.fileHashBySHA256(ctx, oid)
	if !ok {
		return "", false
	}
	return fileHash.String(), true
}

// fileHashBySHA256 resolves a hex sha256 digest through the storage sha256 index; ok is false when it is malformed or the lookup fails.
func (m *Mirror) fileHashBySHA256(ctx context.Context, hexDigest string) (xet.FileHash, [sha256.Size]byte, bool) {
	raw, err := hex.DecodeString(hexDigest)
	if err != nil || len(raw) != sha256.Size {
		return xet.FileHash{}, [sha256.Size]byte{}, false
	}
	digest := [sha256.Size]byte(raw)
	fileHash, err := m.storage.GetFileHashBySHA256(ctx, digest)
	if err != nil {
		return xet.FileHash{}, digest, false
	}
	return fileHash, digest, true
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
