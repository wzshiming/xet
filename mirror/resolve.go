package mirror

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

// handleResolve serves GET/HEAD for hub-style download paths.
func (h *Handler) handleResolve(w http.ResponseWriter, r *http.Request, key string) {
	m := resolveRe.FindStringSubmatch(key)
	rev := m[2]

	// Branch revisions are pinned to the upstream commit they point at, so
	// entries, tasks, and spools are all keyed by immutable content. The
	// branch mapping is re-checked on the revalidation cadence; one probe
	// covers every file of the repo branch and is reused by the ingest task.
	var preProbe *probeResult
	if !commitRevRe.MatchString(rev) {
		commit, pr, ok := h.branchCommit(key, m)
		preProbe = pr
		if ok {
			rev = commit
			key = "/" + m[1] + "/resolve/" + rev + "/" + m[3]
		}
	}

	h.mu.Lock()
	t := h.tasks[key]
	e := h.entries[key]
	h.mu.Unlock()

	if t != nil {
		h.serveTaskOrEntry(w, r, key, t)
		return
	}
	if e != nil {
		switch e.State {
		case stateReady:
			if h.needsRevalidate(e, rev) {
				e = h.revalidate(r.Context(), key, e)
			}
			if e != nil {
				h.serveReady(w, r, e)
				return
			}
			// stale: fall through and re-ingest
		case stateFailed:
			if time.Now().Before(e.nextRetry) {
				h.serveFailed(w, e)
				return
			}
		}
	}

	t, e, err := h.startTask(key, preProbe)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if e != nil {
		// Lost the race: another request finished (or failed) the ingest first.
		h.serveEntry(w, r, e)
		return
	}
	h.serveTaskOrEntry(w, r, key, t)
}

// serveEntry answers from a completed entry, ready or failed.
func (h *Handler) serveEntry(w http.ResponseWriter, r *http.Request, e *fileEntry) {
	if e.State == stateReady {
		h.serveReady(w, r, e)
	} else {
		h.serveFailed(w, e)
	}
}

// serveTaskOrEntry streams from the in-flight task, falling back to the entry
// published by a completed ingest when the spool was drained and removed
// before this request could attach.
func (h *Handler) serveTaskOrEntry(w http.ResponseWriter, r *http.Request, key string, t *task) {
	if h.serveFromTask(w, r, t) {
		return
	}
	h.mu.Lock()
	e := h.entries[key]
	h.mu.Unlock()
	if e == nil {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}
	h.serveEntry(w, r, e)
}

// needsRevalidate reports whether a ready entry must be re-checked upstream.
func (h *Handler) needsRevalidate(e *fileEntry, rev string) bool {
	if h.revalidateInterval < 0 || commitRevRe.MatchString(rev) {
		return false
	}
	return time.Since(e.CheckedAt) >= h.revalidateInterval
}

// revalidate re-probes the upstream. It returns the entry to serve, or nil
// when the entry went stale and must be re-ingested. Upstream errors keep
// serving the cached copy.
func (h *Handler) revalidate(ctx context.Context, key string, e *fileEntry) *fileEntry {
	pr, err := h.probe(ctx, key)
	if err != nil || pr.status < 200 || pr.status >= 300 {
		return e
	}
	if pr.etag == e.ETag {
		e.CheckedAt = time.Now()
		_ = persistEntry(h.indexDir, e)
		return e
	}
	h.mu.Lock()
	if h.entries[key] == e {
		delete(h.entries, key)
	}
	h.mu.Unlock()
	_ = os.Remove(indexEntryPath(h.indexDir, e.Commit, e.Key))
	return nil
}

// serveReady answers a resolve request for a fully cached file: metadata plus
// xet Link headers for capable clients, and a redirect to the sha256 bridge
// for everyone else.
func (h *Handler) serveReady(w http.ResponseWriter, r *http.Request, e *fileEntry) {
	base := h.externalBase(r)
	writeMetadataHeaders(w, e.ETag, e.Size, e.Commit)
	if e.FileHash != "" {
		w.Header().Add("Link", fmt.Sprintf("<%s%s>; rel=\"xet-auth\", <%s/v1/reconstructions/%s>; rel=\"xet-reconstruction-info\"", base, tokenEndpointPath, base, e.FileHash))
		w.Header().Set("X-Xet-Hash", e.FileHash)
	}
	if e.Size == 0 {
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(http.StatusOK)
		return
	}
	// The Location must be absolute: hub clients follow relative redirects
	// before reading metadata, which would strip the xet headers off the
	// response they end up looking at.
	http.Redirect(w, r, base+"/xet-bridge/"+e.SHA256, http.StatusFound)
}

func (h *Handler) serveFailed(w http.ResponseWriter, e *fileEntry) {
	serveFetchError(w, e.notFound, e.lastErr)
}

// serveFetchError maps a failed ingest to its HTTP response: 404 when the
// upstream lacks the file, 502 with the underlying error otherwise.
func serveFetchError(w http.ResponseWriter, notFound bool, err error) {
	if notFound {
		http.Error(w, "File not found upstream", http.StatusNotFound)
		return
	}
	msg := "upstream fetch failed"
	if err != nil {
		msg = err.Error()
	}
	http.Error(w, msg, http.StatusBadGateway)
}

// serveFromTask streams a file that is still being ingested. Bytes are served
// from the growing spool; range requests for regions not yet spooled block
// until the data lands. It reports false when the ingest finished and the
// spool was already drained before this request could attach.
func (h *Handler) serveFromTask(w http.ResponseWriter, r *http.Request, t *task) bool {
	select {
	case <-t.probed:
	case <-r.Context().Done():
		return true
	}
	if t.probeErr != nil {
		serveFetchError(w, t.notFound, t.probeErr)
		return true
	}

	pr := t.probe
	// The size may only be learned from the ingest download's first response
	// headers (e.g. modelscope.cn sends none on HEAD); downstream hub clients
	// refuse metadata without it, so wait rather than answer size-less.
	select {
	case <-t.sized:
	case <-r.Context().Done():
		return true
	}
	size := t.size.Load()
	if r.Method == http.MethodHead {
		writeMetadataHeaders(w, pr.etag, size, pr.commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		if size >= 0 {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		}
		w.WriteHeader(http.StatusOK)
		return true
	}

	if size < 0 {
		// Total size unknown until the ingest completes (xet upstream whose
		// probe carried no size): ServeContent cannot handle that, so stream
		// the whole body as it lands, ignoring Range.
		rc := t.spool.newReader(r.Context(), 0)
		if rc == nil {
			return false
		}
		defer func() {
			_ = rc.Close()
		}()
		writeMetadataHeaders(w, pr.etag, size, pr.commit)
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.Copy(w, rc)
		return true
	}

	rs := t.spool.newSeekReader(r.Context(), size)
	if rs == nil {
		return false
	}
	defer func() {
		_ = rs.Close()
	}()
	writeMetadataHeaders(w, pr.etag, size, pr.commit)
	w.Header().Set("Content-Type", "application/octet-stream")
	http.ServeContent(w, r, "", time.Time{}, rs)
	return true
}

// writeMetadataHeaders emits the header set downstream tooling relies on,
// mirroring what the upstream hub advertised.
func writeMetadataHeaders(w http.ResponseWriter, etag string, size int64, commit string) {
	if etag != "" {
		quoted := `"` + etag + `"`
		w.Header().Set("ETag", quoted)
		w.Header().Set("X-Linked-Etag", quoted)
	}
	if size >= 0 {
		w.Header().Set("X-Linked-Size", strconv.FormatInt(size, 10))
	}
	if commit != "" {
		w.Header().Set("X-Repo-Commit", commit)
	}
	w.Header().Set("Accept-Ranges", "bytes")
}
