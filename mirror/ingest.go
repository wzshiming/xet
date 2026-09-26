package mirror

import (
	"context"
	"errors"
	"fmt"
)

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

// entryErr maps a failed entry to the error Ingest reports; not-found
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

// Ingestion is a started or joined ingest. Wait on Done, then read Entry;
// abandoning an Ingestion never cancels the ingest itself, the same contract
// as a client disconnect on the HTTP path.
type Ingestion struct {
	done  chan struct{}
	entry *Entry // set before done closes
	err   error  // set before done closes
}

// Done is closed once the ingest finished: the entry is ready or failed.
func (in *Ingestion) Done() <-chan struct{} { return in.done }

// Entry reports the outcome: the ready entry, or the ingest failure with
// not-found matching ErrUpstreamNotFound. Before Done is closed it returns
// nil, nil.
func (in *Ingestion) Entry() (*Entry, error) {
	select {
	case <-in.done:
		return in.entry, in.err
	default:
		return nil, nil
	}
}

// Ingest starts ingesting the file at upstreamURL (a hub download URL, see
// Resolve) into local storage — bytes fetched, xorbs and shards landed, index
// entry persisted — or joins the in-flight ingest, and returns immediately:
// wait on Done, then read Entry. token is the hub bearer token for the URL's
// origin. The URL path is taken in its escaped form, so Ingest and Resolve
// share tasks, entries, and spools.
//
// The whole resolution (branch pinning, upstream probes, task wait) runs off
// the calling goroutine, deduplicated per file: concurrent Ingests of one
// file share a single flight. Ready entries resolve quickly (revalidated on
// the usual cadence), as do failed ingests still inside their retry backoff.
func (m *Mirror) Ingest(upstreamURL, token string) (*Ingestion, error) {
	origin, key, err := parseUpstreamURL(upstreamURL)
	if err != nil {
		return nil, err
	}

	in := &Ingestion{done: make(chan struct{})}
	ch := m.flight.DoChan("ingest\x00"+key.String(), func() (any, error) {
		return m.ingest(origin, token, key)
	})
	go func() {
		res := <-ch
		if e, ok := res.Val.(*Entry); ok {
			in.entry = e
		}
		in.err = res.Err
		close(in.done)
	}()
	return in, nil
}

// ingest resolves one Ingest flight through the shared acquire flow and
// blocks until the entry is ready or failed.
func (m *Mirror) ingest(origin, token string, key resolveKey) (*Entry, error) {
	key, t, e, err := m.acquire(context.Background(), origin, token, key)
	if err != nil {
		return nil, err
	}
	if t != nil {
		<-t.done
		m.mu.Lock()
		e = m.entries[key]
		m.mu.Unlock()
		if e == nil {
			// Only possible when a concurrent revalidation found the fresh
			// entry already stale; the same window Resolve reports as a
			// drained stream.
			return nil, fmt.Errorf("mirror: %q changed upstream during ingest", key)
		}
	}
	if e.State == stateReady {
		return exportEntry(key, e), nil
	}
	return nil, entryErr(e)
}
