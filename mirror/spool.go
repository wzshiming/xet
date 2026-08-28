package mirror

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// spool is an append-only file that is written by exactly one ingest download
// and concurrently tail-read by any number of stream readers. Readers block
// at the current end until more bytes land or the writer finishes.
//
// The file name is <keyhash>-<etag>-<size>.spool, so the content identity is
// part of the path itself: a later task that probes the same etag and size
// computes the same name and resumes from the existing length, surviving task
// failures and process restarts with no sidecar state. A changed etag or size
// yields a different name, and stale siblings of the key are swept on open.
// The file is only unlinked after a successful ingest, or via markRemove when
// the bytes are known to be unusable.
type spool struct {
	f *os.File

	mu        sync.Mutex
	cond      *sync.Cond
	written   int64
	done      bool
	err       error
	refs      int
	retired   bool // file handle closed, no new readers
	removable bool // unlink the file when retiring
}

// hexTokenRe matches etags that are safe and meaningful to embed verbatim,
// notably the sha256 etags hubs advertise for LFS files.
var hexTokenRe = regexp.MustCompile(`^[0-9a-fA-F]{1,64}$`)

func spoolKeyPrefix(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:16]) + "-"
}

func spoolETagToken(etag string) string {
	if etag == "" {
		return "noetag"
	}
	if hexTokenRe.MatchString(etag) {
		return strings.ToLower(etag)
	}
	sum := sha256.Sum256([]byte(etag))
	return hex.EncodeToString(sum[:16])
}

// spoolFileName builds the deterministic spool name for a resolve key and the
// upstream validators known at probe time; size < 0 (unknown) becomes "u".
func spoolFileName(key, etag string, size int64) string {
	sizeTok := "u"
	if size >= 0 {
		sizeTok = strconv.FormatInt(size, 10)
	}
	return spoolKeyPrefix(key) + spoolETagToken(etag) + "-" + sizeTok + ".spool"
}

// sweepStaleSpools removes other spool files of the same key (older etag or
// size), which can no longer be resumed.
func sweepStaleSpools(dir, key, keep string) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	prefix := spoolKeyPrefix(key)
	for _, ent := range ents {
		n := ent.Name()
		if n != keep && strings.HasPrefix(n, prefix) && strings.HasSuffix(n, ".spool") {
			_ = os.Remove(filepath.Join(dir, n))
		}
	}
}

// openSpool opens (or creates) the spool for key and the probed validators,
// resuming existing partial bytes: the name already proves the etag and size
// match. Without an etag there is no trustworthy identity, so the file is
// truncated and starts fresh.
func openSpool(dir, key, etag string, expectedSize int64) (*spool, error) {
	name := spoolFileName(key, etag, expectedSize)
	sweepStaleSpools(dir, key, name)

	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open spool file: %w", err)
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat spool file: %w", err)
	}
	size := st.Size()

	resume := etag != "" && (expectedSize < 0 || size <= expectedSize)
	if !resume && size > 0 {
		if err := f.Truncate(0); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("truncate stale spool: %w", err)
		}
		size = 0
	}
	if _, err := f.Seek(size, io.SeekStart); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("seek spool file: %w", err)
	}

	s := &spool{f: f, written: size}
	s.cond = sync.NewCond(&s.mu)
	return s, nil
}

// Write appends p and wakes waiting readers. Only the ingest goroutine calls it.
func (s *spool) Write(p []byte) (int, error) {
	n, err := s.f.Write(p)
	if n > 0 {
		s.mu.Lock()
		s.written += int64(n)
		s.cond.Broadcast()
		s.mu.Unlock()
	}
	return n, err
}

// Seek supports the io.WriteSeeker contract needed by client.DownloadFile,
// which seeks to the end for resume support before writing sequentially.
func (s *spool) Seek(offset int64, whence int) (int64, error) {
	pos, err := s.f.Seek(offset, whence)
	if err != nil {
		return pos, err
	}
	s.mu.Lock()
	s.written = pos
	s.mu.Unlock()
	return pos, nil
}

// finish marks the spool complete (err == nil) or broken, and wakes readers.
// The backing file is kept either way so a later task can resume partial
// bytes or re-ingest completed ones; only markRemove schedules the unlink.
func (s *spool) finish(err error) {
	s.mu.Lock()
	if !s.done {
		s.done = true
		s.err = err
		s.cond.Broadcast()
	}
	retire := s.refs <= 0 && !s.retired
	if retire {
		s.retired = true
	}
	remove := retire && s.removable
	s.mu.Unlock()
	if retire {
		s.close(remove)
	}
}

// markRemove schedules the backing file for unlink once the spool retires
// (after the ingest published the entry, or when the bytes are unusable).
func (s *spool) markRemove() {
	s.mu.Lock()
	s.removable = true
	retired := s.retired
	s.mu.Unlock()
	if retired {
		s.unlink()
	}
}

func (s *spool) close(remove bool) {
	_ = s.f.Close()
	if remove {
		s.unlink()
	}
}

func (s *spool) unlink() {
	_ = os.Remove(s.f.Name())
}

// size returns the bytes written so far.
func (s *spool) size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.written
}

// waitFor blocks until at least one byte past offset is available, the writer
// finished, or ctx is canceled. It returns the currently available length.
func (s *spool) waitFor(ctx context.Context, offset int64) (int64, bool, error) {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()

	s.mu.Lock()
	defer s.mu.Unlock()
	for s.written <= offset && !s.done {
		if ctx.Err() != nil {
			return s.written, s.done, ctx.Err()
		}
		s.cond.Wait()
	}
	if s.done && s.err != nil {
		return s.written, true, s.err
	}
	return s.written, s.done, nil
}

// acquire registers a reader reference. It reports false when the spool has
// already been retired.
func (s *spool) acquire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.retired {
		return false
	}
	s.refs++
	return true
}

// release drops a reader reference, retiring the spool when the ingest is
// done and no readers remain.
func (s *spool) release() {
	s.mu.Lock()
	s.refs--
	retire := s.refs <= 0 && s.done && !s.retired
	if retire {
		s.retired = true
	}
	remove := retire && s.removable
	s.mu.Unlock()
	if retire {
		s.close(remove)
	}
}

// readAt reads from the backing file without waiting.
func (s *spool) readAt(p []byte, off int64) (int, error) {
	return s.f.ReadAt(p, off)
}

// newReader returns a tail-following reader from offset until the writer
// completes, or nil when the spool has already been retired. The request ctx
// only interrupts waits, never the ingest itself.
func (s *spool) newReader(ctx context.Context, offset int64) io.ReadCloser {
	if !s.acquire() {
		return nil
	}
	return &spoolReader{ctx: ctx, s: s, pos: offset, size: -1}
}

// newSeekReader returns a blocking ReadSeekCloser over the final size of the
// file, suitable for http.ServeContent range handling while bytes still land.
// It returns nil when the spool has already been retired.
func (s *spool) newSeekReader(ctx context.Context, size int64) io.ReadSeekCloser {
	if !s.acquire() {
		return nil
	}
	return &spoolReader{ctx: ctx, s: s, size: size}
}

// spoolReader tail-reads the spool. size bounds reads and SeekEnd when the
// final length is known; -1 means unknown, and EOF then comes from the writer
// finishing.
type spoolReader struct {
	ctx    context.Context
	s      *spool
	pos    int64
	size   int64
	closed bool
}

func (r *spoolReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	case io.SeekEnd:
		r.pos = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if r.pos < 0 {
		return 0, fmt.Errorf("negative position")
	}
	return r.pos, nil
}

func (r *spoolReader) Read(p []byte) (int, error) {
	if r.size >= 0 && r.pos >= r.size {
		return 0, io.EOF
	}
	avail, done, err := r.s.waitFor(r.ctx, r.pos)
	if err != nil {
		return 0, err
	}
	if avail <= r.pos {
		if r.size < 0 && done {
			return 0, io.EOF
		}
		return 0, io.ErrUnexpectedEOF
	}
	limit := int64(len(p))
	if r.size >= 0 {
		if rest := r.size - r.pos; rest < limit {
			limit = rest
		}
	}
	if rest := avail - r.pos; rest < limit {
		limit = rest
	}
	n, err := r.s.readAt(p[:limit], r.pos)
	r.pos += int64(n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (r *spoolReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.s.release()
	return nil
}
