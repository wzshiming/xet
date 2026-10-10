// Package spool handles the protocol-agnostic spooling of downloaded bytes on
// the way into local storage: an append-only, tail-readable spool that buffers
// in memory and spills to a file named by content identity, and the
// verify-and-ingest step into storage. Nothing here is specific to any hub.
//
// Spool is the ledger of the downloads in flight through one spool directory,
// keyed by spool path. Accept hands a caller the writer of a new flight or a
// follower of the one already spooling the same content on the same origin;
// the writer's Finish verifies and ingests the bytes, and Wait settles every
// Item on that one result checked against its own expectations. Sweep removes
// the idle spool files; a leftover spilled by a crashed process is resumed by
// the next Accept that names it, never scanned for.
package spool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/wzshiming/ioswmr"
	"github.com/wzshiming/xet/storage"
)

// Spool is the ledger of downloads in flight through one spool directory, keyed by spool path; content spools are shared within an origin.
type Spool struct {
	dir     string
	st      storage.Storage
	mu      sync.Mutex // flights
	openMu  sync.Mutex // serializes spool open+register against Sweep
	flights map[string]*flight
}

// NewSpool returns the spool ledger over dir, ingesting into st.
func NewSpool(dir string, st storage.Storage) (*Spool, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("spool: create %s: %w", dir, err)
	}
	return &Spool{dir: dir, st: st, flights: map[string]*flight{}}, nil
}

// Source is what a caller knows about the content it wants spooled.
type Source struct {
	Origin string // trust domain shared downloads are scoped to (scheme://host for hubs)
	Key    string // the caller's name for the file; names its private spools
	ETag   string // upstream validator; a content hash (40- or 64-hex) makes the spool shareable within Origin
	Size   int64  // expected length, -1 when unknown
	SHA256 string // lowercase hex digest the bytes must hash to, "" when unknown
}

// flight is one download: its spool, size knowledge and outcome, shared by every Item attached to it. Each Item holds the spool until Release; the writer's hold outlasts the registration, so a registered flight's spool is live.
type flight struct {
	swmr     ioswmr.SWMR
	path     string // the spool file, present only once spilled or adopted
	sized    chan struct{}
	sizeOnce sync.Once
	size     atomic.Int64 // -1 until known
	done     chan struct{}
	once     sync.Once // Finish runs once
	res      Result
	err      error
}

// setSize records the content length once known (first value wins) and unblocks size waiters; n < 0 only signals.
func (f *flight) setSize(n int64) {
	if n >= 0 {
		f.size.CompareAndSwap(-1, n)
	}
	f.sizeOnce.Do(func() { close(f.sized) })
}

// Item is one caller's handle on a flight: the writer that downloads into the spool, or a follower reading it; every Item must be Released.
type Item struct {
	s           *Spool
	f           *flight
	src         Source
	hold        io.Closer     // keeps the spool alive until Release
	w           ioswmr.Writer // the download's writer; nil for a follower
	releaseOnce sync.Once
}

var errNotWriter = errors.New("spool: not the writer")

// Writer reports whether this Item downloads into the spool rather than following it.
func (it *Item) Writer() bool { return it.w != nil }

// Write appends the writer's downloaded bytes to the spool.
func (it *Item) Write(p []byte) (int, error) {
	if it.w == nil {
		return 0, errNotWriter
	}
	return it.w.Write(p)
}

// Seek supports the io.WriteSeeker contract of client.DownloadFile, which seeks to the end for the resume offset before writing sequentially; a spool never rewinds, so every other target is refused.
func (it *Item) Seek(offset int64, whence int) (int64, error) {
	if it.w == nil {
		return 0, errNotWriter
	}
	// ioswmr's Writer.Seek refuses a Seek after any Write; retried downloads seek again.
	end := it.Written()
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent, io.SeekEnd:
		offset += end
	default:
		return 0, os.ErrInvalid
	}
	if offset != end {
		return 0, ioswmr.ErrUnsupportedSeek
	}
	return end, nil
}

// Written returns the bytes spooled so far.
func (it *Item) Written() int64 { return int64(it.f.swmr.Length()) }

// path returns the spool file's path, present only after a spill or when adopted.
func (it *Item) path() string { return it.f.path }

// NewReader returns a tail-following reader of the spool from offset, an error once it retired; Close unblocks a waiting Read.
func (it *Item) NewReader(offset int) (io.ReadCloser, error) {
	return it.f.swmr.NewReader(offset)
}

// NewSeekReader returns a blocking ReadSeekCloser over the spool's final size positioned at offset, for http.ServeContent while bytes still land, an error once it retired.
func (it *Item) NewSeekReader(offset, size int) (io.ReadSeekCloser, error) {
	return it.f.swmr.NewReadSeeker(offset, size)
}

// Size returns the flight's content length, -1 until known.
func (it *Item) Size() int64 { return it.f.size.Load() }

// Sized is closed once Size is known or the writer gave up learning it early.
func (it *Item) Sized() <-chan struct{} { return it.f.sized }

// SetSize records the length the writer learned, or n < 0 that no early source remains; followers' calls are ignored.
func (it *Item) SetSize(n int64) {
	if it.w != nil {
		it.f.setSize(n)
	}
}

// Accept attaches src to the download of its content: as a follower of the flight already spooling it on src.Origin, else as the writer of a new one; ctx bounds only the wait for a leader's size. A spool has one writer: a second Accept naming the same spool while it is in flight is refused.
func (s *Spool) Accept(ctx context.Context, src Source) (*Item, error) {
	content := isContentHash(src.ETag)
	var sharedPath string
	if content {
		sharedPath = filepath.Join(s.dir, fileName(src.Origin, src.Key, src.ETag, src.Size, true))
		s.mu.Lock()
		leader := s.flights[sharedPath]
		s.mu.Unlock()
		follow := leader != nil
		if follow && src.Size >= 0 {
			select {
			case <-leader.sized: // bounded by the leader's first response
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			switch ls := leader.size.Load(); {
			case ls < 0:
				follow = false // unknown until the leader finishes: a body of a possibly wrong length is not served
			case ls != src.Size:
				return nil, fmt.Errorf("spool: size %d disagrees with the shared download's %d", src.Size, ls)
			}
		}
		// The hold fails only if the leader retired since the lookup; then this caller opens its own spool.
		if follow {
			if hold, err := leader.swmr.NewReader(0); err == nil {
				return &Item{s: s, f: leader, src: src, hold: hold}, nil
			}
		}
	}

	s.openMu.Lock()
	defer s.openMu.Unlock()
	s.mu.Lock()
	// The content path is shared only while nobody holds it: a crashed process's leftover is resumed, a registered leader never co-written.
	shared := content && s.flights[sharedPath] == nil
	name := fileName(src.Origin, src.Key, src.ETag, src.Size, shared)
	path := filepath.Join(s.dir, name)
	busy := s.flights[path] != nil
	s.mu.Unlock()
	if busy {
		return nil, fmt.Errorf("spool: %s is already being written", name)
	}
	swmr, err := open(path, src.ETag, src.Size)
	if err != nil {
		return nil, err
	}
	hold, _ := swmr.NewReader(0) // a fresh spool cannot have retired; the writer's hold lasts until Release
	f := &flight{swmr: swmr, path: path, sized: make(chan struct{}), done: make(chan struct{})}
	f.size.Store(-1)
	if src.Size >= 0 {
		f.setSize(src.Size)
	}
	s.mu.Lock()
	s.flights[path] = f
	s.mu.Unlock()
	return &Item{s: s, f: f, src: src, hold: hold, w: swmr.Writer()}, nil
}

// Finish ends the writer's download with err and, on success, verifies and ingests the spooled bytes into storage; once per flight, a no-op for followers, ctx bounds the ingest.
func (it *Item) Finish(ctx context.Context, err error) {
	if it.w == nil {
		return
	}
	s, f := it.s, it.f
	f.once.Do(func() {
		var res Result
		size := it.Written()
		if want := f.size.Load(); err == nil && want >= 0 && size != want {
			err = fmt.Errorf("spool: size mismatch: got %d bytes, want %d", size, want)
		}
		if err != nil {
			_ = it.w.CloseWithError(err)
		} else {
			f.size.Store(size)
			f.setSize(-1)                // definitive size stored above; size waiters need not wait for the ingest
			_ = it.w.CloseWithError(nil) // readers drain to EOF while the ingest runs
			if rs, e := it.NewSeekReader(0, int(size)); e != nil {
				err = e
			} else {
				res, err = ingest(ctx, s.st, rs, it.src.SHA256)
				_ = rs.Close()
			}
		}
		f.setSize(-1)
		f.res, f.err = res, err
		close(f.done)
		// Deregistered before the writer's Release, so a registered flight always has a live spool.
		s.mu.Lock()
		if s.flights[f.path] == f {
			delete(s.flights, f.path)
		}
		s.mu.Unlock()
	})
}

// ErrCorrupt marks spooled bytes that failed verification; the spool must be
// discarded rather than kept for resume.
var ErrCorrupt = errors.New("spool corrupt")

// Result describes a spooled file that landed in storage; FileHash is empty for empty files, which are never uploaded.
type Result struct {
	SHA256   string
	FileHash string
	Size     int64
}

// ingest verifies the spooled bytes against wantSHA256 (when given) and
// stores them in st.
func ingest(ctx context.Context, st storage.Storage, r io.ReadSeeker, wantSHA256 string) (Result, error) {
	hasher := sha256.New()
	size, err := io.Copy(hasher, r)
	if err != nil {
		return Result{}, fmt.Errorf("hash spool: %w", err)
	}
	digest := hex.EncodeToString(hasher.Sum(nil))
	if wantSHA256 != "" && digest != wantSHA256 {
		return Result{}, fmt.Errorf("%w: sha256 mismatch: got %s, expected %s", ErrCorrupt, digest, wantSHA256)
	}

	res := Result{SHA256: digest, Size: size}
	if size > 0 {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return Result{}, fmt.Errorf("rewind spool: %w", err)
		}
		fileHash, err := storage.PutFile(ctx, st, r)
		if err != nil {
			return Result{}, fmt.Errorf("ingest into storage: %w", err)
		}
		res.FileHash = fileHash.String()
	}
	return res, nil
}

// Wait blocks until the Item's flight ended and returns its result, checked against the Item's own Source.
func (s *Spool) Wait(ctx context.Context, it *Item) (Result, error) {
	f := it.f
	select {
	case <-f.done:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	if f.err != nil {
		return Result{}, f.err
	}
	res := f.res
	if it.src.Size >= 0 && res.Size != it.src.Size {
		return Result{}, fmt.Errorf("spool: size mismatch: got %d bytes, want %d", res.Size, it.src.Size)
	}
	if it.src.SHA256 != "" && res.SHA256 != it.src.SHA256 {
		return Result{}, fmt.Errorf("%w: sha256 mismatch: shared download is %s, expected %s", ErrCorrupt, res.SHA256, it.src.SHA256)
	}
	return res, nil
}

// Release drops the Item's hold on the spool; a writer released before Finish fails its flight so followers never hang.
func (it *Item) Release() {
	it.releaseOnce.Do(func() {
		if it.w != nil {
			it.Finish(context.Background(), errors.New("spool: writer released before finish"))
		}
		_ = it.hold.Close()
	})
}

// isContentHash reports whether etag is a content hash: the git blob sha1 of regular hub files or the LFS sha256.
func isContentHash(etag string) bool {
	if len(etag) != 40 && len(etag) != 64 {
		return false
	}
	_, err := hex.DecodeString(etag)
	return err == nil
}

func digest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

func keyPrefix(key string) string {
	return digest(key) + "-"
}

// originPrefix hashes a different domain than keyPrefix, so an origin and a key never share a prefix.
func originPrefix(origin string) string {
	return digest("origin\x00"+origin) + "-"
}

// fileName puts the content identity in a spool's name, so the next task
// naming the same content resumes a crashed process's leftover with no
// sidecar state. A shared spool with a content-hash etag is
// <originhash>-<hash>.spool, resumed and followed by every key of the origin,
// while another origin advertising the same etag gets its own file. Any other
// spool is the key's, named by a digest of the etag and size so a changed etag
// or size yields a different name; a task downloading beside the content's
// current leader is named that way too, so the content spool has one writer.
func fileName(origin, key, etag string, size int64, shared bool) string {
	if shared && isContentHash(etag) {
		return originPrefix(origin) + strings.ToLower(etag) + ".spool"
	}
	return keyPrefix(key) + digest(etag+"\x00"+strconv.FormatInt(size, 10)) + ".spool"
}

// open returns the append-only buffer of the download spooled at path: one
// writer appends while any number of readers tail it, blocking at the current
// end until more bytes land or the writer finishes. A leftover file at path is
// adopted, as fileName put the content identity in its name; one that is
// empty, has no etag to trust or is longer than expectedSize cannot be this
// content and is removed. A fresh spool stays in memory until its bytes
// outgrow the pooled tier, then spills to path; the buffer unlinks the file
// once the writer finished and the last hold or reader let go. A flight is
// deregistered before its writer releases, so a later Accept can adopt the
// file while this buffer still drains and then lose the name to this buffer's
// Close: the bytes stay fd-backed, only crash-resume for that window is lost.
func open(path, etag string, expectedSize int64) (ioswmr.SWMR, error) {
	var buf ioswmr.Buffer
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	switch {
	case err == nil:
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("stat spool file: %w", err)
		}
		if etag != "" && st.Size() > 0 && (expectedSize < 0 || st.Size() <= expectedSize) {
			buf = ioswmr.NewTemporaryFileBuffer(func() (*os.File, error) { return f, nil })
		} else {
			_ = f.Close()
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("remove stale spool: %w", err)
			}
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, fmt.Errorf("open spool file: %w", err)
	}
	if buf == nil {
		buf = ioswmr.NewMemoryOrTemporaryFileBuffer(nil, func() (*os.File, error) {
			return os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
		})
	}

	swmr := ioswmr.NewSWMR(buf, ioswmr.WithAutoClose())
	// The resume seek publishes an adopted file's bytes to readers and positions the writer after them.
	if _, err := swmr.Writer().Seek(0, io.SeekEnd); err != nil {
		_ = buf.Close()
		return nil, fmt.Errorf("seek spool file: %w", err)
	}
	return swmr, nil
}
