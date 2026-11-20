package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/wzshiming/xet/shard"
)

// reconstructedFile exposes a shard file as an io.ReadSeekCloser and an
// io.WriterTo. Its bytes come from a decode pipeline started by the first Read
// or WriteTo that needs them, so http.ServeContent answers HEAD and range
// requests without extra I/O. Calls are serialized; Close fails a Read,
// WriteTo or Seek still waiting with fs.ErrClosed.
type reconstructedFile struct {
	ctx      context.Context
	life     context.Context // child of ctx every pipeline derives from
	cancel   context.CancelFunc
	storage  Storage
	entries  []shard.FileDataSequenceEntry
	size     int64
	mu       sync.Mutex // held through each Read, WriteTo, Seek and Close
	position int64
	closed   bool
	err      error // sticky until a Seek away

	pipe     *pipeline
	cur      *frame // delivered chunk holding position; nil without pipe
	chunkOff int64  // file offset of cur.data[0]
}

// NewReconstructedFile exposes the file whose terms are entries.
func NewReconstructedFile(ctx context.Context, storage Storage, entries []shard.FileDataSequenceEntry) io.ReadSeekCloser {
	f := &reconstructedFile{
		ctx:     ctx,
		storage: storage,
		entries: entries,
	}
	f.life, f.cancel = context.WithCancel(ctx)
	for i := range f.entries {
		f.size += int64(f.entries[i].UnpackedSegBytes)
	}
	return f
}

func (f *reconstructedFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.position >= f.size {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && f.position < f.size {
		data, withheld, err := f.unread()
		if err != nil {
			if withheld && n > 0 {
				return n, nil // the sticky error ends the next Read
			}
			return n, err
		}
		copied := copy(p[n:], data)
		f.position += int64(copied)
		n += copied
	}
	return n, nil
}

// WriteTo writes the file from the current position on to w, one Write per
// decoded chunk of at most 128 KiB, whose bytes are valid only during the call.
// Storage, Close and context failures end it as they end Read; a failed or
// short Write stops the pipeline without sticking, so the next call resumes
// after the bytes written. Close waits for a Write in progress to return, as
// mu is held through w.Write.
func (f *reconstructedFile) WriteTo(w io.Writer) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}

	var n int64
	for f.position < f.size {
		data, _, err := f.unread()
		if err != nil {
			return n, err
		}
		m, err := w.Write(data)
		f.position += int64(m)
		n += int64(m)
		if err == nil && m < len(data) {
			err = io.ErrShortWrite
		}
		if err != nil {
			f.stop()
			return n, err
		}
	}
	return n, nil
}

func (f *reconstructedFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.closing() {
		return 0, fmt.Errorf("seek reconstructed file: %w", fs.ErrClosed)
	}
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = f.position + offset
	case io.SeekEnd:
		next = f.size + offset
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek position %d", next)
	}
	if next == f.position {
		return next, nil
	}
	// Outside the delivered chunk, the next Read restarts the pipeline.
	if f.cur == nil || next < f.chunkOff || next > f.chunkOff+int64(len(f.cur.data)) {
		f.stop()
		f.err = nil
	}
	f.position = next
	return next, nil
}

func (f *reconstructedFile) Close() error {
	// Cancel before locking: a Read waiting on the pipeline wakes and lets go of mu.
	f.cancel()
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		f.stop()
	}
	return nil
}

// closing reports whether Close has cancelled life while ctx is live.
func (f *reconstructedFile) closing() bool {
	return f.life.Err() != nil && f.ctx.Err() == nil
}

// check reports a Close or a cancelled parent, stopping the pipeline only for
// the parent: Close stops it itself.
func (f *reconstructedFile) check() error {
	if f.closed || f.closing() {
		return fmt.Errorf("read reconstructed file: %w", fs.ErrClosed)
	}
	if err := f.ctx.Err(); err != nil {
		f.stop()
		return err
	}
	return nil
}

// unread returns the bytes still to deliver from position to the end of their
// chunk; withheld marks an error that held back the file's final chunk.
func (f *reconstructedFile) unread() (data []byte, withheld bool, err error) {
	if f.err != nil {
		return nil, false, f.err
	}
	if f.cur == nil || f.position == f.chunkOff+int64(len(f.cur.data)) {
		if err := f.advance(); err != nil {
			return nil, false, f.fail(err)
		}
		// Withhold the final chunk until the fetcher has checked the last term's size.
		if f.chunkOff+int64(len(f.cur.data)) >= f.size {
			if err := f.pipe.drain(f.life); err != nil {
				return nil, true, f.fail(err)
			}
		}
	}
	return f.cur.data[f.position-f.chunkOff:], false, nil
}

// advance makes cur the next delivered chunk holding position, starting the
// pipeline at the term holding position when none runs.
func (f *reconstructedFile) advance() error {
	if f.cur != nil {
		f.chunkOff += int64(len(f.cur.data))
		f.pipe.release(f.cur)
		f.cur = nil
	}
	if f.pipe == nil {
		var term int
		term, f.chunkOff = f.termAt(f.position)
		f.pipe = startPipeline(f.life, f.storage, f.entries[term:])
	}
	for {
		fr, err := f.pipe.next(f.life)
		if err != nil {
			return err
		}
		// A restarted pipeline begins at its term's first chunk: discard up to position.
		if end := f.chunkOff + int64(len(fr.data)); f.position >= end {
			f.chunkOff = end
			f.pipe.release(fr)
			continue
		}
		f.cur = fr
		return nil
	}
}

// termAt returns the index and file offset of the term holding position,
// which is never an empty term.
func (f *reconstructedFile) termAt(position int64) (int, int64) {
	var off int64
	for i := range f.entries {
		end := off + int64(f.entries[i].UnpackedSegBytes)
		if position < end {
			return i, off
		}
		off = end
	}
	return len(f.entries), off
}

// fail stops the pipeline and makes err sticky.
func (f *reconstructedFile) fail(err error) error {
	// A Close or a cancelled parent surfaces as itself, not as the storage error it caused.
	if f.closing() {
		err = fmt.Errorf("read reconstructed file: %w", fs.ErrClosed)
	} else if ctxErr := f.ctx.Err(); ctxErr != nil {
		err = ctxErr
	}
	f.stop()
	f.err = err
	return err
}

// stop tears the pipeline down, returning every buffer it held.
func (f *reconstructedFile) stop() {
	if f.pipe == nil {
		return
	}
	f.pipe.stop()
	f.pipe, f.cur = nil, nil
}
