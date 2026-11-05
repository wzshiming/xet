package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"sync"

	"github.com/wzshiming/xet/shard"
)

// reconstructedFile exposes a shard file as an io.ReadSeekCloser. Its bytes
// come from a decode pipeline started by the first Read that needs them, so
// http.ServeContent answers HEAD and range requests without extra I/O.
// Calls are serialized; Close fails a Read or Seek still waiting with fs.ErrClosed.
type reconstructedFile struct {
	ctx      context.Context
	life     context.Context // child of ctx every pipeline derives from
	cancel   context.CancelFunc
	storage  Storage
	entries  []shard.FileDataSequenceEntry
	size     int64
	mu       sync.Mutex // held through each Read, Seek and Close
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
	if f.closed || f.closing() {
		return 0, fmt.Errorf("read reconstructed file: %w", fs.ErrClosed)
	}
	if err := f.ctx.Err(); err != nil {
		f.stop()
		return 0, err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if f.err != nil {
		return 0, f.err
	}
	if f.position >= f.size {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && f.position < f.size {
		if f.cur == nil || f.position == f.chunkOff+int64(len(f.cur.data)) {
			if err := f.advance(); err != nil {
				return n, f.fail(err)
			}
			// Withhold the final chunk until the fetcher has checked the last term's size.
			if f.chunkOff+int64(len(f.cur.data)) >= f.size {
				if err := f.pipe.drain(f.life); err != nil {
					err = f.fail(err)
					if n == 0 {
						return 0, err
					}
					return n, nil // the sticky error ends the next Read
				}
			}
		}
		copied := copy(p[n:], f.cur.data[f.position-f.chunkOff:])
		f.position += int64(copied)
		n += copied
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
