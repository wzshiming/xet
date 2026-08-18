package storage

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrGCBusy reports that a sweep or compaction is already running.
var ErrGCBusy = errors.New("another GC operation is in progress")

// Collector is the full maintenance surface, composing what Unlink and Sweep
// need with what Compact needs. Both FileStorage and S3Storage implement it.
type Collector interface {
	SweepStore
	CompactStore
}

var (
	_ Collector = (*FileStorage)(nil)
	_ Collector = (*S3Storage)(nil)
)

// FileRemovedHook is called after a file's index entries are removed, e.g. to
// drop matching mirror index entries. sha256Hex is empty when the unlink
// could not resolve one.
type FileRemovedHook func(ctx context.Context, sha256Hex, fileHash string) error

// GC coordinates the destructive maintenance operations of one store for
// in-process callers and HTTP endpoints alike. Sweep and Compact share a
// single-flight guard because they must not overlap (see Compact); route
// every sweep and compaction of a store through the same GC instance.
type GC struct {
	st              Collector
	fileRemovedHook FileRemovedHook

	active atomic.Bool
}

// GCOption configures a GC.
type GCOption func(*GC)

// WithFileRemovedHook sets a callback invoked after each successful Unlink.
func WithFileRemovedHook(fn FileRemovedHook) GCOption {
	return func(g *GC) {
		g.fileRemovedHook = fn
	}
}

// NewGC returns a coordinator for the store's GC operations.
func NewGC(st Collector, opts ...GCOption) *GC {
	g := &GC{st: st}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Unlink removes the index entries that make the file reachable and runs the
// file-removed hook. A hook failure does not undo the unlink; it is reported
// in the result's HookError. Unlink is deliberately not serialized against
// Sweep and Compact: the deletes are idempotent and safe alongside both.
func (g *GC) Unlink(ctx context.Context, hashStr string, kind HashKind) (*UnlinkResult, error) {
	res, err := Unlink(ctx, g.st, hashStr, kind)
	if err != nil {
		return res, err
	}
	if g.fileRemovedHook != nil {
		if hookErr := g.fileRemovedHook(ctx, res.SHA256, res.FileHash); hookErr != nil {
			res.HookError = hookErr.Error()
		}
	}
	return res, nil
}

// Sweep runs one mark-and-sweep pass, or returns ErrGCBusy while another
// sweep or compaction is running.
func (g *GC) Sweep(ctx context.Context, opts SweepOptions) (*SweepReport, error) {
	if !g.active.CompareAndSwap(false, true) {
		return nil, ErrGCBusy
	}
	defer g.active.Store(false)
	return Sweep(ctx, g.st, opts)
}

// Compact runs one compaction pass, or returns ErrGCBusy while another sweep
// or compaction is running.
func (g *GC) Compact(ctx context.Context, opts CompactOptions) (*CompactReport, error) {
	if !g.active.CompareAndSwap(false, true) {
		return nil, ErrGCBusy
	}
	defer g.active.Store(false)
	return Compact(ctx, g.st, opts)
}

// ListFiles enumerates the store's files with their SHA-256 mappings and
// sizes. It is read-only and, like Unlink, not serialized against Sweep or
// Compact.
func (g *GC) ListFiles(ctx context.Context) ([]FileListEntry, error) {
	return ListFiles(ctx, g.st)
}
