package storage

import (
	"context"
	"errors"
	"sync/atomic"
)

// ErrGCBusy reports that another sweep is already running.
var ErrGCBusy = errors.New("another GC operation is in progress")

var (
	_ SweepStore = (*FileStorage)(nil)
	_ SweepStore = (*S3Storage)(nil)
)

// GC coordinates the destructive maintenance operations of one store for
// in-process callers and HTTP endpoints alike. Sweep runs single-flight:
// route every sweep of a store through the same GC instance so concurrent
// requests fail fast with ErrGCBusy instead of queueing on the store guard.
type GC struct {
	st SweepStore

	active atomic.Bool
}

// NewGC returns a coordinator for the store's GC operations.
func NewGC(st SweepStore) *GC {
	return &GC{st: st}
}

// Unlink removes the file index entry (by xet file hash only; see Unlink).
// It is deliberately not serialized against Sweep: the deletes are
// idempotent and safe alongside it.
func (g *GC) Unlink(ctx context.Context, fileHashStr string) (*UnlinkResult, error) {
	return Unlink(ctx, g.st, fileHashStr)
}

// Sweep runs one mark-and-sweep pass, or returns ErrGCBusy while another
// sweep is running.
func (g *GC) Sweep(ctx context.Context, opts SweepOptions) (*SweepReport, error) {
	if !g.active.CompareAndSwap(false, true) {
		return nil, ErrGCBusy
	}
	defer g.active.Store(false)
	return Sweep(ctx, g.st, opts)
}

// ListFiles enumerates the store's files with their SHA-256 mappings and
// sizes. It is read-only and, like Unlink, not serialized against Sweep.
func (g *GC) ListFiles(ctx context.Context) ([]FileListEntry, error) {
	return ListFiles(ctx, g.st)
}
