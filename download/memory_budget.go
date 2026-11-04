package download

import (
	"container/list"
	"context"
	"sync"

	"github.com/wzshiming/xet/internal/sysmem"
)

// MemorySizes sizes a MemoryBudget in bytes: its target is
// min(Base + active×PerDownload, Limit) while active downloads are attached.
type MemorySizes struct {
	Base, PerDownload, Limit int64
}

// DefaultMemorySizes sizes the budget from the memory this process can use,
// as xet-core sizes its reconstruction download buffer: a sixteenth of it as
// Base, a sixty-fourth as PerDownload and a quarter as Limit, each rounded
// down to a multiple of 8 MiB and raised to at least 64 MiB, 16 MiB and
// 264 MiB. When that memory is unknown it returns 2 GiB, 512 MiB and 8 GiB.
func DefaultMemorySizes() MemorySizes {
	return defaultMemorySizes(sysmem.Usable)
}

func defaultMemorySizes(usable func() (int64, bool)) MemorySizes {
	ram, ok := usable()
	if !ok {
		return MemorySizes{Base: 2 << 30, PerDownload: 512 << 20, Limit: 8 << 30}
	}
	return MemorySizes{
		Base:        memorySize(ram/16, 64<<20),
		PerDownload: memorySize(ram/64, 16<<20),
		Limit:       memorySize(ram/4, 264<<20),
	}
}

// memorySize rounds n down to a multiple of 8 MiB and raises it to floor.
func memorySize(n, floor int64) int64 {
	const step = 8 << 20
	return max(n/step*step, floor)
}

// MemoryBudget bounds the memory that downloads hold in reconstruction
// terms, as xet-core's reconstruction download buffer does. Its unit is a
// term's whole unpacked length: a reader acquires it before the term's fetch
// starts and releases it once the term is delivered.
//
// Acquire admits n bytes while used+n stays within the target,
// min(Base + active×PerDownload, Limit), where active counts the attached
// downloads, and blocks otherwise; attaching or detaching a download changes
// the target at once. Waiters are served in arrival order. A request larger
// than the target is admitted once nothing is used, so one oversized term
// cannot deadlock.
//
// A nil *MemoryBudget is unlimited. Methods are safe for concurrent use.
type MemoryBudget struct {
	sizes MemorySizes

	mu      sync.Mutex
	active  int64
	used    int64
	waiters list.List // of *budgetWaiter, in arrival order
}

type budgetWaiter struct {
	n     int64
	ready chan struct{} // closed once n is added to used
}

// NewMemoryBudget returns a budget sized by sizes with no download attached.
func NewMemoryBudget(sizes MemorySizes) *MemoryBudget {
	return &MemoryBudget{sizes: sizes}
}

// Attach registers an active download, raising the target by PerDownload up
// to Limit, until detach is called; calls of detach after the first do
// nothing.
func (b *MemoryBudget) Attach() (detach func()) {
	if b == nil {
		return func() {}
	}
	b.mu.Lock()
	b.active++
	b.admitLocked()
	b.mu.Unlock()
	return sync.OnceFunc(func() {
		b.mu.Lock()
		// A lower target admits no waiter.
		b.active--
		b.mu.Unlock()
	})
}

// Acquire blocks until n more bytes fit, after every earlier waiter, and
// counts them as used. If ctx is done first it returns ctx.Err() and holds
// nothing. n <= 0 returns nil at once.
func (b *MemoryBudget) Acquire(ctx context.Context, n int64) error {
	if b == nil || n <= 0 {
		return nil
	}
	b.mu.Lock()
	if b.waiters.Len() == 0 && b.fitsLocked(n) {
		b.used += n
		b.mu.Unlock()
		return nil
	}
	w := &budgetWaiter{n: n, ready: make(chan struct{})}
	elem := b.waiters.PushBack(w)
	b.mu.Unlock()

	select {
	case <-w.ready:
		return nil
	case <-ctx.Done():
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	select {
	case <-w.ready:
		// Admitted after ctx ended: give the bytes back.
		b.used -= n
	default:
		b.waiters.Remove(elem)
	}
	b.admitLocked()
	return ctx.Err()
}

// Release returns n acquired bytes and admits the waiters that now fit.
// n <= 0 does nothing; releasing more than is used panics.
func (b *MemoryBudget) Release(n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.used {
		panic("download: MemoryBudget released more than it holds")
	}
	b.used -= n
	b.admitLocked()
}

// admitLocked admits waiters in arrival order while the first one fits.
func (b *MemoryBudget) admitLocked() {
	for e := b.waiters.Front(); e != nil; e = b.waiters.Front() {
		w := e.Value.(*budgetWaiter)
		if !b.fitsLocked(w.n) {
			return
		}
		b.used += w.n
		b.waiters.Remove(e)
		close(w.ready)
	}
}

func (b *MemoryBudget) fitsLocked(n int64) bool {
	return b.used == 0 || b.used+n <= min(b.sizes.Base+b.active*b.sizes.PerDownload, b.sizes.Limit)
}
