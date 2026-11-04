package download

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wzshiming/xet/internal/sysmem"
)

// blockWindow is how long an Acquire must stay pending to count as blocked.
const blockWindow = 50 * time.Millisecond

// hangGuard bounds every wait for an Acquire that must return, so a broken
// budget fails the test instead of hanging it.
const hangGuard = 10 * time.Second

// budgetWithTarget returns a budget whose target is target while no download
// is attached.
func budgetWithTarget(target int64) *MemoryBudget {
	return NewMemoryBudget(MemorySizes{Base: target, Limit: target})
}

// mustAcquire acquires n from b and fails the test if that blocks.
func mustAcquire(t *testing.T, b *MemoryBudget, n int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), hangGuard)
	defer cancel()
	if err := b.Acquire(ctx, n); err != nil {
		t.Fatalf("Acquire(%d) with %d used: %v", n, usedOf(b), err)
	}
}

// acquireAsync runs b.Acquire(ctx, n) on its own goroutine and delivers its
// result.
func acquireAsync(ctx context.Context, b *MemoryBudget, n int64) <-chan error {
	done := make(chan error, 1)
	go func() { done <- b.Acquire(ctx, n) }()
	return done
}

// awaitQueued waits until n Acquire calls wait in b.
func awaitQueued(t *testing.T, b *MemoryBudget, n int) {
	t.Helper()
	for deadline := time.Now().Add(hangGuard); ; time.Sleep(time.Millisecond) {
		b.mu.Lock()
		queued := b.waiters.Len()
		b.mu.Unlock()
		if queued == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d Acquire calls wait after %v, want %d", queued, hangGuard, n)
		}
	}
}

// requireBlocked fails the test if the Acquire behind done returns within
// blockWindow.
func requireBlocked(t *testing.T, done <-chan error, what string) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("%s returned %v, want it to block", what, err)
	case <-time.After(blockWindow):
	}
}

// awaitAcquire returns the result of the Acquire behind done.
func awaitAcquire(t *testing.T, done <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(hangGuard):
		t.Fatalf("%s still blocked after %v", what, hangGuard)
		return nil
	}
}

// requireAdmitted fails the test unless the Acquire behind done returns nil.
func requireAdmitted(t *testing.T, done <-chan error, what string) {
	t.Helper()
	if err := awaitAcquire(t, done, what); err != nil {
		t.Fatalf("%s = %v, want nil", what, err)
	}
}

// usedOf returns the bytes b has admitted and not released.
func usedOf(b *MemoryBudget) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// attachedOf returns the downloads attached to b.
func attachedOf(b *MemoryBudget) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.active
}

// requireTarget checks that b admits exactly want bytes in all, holding one
// byte first so that the oversized rule cannot hide the target, and then
// releases them.
func requireTarget(t *testing.T, b *MemoryBudget, want int64) {
	t.Helper()
	mustAcquire(t, b, 1)
	mustAcquire(t, b, want-1)
	over := acquireAsync(context.Background(), b, 1)
	requireBlocked(t, over, fmt.Sprintf("Acquire(1) with %d used, the target being %d", want, want))
	b.Release(1)
	requireAdmitted(t, over, fmt.Sprintf("Acquire(1) with %d used, the target being %d", want-1, want))
	b.Release(want)
}

func TestMemoryBudgetAcquireBlocksUntilRelease(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 100)
	done := acquireAsync(context.Background(), b, 1)
	awaitQueued(t, b, 1)
	requireBlocked(t, done, "Acquire(1) with 100 of 100 used")
	b.Release(1)
	requireAdmitted(t, done, "Acquire(1) after Release(1)")
}

func TestMemoryBudgetTargetFollowsAttachedDownloads(t *testing.T) {
	b := NewMemoryBudget(MemorySizes{Base: 100, PerDownload: 10, Limit: 125})
	b.Attach()
	requireTarget(t, b, 110)
	second := b.Attach()
	requireTarget(t, b, 120)
	third := b.Attach()
	requireTarget(t, b, 125)
	third()
	requireTarget(t, b, 120)
	second()
	requireTarget(t, b, 110)
}

func TestMemoryBudgetAttachAdmitsWaiters(t *testing.T) {
	b := NewMemoryBudget(MemorySizes{Base: 100, PerDownload: 10, Limit: 125})
	b.Attach()
	mustAcquire(t, b, 5)
	done := acquireAsync(context.Background(), b, 115)
	awaitQueued(t, b, 1)
	b.Attach()
	requireAdmitted(t, done, "Acquire(115) with 5 used after a second Attach")
}

func TestMemoryBudgetServesWaitersInArrivalOrder(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 90)
	first := acquireAsync(context.Background(), b, 50)
	awaitQueued(t, b, 1)
	second := acquireAsync(context.Background(), b, 5)
	requireBlocked(t, second, "Acquire(5) with 90 of 100 used while Acquire(50) waits")
	awaitQueued(t, b, 2)
	// The 90 come back in steps: at 80 used only the later Acquire(5)
	// would fit, at 50 the earlier Acquire(50) fits but not both.
	b.Release(10)
	requireBlocked(t, second, "Acquire(5) with 80 of 100 used while Acquire(50) waits")
	b.Release(30)
	requireAdmitted(t, first, "Acquire(50) with 50 of 100 used")
	requireBlocked(t, second, "Acquire(5) with 100 of 100 used")
	b.Release(50)
	requireAdmitted(t, second, "Acquire(5) with 50 of 100 used")
}

func TestMemoryBudgetReleaseAdmitsTwoFittingWaiters(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 100)
	first := acquireAsync(context.Background(), b, 30)
	awaitQueued(t, b, 1)
	second := acquireAsync(context.Background(), b, 40)
	awaitQueued(t, b, 2)
	b.Release(100)
	requireAdmitted(t, first, "Acquire(30) after Release(100)")
	requireAdmitted(t, second, "Acquire(40) queued behind Acquire(30) after Release(100)")
	if used := usedOf(b); used != 70 {
		t.Fatalf("%d used after one Release admitted Acquire(30) and Acquire(40), want 70", used)
	}
}

func TestMemoryBudgetAcquireStopsWhenContextIsDone(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 90)
	ctx, cancel := context.WithCancel(context.Background())
	cancelled := acquireAsync(ctx, b, 50)
	awaitQueued(t, b, 1)
	later := acquireAsync(context.Background(), b, 5)
	awaitQueued(t, b, 2)
	cancel()
	if err := awaitAcquire(t, cancelled, "Acquire(50) after its ctx is cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(50) after its ctx is cancelled = %v, want %v", err, context.Canceled)
	}
	requireAdmitted(t, later, "Acquire(5) queued behind the cancelled Acquire(50)")
	if used := usedOf(b); used != 95 {
		t.Fatalf("%d used after the cancelled Acquire(50) and Acquire(5), want 95", used)
	}
	b.Release(95)
	if used := usedOf(b); used != 0 {
		t.Fatalf("%d used after Release(95), want 0", used)
	}
}

func TestMemoryBudgetAcquireCancelledWhileAdmittedKeepsNothing(t *testing.T) {
	// Release can admit the waiter between its ctx ending and its return.
	for round := range 200 {
		b := budgetWithTarget(100)
		mustAcquire(t, b, 100)
		ctx, cancel := context.WithCancel(context.Background())
		done := acquireAsync(ctx, b, 10)
		awaitQueued(t, b, 1)
		cancel()
		b.Release(100)
		err := awaitAcquire(t, done, "Acquire(10) racing its cancel and Release(100)")
		want := int64(10)
		if err != nil {
			want = 0
		}
		if used := usedOf(b); used != want {
			t.Fatalf("round %d: Acquire(10) racing its cancel and Release(100) = %v with %d used, want %d", round, err, used, want)
		}
	}
}

func TestMemoryBudgetAdmitsOversizedAcquireWhenNothingIsUsed(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 1)
	big := acquireAsync(context.Background(), b, 150)
	awaitQueued(t, b, 1)
	requireBlocked(t, big, "Acquire(150) with 1 of 100 used")
	b.Release(1)
	requireAdmitted(t, big, "Acquire(150) with nothing used")
	small := acquireAsync(context.Background(), b, 1)
	awaitQueued(t, b, 1)
	requireBlocked(t, small, "Acquire(1) with 150 of 100 used")
	b.Release(150)
	requireAdmitted(t, small, "Acquire(1) after Release(150)")
}

func TestNilMemoryBudgetIsUnlimited(t *testing.T) {
	var b *MemoryBudget
	detach := b.Attach()
	if err := b.Acquire(context.Background(), 1<<62); err != nil {
		t.Fatalf("Acquire(1<<62) on a nil budget = %v, want nil", err)
	}
	b.Release(1 << 62)
	detach()
}

func TestMemoryBudgetReleaseBeyondUsedPanics(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 10)
	defer func() {
		if recover() == nil {
			t.Fatal("Release(11) with 10 used did not panic")
		}
	}()
	b.Release(11)
}

func TestMemoryBudgetDetachIsIdempotent(t *testing.T) {
	b := NewMemoryBudget(MemorySizes{Base: 100, PerDownload: 10, Limit: 125})
	b.Attach()
	detach := b.Attach()
	detach()
	detach()
	requireTarget(t, b, 110)
}

func TestMemoryBudgetIgnoresNonPositiveAmounts(t *testing.T) {
	b := budgetWithTarget(100)
	mustAcquire(t, b, 100)
	waiting := acquireAsync(context.Background(), b, 1)
	awaitQueued(t, b, 1)
	for _, n := range []int64{0, -10} {
		mustAcquire(t, b, n)
		b.Release(n)
		if used := usedOf(b); used != 100 {
			t.Fatalf("%d used after Acquire(%d) and Release(%d) with 100 used, want 100", used, n, n)
		}
	}
	b.Release(100)
	requireAdmitted(t, waiting, "Acquire(1) after Release(100)")
}

func TestDefaultMemorySizesFollowXetCore(t *testing.T) {
	const mib, gib = 1 << 20, 1 << 30
	for _, c := range []struct {
		name string
		ram  int64
		ok   bool
		want MemorySizes
	}{
		{"a sixteenth, a sixty-fourth and a quarter", 16 * gib, true, MemorySizes{Base: gib, PerDownload: 256 * mib, Limit: 4 * gib}},
		{"the Limit floor", gib, true, MemorySizes{Base: 64 * mib, PerDownload: 16 * mib, Limit: 264 * mib}},
		{"every floor", 512 * mib, true, MemorySizes{Base: 64 * mib, PerDownload: 16 * mib, Limit: 264 * mib}},
		{"rounded down to 8 MiB", 16*gib + 24*mib, true, MemorySizes{Base: gib, PerDownload: 256 * mib, Limit: 4 * gib}},
		{"rounded down to 8 MiB, not 16", 16*gib + 200*mib, true, MemorySizes{Base: gib + 8*mib, PerDownload: 256 * mib, Limit: 4*gib + 48*mib}},
		{"static sizes for unknown memory", 0, false, MemorySizes{Base: 2 * gib, PerDownload: 512 * mib, Limit: 8 * gib}},
	} {
		got := defaultMemorySizes(func() (int64, bool) { return c.ram, c.ok })
		if got != c.want {
			t.Errorf("%s: defaultMemorySizes of %d bytes (known %t) = %+v, want %+v", c.name, c.ram, c.ok, got, c.want)
		}
	}
}

func TestDefaultMemorySizesReadUsableMemory(t *testing.T) {
	got, want := DefaultMemorySizes(), defaultMemorySizes(sysmem.Usable)
	if got != want {
		t.Fatalf("DefaultMemorySizes() = %+v, want %+v from sysmem.Usable", got, want)
	}
	t.Logf("DefaultMemorySizes() = %+v", got)
}
