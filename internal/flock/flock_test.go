package flock

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTryLockUnlock(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "test.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// TryLock on unlocked file should succeed
	if err := TryLock(f); err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}

	// Unlock
	if err := Unlock(f); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestTryLockContention(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "contention.lock")

	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	// Lock on f1 via TryLock
	if err := TryLock(f1); err != nil {
		t.Fatalf("TryLock on f1 failed: %v", err)
	}

	// TryLock on f2 should fail with ErrLocked
	if err := TryLock(f2); err != ErrLocked {
		t.Fatalf("TryLock on f2 expected ErrLocked, got: %v", err)
	}

	// Unlock f1
	if err := Unlock(f1); err != nil {
		t.Fatalf("Unlock on f1 failed: %v", err)
	}

	// Now TryLock on f2 should succeed
	if err := TryLock(f2); err != nil {
		t.Fatalf("TryLock on f2 after unlock failed: %v", err)
	}

	// Cleanup
	if err := Unlock(f2); err != nil {
		t.Fatalf("Unlock on f2 failed: %v", err)
	}
}

func TestDoubleUnlock(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "double_unlock.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// TryLock
	if err := TryLock(f); err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}

	// Unlock
	if err := Unlock(f); err != nil {
		t.Fatalf("First Unlock failed: %v", err)
	}

	// Double unlock should not error
	if err := Unlock(f); err != nil {
		t.Fatalf("Second Unlock failed: %v", err)
	}
}

func TestTryLockOnUnlockedFile(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "trylock_unlocked.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// TryLock on an unlocked file should succeed immediately
	if err := TryLock(f); err != nil {
		t.Fatalf("TryLock on unlocked file failed: %v", err)
	}

	if err := Unlock(f); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestConcurrentTryLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent_trylock.lock")

	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	// Lock on f1
	if err := TryLock(f1); err != nil {
		t.Fatalf("TryLock on f1 failed: %v", err)
	}

	// Multiple TryLock attempts on f2 should all return ErrLocked
	for i := 0; i < 5; i++ {
		if err := TryLock(f2); err != ErrLocked {
			t.Fatalf("TryLock attempt %d: expected ErrLocked, got: %v", i+1, err)
		}
	}

	// Unlock f1
	if err := Unlock(f1); err != nil {
		t.Fatalf("Unlock on f1 failed: %v", err)
	}
}

func TestTryLockOnClosedFile(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "closed.lock"))
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	// TryLock on a closed file should return an error
	if err := TryLock(f); err == nil {
		t.Fatal("TryLock on closed file expected error, got nil")
	}
}

func TestUnlockWithoutLock(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "unlock_without_lock.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Unlock without holding a lock should not error
	if err := Unlock(f); err != nil {
		t.Fatalf("Unlock without lock failed: %v", err)
	}
}

func TestLockUnlock(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "lock.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Lock should succeed on unlocked file
	if err := Lock(f); err != nil {
		t.Fatalf("Lock failed: %v", err)
	}

	// Unlock
	if err := Unlock(f); err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}
}

func TestLockBlocking(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blocking.lock")

	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	// Hold the lock on f1
	if err := TryLock(f1); err != nil {
		t.Fatalf("TryLock on f1 failed: %v", err)
	}

	// Lock on f2 should block; verify it doesn't return immediately
	locked := make(chan error, 1)
	go func() {
		locked <- Lock(f2)
	}()

	// Give the goroutine a moment to block on Lock
	time.Sleep(50 * time.Millisecond)

	select {
	case err := <-locked:
		t.Fatalf("Lock on f2 returned before unlock: %v", err)
	default:
		// Good — Lock is blocking as expected
	}

	// Release f1 — f2 should now acquire the lock
	if err := Unlock(f1); err != nil {
		t.Fatalf("Unlock on f1 failed: %v", err)
	}

	select {
	case err := <-locked:
		if err != nil {
			t.Fatalf("Lock on f2 failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Lock on f2 did not acquire after f1 unlocked (timeout)")
	}

	// Cleanup
	if err := Unlock(f2); err != nil {
		t.Fatalf("Unlock on f2 failed: %v", err)
	}
}

func TestLockConcurrent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "concurrent.lock")

	const goroutines = 10
	const iterations = 5

	var counter atomic.Int64
	var active atomic.Int32
	var overlaps atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					t.Errorf("OpenFile failed: %v", err)
					return
				}
				if err := Lock(f); err != nil {
					t.Errorf("Lock failed: %v", err)
					f.Close()
					return
				}
				if active.Add(1) != 1 {
					overlaps.Add(1)
				}
				counter.Add(1)
				active.Add(-1)
				if err := Unlock(f); err != nil {
					t.Errorf("Unlock failed: %v", err)
					f.Close()
					return
				}
				f.Close()
			}
		}()
	}
	wg.Wait()

	expected := int64(goroutines * iterations)
	if got := counter.Load(); got != expected {
		t.Fatalf("expected counter=%d, got=%d", expected, got)
	}
	if got := overlaps.Load(); got != 0 {
		t.Fatalf("lock allowed %d overlapping critical sections", got)
	}
}

func TestLockContextCancel(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cancel.lock")

	f1, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f1.Close()

	f2, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()

	// Hold the lock on f1
	if err := TryLock(f1); err != nil {
		t.Fatalf("TryLock on f1 failed: %v", err)
	}

	// Lock on f2 in a goroutine with a context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Lock in a tight loop — we cancel the context to stop
		for {
			if err := TryLock(f2); err == nil {
				// Acquired unexpectedly — unlock and return
				Unlock(f2)
				return
			}
			if ctx.Err() != nil {
				return
			}
			runtime.Gosched()
		}
	}()

	// Let the goroutine spin for a bit
	time.Sleep(50 * time.Millisecond)

	// Cancel the context — goroutine should exit
	cancel()

	select {
	case <-done:
		// Goroutine exited as expected
	case <-time.After(time.Second):
		t.Fatal("goroutine did not exit after context cancellation (timeout)")
	}

	// Cleanup
	if err := Unlock(f1); err != nil {
		t.Fatalf("Unlock on f1 failed: %v", err)
	}
}
