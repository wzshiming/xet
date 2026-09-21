package flock

import (
	"context"
	"errors"
	"os"
	"time"
)

// ErrLocked is returned when the file is already locked by another process.
var ErrLocked = errors.New("file is locked")

const (
	lockSpinMaxSleep = 100 * time.Millisecond // maximum sleep between retries
)

// Lock waits until the lock is acquired.
func Lock(f *os.File) error {
	return LockContext(context.Background(), f)
}

// LockContext is Lock that gives up with ctx.Err() once ctx is done.
func LockContext(ctx context.Context, f *os.File) error {
	for {
		err := TryLock(f)
		if err == nil || !errors.Is(err, ErrLocked) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(lockSpinMaxSleep):
		}
	}
}
