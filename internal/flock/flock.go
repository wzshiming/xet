package flock

import (
	"errors"
	"os"
	"runtime"
	"time"
)

// ErrLocked is returned when the file is already locked by another process.
var ErrLocked = errors.New("file is locked")

const (
	lockSpinMaxSleep = 100 * time.Millisecond // maximum sleep between retries
)

// Lock repeatedly calls TryLock until the lock is acquired.
//
// Each iteration yields the processor via runtime.Gosched before retrying.
// When the lock remains contended beyond lockSpinBase attempts, it
// switches to exponential backoff sleep (capped at lockSpinMaxSleep) to
// avoid consuming CPU.
func Lock(f *os.File) error {
	for {
		err := TryLock(f)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrLocked) {
			return err
		}

		runtime.Gosched()

		time.Sleep(lockSpinMaxSleep)
	}
}
