//go:build unix

package flock

import (
	"errors"
	"os"

	"syscall"
)

// TryLock attempts to acquire an exclusive file lock on the open file f
// without blocking. Returns ErrLocked if the lock is held by another
// process.
func TryLock(f *os.File) error {
	err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil && errors.Is(err, syscall.EWOULDBLOCK) {
		return ErrLocked
	}
	return err
}

// Unlock releases a file lock on the open file f.
func Unlock(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}
