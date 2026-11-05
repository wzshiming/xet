//go:build unix

package server_test

import (
	"syscall"
	"testing"
	"time"
)

func readRusage(tb testing.TB) rusage {
	tb.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		tb.Fatal(err)
	}
	// The conversions absorb the per-platform field types.
	return rusage{
		usr:    time.Duration(ru.Utime.Nano()),
		sys:    time.Duration(ru.Stime.Nano()),
		minflt: int64(ru.Minflt),
		majflt: int64(ru.Majflt),
	}
}
