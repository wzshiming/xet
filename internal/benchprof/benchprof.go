// Package benchprof CPU-profiles only a benchmark's timed region: call Start right before for b.Loop() and stop right after it.
package benchprof

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"testing"
)

var seq atomic.Uint64

// Start CPU-profiles tb into $XET_BENCH_PROFILE_DIR/<name>-<seq>.pprof until stop is called; with that variable unset it does nothing.
// pprof allows one CPU profile per process, so Start must not be nested.
// For the same reason $XET_BENCH_PROFILE_DIR cannot be combined with go test -cpuprofile.
func Start(tb testing.TB) (stop func()) {
	tb.Helper()
	dir := os.Getenv("XET_BENCH_PROFILE_DIR")
	if dir == "" {
		return func() {}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("benchprof: %s: %v", dir, err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%s-%d.pprof", strings.ReplaceAll(tb.Name(), "/", "_"), seq.Add(1)))
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("benchprof: %s: %v", path, err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		os.Remove(path)
		tb.Fatalf("benchprof: %s: %v", path, err)
	}
	return func() {
		tb.Helper()
		pprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			tb.Fatalf("benchprof: %s: %v", path, err)
		}
	}
}
