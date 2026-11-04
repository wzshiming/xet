package storagetest

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/wzshiming/xet/storage/memory"
)

func TestAssertFileStoredRatio(t *testing.T) {
	const lo, hi = 0.85, 0.92
	t.Run("class=bf16/in-band", func(t *testing.T) {
		st := memory.NewStorage()
		f := PutXorbFile(t, t.Context(), st, ChunkClassBF16.Xorbs(1, 16, 1<<16))
		AssertFileCompression(t, t.Context(), st, f, ChunkClassBF16.Compression)
		t.Logf("stored/raw %.4f", AssertFileStoredRatio(t, t.Context(), st, f, lo, hi))
	})
	t.Run("class=none/out-of-band", func(t *testing.T) {
		st := memory.NewStorage()
		f := PutXorbFile(t, t.Context(), st, ChunkClasses[0].Xorbs(1, 16, 1<<16))
		msg := fatalOf(t, func(tb testing.TB) { AssertFileStoredRatio(tb, t.Context(), st, f, lo, hi) })
		if !strings.Contains(msg, "outside") {
			t.Fatalf("class none: Fatal %q, want a ratio outside [%.2f, %.2f]", msg, lo, hi)
		}
	})
}

// fatalOf runs assert in its own goroutine on a TB whose Fatal ends only that
// goroutine, and returns the Fatal message, "" if none.
func fatalOf(t *testing.T, assert func(testing.TB)) string {
	r := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		assert(r)
	}()
	<-done
	return r.msg
}

type fatalRecorder struct {
	testing.TB
	msg string
}

func (r *fatalRecorder) Helper() {}

func (r *fatalRecorder) Fatal(args ...any) {
	r.msg = fmt.Sprint(args...)
	runtime.Goexit()
}

func (r *fatalRecorder) Fatalf(format string, args ...any) {
	r.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}
