package spool

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/wzshiming/xet/storage"
)

// Sweep removes idle spools past the grace, never one a flight holds, and a dry run only counts.
func TestSpoolSweep(t *testing.T) {
	ctx := context.Background()
	q, _ := newTestSpool(t)
	const origin = "https://hub.example"
	old := time.Now().Add(-48 * time.Hour)

	// Crash leftovers: no flight holds them.
	stale := filepath.Join(q.dir, fileName(origin, "/org/repo/resolve/main/stale.bin", "etag-stale", 100, true))
	if err := os.WriteFile(stale, make([]byte, 40), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(q.dir, fileName(origin, "/org/repo/resolve/main/fresh.bin", "etag-fresh", 100, true))
	if err := os.WriteFile(fresh, make([]byte, 60), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(q.dir, "notes.txt"), []byte("not a spool"), 0o644); err != nil {
		t.Fatal(err)
	}

	// An in-flight writer holds its spool however old the file looks.
	data := make([]byte, 64*1024)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])
	held, err := q.Accept(ctx, Source{Origin: origin, Key: "/org/repo/resolve/main/stall.bin", ETag: digest, Size: int64(len(data)), SHA256: digest})
	if err != nil {
		t.Fatal(err)
	}
	const head = 40 * 1024 // past the memory tier, so the spool has spilled to its file
	if _, err := held.Write(data[:head]); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(held.path(), old, old); err != nil {
		t.Fatal(err)
	}

	all := []string{filepath.Base(stale), filepath.Base(fresh), filepath.Base(held.path())}
	slices.Sort(all)
	got, err := q.Sweep(ctx, storage.SweepOptions{Grace: -1, DryRun: true})
	if err != nil || got != (SweepResult{DryRun: true, SweptSpools: 2, ReclaimedBytes: 100}) {
		t.Fatalf("dry-run Sweep = %+v, %v; want a dry run of 2 spools, 100 bytes", got, err)
	}
	if files := spoolFiles(t, q.dir); !slices.Equal(files, all) {
		t.Fatalf("spool files after dry run = %v, want %v", files, all)
	}

	got, err = q.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{SweptSpools: 1, ReclaimedBytes: 40}) {
		t.Fatalf("default-grace Sweep = %+v, %v; want 1 spool, 40 bytes", got, err)
	}
	want := []string{filepath.Base(fresh), filepath.Base(held.path())}
	slices.Sort(want)
	if files := spoolFiles(t, q.dir); !slices.Equal(files, want) {
		t.Fatalf("spool files after default-grace sweep = %v, want %v", files, want)
	}

	got, err = q.Sweep(ctx, storage.SweepOptions{Grace: -1})
	if err != nil || got != (SweepResult{SweptSpools: 1, ReclaimedBytes: 60}) {
		t.Fatalf("no-grace Sweep = %+v, %v; want 1 spool, 60 bytes", got, err)
	}
	if files := spoolFiles(t, q.dir); !slices.Equal(files, []string{filepath.Base(held.path())}) {
		t.Fatalf("spool files after full sweep = %v, want only the in-flight spool", files)
	}
	if _, err := os.Stat(filepath.Join(q.dir, "notes.txt")); err != nil {
		t.Fatalf("non-spool file swept: %v", err)
	}

	if _, err := held.Write(data[head:]); err != nil {
		t.Fatal(err)
	}
	held.Finish(ctx, nil)
	if res, err := q.Wait(ctx, held); err != nil || res.SHA256 != digest {
		t.Fatalf("held writer after the sweeps = %+v, %v; want its success", res, err)
	}
	held.Release()
	if files := spoolFiles(t, q.dir); len(files) != 0 {
		t.Fatalf("spool files after the held flight was released = %v, want none", files)
	}

	t.Run("missing dir", func(t *testing.T) {
		absent := &Spool{dir: filepath.Join(t.TempDir(), "absent")}
		if got, err := absent.Sweep(ctx, storage.SweepOptions{Grace: -1}); err != nil || got != (SweepResult{}) {
			t.Fatalf("Sweep of a missing dir = %+v, %v; want zero", got, err)
		}
	})
}

// Spool opens run under openMu; the sweep waits for one in flight instead of racing it.
func TestSpoolSweepHoldsOpenMu(t *testing.T) {
	ctx := context.Background()
	q, _ := newTestSpool(t)
	stale := filepath.Join(q.dir, "x.spool")
	if err := os.WriteFile(stale, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}

	q.openMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(q.openMu.Unlock) }
	defer unlock()
	type outcome struct {
		res SweepResult
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := q.Sweep(ctx, storage.SweepOptions{})
		done <- outcome{res, err}
	}()
	select {
	case o := <-done:
		t.Fatalf("Sweep finished (%+v, %v) while openMu was held", o.res, o.err)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("spool removed while openMu was held: %v", err)
	}

	unlock()
	select {
	case o := <-done:
		if o.err != nil || o.res != (SweepResult{SweptSpools: 1, ReclaimedBytes: 5}) {
			t.Fatalf("Sweep after openMu was released = %+v, %v; want 1 spool, 5 bytes", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Sweep did not finish after openMu was released")
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spool after the sweep: %v, want removed", err)
	}
}

// A spool the pass cannot unlink is left for the next pass, uncounted, without failing the pass.
func TestSpoolSweepSkipsUnremovable(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("needs file permissions the process cannot bypass")
	}
	ctx := context.Background()
	q, _ := newTestSpool(t)
	stale := filepath.Join(q.dir, "x.spool")
	if err := os.WriteFile(stale, []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(q.dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(q.dir, 0o755) })

	got, err := q.Sweep(ctx, storage.SweepOptions{DryRun: true})
	if err != nil || got != (SweepResult{DryRun: true, SweptSpools: 1, ReclaimedBytes: 5}) {
		t.Fatalf("dry-run Sweep under a read-only spool dir = %+v, %v; want a dry run of 1 spool, 5 bytes", got, err)
	}
	got, err = q.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{}) {
		t.Fatalf("Sweep under a read-only spool dir = %+v, %v; want nothing counted and no error", got, err)
	}
	if _, err := os.Stat(stale); err != nil {
		t.Fatalf("unremovable spool after the sweep: %v", err)
	}

	if err := os.Chmod(q.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err = q.Sweep(ctx, storage.SweepOptions{})
	if err != nil || got != (SweepResult{SweptSpools: 1, ReclaimedBytes: 5}) {
		t.Fatalf("Sweep once the spool dir is writable = %+v, %v; want 1 spool, 5 bytes", got, err)
	}
	if _, err := os.Stat(stale); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("spool after the sweep: %v, want removed", err)
	}
}
