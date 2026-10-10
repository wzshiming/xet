package spool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/wzshiming/xet/storage"
)

// Usage counts the spool files on disk, idle or in flight, and nothing else in the directory.
func TestSpoolUsage(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestSpool(t)
	if got, err := s.Usage(ctx); err != nil || got != (storage.ObjectUsage{}) {
		t.Fatalf("usage of a fresh spool = %+v, %v; want zero", got, err)
	}

	for name, size := range map[string]int{"a.spool": 40, "b.spool": 60, "notes.txt": 100} {
		if err := os.WriteFile(filepath.Join(s.dir, name), make([]byte, size), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want := storage.ObjectUsage{Count: 2, Bytes: 100}
	if got, err := s.Usage(ctx); err != nil || got != want {
		t.Fatalf("usage with leftovers = %+v, %v; want %+v", got, err, want)
	}

	// An in-flight spool counts once its bytes spilled to disk.
	data := make([]byte, 64*1024)
	it, err := s.Accept(ctx, Source{Origin: "https://hub.example", Key: "/org/repo/resolve/main/f.bin", ETag: "etag1", Size: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Release()
	if _, err := it.Write(data); err != nil {
		t.Fatal(err)
	}
	inFlight := storage.ObjectUsage{Count: 3, Bytes: 100 + int64(len(data))}
	if got, err := s.Usage(ctx); err != nil || got != inFlight {
		t.Fatalf("usage with a spilled flight = %+v, %v; want %+v", got, err, inFlight)
	}
	it.Finish(ctx, nil)
	if _, err := s.Wait(ctx, it); err != nil {
		t.Fatal(err)
	}
	it.Release()
	if got, err := s.Usage(ctx); err != nil || got != want {
		t.Fatalf("usage after the flight was released = %+v, %v; want %+v", got, err, want)
	}

	t.Run("missing dir", func(t *testing.T) {
		absent := &Spool{dir: filepath.Join(t.TempDir(), "absent")}
		if got, err := absent.Usage(ctx); err != nil || got != (storage.ObjectUsage{}) {
			t.Fatalf("usage of a missing dir = %+v, %v; want zero", got, err)
		}
		if _, err := os.Stat(absent.dir); !os.IsNotExist(err) {
			t.Fatalf("usage must not create the directory: %v", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		for _, sp := range []*Spool{absent, s} {
			if got, err := sp.Usage(canceled); !errors.Is(err, context.Canceled) || got != (storage.ObjectUsage{}) {
				t.Fatalf("usage with a canceled ctx = %+v, %v; want zero, context.Canceled", got, err)
			}
		}
	})

	t.Run("scan failure", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("ENOTDIR is not reported on windows")
		}
		file := filepath.Join(t.TempDir(), "file")
		if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
			t.Fatal(err)
		}
		broken := &Spool{dir: filepath.Join(file, "spool")}
		if got, err := broken.Usage(ctx); !errors.Is(err, syscall.ENOTDIR) || got != (storage.ObjectUsage{}) {
			t.Fatalf("usage under a file = %+v, %v; want zero, ENOTDIR", got, err)
		}
	})
}
