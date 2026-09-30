package client

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"

	"github.com/wzshiming/xet/download"
	"github.com/wzshiming/xet/upload"
)

func TestCacheUsageReportsConfiguredCacheDir(t *testing.T) {
	cacheDir := t.TempDir()
	cacheClient, err := NewClient(WithCache(NewCache(cacheDir, 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cacheClient.Usage(t.Context()); err != nil || got != (Usage{}) {
		t.Fatalf("usage of empty cache = %+v, %v; want zero", got, err)
	}

	files := map[string]string{"download/aa/bb/entry": "cached bytes", "upload/xet-upload-xorb-1": "staged", "upload/chunks/entry": "cached location", "stray": "neither"}
	for name, data := range files {
		filePath := filepath.Join(cacheDir, name)
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filePath, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want := Usage{
		Download: download.CacheUsage{Count: 1, Bytes: int64(len(files["download/aa/bb/entry"]))},
		Upload:   upload.CacheUsage{Count: 2, Bytes: int64(len(files["upload/xet-upload-xorb-1"]) + len(files["upload/chunks/entry"]))},
	}
	if got, err := cacheClient.Usage(t.Context()); err != nil || got != want {
		t.Fatalf("usage = %+v, %v; want %+v", got, err, want)
	}
}

func TestCacheUsageForwardsContextAndDirectoryErrors(t *testing.T) {
	c, err := NewClient(WithCache(NewCache(filepath.Join(t.TempDir(), "missing"), 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := c.Usage(ctx); !errors.Is(err, context.Canceled) || got != (Usage{}) {
		t.Fatalf("usage with canceled ctx = %+v, %v; want zero, context.Canceled", got, err)
	}

	if runtime.GOOS == "windows" {
		t.Skip("ENOTDIR is not reported on windows")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err = NewClient(WithCache(NewCache(filepath.Join(file, "cache"), 0, 0)))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Usage(t.Context()); !errors.Is(err, syscall.ENOTDIR) || got != (Usage{}) {
		t.Fatalf("usage through a file = %+v, %v; want zero, ENOTDIR", got, err)
	}
}
