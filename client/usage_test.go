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
)

func TestCacheUsageReportsConfiguredCacheDir(t *testing.T) {
	cacheDir := t.TempDir()
	cacheClient, err := NewClient(WithCacheDir(cacheDir))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cacheClient.CacheUsage(t.Context()); err != nil || got != (download.CacheUsage{}) {
		t.Fatalf("usage of empty cache = %+v, %v; want zero", got, err)
	}

	var want download.CacheUsage
	for name, data := range map[string]string{"chunks/entry": "cached bytes", "staging.tmp": "partial"} {
		filePath := filepath.Join(cacheDir, name)
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filePath, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
		want.Count++
		want.Bytes += int64(len(data))
	}
	if got, err := cacheClient.CacheUsage(t.Context()); err != nil || got != want {
		t.Fatalf("usage = %+v, %v; want %+v", got, err, want)
	}
}

func TestCacheUsageForwardsContextAndDirectoryErrors(t *testing.T) {
	c, err := NewClient(WithCacheDir(filepath.Join(t.TempDir(), "missing")))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := c.CacheUsage(ctx); !errors.Is(err, context.Canceled) || got != (download.CacheUsage{}) {
		t.Fatalf("usage with canceled ctx = %+v, %v; want zero, context.Canceled", got, err)
	}

	if runtime.GOOS == "windows" {
		t.Skip("ENOTDIR is not reported on windows")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	c, err = NewClient(WithCacheDir(filepath.Join(file, "cache")))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.CacheUsage(t.Context()); !errors.Is(err, syscall.ENOTDIR) || got != (download.CacheUsage{}) {
		t.Fatalf("usage through a file = %+v, %v; want zero, ENOTDIR", got, err)
	}
}
