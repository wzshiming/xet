package benchprof_test

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet/internal/benchprof"
)

const envDir = "XET_BENCH_PROFILE_DIR"

func TestStartWithoutDirWritesNothing(t *testing.T) {
	t.Setenv(envDir, "")
	dir := t.TempDir()
	t.Chdir(dir) // an empty profile dir would resolve against the working directory
	benchprof.Start(t)()
	if names := profiles(t, dir); len(names) != 0 {
		t.Errorf("Start with %s empty wrote %v", envDir, names)
	}
}

func TestStartWritesCPUProfile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "profiles") // Start must create it
	t.Setenv(envDir, dir)
	t.Run("sub", func(t *testing.T) {
		benchprof.Start(t)()
	})
	names := profiles(t, dir)
	if len(names) != 1 || !regexp.MustCompile(`^TestStartWritesCPUProfile_sub-[1-9][0-9]*\.pprof$`).MatchString(names[0]) {
		t.Fatalf("Start wrote %v, want one TestStartWritesCPUProfile_sub-<seq>.pprof", names)
	}
	f, err := os.Open(filepath.Join(dir, names[0]))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("profile %s is not gzip: %v", names[0], err)
	}
	if data, err := io.ReadAll(zr); err != nil || len(data) == 0 {
		t.Errorf("profile %s holds %d bytes: %v", names[0], len(data), err)
	}
}

func TestStartTwiceWritesTwoProfiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envDir, dir)
	benchprof.Start(t)()
	benchprof.Start(t)()
	if names := profiles(t, dir); len(names) != 2 {
		t.Errorf("Start twice wrote %v, want two profiles", names)
	}
}

func TestStartFailsWhenDirCannotBeMade(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(file, "profiles")
	t.Setenv(envDir, dir)
	if msg := startFatal(t); !strings.Contains(msg, dir) {
		t.Errorf("Start under a regular file failed with %q, want a message naming %s", msg, dir)
	}
}

func TestStartFailsWhenProfileCannotBeCreated(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envDir, dir)
	t.Run(strings.Repeat("x", 300), func(t *testing.T) { // longer than a file name may be
		if msg := startFatal(t); !strings.Contains(msg, dir) {
			t.Errorf("Start failed with %q, want a message naming %s", msg, dir)
		}
	})
}

func TestStartFailsWhenNested(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(envDir, dir)
	stop := benchprof.Start(t)
	defer stop()
	outer := profiles(t, dir)
	if msg := startFatal(t); !strings.Contains(msg, dir) {
		t.Errorf("nested Start failed with %q, want a message naming %s", msg, dir)
	}
	if names := profiles(t, dir); !slices.Equal(names, outer) {
		t.Errorf("after the nested Start failed, %s holds %v, want only the outer Start's %v", dir, names, outer)
	}
}

// fatalTB records what it is failed with, then ends the goroutine as testing.T's Fatalf does.
type fatalTB struct {
	testing.TB
	msg string
}

func (tb *fatalTB) Fatalf(format string, args ...any) {
	tb.msg = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// startFatal runs Start and its stop for t on a goroutine of their own and returns what they failed t with.
func startFatal(t *testing.T) string {
	tb := &fatalTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		benchprof.Start(tb)()
	}()
	<-done
	return tb.msg
}

func profiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	return names
}
