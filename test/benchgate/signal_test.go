//go:build unix

package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// blockingMix is a benchmark of package b that writes its process ID to the file started, then sleeps.
var blockingMix = map[string]string{"b/b_test.go": `package b

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func BenchmarkMix(b *testing.B) {
	os.WriteFile("started.tmp", []byte(strconv.Itoa(os.Getpid())), 0o644)
	os.Rename("started.tmp", "started")
	time.Sleep(time.Minute)
}
`}

func TestSignalStopsTheRunAndExitsTwo(t *testing.T) {
	t.Parallel()
	bin := buildCommand(t)
	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()
			head, tmp := tree(t, blockingMix), t.TempDir()
			cmd := exec.Command(bin, commandLine(tree(t, nil), head, writeConfig(t, judgeAll), filepath.Join(t.TempDir(), "out"), nil)...)
			cmd.Env = append(os.Environ(), "TMPDIR="+tmp)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			exited := make(chan struct{})
			go func() {
				cmd.Wait()
				close(exited)
			}()
			pid := 0
			for pid == 0 {
				select {
				case <-exited:
					t.Fatalf("exited before the benchmark started:\n%s", &stderr)
				case <-time.After(10 * time.Millisecond):
				}
				started, _ := os.ReadFile(filepath.Join(head, "b", "started"))
				pid, _ = strconv.Atoi(string(started))
			}

			cmd.Process.Signal(sig)
			select {
			case <-exited:
			case <-time.After(time.Minute):
				cmd.Process.Kill()
				<-exited
				t.Fatal("still running a minute after the signal")
			}
			if code := cmd.ProcessState.ExitCode(); code != 2 {
				t.Errorf("exit code %d, want 2\nstderr:\n%s", code, &stderr)
			}
			if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
				t.Errorf("benchmark process %d is still there: %v", pid, err)
			}
			entries, _ := os.ReadDir(tmp)
			for _, e := range entries {
				if strings.HasPrefix(e.Name(), "benchgate-sample-") {
					t.Errorf("the sampler left %s behind", e.Name())
				}
			}
		})
	}
}
