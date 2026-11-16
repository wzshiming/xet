package main

import (
	"bytes"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// flags are a complete command line apart from the output directory.
func flags(t *testing.T, config string) []string {
	return []string{"-base", t.TempDir(), "-head", t.TempDir(), "-base-label", "b", "-head-label", "h", "-config", config}
}

func TestMissingFlagExitsTwoNamingIt(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"-base", "-head", "-base-label", "-head-label", "-config", "-out"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			args := append(flags(t, fakeConfig(t)), "-out", filepath.Join(t.TempDir(), "out"))
			i := slices.Index(args, name)
			args = slices.Delete(args, i, i+2)
			f := &fake{}
			var stderr bytes.Buffer
			if code := run(t.Context(), args, &bytes.Buffer{}, &stderr, f.run); code != 2 {
				t.Errorf("exit code %d, want 2", code)
			}
			if len(f.requests) != 0 {
				t.Errorf("sampled %d times, want no sampling", len(f.requests))
			}
			if !strings.Contains(stderr.String(), "missing "+name+"\n") {
				t.Errorf("stderr %q does not name %s", stderr.String(), name)
			}
		})
	}
}

func TestArgumentExitsTwo(t *testing.T) {
	t.Parallel()
	f := &fake{}
	o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t), "stray")
	wantCode(t, o, 2)
	if len(f.requests) != 0 || !strings.Contains(o.stderr, `"stray"`) {
		t.Errorf("sampled %d times, stderr %q; want no sampling and the argument named", len(f.requests), o.stderr)
	}
}

func TestOutputDirectoryWithFilesExitsTwo(t *testing.T) {
	t.Parallel()
	out := t.TempDir()
	writeFile(t, filepath.Join(out, "old.txt"), "")
	f := &fake{}
	var stderr bytes.Buffer
	if code := run(t.Context(), append(flags(t, fakeConfig(t)), "-out", out), &bytes.Buffer{}, &stderr, f.run); code != 2 {
		t.Errorf("exit code %d, want 2", code)
	}
	if len(f.requests) != 0 || !strings.Contains(stderr.String(), "-out") {
		t.Errorf("sampled %d times, stderr %q; want no sampling and -out named", len(f.requests), &stderr)
	}
}

func TestGroupsFlagSamplesOnlyTheNamedGroups(t *testing.T) {
	t.Parallel()
	f := &fake{passes: []map[string]canned{{}}}
	wantCode(t, command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t), "-groups", "io"), 0)
	if len(f.requests) != 1 || len(f.requests[0].Groups) != 1 || f.requests[0].Groups[0].Name != "io" {
		t.Errorf("requests %+v, want one of group io alone", f.requests)
	}
}

func TestGroupsFlagWithAnUnknownGroupExitsTwo(t *testing.T) {
	t.Parallel()
	f := &fake{}
	o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t), "-groups", "cpu,nope")
	wantCode(t, o, 2)
	if len(f.requests) != 0 || !strings.Contains(o.stderr, `"nope"`) {
		t.Errorf("sampled %d times, stderr %q; want no sampling and group nope named", len(f.requests), o.stderr)
	}
}

func TestMeasureIdenticalFlagOverridesTheConfig(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		config bool
		flags  []string
		want   bool
	}{
		{false, nil, false},
		{true, nil, true},
		{false, []string{"-measure-identical"}, true},
		{true, []string{"-measure-identical=false"}, false},
	} {
		config := edited(t, func(m map[string]any, _ []map[string]any) { m["measureIdentical"] = c.config })
		f := &fake{passes: []map[string]canned{{}}}
		wantCode(t, command(t, f.run, t.TempDir(), t.TempDir(), config, c.flags...), 0)
		if got := f.requests[0].MeasureIdentical; got != c.want {
			t.Errorf("config %v, flags %q: measured identical binaries %v, want %v", c.config, c.flags, got, c.want)
		}
	}
}
