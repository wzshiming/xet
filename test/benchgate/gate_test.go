package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// fake is a sampler that answers each call with the next of its passes and records what it was asked and wrote.
type fake struct {
	passes   []map[string]canned  // by group name
	envs     []sample.Environment // by pass, zero beyond
	requests []sample.Request
	written  [][2]string // path and content of each file
}

// canned is a group's outcome: the contents of its sample files, or none, and its packages' states,
// by default all measured with samples and identical without.
type canned struct {
	base, head string
	states     []sample.Status
}

func (f *fake) run(_ context.Context, req sample.Request) (sample.Result, error) {
	f.requests = append(f.requests, req)
	if len(f.requests) > len(f.passes) {
		return sample.Result{}, fmt.Errorf("sampling %d times, want %d", len(f.requests), len(f.passes))
	}
	res := sample.Result{}
	if i := len(f.requests) - 1; i < len(f.envs) {
		res.Env = f.envs[i]
	}
	for _, g := range req.Groups {
		c := f.passes[len(f.requests)-1][g.Name]
		gr := sample.GroupResult{Name: g.Name, Packages: c.states}
		state := sample.Identical
		if c.base != "" {
			state = sample.Measured
			gr.Base, gr.Head = filepath.Join(req.Out, g.Name+".base.txt"), filepath.Join(req.Out, g.Name+".head.txt")
			for _, file := range [][2]string{{gr.Base, c.base}, {gr.Head, c.head}} {
				if err := os.MkdirAll(req.Out, 0o755); err != nil {
					return sample.Result{}, err
				}
				if err := os.WriteFile(file[0], []byte(file[1]), 0o644); err != nil {
					return sample.Result{}, err
				}
				f.written = append(f.written, file)
			}
		}
		if gr.Packages == nil {
			for _, p := range g.Packages {
				gr.Packages = append(gr.Packages, sample.Status{Package: p, State: state})
			}
		}
		res.Groups = append(res.Groups, gr)
	}
	return res, nil
}

// bench is a benchmark's value of each metric.
type bench struct {
	name              string
	ns, bytes, allocs float64
}

// samples is a sample file of package example.com/fake with ten results of each benchmark.
func samples(benches ...bench) string {
	var b strings.Builder
	b.WriteString("pkg: example.com/fake\n")
	for range 10 {
		for _, x := range benches {
			fmt.Fprintf(&b, "%s 1000 %g ns/op %g B/op %g allocs/op\n", x.name, x.ns, x.bytes, x.allocs)
		}
	}
	return b.String()
}

// fakeConfig writes a configuration whose group cpu judges packages p and q at 10% and whose group io
// reports package r, and returns its path.
func fakeConfig(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gate.json")
	writeFile(t, path, `{
	"alpha": 0.05,
	"minSamples": 10,
	"groups": [
		{"name": "cpu", "packages": ["p", "q"], "bench": ".", "rounds": 10, "thresholds": {"sec/op": 0.1, "B/op": 0.1, "allocs/op": 0.1}},
		{"name": "io", "packages": ["r"], "bench": ".", "rounds": 10, "reportOnly": true}
	]
}
`)
	return path
}

var (
	steady  = bench{"BenchmarkA-8", 100, 64, 1}
	slower  = bench{"BenchmarkA-8", 200, 64, 1}
	bigger  = bench{"BenchmarkA-8", 100, 128, 1}
	another = bench{"BenchmarkB-8", 50, 0, 0}
)

// regressed is a group whose BenchmarkA-8 got twice as slow; nothing changed in clean.
var (
	regressed = canned{base: samples(steady, another), head: samples(slower, another)}
	clean     = canned{base: samples(steady, another), head: samples(steady, another)}
)

func TestSuspectThatDoesNotHoldOnRecheckIsExcluded(t *testing.T) {
	t.Parallel()
	f := &fake{passes: []map[string]canned{{"cpu": regressed}, {"cpu": clean}}}
	o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t))
	wantCode(t, o, 0)
	if excluded := rows(section(t, o.report, "## Excluded after re-check")); !hasRow(excluded, "cpu", "BenchmarkA-8", "sec/op") {
		t.Errorf("excluded after re-check %q, want the sec/op row of BenchmarkA-8", excluded)
	}
}

func TestSuspectMustRegressInTheSameMetricOnRecheck(t *testing.T) {
	t.Parallel()
	f := &fake{passes: []map[string]canned{{"cpu": regressed}, {"cpu": {base: samples(steady, another), head: samples(bigger, another)}}}}
	o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t))
	wantCode(t, o, 0)
	if excluded := rows(section(t, o.report, "## Excluded after re-check")); !hasRow(excluded, "cpu", "BenchmarkA-8", "sec/op") {
		t.Errorf("excluded after re-check %q, want the sec/op row of BenchmarkA-8", excluded)
	}
}

func TestRecheckAsksForTheSuspectRowsAlone(t *testing.T) {
	t.Parallel()
	both := canned{base: samples(steady, another), head: samples(bench{"BenchmarkA-8", 200, 128, 1}, another)}
	reported := canned{base: samples(bench{"BenchmarkC-8", 100, 0, 0}), head: samples(bench{"BenchmarkC-8", 200, 0, 0})}
	f := &fake{passes: []map[string]canned{{"cpu": both, "io": reported}, {"cpu": clean}}}
	command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t), "-measure-identical")
	if len(f.requests) != 2 {
		t.Fatalf("sampled %d times, want twice", len(f.requests))
	}
	first, recheck := f.requests[0], f.requests[1]
	want := first
	want.Groups = []sample.Group{first.Groups[0]}
	want.Groups[0].Rows = []sample.Row{{Pkg: "example.com/fake", Name: "BenchmarkA-8"}}
	want.Out = recheck.Out
	if !reflect.DeepEqual(recheck, want) {
		t.Errorf("re-check request %+v, want %+v", recheck, want)
	}
}

func TestRechecksOnlyWithSuspects(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name   string
		passes []map[string]canned
	}{
		{"suspect", []map[string]canned{{"cpu": regressed}, {"cpu": clean}}},
		{"none", []map[string]canned{{"cpu": clean}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := &fake{passes: c.passes}
			o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t))
			wantCode(t, o, 0)
			if len(f.requests) != len(c.passes) {
				t.Errorf("sampled %d times, want %d", len(f.requests), len(c.passes))
			}
		})
	}
}

func TestBothPassesKeepTheirSamplesInTheOutputDirectory(t *testing.T) {
	t.Parallel()
	f := &fake{passes: []map[string]canned{{"cpu": regressed}, {"cpu": clean}}}
	o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t))
	if len(f.requests) != 2 {
		t.Fatalf("sampled %d times, want twice", len(f.requests))
	}
	for _, file := range f.written {
		if !strings.HasPrefix(file[0], o.out+string(filepath.Separator)) {
			t.Errorf("sample file %s is outside the output directory %s", file[0], o.out)
		}
		if got, err := os.ReadFile(file[0]); err != nil || string(got) != file[1] {
			t.Errorf("%s does not hold the samples written there (%v)", file[0], err)
		}
	}
}
