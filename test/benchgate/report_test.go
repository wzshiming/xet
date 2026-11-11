package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// fullConfig judges groups cpu and mem and reports group io, which it lists between them.
const fullConfig = `{
	"alpha": 0.05,
	"minSamples": 10,
	"groups": [
		{"name": "cpu", "packages": ["p", "q", "r", "s"], "bench": "A|B", "benchtime": "100x", "rounds": 10, "thresholds": {"sec/op": 0.1, "B/op": 0.2, "allocs/op": 0}},
		{"name": "io", "packages": ["t"], "bench": ".", "rounds": 5, "reportOnly": true},
		{"name": "mem", "packages": ["u"], "bench": ".", "rounds": 10, "thresholds": {"B/op": 0.1}}
	]
}`

// alternating is a sample file of package example.com/fake with ten results of name, alternately at lo and hi ns/op.
func alternating(name string, lo, hi float64) string {
	var b strings.Builder
	b.WriteString("pkg: example.com/fake\n")
	for i := range 10 {
		fmt.Fprintf(&b, "%s 1000 %g ns/op 0 B/op 0 allocs/op\n", name, []float64{lo, hi}[i%2])
	}
	return b.String()
}

// loads are ten rounds' load averages, told apart by first.
func loads(first int) []string {
	var l []string
	for r := range 10 {
		l = append(l, fmt.Sprintf("%d.%02d 0.50 0.90", first, r+1))
	}
	return l
}

// fullRun is a gate run whose report has something in every section: in group cpu, BenchmarkA-8 regresses
// in both passes and BenchmarkC-8 in the first alone, BenchmarkN-8 is new, BenchmarkR-8 removed and
// BenchmarkZ-8 noisy, and three packages are not measured; group mem regresses in the first pass and its
// base fails in the re-check; report-only group io doubles with three head samples.
type fullRun struct {
	base, head, config string
	outcome
}

func runFull(t *testing.T, args ...string) fullRun {
	t.Helper()
	cpu := canned{
		base: samples(bench{"BenchmarkA-8", 100, 64, 1}, bench{"BenchmarkC-8", 100, 64, 1}, bench{"BenchmarkR-8", 100, 0, 0}, bench{"BenchmarkZ-8", 100, 0, 0}),
		head: samples(bench{"BenchmarkA-8", 200, 64, 1}, bench{"BenchmarkC-8", 200, 64, 1}, bench{"BenchmarkN-8", 100, 0, 0}) + alternating("BenchmarkZ-8", 70, 130),
		states: []sample.Status{
			{Package: "p", State: sample.Measured}, {Package: "q", State: sample.Identical},
			{Package: "r", State: sample.BaseMissing}, {Package: "s", State: sample.BaseFailed},
		},
	}
	f := &fake{
		passes: []map[string]canned{
			{
				"cpu": cpu,
				"io":  {base: samples(bench{"BenchmarkD-8", 100, 0, 0}), head: strings.Repeat("pkg: example.com/fake\nBenchmarkD-8 1000 200 ns/op 0 B/op 0 allocs/op\n", 3)},
				"mem": {base: samples(bench{"BenchmarkE-8", 100, 64, 1}), head: samples(bench{"BenchmarkE-8", 100, 128, 1})},
			},
			{
				"cpu": {
					base:   samples(bench{"BenchmarkA-8", 100, 64, 1}, bench{"BenchmarkC-8", 100, 64, 1}),
					head:   samples(bench{"BenchmarkA-8", 210, 64, 1}, bench{"BenchmarkC-8", 100, 64, 1}),
					states: []sample.Status{{Package: "p", State: sample.Measured}},
				},
				"mem": {states: []sample.Status{{Package: "u", State: sample.BaseFailed}}},
			},
		},
		envs: []sample.Environment{
			{CPU: "Fake CPU @ 3.00GHz", Cores: 8, OSArch: "linux/amd64", GoVersion: "go1.27.3", Loads: loads(0)},
			{CPU: "Fake CPU @ 3.00GHz", Cores: 8, OSArch: "linux/amd64", GoVersion: "go1.27.3", Loads: loads(1)},
		},
	}
	r := fullRun{base: t.TempDir(), head: t.TempDir()}
	r.config = filepath.Join(r.head, ".github", "bench-gate.json")
	writeFile(t, r.config, fullConfig)
	r.out = filepath.Join(t.TempDir(), "out")
	var stdout, stderr bytes.Buffer
	r.code = run(t.Context(), append([]string{
		"-base", r.base, "-head", r.head, "-base-label", "abc1234", "-head-label", "def5678 (merge)",
		"-config", r.config, "-out", r.out,
	}, args...), &stdout, &stderr, f.run)
	r.report, r.stderr = stdout.String(), stderr.String()
	return r
}

func TestReportHasEverySection(t *testing.T) {
	t.Parallel()
	r := runFull(t)
	wantCode(t, r.outcome, 1)
	report := r.report

	t.Run("conclusion", func(t *testing.T) {
		if lines := strings.SplitN(report, "\n", 4); len(lines) < 3 || lines[2] != "**FAIL**: 1 confirmed regression." {
			t.Errorf("report starts %q, want the title and the conclusion", lines)
		}
	})
	t.Run("trees", func(t *testing.T) {
		if got, want := section(t, report, "## Trees"), "\n- Base: abc1234\n- Head: def5678 (merge)\n\n"; got != want {
			t.Errorf("trees %q, want %q", got, want)
		}
	})
	t.Run("environment", func(t *testing.T) {
		want := "\n- CPU: Fake CPU @ 3.00GHz\n- Cores: 8\n- OS/arch: linux/amd64\n- Go: go1.27.3\n\n"
		if got := section(t, report, "## Environment"); got != want {
			t.Errorf("environment %q, want %q", got, want)
		}
	})
	t.Run("policy", func(t *testing.T) {
		policy := section(t, report, "## Policy")
		if want := "Significance level 0.05; a row is judged with at least 10 samples on each side; identical test binaries are not measured."; !strings.Contains(policy, want) {
			t.Errorf("policy %q does not say %q", policy, want)
		}
		want := [][]string{
			{"cpu", "p, q, r, s", "`A\\|B`", "100x", "10", "sec/op +10%, B/op +20%, allocs/op +0%"},
			{"io", "t", "`.`", "default", "5", "report only"},
			{"mem", "u", "`.`", "default", "10", "B/op +10%"},
		}
		if got := rows(policy); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("policy rows %q, want %q", got, want)
		}
	})
	t.Run("confirmed regressions", func(t *testing.T) {
		want := [][]string{{"cpu", "example.com/fake", "BenchmarkA-8", "sec/op", "+10%",
			"100ns → 200ns, +100.00%, p=0.000", "100ns → 210ns, +110.00%, p=0.000, regression"}}
		if got := rows(section(t, report, "## Confirmed regressions")); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("confirmed regressions %q, want %q", got, want)
		}
	})
	t.Run("excluded after re-check", func(t *testing.T) {
		want := [][]string{
			{"cpu", "example.com/fake", "BenchmarkC-8", "sec/op", "+10%", "100ns → 200ns, +100.00%, p=0.000", "100ns → 100ns, +0.00%, p=1.000, not significant"},
			{"mem", "example.com/fake", "BenchmarkE-8", "B/op", "+10%", "64B → 128B, +100.00%, p=0.000", "not measured"},
		}
		if got := rows(section(t, report, "## Excluded after re-check")); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("excluded after re-check %q, want %q", got, want)
		}
	})
	t.Run("comparison", func(t *testing.T) {
		comparison := section(t, report, "## Comparison")
		cpu, mem, io := strings.Index(comparison, "\n### cpu (judged)\n"), strings.Index(comparison, "\n### mem (judged)\n"), strings.Index(comparison, "\n### io (report only)\n")
		if cpu < 0 || mem < cpu || io < mem {
			t.Errorf("comparison headings at %d, %d, %d; want cpu, mem, then io", cpu, mem, io)
		}
		rowsOf := func(heading string) [][]string { return rows(section(t, report, heading)) }
		if got, want := rowsOf("### cpu (judged)"), []string{"example.com/fake", "BenchmarkA-8", "sec/op", "100ns ±0%", "200ns ±0%", "+100.00%", "0.000", "10+10", "regression", ""}; !slices.ContainsFunc(got, func(r []string) bool { return slices.Equal(r, want) }) {
			t.Errorf("cpu rows %q, want %q", got, want)
		}
		for _, c := range []struct {
			heading string
			cells   []string
		}{
			{"### cpu (judged)", []string{"BenchmarkN-8", "sec/op", "new"}},
			{"### cpu (judged)", []string{"BenchmarkN-8", "B/op", "", "0B ±0%", "new"}},
			{"### cpu (judged)", []string{"BenchmarkR-8", "sec/op", "removed"}},
			{"### cpu (judged)", []string{"BenchmarkZ-8", "sec/op", "100ns ±30%", "noisy"}},
			{"### mem (judged)", []string{"BenchmarkE-8", "B/op", "regression"}},
			{"### io (report only)", []string{"BenchmarkD-8", "sec/op", "100ns ±0%", "200ns ±∞", "report only"}},
		} {
			if got := rowsOf(c.heading); !hasRow(got, c.cells...) {
				t.Errorf("%s rows %q, want a row with %q", c.heading, got, c.cells)
			}
		}
		if cpu := section(t, report, "### cpu (judged)"); !strings.Contains(cpu, "`first/cpu.base.txt`, `first/cpu.head.txt`") || !strings.Contains(cpu, "`recheck/cpu.base.txt`, `recheck/cpu.head.txt`") {
			t.Errorf("cpu comparison %q does not name the sample files of both passes", cpu)
		}
	})
	t.Run("not measured", func(t *testing.T) {
		want := [][]string{
			{"first", "cpu", "q", "identical test binaries"},
			{"first", "cpu", "r", "base has no such package"},
			{"first", "cpu", "s", "base failed to build or run"},
			{"re-check", "mem", "u", "base failed to build or run"},
		}
		if got := rows(section(t, report, "## Not measured")); !slices.EqualFunc(got, want, slices.Equal) {
			t.Errorf("not measured %q, want %q", got, want)
		}
	})
	t.Run("load per round", func(t *testing.T) {
		got := rows(section(t, report, "## Load per round"))
		if len(got) != 10 || !hasRow(got, "1", "0.01 0.50 0.90", "1.01 0.50 0.90") || !hasRow(got, "10", "0.10 0.50 0.90", "1.10 0.50 0.90") {
			t.Errorf("loads %q, want ten rounds of both passes", got)
		}
	})
	t.Run("reproduce", func(t *testing.T) {
		want := "`go -C <head>/test/benchgate run . -base <base> -head <head> -base-label abc1234 -head-label 'def5678 (merge)' -config <head>/.github/bench-gate.json -out <out>`"
		if got := section(t, report, "## Reproduce"); !strings.Contains(got, want) {
			t.Errorf("reproduce %q does not hold %s", got, want)
		}
	})
}

func TestReportNamesLocalPathsOnlyByPlaceholders(t *testing.T) {
	t.Parallel()
	r := runFull(t)
	for _, p := range []string{"<base>", "<head>", "<head>/.github/bench-gate.json", "<out>"} {
		if !strings.Contains(r.report, p) {
			t.Errorf("report does not name %s", p)
		}
	}
	home, _ := os.UserHomeDir()
	for _, p := range []string{r.base, r.head, r.config, r.out, os.TempDir(), home} {
		if p != "" && strings.Contains(r.report, p) {
			t.Errorf("report holds local path %s", p)
		}
	}
	if m := regexp.MustCompile("(^|[\\s`(])/[^\\s]*").FindAllString(r.report, -1); m != nil {
		t.Errorf("report holds absolute paths %q", m)
	}
}

func TestReportIsWrittenToTheOutputDirectory(t *testing.T) {
	t.Parallel()
	r := runFull(t)
	if got, err := os.ReadFile(filepath.Join(r.out, "report.md")); err != nil || string(got) != r.report {
		t.Errorf("report.md %q (%v), want what the command printed", got, err)
	}
}

func TestNumbersAreShownWithUnitPrefixes(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		v      float64
		metric string
		want   string
	}{
		{1.5, "sec/op", "1.5s"},
		{2.5e-3, "sec/op", "2.5ms"},
		{1.234e-6, "sec/op", "1.234µs"},
		{100e-9, "sec/op", "100ns"},
		{0, "sec/op", "0ns"},
		{64, "B/op", "64B"},
		{2048, "B/op", "2KiB"},
		{3 << 20, "B/op", "3MiB"},
		{1 << 30, "B/op", "1GiB"},
		{1, "allocs/op", "1"},
		{1500, "allocs/op", "1.5k"},
		{2.5e9, "B/s", "2.5G"},
		{3e6, "cpu-s/GiB", "3M"},
	} {
		if got := value(c.v, c.metric); got != c.want {
			t.Errorf("value(%v, %q) = %q, want %q", c.v, c.metric, got, c.want)
		}
	}
	for d, want := range map[float64]string{1: "+100.00%", -0.5: "-50.00%", 0: "+0.00%", math.Inf(1): "+∞"} {
		if got := change(d); got != want {
			t.Errorf("change(%v) = %q, want %q", d, got, want)
		}
	}
}

func TestConclusionLine(t *testing.T) {
	t.Parallel()
	twice := canned{base: samples(steady, another), head: samples(slower, bench{"BenchmarkB-8", 100, 0, 0})}
	for _, c := range []struct {
		name   string
		passes []map[string]canned
		want   string
	}{
		{"nothing measured", []map[string]canned{{}}, "**PASS**: nothing was measured."},
		{"no regression", []map[string]canned{{"cpu": clean}}, "**PASS**: no confirmed regression."},
		{"excluded", []map[string]canned{{"cpu": regressed}, {"cpu": clean}}, "**PASS**: no confirmed regression."},
		{"two confirmed", []map[string]canned{{"cpu": twice}, {"cpu": twice}}, "**FAIL**: 2 confirmed regressions."},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := &fake{passes: c.passes}
			o := command(t, f.run, t.TempDir(), t.TempDir(), fakeConfig(t))
			if lines := strings.SplitN(o.report, "\n", 4); len(lines) < 3 || lines[2] != c.want {
				t.Errorf("report starts %q, want the title and %q", lines, c.want)
			}
		})
	}
}

func TestReproduceRepeatsTheOverridesAndHidesPaths(t *testing.T) {
	t.Parallel()
	r := runFull(t, "-groups", "cpu,mem", "-measure-identical=false")
	want := "`go -C <head>/test/benchgate run . -base <base> -head <head> -base-label abc1234 -head-label 'def5678 (merge)' -config <head>/.github/bench-gate.json -groups cpu,mem -measure-identical=false -out <out>`"
	if got := section(t, r.report, "## Reproduce"); !strings.Contains(got, want) {
		t.Errorf("reproduce %q does not hold %s", got, want)
	}

	f := &fake{passes: []map[string]canned{{}}}
	var stdout bytes.Buffer
	run(t.Context(), []string{
		"-base", t.TempDir(), "-head", t.TempDir(), "-base-label", "it's", "-head-label", "h",
		"-config", fakeConfig(t), "-out", filepath.Join(t.TempDir(), "out"),
	}, &stdout, &bytes.Buffer{}, f.run)
	want = "-base-label 'it'\\''s' -head-label h -config <config> -out <out>`"
	if got := section(t, stdout.String(), "## Reproduce"); !strings.Contains(got, want) {
		t.Errorf("reproduce %q does not hold %s", got, want)
	}
}
