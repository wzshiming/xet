package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// packageA has two benchmarks of about a microsecond: Work runs its loop the given times over and
// starts with the given statement, Steady never changes.
const packageA = `package a

var keep *[16]byte

func Work(n int) int {
	%s
	h := 0
	for i := range %d * n {
		h = h*31 + i
	}
	return h
}

func Steady(n int) int {
	h := 0
	for i := range n {
		h = h*37 + i
	}
	return h
}
`

const packageATest = `package a

import "testing"

var sink int

func BenchmarkWork(b *testing.B) {
	for range b.N {
		sink = Work(1000)
	}
}

func BenchmarkSteady(b *testing.B) {
	for range b.N {
		sink = Steady(1000)
	}
}
`

// packageB has one benchmark of about a microsecond, whose loop runs the given times over.
const packageB = `package b

func Mix(n int) int {
	h := 0
	for i := range %d * n {
		h = h*41 + i
	}
	return h
}
`

const packageBTest = `package b

import "testing"

var sink int

func BenchmarkMix(b *testing.B) {
	for range b.N {
		sink = Mix(1000)
	}
}
`

var synth = map[string]string{
	"go.mod":      "module example.com/synth\n\ngo 1.22\n",
	"a/a.go":      fmt.Sprintf(packageA, "", 1),
	"a/a_test.go": packageATest,
	"b/b.go":      fmt.Sprintf(packageB, 1),
	"b/b_test.go": packageBTest,
}

var (
	slowWork       = map[string]string{"a/a.go": fmt.Sprintf(packageA, "", 2)}
	allocWork      = map[string]string{"a/a.go": fmt.Sprintf(packageA, "keep = new([16]byte)", 1)}
	slowMix        = map[string]string{"b/b.go": fmt.Sprintf(packageB, 2)}
	brokenA        = map[string]string{"a/a.go": "package a\n\nfunc Work(n int) int { return undefined }\n"}
	withoutB       = map[string]string{"b/b.go": "", "b/b_test.go": ""}
	withoutBTests  = map[string]string{"b/b_test.go": ""}
	failingMix     = map[string]string{"b/b_test.go": "package b\n\nimport \"testing\"\n\nfunc BenchmarkMix(b *testing.B) { b.Fatal(\"failed on purpose\") }\n"}
	failingSlowMix = map[string]string{"b/b.go": slowMix["b/b.go"], "b/b_test.go": failingMix["b/b_test.go"]}
)

// judgeAll judges every metric at a threshold the tests' 2x effects exceed and machine noise does not.
const judgeAll = `{"sec/op": 0.5, "B/op": 0.5, "allocs/op": 0.5}`

// tree writes synth with changes into a new directory; an empty change removes the file.
func tree(t *testing.T, changes map[string]string) string {
	t.Helper()
	files := maps.Clone(synth)
	for name, content := range changes {
		if content == "" {
			delete(files, name)
		} else {
			files[name] = content
		}
	}
	dir := t.TempDir()
	for name, content := range files {
		writeFile(t, filepath.Join(dir, filepath.FromSlash(name)), content)
	}
	return dir
}

// writeConfig writes a configuration whose group cpu judges package a by thresholds and whose group io
// reports package b, and returns its path.
func writeConfig(t *testing.T, thresholds string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gate.json")
	writeFile(t, path, `{
	"alpha": 0.05,
	"minSamples": 10,
	"groups": [
		{"name": "cpu", "packages": ["a"], "bench": ".", "benchtime": "2000x", "rounds": 10, "thresholds": `+thresholds+`},
		{"name": "io", "packages": ["b"], "bench": ".", "benchtime": "2000x", "rounds": 10, "reportOnly": true}
	]
}
`)
	return path
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// outcome is what a run of the command left behind.
type outcome struct {
	code           int
	report, stderr string // the report is what the command printed
	out            string // output directory
}

// commandLine is the command's arguments for base, head, config and output directory out, then flags.
func commandLine(base, head, config, out string, flags []string) []string {
	return append([]string{
		"-base", base, "-head", head, "-base-label", "base-commit", "-head-label", "head-commit",
		"-config", config, "-out", out,
	}, flags...)
}

// command runs the command on base and head with a configuration, a new output directory and flags.
func command(t *testing.T, sampleRun sampler, base, head, config string, flags ...string) outcome {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	var stdout, stderr bytes.Buffer
	code := run(t.Context(), commandLine(base, head, config, out, flags), &stdout, &stderr, sampleRun)
	return outcome{code: code, report: stdout.String(), stderr: stderr.String(), out: out}
}

// buildCommand builds the command into a new directory and returns its path.
func buildCommand(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "benchgate")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// process runs the built command bin as a script would, otherwise as command does.
func process(t *testing.T, bin, base, head, config string, flags ...string) outcome {
	t.Helper()
	out := filepath.Join(t.TempDir(), "out")
	cmd := exec.Command(bin, commandLine(base, head, config, out, flags)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil && cmd.ProcessState == nil {
		t.Fatal(err)
	}
	return outcome{code: cmd.ProcessState.ExitCode(), report: stdout.String(), stderr: stderr.String(), out: out}
}

func wantCode(t *testing.T, o outcome, want int) {
	t.Helper()
	if o.code != want {
		t.Fatalf("exit code %d, want %d\nstderr:\n%s\nreport:\n%s", o.code, want, o.stderr, o.report)
	}
}

// section is the part of report under heading, up to the next heading of its level or above.
func section(t *testing.T, report, heading string) string {
	t.Helper()
	_, rest, ok := strings.Cut(report, "\n"+heading+"\n")
	if !ok {
		t.Fatalf("no %q in the report:\n%s", heading, report)
	}
	level := strings.IndexByte(heading, ' ')
	offset := 0
	for line := range strings.Lines(rest) {
		if n := len(line) - len(strings.TrimLeft(line, "#")); n > 0 && n <= level && strings.HasPrefix(line[n:], " ") {
			return rest[:offset]
		}
		offset += len(line)
	}
	return rest
}

// rows are the cells of the table rows in s, without headers and separators.
func rows(s string) [][]string {
	var table [][]string
	n := 0
	for line := range strings.Lines(s) {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "|") {
			n = 0
			continue
		}
		if n++; n > 2 {
			table = append(table, strings.Split(strings.TrimSuffix(strings.TrimPrefix(line, "| "), " |"), " | "))
		}
	}
	return table
}

// hasRow tells whether a row of table has a cell equal to each of cells.
func hasRow(table [][]string, cells ...string) bool {
	return slices.ContainsFunc(table, func(row []string) bool {
		for _, c := range cells {
			if !slices.Contains(row, c) {
				return false
			}
		}
		return true
	})
}

// procs is the suffix go test gives benchmark names at this process's GOMAXPROCS.
func procs() string {
	if p := runtime.GOMAXPROCS(0); p != 1 {
		return "-" + strconv.Itoa(p)
	}
	return ""
}

func TestIdenticalTreesPassUnmeasured(t *testing.T) {
	t.Parallel()
	o := command(t, sample.Run, tree(t, nil), tree(t, nil), writeConfig(t, judgeAll))
	wantCode(t, o, 0)
	notMeasured := rows(section(t, o.report, "## Not measured"))
	for _, pkg := range []string{"a", "b"} {
		if !hasRow(notMeasured, pkg, "identical test binaries") {
			t.Errorf("not measured %q, want package %s for identical test binaries", notMeasured, pkg)
		}
	}
}

func TestIdenticalTreesMeasuredAnywayPass(t *testing.T) {
	o := command(t, sample.Run, tree(t, nil), tree(t, nil), writeConfig(t, judgeAll), "-measure-identical")
	wantCode(t, o, 0)
	if cpu := rows(section(t, o.report, "### cpu (judged)")); !hasRow(cpu, "example.com/synth/a", "BenchmarkWork"+procs(), "sec/op") {
		t.Errorf("cpu comparison %q, want a sec/op row of BenchmarkWork%s", cpu, procs())
	}
}

func TestTwiceSlowerBenchmarkIsTheOnlyConfirmedRegression(t *testing.T) {
	o := command(t, sample.Run, tree(t, nil), tree(t, slowWork), writeConfig(t, judgeAll))
	wantCode(t, o, 1)
	if confirmed := rows(section(t, o.report, "## Confirmed regressions")); len(confirmed) != 1 || !hasRow(confirmed, "BenchmarkWork"+procs(), "sec/op") {
		t.Errorf("confirmed regressions %q, want the sec/op row of BenchmarkWork%s alone", confirmed, procs())
	}
}

func TestExtraAllocationFailsOnAllocs(t *testing.T) {
	o := command(t, sample.Run, tree(t, nil), tree(t, allocWork), writeConfig(t, `{"sec/op": 0.5, "allocs/op": 0.5}`))
	wantCode(t, o, 1)
	if confirmed := rows(section(t, o.report, "## Confirmed regressions")); len(confirmed) != 1 || !hasRow(confirmed, "BenchmarkWork"+procs(), "allocs/op") {
		t.Errorf("confirmed regressions %q, want the allocs/op row of BenchmarkWork%s alone", confirmed, procs())
	}
}

func TestTwiceSlowerReportOnlyBenchmarkPassesAndIsReported(t *testing.T) {
	o := command(t, sample.Run, tree(t, nil), tree(t, slowMix), writeConfig(t, judgeAll))
	wantCode(t, o, 0)
	if io := rows(section(t, o.report, "### io (report only)")); !hasRow(io, "example.com/synth/b", "BenchmarkMix"+procs(), "sec/op") {
		t.Errorf("io comparison %q, want a sec/op row of BenchmarkMix%s", io, procs())
	}
}

func TestHeadBuildFailureExitsTwoWithoutConclusion(t *testing.T) {
	t.Parallel()
	o := command(t, sample.Run, tree(t, nil), tree(t, brokenA), writeConfig(t, judgeAll))
	wantCode(t, o, 2)
	if o.report != "" {
		t.Errorf("printed %q, want no report", o.report)
	}
	filepath.WalkDir(o.out, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			t.Errorf("output directory holds %s, want nothing", path)
		}
		return nil
	})
	if !strings.Contains(o.stderr, "head") {
		t.Errorf("stderr %q does not name the head side", o.stderr)
	}
}

func TestBaseWithoutAPackagePassesAndSaysSo(t *testing.T) {
	t.Parallel()
	o := command(t, sample.Run, tree(t, withoutB), tree(t, nil), writeConfig(t, judgeAll))
	wantCode(t, o, 0)
	if notMeasured := rows(section(t, o.report, "## Not measured")); !hasRow(notMeasured, "b", "base has no such package") {
		t.Errorf("not measured %q, want package b for a base without it", notMeasured)
	}
}

func TestBuiltCommandExitsTwoUnlessOnlyTheBaseFails(t *testing.T) {
	t.Parallel()
	bin := buildCommand(t)
	for _, c := range []struct {
		name       string
		base, head map[string]string
		code       int
	}{
		{"benchmark failing on both sides", failingMix, failingSlowMix, 2},
		{"benchmark failing on the head side", nil, failingMix, 2},
		{"benchmark failing on the base side", failingMix, nil, 0},
		{"package without test files on both sides", withoutBTests, withoutBTests, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			o := process(t, bin, tree(t, c.base), tree(t, c.head), writeConfig(t, judgeAll))
			wantCode(t, o, c.code)
			if c.code == 2 && !strings.Contains(o.stderr, `package "b": head`) {
				t.Errorf("stderr %q does not name the head side of package b", o.stderr)
			}
			if c.code == 0 {
				if notMeasured := rows(section(t, o.report, "## Not measured")); !hasRow(notMeasured, "b", "base failed to build or run") {
					t.Errorf("not measured %q, want package b for its failing base", notMeasured)
				}
				if !strings.Contains(o.stderr, "failed on purpose") {
					t.Errorf("stderr %q does not say why the base failed", o.stderr)
				}
			}
		})
	}
}
