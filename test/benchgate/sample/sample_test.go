package sample_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/perf/benchfmt"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// synth is a module of two independent packages with benchmarks.
var synth = map[string]string{
	"go.mod": "module example.com/synth\n\ngo 1.22\n",
	"a/a.go": `package a

func Sum(n int) int {
	s := 0
	for i := 0; i < n; i++ {
		s += i
	}
	return s
}
`,
	"a/a_test.go": `package a

import (
	"fmt"
	"testing"
)

var sink []byte

func BenchmarkSum(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Sum(100)
	}
}

func BenchmarkAlloc(b *testing.B) {
	for i := 0; i < b.N; i++ {
		sink = make([]byte, 64)
	}
}

func BenchmarkSub(b *testing.B) {
	for _, n := range []int{1, 2} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				Sum(n)
			}
		})
	}
}
`,
	"b/b.go": `package b

func Double(n int) int { return 2 * n }
`,
	"b/b_test.go": `package b

import "testing"

func BenchmarkDouble(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Double(i)
	}
}
`,
}

// changedA changes what package a compiles to.
var changedA = map[string]string{"a/a.go": strings.Replace(synth["a/a.go"], "s += i", "s += 2 * i", 1)}

// changedB changes what package b compiles to.
var changedB = strings.Replace(synth["b/b.go"], "2 * n", "3 * n", 1)

// brokenB does not compile.
const brokenB = "package b\n\nfunc Double(n int) int { return undefined }\n"

// failsFromRun is a benchmark of package b that fails from its nth process on, counted in the package directory.
func failsFromRun(n int) string {
	return fmt.Sprintf(`package b

import (
	"os"
	"testing"
)

func BenchmarkDouble(b *testing.B) {
	runs, _ := os.ReadFile("runs")
	if err := os.WriteFile("runs", append(runs, '.'), 0o644); err != nil {
		b.Fatal(err)
	}
	if len(runs)+1 >= %d {
		b.Fatal("failed on purpose")
	}
	for i := 0; i < b.N; i++ {
		Double(i)
	}
}
`, n)
}

// failsOnSecondRun is a benchmark of package b that fails from its second process on, in the package directory.
const failsOnSecondRun = `package b

import (
	"os"
	"testing"
)

func BenchmarkDouble(b *testing.B) {
	if _, err := os.Stat("ran"); err == nil {
		b.Fatal("ran before")
	}
	if err := os.WriteFile("ran", nil, 0o644); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		Double(i)
	}
}
`

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
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// request samples packages a and b of base and head as one group into a directory yet to be made.
func request(t *testing.T, base, head string, rounds int) sample.Request {
	return sample.Request{
		Base:   base,
		Head:   head,
		Groups: []sample.Group{{Name: "synth", Packages: []string{"a", "b"}, Bench: ".", Benchtime: "1x", Rounds: rounds}},
		Out:    filepath.Join(t.TempDir(), "out"),
	}
}

func run(t *testing.T, req sample.Request) sample.Result {
	t.Helper()
	res, err := sample.Run(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// states maps "group:package" to its state.
func states(res sample.Result) map[string]sample.State {
	m := make(map[string]sample.State)
	for _, g := range res.Groups {
		for _, p := range g.Packages {
			m[g.Name+":"+p.Package] = p.State
		}
	}
	return m
}

func wantStates(t *testing.T, res sample.Result, want map[string]sample.State) {
	t.Helper()
	if got := states(res); !maps.Equal(got, want) {
		t.Errorf("states %v, want %v", got, want)
	}
}

func wantNoFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("%s holds %s, want no sample files", dir, e.Name())
	}
}

// wantError checks that err names every part.
func wantError(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Run succeeded, want an error naming %q", parts)
	}
	for _, p := range parts {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("error %q does not name %s", err, p)
		}
	}
}

// row is a result line's pkg and benchmark name.
type row struct{ pkg, name string }

// procs is the suffix Go gives benchmark names at this process's GOMAXPROCS.
func procs() string {
	if p := runtime.GOMAXPROCS(0); p != 1 {
		return "-" + strconv.Itoa(p)
	}
	return ""
}

// eachResult parses a sample file as benchstat does and calls f for each result line.
func eachResult(t *testing.T, path string, f func(*benchfmt.Result)) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	r := benchfmt.NewReader(file, path)
	for r.Scan() {
		switch res := r.Result().(type) {
		case *benchfmt.SyntaxError:
			t.Error(res)
		case *benchfmt.Result:
			f(res)
		}
	}
	if err := r.Err(); err != nil {
		t.Fatal(err)
	}
}

// readSamples counts each row's values in a sample file by unit.
func readSamples(t *testing.T, path string) map[row]map[string]int {
	t.Helper()
	rows := make(map[row]map[string]int)
	eachResult(t, path, func(res *benchfmt.Result) {
		k := row{res.GetConfig("pkg"), "Benchmark" + string(res.Name)}
		if rows[k] == nil {
			rows[k] = make(map[string]int)
		}
		for _, v := range res.Values {
			rows[k][v.Unit]++
		}
	})
	return rows
}

func TestIdenticalTreesAreNotMeasured(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, nil), 3)
	res := run(t, req)
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Identical, "synth:b": sample.Identical})
	wantNoFiles(t, req.Out)
}

func TestMeasureIdenticalGivesEveryRowRoundsSamplesWithAllocs(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, nil), 3)
	req.MeasureIdentical = true
	res := run(t, req)
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.Measured})
	three := map[string]int{"sec/op": 3, "B/op": 3, "allocs/op": 3}
	want := map[row]map[string]int{
		{"example.com/synth/a", "BenchmarkSum" + procs()}:     three,
		{"example.com/synth/a", "BenchmarkAlloc" + procs()}:   three,
		{"example.com/synth/a", "BenchmarkSub/n=1" + procs()}: three,
		{"example.com/synth/a", "BenchmarkSub/n=2" + procs()}: three,
		{"example.com/synth/b", "BenchmarkDouble" + procs()}:  three,
	}
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		if got := readSamples(t, file); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: values by row and unit %v, want %v", file, got, want)
		}
	}
}

func TestOnlyTheChangedPackageIsMeasured(t *testing.T) {
	t.Parallel()
	res := run(t, request(t, tree(t, nil), tree(t, changedA), 2))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.Identical})
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		rows := readSamples(t, file)
		if len(rows) == 0 {
			t.Errorf("%s has no rows", file)
		}
		for k := range rows {
			if k.pkg != "example.com/synth/a" {
				t.Errorf("%s has row %+v, want rows of example.com/synth/a only", file, k)
			}
		}
	}
}

func TestRunsAlternateRoundByRoundAfterAllBuilds(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, nil), 3)
	req.MeasureIdentical = true
	req.Groups = []sample.Group{
		{Name: "a", Packages: []string{"a"}, Bench: "Sum", Benchtime: "1x", Rounds: 3},
		{Name: "b", Packages: []string{"b"}, Bench: ".", Benchtime: "1x", Rounds: 3},
	}
	order := run(t, req).Env.Order
	i := slices.IndexFunc(order, func(s sample.Step) bool { return s.Round > 0 })
	if i < 0 {
		t.Fatalf("no run in %+v", order)
	}
	builds, runs := order[:i], order[i:]

	slices.SortFunc(builds, func(x, y sample.Step) int {
		return cmp.Or(strings.Compare(x.Package, y.Package), strings.Compare(string(x.Side), string(y.Side)))
	})
	wantBuilds := []sample.Step{
		{Package: "a", Side: sample.Base}, {Package: "a", Side: sample.Head},
		{Package: "b", Side: sample.Base}, {Package: "b", Side: sample.Head},
	}
	if !slices.Equal(builds, wantBuilds) {
		t.Errorf("builds before the first run %+v, want %+v", builds, wantBuilds)
	}

	first, second := sample.Base, sample.Head
	if runs[0].Side == sample.Head {
		first, second = second, first
	}
	var wantRuns []sample.Step
	for r := 1; r <= 3; r++ {
		sides := []sample.Side{first, second}
		if r%2 == 0 {
			sides = []sample.Side{second, first}
		}
		for _, g := range []string{"a", "b"} {
			for _, side := range sides {
				wantRuns = append(wantRuns, sample.Step{Round: r, Group: g, Package: g, Side: side})
			}
		}
	}
	if !slices.Equal(runs, wantRuns) {
		t.Errorf("runs %+v, want %+v", runs, wantRuns)
	}
}

func TestHeadBuildFailureIsAnErrorBeforeAnySample(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, map[string]string{"a/a.go": changedA["a/a.go"], "b/b.go": brokenB}), 2)
	_, err := sample.Run(t.Context(), req)
	wantError(t, err, `group "synth"`, `package "b"`, "head")
	wantNoFiles(t, req.Out)
}

func TestBaseWithoutThePackageIsBaseMissing(t *testing.T) {
	t.Parallel()
	base := tree(t, map[string]string{"b/b.go": "", "b/b_test.go": ""})
	res := run(t, request(t, base, tree(t, changedA), 2))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.BaseMissing})
}

func TestHeadPackageWithoutTestFilesIsABuildError(t *testing.T) {
	t.Parallel()
	noTests := map[string]string{"b/b_test.go": ""}
	for _, c := range []struct {
		name string
		base map[string]string
	}{
		{"on both sides", noTests},
		{"on the head side", nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			req := request(t, tree(t, c.base), tree(t, noTests), 2)
			_, err := sample.Run(t.Context(), req)
			wantError(t, err, `group "synth"`, `package "b"`, "head build failed", "no test files")
			wantNoFiles(t, req.Out)
		})
	}
}

func TestBasePackageWithoutTestFilesIsBaseFailed(t *testing.T) {
	t.Parallel()
	res := run(t, request(t, tree(t, map[string]string{"b/b_test.go": ""}), tree(t, changedA), 2))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.BaseFailed})
}

func TestBaseRunFailureIsBaseFailedWithoutSamplesOfThePackage(t *testing.T) {
	t.Parallel()
	base := tree(t, map[string]string{"b/b_test.go": failsOnSecondRun})
	res := run(t, request(t, base, tree(t, changedA), 3))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.BaseFailed})
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		for k := range readSamples(t, file) {
			if k.pkg == "example.com/synth/b" {
				t.Errorf("%s has row %+v of the package whose base failed", file, k)
			}
		}
	}
}

func TestBaseBuildFailureIsBaseFailed(t *testing.T) {
	t.Parallel()
	res := run(t, request(t, tree(t, map[string]string{"b/b.go": brokenB}), tree(t, changedA), 2))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.BaseFailed})
}

func TestHeadRunFailureIsAnError(t *testing.T) {
	t.Parallel()
	_, err := sample.Run(t.Context(), request(t, tree(t, nil), tree(t, map[string]string{"b/b_test.go": failsOnSecondRun}), 2))
	wantError(t, err, `group "synth"`, `package "b"`, "head")
}

func TestRunFailureOnBothSidesIsAnErrorInAnyRound(t *testing.T) {
	t.Parallel()
	for n := 1; n <= 3; n++ {
		t.Run(fmt.Sprintf("from run %d", n), func(t *testing.T) {
			t.Parallel()
			base := tree(t, map[string]string{"b/b_test.go": failsFromRun(n)})
			head := tree(t, map[string]string{"b/b.go": changedB, "b/b_test.go": failsFromRun(n)})
			_, err := sample.Run(t.Context(), request(t, base, head, 3))
			wantError(t, err, `group "synth"`, `package "b"`, "head run", fmt.Sprintf("round %d", n))
		})
	}
}

func TestHeadRunsInTheRoundItsBaseFailed(t *testing.T) {
	t.Parallel()
	res := run(t, request(t, tree(t, map[string]string{"b/b_test.go": failsFromRun(1)}), tree(t, changedA), 3))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.BaseFailed})
	var runs []sample.Step
	for _, s := range res.Env.Order {
		if s.Round > 0 && s.Package == "b" {
			runs = append(runs, s)
		}
	}
	want := []sample.Step{{Round: 1, Group: "synth", Package: "b", Side: sample.Base}, {Round: 1, Group: "synth", Package: "b", Side: sample.Head}}
	if !slices.Equal(runs, want) {
		t.Errorf("runs of package b %+v, want %+v", runs, want)
	}
}

func TestFailingUnitTestDoesNotStopSampling(t *testing.T) {
	t.Parallel()
	fails := map[string]string{"a/fail_test.go": "package a\n\nimport \"testing\"\n\nfunc TestFails(t *testing.T) { t.Fatal(\"always\") }\n"}
	req := request(t, tree(t, fails), tree(t, fails), 1)
	req.MeasureIdentical = true
	wantStates(t, run(t, req), map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.Measured})
}

func TestVetFindingsDoNotStopTheBuild(t *testing.T) {
	t.Parallel()
	vet := map[string]string{"a/vet_test.go": "package a\n\nimport \"fmt\"\n\nvar _ = fmt.Sprintf(\"%d\", \"x\")\n"}
	req := request(t, tree(t, vet), tree(t, vet), 1)
	req.MeasureIdentical = true
	wantStates(t, run(t, req), map[string]sample.State{"synth:a": sample.Measured, "synth:b": sample.Measured})
}

func TestRowSelectionRunsOnlyTheSelectedRow(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, nil), 2)
	req.MeasureIdentical = true
	selected := sample.Row{Pkg: "example.com/synth/a", Name: "BenchmarkSub/n=1" + procs()}
	req.Groups[0].Rows = []sample.Row{selected}
	res := run(t, req)
	want := map[row]map[string]int{{selected.Pkg, selected.Name}: {"sec/op": 2, "B/op": 2, "allocs/op": 2}}
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		if got := readSamples(t, file); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: values by row and unit %v, want %v", file, got, want)
		}
	}
}

func TestBothSidesRunAtTheSamplersGOMAXPROCS(t *testing.T) {
	t.Setenv("GOMAXPROCS", strconv.Itoa(runtime.GOMAXPROCS(0)+1))
	req := request(t, tree(t, nil), tree(t, nil), 1)
	req.MeasureIdentical = true
	res := run(t, req)
	want := []row{
		{"example.com/synth/a", "BenchmarkAlloc" + procs()},
		{"example.com/synth/a", "BenchmarkSub/n=1" + procs()},
		{"example.com/synth/a", "BenchmarkSub/n=2" + procs()},
		{"example.com/synth/a", "BenchmarkSum" + procs()},
		{"example.com/synth/b", "BenchmarkDouble" + procs()},
	}
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		got := slices.SortedFunc(maps.Keys(readSamples(t, file)), func(x, y row) int {
			return cmp.Or(strings.Compare(x.pkg, y.pkg), strings.Compare(x.name, y.name))
		})
		if !slices.Equal(got, want) {
			t.Errorf("%s: rows %v, want %v", file, got, want)
		}
	}
}

func TestBuildsUseTheGoCommandOnPath(t *testing.T) {
	t.Parallel()
	pinned := map[string]string{"go.mod": synth["go.mod"] + "\ntoolchain go1.999.0\n"}
	res := run(t, request(t, tree(t, nil), tree(t, pinned), 1))
	wantStates(t, res, map[string]sample.State{"synth:a": sample.Identical, "synth:b": sample.Identical})
}

func TestEnvironmentDescribesMachineAndToolchain(t *testing.T) {
	t.Parallel()
	base := tree(t, nil)
	req := request(t, base, tree(t, nil), 1)
	req.MeasureIdentical = true
	res := run(t, req)

	cmd := exec.Command("go", "env", "GOVERSION")
	cmd.Dir = base
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	goVersion, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	cpu := sample.Unknown
	eachResult(t, res.Groups[0].Head, func(r *benchfmt.Result) {
		if c := r.GetConfig("cpu"); c != "" {
			cpu = strings.TrimSpace(c)
		}
	})
	for _, c := range []struct {
		field     string
		got, want any
	}{
		{"CPU", res.Env.CPU, cpu},
		{"Cores", res.Env.Cores, runtime.NumCPU()},
		{"OSArch", res.Env.OSArch, runtime.GOOS + "/" + runtime.GOARCH},
		{"GoVersion", res.Env.GoVersion, strings.TrimSpace(string(goVersion))},
	} {
		if c.got != c.want {
			t.Errorf("%s %#v, want %#v", c.field, c.got, c.want)
		}
	}
}

func TestEnvironmentRecordsLoadBeforeEachRound(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, nil), 3)
	req.MeasureIdentical = true
	loads := run(t, req).Env.Loads
	if len(loads) != 3 {
		t.Fatalf("loads %q, want one for each of 3 rounds", loads)
	}
	readable := runtime.GOOS == "linux" || runtime.GOOS == "darwin"
	for _, l := range loads {
		if l == sample.Unknown && !readable {
			continue
		}
		f := strings.Fields(l)
		for _, x := range f {
			if v, err := strconv.ParseFloat(x, 64); err != nil || v < 0 {
				t.Errorf("load %q has %q, want load averages", l, x)
			}
		}
		if len(f) != 3 {
			t.Errorf("load %q, want the 1, 5 and 15 minute load averages", l)
		}
	}
}

func TestRunRejectsInvalidGroups(t *testing.T) {
	t.Parallel()
	base, head := tree(t, nil), tree(t, changedA)
	group := func(name string, rounds int) sample.Group {
		return sample.Group{Name: name, Packages: []string{"a"}, Bench: ".", Benchtime: "1x", Rounds: rounds}
	}
	for _, c := range []struct {
		why    string
		groups []sample.Group
	}{
		{"empty name", []sample.Group{group("", 1)}},
		{"name leaving the output directory", []sample.Group{group("../g", 1)}},
		{"name with a space", []sample.Group{group("g h", 1)}},
		{"duplicate name", []sample.Group{group("g", 1), group("g", 1)}},
		{"no rounds", []sample.Group{group("g", 0)}},
	} {
		req := sample.Request{Base: base, Head: head, Groups: c.groups, Out: filepath.Join(t.TempDir(), "out")}
		if _, err := sample.Run(t.Context(), req); err == nil {
			t.Errorf("%s: Run succeeded, want an error", c.why)
		}
	}
}

func TestCancelledContextStopsRun(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := sample.Run(ctx, request(t, tree(t, nil), tree(t, changedA), 1))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Run with a cancelled context: %v, want context.Canceled", err)
	}
}

func TestCancelledDuringARunIsAnError(t *testing.T) {
	t.Parallel()
	const blocks = `package b

import (
	"os"
	"testing"
	"time"
)

func BenchmarkDouble(b *testing.B) {
	os.WriteFile("started", nil, 0o644)
	time.Sleep(time.Minute)
}
`
	for _, side := range []sample.Side{sample.Base, sample.Head} {
		t.Run(string(side), func(t *testing.T) {
			t.Parallel()
			trees := map[sample.Side]string{sample.Base: tree(t, nil), sample.Head: tree(t, nil)}
			trees[side] = tree(t, map[string]string{"b/b_test.go": blocks})
			ctx, cancel := context.WithCancel(t.Context())
			go func() {
				for {
					if _, err := os.Stat(filepath.Join(trees[side], "b", "started")); err == nil {
						cancel()
						return
					}
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Millisecond):
					}
				}
			}()
			_, err := sample.Run(ctx, request(t, trees[sample.Base], trees[sample.Head], 1))
			if !errors.Is(err, context.Canceled) {
				t.Errorf("Run cancelled during a %s run: %v, want context.Canceled", side, err)
			}
		})
	}
}

func TestBenchtimeAppliesToEveryRun(t *testing.T) {
	t.Parallel()
	req := request(t, tree(t, nil), tree(t, changedA), 2)
	req.Groups[0].Benchtime = "3x"
	res := run(t, req)
	for _, file := range []string{res.Groups[0].Base, res.Groups[0].Head} {
		n := 0
		eachResult(t, file, func(r *benchfmt.Result) {
			n++
			if r.Iters != 3 {
				t.Errorf("%s: %s ran %d iterations, want 3", file, r.Name, r.Iters)
			}
		})
		if n == 0 {
			t.Errorf("%s has no results", file)
		}
	}
}
