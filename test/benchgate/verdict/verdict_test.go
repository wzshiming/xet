package verdict_test

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/wzshiming/xet/test/benchgate/verdict"
)

var gate = verdict.Policy{
	Thresholds: map[string]float64{"sec/op": 0.10, "B/op": 0.10, "allocs/op": 0.10},
	Alpha:      0.05,
	MinSamples: 10,
}

func compare(t *testing.T, dir string, p verdict.Policy) verdict.Result {
	t.Helper()
	res, err := verdict.Compare(filepath.Join("testdata", dir, "base.txt"), filepath.Join("testdata", dir, "head.txt"), p)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func find(t *testing.T, res verdict.Result, pkg, name, metric string) verdict.Row {
	t.Helper()
	rows := make(map[verdict.Key]verdict.Row)
	for _, r := range res.Rows {
		rows[r.Key] = r
	}
	k := verdict.Key{Pkg: pkg, Name: name, Metric: metric}
	r, ok := rows[k]
	if !ok {
		t.Fatalf("no row %+v", k)
	}
	return r
}

// wantClass checks the class of an example.com/a row and that only regressions are listed as such.
func wantClass(t *testing.T, res verdict.Result, name, metric string, want verdict.Class) {
	t.Helper()
	r := find(t, res, "example.com/a", name, metric)
	if r.Class != want {
		t.Errorf("%s %s: class %q, want %q", name, metric, r.Class, want)
	}
	if listed := slices.Contains(res.Regressions, r.Key); listed != (want == verdict.Regression) {
		t.Errorf("%s %s: listed in Regressions %v, want %v", name, metric, listed, !listed)
	}
}

func TestAAHasNoRegression(t *testing.T) {
	if res := compare(t, "aa", gate); len(res.Regressions) != 0 {
		t.Errorf("regressions %+v, want none", res.Regressions)
	}
}

func TestSlowerBy20PercentWithTenSamplesIsRegression(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkSlower20-8", "sec/op", verdict.Regression)
}

func TestSlowerBy20PercentWithThreeSamplesIsTooFewSamples(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkSlower20Few-8", "sec/op", verdict.TooFewSamples)
}

func TestFasterBy20PercentIsImprovement(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkFaster-8", "sec/op", verdict.Improvement)
}

func TestSignificantButSlowerBy3PercentIsBelowThreshold(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkSlower3-8", "sec/op", verdict.BelowThreshold)
}

func TestSlowerBy30PercentWithOverlappingSamplesIsNotSignificant(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkOverlap-8", "sec/op", verdict.NotSignificant)
}

func TestEqualAllocsAreNotSignificant(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkAllocsSame-8", "allocs/op", verdict.NotSignificant)
}

func TestBytesUpBy5PercentIsBelowThreshold(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkBytes5-8", "B/op", verdict.BelowThreshold)
}

func TestAllocsFromZeroToOneIsRegression(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkAllocs-8", "allocs/op", verdict.Regression)
}

func TestBytesUpBy15PercentIsRegression(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkBytes15-8", "B/op", verdict.Regression)
}

func TestEmptyPolicyOnlyReports(t *testing.T) {
	res := compare(t, "ab", verdict.Policy{Alpha: 0.05, MinSamples: 10})
	if len(res.Regressions) != 0 {
		t.Errorf("regressions %+v, want none", res.Regressions)
	}
	for _, r := range res.Rows {
		if r.Base.N > 0 && r.Head.N > 0 && r.Class != verdict.ReportOnly {
			t.Errorf("%+v: class %q, want %q", r.Key, r.Class, verdict.ReportOnly)
		}
	}
}

func TestMetricsOutsideThePolicyAreOnlyReported(t *testing.T) {
	res := compare(t, "ab", gate)
	wantClass(t, res, "BenchmarkSlower20-8", "B/s", verdict.ReportOnly)
	wantClass(t, res, "BenchmarkFaster-8", "cpu-s/GiB", verdict.ReportOnly)
}

func TestNoisyMarksAConfidenceIntervalWiderThanTheThreshold(t *testing.T) {
	res := compare(t, "ab", gate)
	for _, c := range []struct {
		name, metric string
		want         bool
	}{
		{"BenchmarkNoisy-8", "sec/op", true},    // head ±15%, base ±0%
		{"BenchmarkAdded-8", "sec/op", true},    // head only, ±13%
		{"BenchmarkSlower3-8", "sec/op", false}, // both ±0%
		{"BenchmarkSlower20-8", "B/s", false},   // not judged
	} {
		if r := find(t, res, "example.com/a", c.name, c.metric); r.Noisy != c.want {
			t.Errorf("%s %s: noisy %v, want %v", c.name, c.metric, r.Noisy, c.want)
		}
	}
}

func TestRowOnlyInHeadIsNew(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkAdded-8", "sec/op", verdict.New)
}

func TestRowOnlyInBaseIsRemoved(t *testing.T) {
	wantClass(t, compare(t, "ab", gate), "BenchmarkDropped-8", "sec/op", verdict.Removed)
}

func TestSameNameInAnotherPackageIsAnotherRow(t *testing.T) {
	res := compare(t, "ab", gate)
	a := find(t, res, "example.com/a", "BenchmarkDup-8", "sec/op")
	b := find(t, res, "example.com/b", "BenchmarkDup-8", "sec/op")
	if math.Round(a.Head.Median*1e9) != 1500 || math.Round(b.Head.Median*1e9) != 1000 {
		t.Errorf("head medians: a %v, b %v; want 1500ns and 1000ns", a.Head.Median, b.Head.Median)
	}
}

func TestRowsFollowFirstAppearanceBaseFirst(t *testing.T) {
	dir := t.TempDir()
	base, head := filepath.Join(dir, "base.txt"), filepath.Join(dir, "head.txt")
	writeFile(t, base, "pkg: p\nBenchmarkB-8 1 2 ns/op 3 B/op\nBenchmarkA-8 1 2 ns/op 3 B/op\n")
	writeFile(t, head, "pkg: p\nBenchmarkC-8 1 2 ns/op\nBenchmarkA-8 1 2 ns/op 3 B/op\n")
	res, err := verdict.Compare(base, head, gate)
	if err != nil {
		t.Fatal(err)
	}
	var got []verdict.Key
	for _, r := range res.Rows {
		got = append(got, r.Key)
	}
	want := []verdict.Key{
		{Pkg: "p", Name: "BenchmarkB-8", Metric: "sec/op"},
		{Pkg: "p", Name: "BenchmarkB-8", Metric: "B/op"},
		{Pkg: "p", Name: "BenchmarkA-8", Metric: "sec/op"},
		{Pkg: "p", Name: "BenchmarkA-8", Metric: "B/op"},
		{Pkg: "p", Name: "BenchmarkC-8", Metric: "sec/op"},
	}
	if !slices.Equal(got, want) {
		t.Errorf("rows %+v, want %+v", got, want)
	}
}

func TestUnreadableFileOrMalformedHeadLineIsAnError(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.txt")
	malformed := filepath.Join(dir, "malformed.txt")
	writeFile(t, malformed, "pkg: p\nBenchmarkA-8\t--- FAIL: BenchmarkA-8\n")
	overlong := filepath.Join(dir, "overlong.txt")
	writeFile(t, overlong, "pkg: "+strings.Repeat("p", 1<<16)+"\n")
	good := filepath.Join("testdata", "ab", "base.txt")
	for _, files := range [][2]string{{missing, good}, {good, missing}, {overlong, good}, {good, overlong}, {good, malformed}} {
		if _, err := verdict.Compare(files[0], files[1], gate); err == nil {
			t.Errorf("Compare(%s, %s) succeeded, want an error", files[0], files[1])
		}
	}
}

func TestMalformedBaseLinesAreLeftOut(t *testing.T) {
	dir := t.TempDir()
	base, head := filepath.Join(dir, "base.txt"), filepath.Join(dir, "head.txt")
	writeFile(t, base, "pkg: p\nBenchmarkA-8 1 2 ns/op\nBenchmarkA-8\tprinted by the benchmark\nUnit ns/op two errors\nBenchmarkA-8 1 3 ns/op\nBenchmarkA-8 x\n")
	writeFile(t, head, "pkg: p\nBenchmarkA-8 1 2 ns/op\n")
	res, err := verdict.Compare(base, head, gate)
	if err != nil {
		t.Fatal(err)
	}
	if r := find(t, res, "p", "BenchmarkA-8", "sec/op"); r.Base.N != 2 {
		t.Errorf("base samples %d, want the 2 of the well-formed lines", r.Base.N)
	}
	if res.Skipped != 3 {
		t.Errorf("skipped %d lines, want the 3 malformed ones", res.Skipped)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
