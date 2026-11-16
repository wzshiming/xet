package verdict_test

import (
	"fmt"
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

// metric is a unit's values on a benchmark's result lines, one per line.
type metric struct {
	unit   string
	values []float64
}

// side is a benchmark's result lines in one sample file: line i reports value i of every metric.
type side []metric

// bench is a benchmark of a package on the base and the head side, absent from a side without metrics.
type bench struct {
	pkg, name  string
	base, head side
}

// lines is a side whose line i reports ns[i] ns/op, bytes B/op and allocs allocs/op.
func lines(ns []float64, bytes, allocs float64) side {
	return side{{"ns/op", ns}, {"B/op", same(bytes, len(ns))}, {"allocs/op", same(allocs, len(ns))}}
}

// with is s whose line i also reports values[i] in unit.
func (s side) with(unit string, values []float64) side {
	return append(slices.Clip(s), metric{unit, values})
}

// sized is a side at 100 ns/op without allocations whose line i reports bytes[i] B/op.
func sized(bytes []float64) side {
	return side{{"ns/op", same(100, len(bytes))}, {"B/op", bytes}, {"allocs/op", same(0, len(bytes))}}
}

// even is n values evenly apart from center·(1-rel) to center·(1+rel).
func even(center, rel float64, n int) []float64 {
	v := make([]float64, n)
	for i := range v {
		v[i] = center * (1 + rel*(float64(2*i)/float64(n-1)-1))
	}
	return v
}

// same is n copies of v.
func same(v float64, n int) []float64 {
	return slices.Repeat([]float64{v}, n)
}

// tight and wide are how far a steady and a noisy benchmark's values spread around their center; the 95%
// interval of the median of ten evenly spread values reaches 7/9 as far: ±0.3% and ±16%.
const tight, wide = 0.004, 0.2

// steady is ten result lines close to ns ns/op, without allocations.
func steady(ns float64) side { return lines(even(ns, tight, 10), 0, 0) }

// noisy is ten result lines spread widely around ns ns/op, without allocations.
func noisy(ns float64) side { return lines(even(ns, wide, 10), 0, 0) }

// zeroBound and zeroMedian report B/op whose median is 4 with an interval down to 0, and 0 with an
// interval up to 4, which benchstat cannot show as percentages.
var (
	zeroBound  = sized(append(same(0, 3), same(4, 7)...))
	zeroMedian = sized(append(same(0, 7), same(4, 3)...))
)

const pkgA, pkgB, pkgAA = "example.com/a", "example.com/b", "example.com/aa"

// ab is an A/B pair: a benchmark of package example.com/a per scenario its name tells, with ten samples a
// side unless named Few or Faster, and a namesake of one in example.com/b.
var ab = []bench{
	{pkgA, "BenchmarkSlower20-8", steady(1000).with("MB/s", even(1000, tight, 10)), steady(1200).with("MB/s", even(833, tight, 10))},
	{pkgA, "BenchmarkSlower20Few-8", lines(even(1000, tight, 3), 0, 0), lines(even(1200, tight, 3), 0, 0)},
	{pkgA, "BenchmarkSlower20FewBase-8", lines(even(1000, tight, 3), 0, 0), steady(1200)},
	{pkgA, "BenchmarkSlower20FewHead-8", steady(1000), lines(even(1200, tight, 3), 0, 0)},
	{pkgA, "BenchmarkFaster-8", steady(1000).with("cpu-s/GiB", even(2, tight, 10)), lines(even(800, tight, 12), 0, 0).with("cpu-s/GiB", even(1.6, tight, 12))},
	{pkgA, "BenchmarkSlower3-8", steady(1000), steady(1030)},
	{pkgA, "BenchmarkOverlap-8", lines(even(1000, 0.5, 10), 0, 0), lines(even(1300, 0.5, 10), 0, 0)}, // medians 30% apart, values overlapping
	{pkgA, "BenchmarkAllocs-8", steady(50), lines(even(52, tight, 10), 16, 1)},
	{pkgA, "BenchmarkAllocsSame-8", lines(even(200, tight, 10), 64, 2), lines(even(200, tight, 10), 64, 2)},
	{pkgA, "BenchmarkBytes5-8", lines(even(500, tight, 10), 1000, 3), lines(even(500, tight, 10), 1050, 3)},
	{pkgA, "BenchmarkBytes15-8", lines(even(500, tight, 10), 1000, 3), lines(even(500, tight, 10), 1150, 3)},
	{pkgA, "BenchmarkDropped-8", steady(400), nil},
	{pkgA, "BenchmarkAdded-8", nil, noisy(300)},
	{pkgA, "BenchmarkNoisy-8", steady(1000), noisy(1000)},
	{pkgA, "BenchmarkNoisyBase-8", noisy(1000), steady(1000)},
	{pkgA, "BenchmarkZeroBound-8", zeroBound, zeroBound},
	{pkgA, "BenchmarkZeroMedian-8", zeroMedian, zeroMedian},
	{pkgA, "BenchmarkDup-8", steady(1000), steady(1500)},
	{pkgB, "BenchmarkDup-8", steady(1000), steady(1000)},
}

// aa is an A/A pair: benchmarks of package example.com/aa run twice, where noise moved the median of the
// noisy BenchmarkDecode by 12.5%.
var aa = []bench{
	{pkgAA, "BenchmarkHash-8", steady(2500), steady(2501)},
	{pkgAA, "BenchmarkEncode-8", lines(even(15000, 0.02, 10), 4096, 3), lines(even(15050, 0.02, 10), 4096, 3)},
	{pkgAA, "BenchmarkDecode-8", lines(even(1000, 0.25, 10), 2048, 2), lines(even(1125, 0.25, 10), 2048, 2)},
	{pkgAA, "BenchmarkSmall-8", steady(25), steady(25)},
}

// write writes the base and the head side of benches to sample files and returns their paths.
func write(t *testing.T, benches []bench) (base, head string) {
	t.Helper()
	dir := t.TempDir()
	base, head = filepath.Join(dir, "base.txt"), filepath.Join(dir, "head.txt")
	writeFile(t, base, results(benches, func(x bench) side { return x.base }))
	writeFile(t, head, results(benches, func(x bench) side { return x.head }))
	return base, head
}

// results prints one side of benches as go test -bench does, with a pkg line wherever the package changes.
func results(benches []bench, sideOf func(bench) side) string {
	var b strings.Builder
	pkg := ""
	for _, x := range benches {
		s := sideOf(x)
		if len(s) == 0 {
			continue
		}
		if x.pkg != pkg {
			pkg = x.pkg
			fmt.Fprintf(&b, "pkg: %s\n", pkg)
		}
		for i := range s[0].values {
			b.WriteString(x.name + " 1000")
			for _, m := range s {
				fmt.Fprintf(&b, " %v %s", m.values[i], m.unit)
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

// compare judges the head side of benches against their base side under p.
func compare(t *testing.T, benches []bench, p verdict.Policy) verdict.Result {
	t.Helper()
	base, head := write(t, benches)
	res, err := verdict.Compare(base, head, p)
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
	r := find(t, res, pkgA, name, metric)
	if r.Class != want {
		t.Errorf("%s %s: class %q, want %q", name, metric, r.Class, want)
	}
	if listed := slices.Contains(res.Regressions, r.Key); listed != (want == verdict.Regression) {
		t.Errorf("%s %s: listed in Regressions %v, want %v", name, metric, listed, !listed)
	}
}

func TestAAHasNoRegression(t *testing.T) {
	if res := compare(t, aa, gate); len(res.Regressions) != 0 {
		t.Errorf("regressions %+v, want none", res.Regressions)
	}
}

func TestSlowerBy20PercentWithTenSamplesIsRegression(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkSlower20-8", "sec/op", verdict.Regression)
}

// Three samples on both sides, on the base alone or on the head alone are too few.
func TestSlowerBy20PercentWithThreeSamplesIsTooFewSamples(t *testing.T) {
	res := compare(t, ab, gate)
	for _, name := range []string{"BenchmarkSlower20Few-8", "BenchmarkSlower20FewBase-8", "BenchmarkSlower20FewHead-8"} {
		wantClass(t, res, name, "sec/op", verdict.TooFewSamples)
	}
}

func TestFasterBy20PercentIsImprovement(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkFaster-8", "sec/op", verdict.Improvement)
}

func TestSignificantButSlowerBy3PercentIsBelowThreshold(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkSlower3-8", "sec/op", verdict.BelowThreshold)
}

func TestSlowerBy30PercentWithOverlappingSamplesIsNotSignificant(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkOverlap-8", "sec/op", verdict.NotSignificant)
}

func TestEqualAllocsAreNotSignificant(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkAllocsSame-8", "allocs/op", verdict.NotSignificant)
}

func TestBytesUpBy5PercentIsBelowThreshold(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkBytes5-8", "B/op", verdict.BelowThreshold)
}

func TestAllocsFromZeroToOneIsRegression(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkAllocs-8", "allocs/op", verdict.Regression)
}

func TestBytesUpBy15PercentIsRegression(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkBytes15-8", "B/op", verdict.Regression)
}

func TestEmptyPolicyOnlyReports(t *testing.T) {
	res := compare(t, ab, verdict.Policy{Alpha: 0.05, MinSamples: 10})
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
	res := compare(t, ab, gate)
	wantClass(t, res, "BenchmarkSlower20-8", "B/s", verdict.ReportOnly)
	wantClass(t, res, "BenchmarkFaster-8", "cpu-s/GiB", verdict.ReportOnly)
}

func TestNoisyMarksAConfidenceIntervalWiderThanTheThreshold(t *testing.T) {
	res := compare(t, ab, gate)
	for _, c := range []struct {
		name, metric string
		want         bool
	}{
		{"BenchmarkNoisy-8", "sec/op", true},     // head ±16%, base ±0%
		{"BenchmarkNoisyBase-8", "sec/op", true}, // base ±16%, head ±0%
		{"BenchmarkAdded-8", "sec/op", true},     // head only, ±16%
		{"BenchmarkSlower3-8", "sec/op", false},  // both ±0%
		{"BenchmarkSlower20-8", "B/s", false},    // not judged
	} {
		if r := find(t, res, pkgA, c.name, c.metric); r.Noisy != c.want {
			t.Errorf("%s %s: noisy %v, want %v", c.name, c.metric, r.Noisy, c.want)
		}
	}
}

func TestRowOnlyInHeadIsNew(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkAdded-8", "sec/op", verdict.New)
}

func TestRowOnlyInBaseIsRemoved(t *testing.T) {
	wantClass(t, compare(t, ab, gate), "BenchmarkDropped-8", "sec/op", verdict.Removed)
}

func TestSameNameInAnotherPackageIsAnotherRow(t *testing.T) {
	res := compare(t, ab, gate)
	a := find(t, res, pkgA, "BenchmarkDup-8", "sec/op")
	b := find(t, res, pkgB, "BenchmarkDup-8", "sec/op")
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
	good := filepath.Join(dir, "good.txt")
	writeFile(t, good, "pkg: p\nBenchmarkA-8 1 2 ns/op\n")
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
