// Package verdict decides which benchmark rows regressed between a base and a head sample file.
package verdict

import (
	"math"
	"os"

	"golang.org/x/perf/benchfmt"
	"golang.org/x/perf/benchmath"
)

// confidence is benchstat's default level for the interval around a median.
const confidence = 0.95

// Policy says which metrics are judged and how strictly.
type Policy struct {
	// Thresholds maps each judged metric, such as sec/op, to its minimum relative increase (0.1 for 10%); empty judges nothing.
	Thresholds map[string]float64
	Alpha      float64 // significance level
	MinSamples int     // samples each side needs before a judged row is judged
}

// Key names a row: one metric of one benchmark in one package.
type Key struct {
	Pkg    string // value of the pkg: line in effect
	Name   string // first field of the result line, such as BenchmarkFoo/n=1-8
	Metric string // unit as benchstat names it, such as sec/op or B/s
}

// Side summarizes one file's samples of a row; N is 0 when the row is absent there.
type Side struct {
	N      int
	Median float64
	Lo, Hi float64 // 95% confidence interval of the median
}

// Class is a row's classification.
type Class string

const (
	Regression     Class = "regression"
	Improvement    Class = "improvement"
	NotSignificant Class = "not significant"
	BelowThreshold Class = "below threshold"
	New            Class = "new"
	Removed        Class = "removed"
	TooFewSamples  Class = "too few samples"
	ReportOnly     Class = "report only" // metric not in the policy
)

// Row compares one Key between the two files; Delta and P are 0 for New and Removed rows.
type Row struct {
	Key
	Base, Head Side
	Delta      float64 // head median / base median - 1, +Inf from a zero base
	P          float64 // Mann-Whitney U p-value
	Class      Class
	Noisy      bool // a side's interval reaches further from its median than the metric's threshold
}

// Result is what Compare finds.
type Result struct {
	Rows        []Row // in order of first appearance, base file first
	Regressions []Key
}

// Compare judges the head samples against the base samples under p, with benchstat's default statistics.
func Compare(basePath, headPath string, p Policy) (Result, error) {
	base, keys, err := load(basePath)
	if err != nil {
		return Result{}, err
	}
	head, headKeys, err := load(headPath)
	if err != nil {
		return Result{}, err
	}
	for _, k := range headKeys {
		if _, ok := base[k]; !ok {
			keys = append(keys, k)
		}
	}
	var res Result
	for _, k := range keys {
		row := judge(k, base[k], head[k], p)
		res.Rows = append(res.Rows, row)
		if row.Class == Regression {
			res.Regressions = append(res.Regressions, k)
		}
	}
	return res, nil
}

func load(path string) (map[Key][]float64, []Key, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	values := make(map[Key][]float64)
	var keys []Key
	r := benchfmt.NewReader(f, path)
	for r.Scan() {
		switch res := r.Result().(type) {
		case *benchfmt.SyntaxError:
			return nil, nil, res
		case *benchfmt.Result:
			for _, v := range res.Values {
				k := Key{Pkg: res.GetConfig("pkg"), Name: "Benchmark" + string(res.Name), Metric: v.Unit}
				if _, ok := values[k]; !ok {
					keys = append(keys, k)
				}
				values[k] = append(values[k], v.Value)
			}
		}
	}
	return values, keys, r.Err()
}

func judge(k Key, base, head []float64, p Policy) Row {
	th := &benchmath.Thresholds{CompareAlpha: p.Alpha}
	row := Row{Key: k}
	var bs, hs *benchmath.Sample
	if base != nil {
		bs = benchmath.NewSample(base, th)
		row.Base = summarize(bs)
	}
	if head != nil {
		hs = benchmath.NewSample(head, th)
		row.Head = summarize(hs)
	}
	t, judged := p.Thresholds[k.Metric]
	row.Noisy = judged && (spread(row.Base) > t || spread(row.Head) > t)
	switch {
	case bs == nil:
		row.Class = New
		return row
	case hs == nil:
		row.Class = Removed
		return row
	}
	row.Delta = delta(row.Base.Median, row.Head.Median)
	row.P = benchmath.AssumeNothing.Compare(bs, hs).P
	switch {
	case !judged:
		row.Class = ReportOnly
	case row.Base.N < p.MinSamples || row.Head.N < p.MinSamples:
		row.Class = TooFewSamples
	case row.P > p.Alpha: // where benchstat shows ~
		row.Class = NotSignificant
	case row.Head.Median > row.Base.Median*(1+t):
		row.Class = Regression
	case row.Head.Median < row.Base.Median*(1-t):
		row.Class = Improvement
	default:
		row.Class = BelowThreshold
	}
	return row
}

func summarize(s *benchmath.Sample) Side {
	sum := benchmath.AssumeNothing.Summary(s, confidence)
	return Side{N: len(s.Values), Median: sum.Center, Lo: sum.Lo, Hi: sum.Hi}
}

// delta is computed as benchstat computes its "vs base" column.
func delta(base, head float64) float64 {
	if base == head {
		return 0
	}
	return head/base - 1
}

// spread is the half-width of the interval relative to the median, as benchstat's ± column; 0 for an absent side.
func spread(s Side) float64 {
	d := max(s.Hi-s.Median, s.Median-s.Lo)
	if d == 0 {
		return 0
	}
	return d / math.Abs(s.Median)
}
