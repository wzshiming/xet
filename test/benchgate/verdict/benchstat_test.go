package verdict_test

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"

	"golang.org/x/perf/benchmath"

	"github.com/wzshiming/xet/test/benchgate/verdict"
)

//go:generate sh -c "go run golang.org/x/perf/cmd/benchstat -format csv base=testdata/ab/base.txt head=testdata/ab/head.txt > testdata/ab/benchstat.csv"

func TestMatchesBenchstat(t *testing.T) {
	rows := make(map[verdict.Key]verdict.Row)
	for _, r := range compare(t, "ab", gate).Rows {
		rows[r.Key] = r
	}

	f, err := os.Open("testdata/ab/benchstat.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	var pkg, unit string
	seen := 0
	for _, rec := range records {
		switch {
		case len(rec) == 1:
			if v, ok := strings.CutPrefix(rec[0], "pkg: "); ok {
				pkg = v
			}
		case rec[0] == "":
			if rec[2] == "CI" {
				unit = rec[1]
			}
		case rec[0] == "geomean":
		default:
			k := verdict.Key{Pkg: pkg, Name: "Benchmark" + rec[0], Metric: unit}
			row, ok := rows[k]
			if !ok {
				t.Errorf("%+v: in benchstat's output, not in Compare's", k)
				continue
			}
			seen++
			checkSide(t, k, "base", row.Base, rec[1:])
			checkSide(t, k, "head", row.Head, rec[3:])
			if len(rec) == 7 {
				checkComparison(t, k, row, rec[5], rec[6])
			}
		}
	}
	if seen != len(rows) {
		t.Errorf("benchstat shows %d rows, Compare returned %d", seen, len(rows))
	}
}

// checkSide compares a side with benchstat's center and CI cells, both empty when the side is absent.
func checkSide(t *testing.T, k verdict.Key, side string, s verdict.Side, cells []string) {
	t.Helper()
	if len(cells) < 2 || cells[0] == "" {
		if s.N != 0 {
			t.Errorf("%+v: %s has %d samples, benchstat none", k, side, s.N)
		}
		return
	}
	if got := fmt.Sprint(s.Median); got != cells[0] {
		t.Errorf("%+v: %s median %s, benchstat %s", k, side, got, cells[0])
	}
	if got := (benchmath.Summary{Center: s.Median, Lo: s.Lo, Hi: s.Hi}).PctRangeString(); got != cells[1] {
		t.Errorf("%+v: %s CI ±%s, benchstat ±%s", k, side, got, cells[1])
	}
}

func checkComparison(t *testing.T, k verdict.Key, row verdict.Row, delta, p string) {
	t.Helper()
	switch delta {
	case "~":
		if row.P <= gate.Alpha {
			t.Errorf("%+v: p=%v is significant, benchstat shows ~", k, row.P)
		}
	case "?":
		if !math.IsInf(row.Delta, 1) {
			t.Errorf("%+v: delta %v from a zero base, want +Inf", k, row.Delta)
		}
	default:
		if got := fmt.Sprintf("%+.2f%%", row.Delta*100); got != delta {
			t.Errorf("%+v: delta %s, benchstat %s", k, got, delta)
		}
	}
	if got := (benchmath.Comparison{P: row.P, N1: row.Base.N, N2: row.Head.N}).String(); got != p {
		t.Errorf("%+v: %s, benchstat %s", k, got, p)
	}
}
