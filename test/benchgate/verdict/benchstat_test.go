package verdict_test

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"math"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/perf/benchmath"

	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// benchstat runs the benchstat of the golang.org/x/perf version go.mod requires on two sample files and
// returns its CSV records.
func benchstat(t *testing.T, base, head string) [][]string {
	t.Helper()
	cmd := exec.Command("go", "run", "golang.org/x/perf/cmd/benchstat", "-format", "csv", "base="+base, "head="+head)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("benchstat: %v\n%s", err, &stderr)
	}
	r := csv.NewReader(bytes.NewReader(out))
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return records
}

func TestMatchesBenchstat(t *testing.T) {
	base, head := write(t, ab)
	res, err := verdict.Compare(base, head, gate)
	if err != nil {
		t.Fatal(err)
	}
	rows := make(map[verdict.Key]verdict.Row)
	for _, r := range res.Rows {
		rows[r.Key] = r
	}

	var pkg, unit string
	seen := 0
	for _, rec := range benchstat(t, base, head) {
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
	if got := s.PctRange(); got != cells[1] {
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
