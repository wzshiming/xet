package main

import (
	"fmt"
	"strings"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// reasons say why a package was not measured.
var reasons = map[sample.State]string{
	sample.Identical:   "identical test binaries",
	sample.BaseMissing: "base has no such package",
	sample.BaseFailed:  "base failed to build or run",
}

// render is the Markdown report of res.
func render(cfg config, res result) []byte {
	var b strings.Builder
	b.WriteString("# Benchmark gate\n\n## Confirmed regressions\n\n")
	var confirmed [][]string
	for _, f := range res.confirmed {
		confirmed = append(confirmed, []string{f.group, f.row.Pkg, f.row.Name, f.row.Metric})
	}
	table(&b, []string{"Group", "Package", "Benchmark", "Metric"}, confirmed)

	b.WriteString("## Comparison\n\n")
	for _, reportOnly := range []bool{false, true} {
		for _, g := range cfg.Groups {
			if g.ReportOnly != reportOnly {
				continue
			}
			kind := "judged"
			if reportOnly {
				kind = "report only"
			}
			fmt.Fprintf(&b, "### %s (%s)\n\n", g.Name, kind)
			var rows [][]string
			for _, r := range res.first.verdicts[g.Name].Rows {
				rows = append(rows, []string{r.Pkg, r.Name, r.Metric, string(r.Class)})
			}
			table(&b, []string{"Package", "Benchmark", "Metric", "Class"}, rows)
		}
	}

	b.WriteString("## Not measured\n\n")
	var unmeasured [][]string
	for _, g := range res.first.Groups {
		for _, p := range g.Packages {
			if p.State != sample.Measured {
				unmeasured = append(unmeasured, []string{g.Name, p.Package, reasons[p.State]})
			}
		}
	}
	table(&b, []string{"Group", "Package", "Reason"}, unmeasured)
	return []byte(b.String())
}

// table writes a Markdown table of header and rows, or "None." without rows.
func table(b *strings.Builder, header []string, rows [][]string) {
	if len(rows) == 0 {
		b.WriteString("None.\n\n")
		return
	}
	line := func(cells []string) {
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
	line(header)
	b.WriteString(strings.Repeat("| --- ", len(header)) + "|\n")
	for _, r := range rows {
		line(r)
	}
	b.WriteString("\n")
}
