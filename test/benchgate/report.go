package main

import (
	"cmp"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/wzshiming/xet/test/benchgate/sample"
	"github.com/wzshiming/xet/test/benchgate/verdict"
)

// reasons say why a package was not measured.
var reasons = map[sample.State]string{
	sample.Identical:   "identical test binaries",
	sample.BaseMissing: "base has no such package",
	sample.BaseFailed:  "base failed to build or run",
}

// render is the Markdown report of res, a run of the gate under cfg as o asked for it.
func render(o options, cfg config, res result) []byte {
	var b strings.Builder
	confirmed, excluded := res.split()
	fmt.Fprintf(&b, "# Benchmark gate\n\n%s\n\n", conclusion(res, len(confirmed)))
	fmt.Fprintf(&b, "## Trees\n\n- Base: %s\n- Head: %s\n\n", o.baseLabel, o.headLabel)
	env := res.first.Env
	fmt.Fprintf(&b, "## Environment\n\n- CPU: %s\n- Cores: %d\n- OS/arch: %s\n- Go: %s\n\n", env.CPU, env.Cores, env.OSArch, env.GoVersion)
	policy(&b, cfg)
	b.WriteString("## Confirmed regressions\n\n")
	suspects(&b, cfg, confirmed)
	b.WriteString("## Excluded after re-check\n\n")
	suspects(&b, cfg, excluded)
	comparison(&b, o, cfg, res)
	notMeasured(&b, res)
	loadPerRound(&b, res)
	fmt.Fprintf(&b, "## Reproduce\n\nWith worktrees `<base>` at %s and `<head>` at %s, build the gate with `go -C <head>/test/benchgate build -o <benchgate> .` and run `%s` with absolute paths and a new directory `<out>`.\n",
		o.baseLabel, o.headLabel, o.command())
	return []byte(b.String())
}

// conclusion is the report's verdict line.
func conclusion(res result, confirmed int) string {
	switch {
	case confirmed == 1:
		return "**FAIL**: 1 confirmed regression."
	case confirmed > 1:
		return fmt.Sprintf("**FAIL**: %d confirmed regressions.", confirmed)
	case len(res.first.verdicts) == 0:
		return "**PASS**: nothing was measured."
	default:
		return "**PASS**: no confirmed regression."
	}
}

func policy(b *strings.Builder, cfg config) {
	identical := "not measured"
	if cfg.MeasureIdentical {
		identical = "measured too"
	}
	fmt.Fprintf(b, "## Policy\n\nSignificance level %v; a row is judged with at least %d samples on each side; identical test binaries are %s.\n\n",
		cfg.Alpha, cfg.MinSamples, identical)
	var rows [][]string
	for _, g := range cfg.Groups {
		thresholds := "report only"
		if !g.ReportOnly {
			var list []string
			for _, m := range judgedMetrics {
				if t, ok := g.Thresholds[m]; ok {
					list = append(list, m+" "+percent(t))
				}
			}
			thresholds = strings.Join(list, ", ")
		}
		rows = append(rows, []string{g.Name, strings.Join(g.Packages, ", "), "`" + g.Bench + "`", cmp.Or(g.Benchtime, "default"), strconv.Itoa(g.Rounds), thresholds})
	}
	table(b, []string{"Group", "Packages", "Benchmarks", "Benchtime", "Rounds", "Thresholds"}, rows)
}

// suspects writes a table of suspects with the numbers of both passes.
func suspects(b *strings.Builder, cfg config, list []suspect) {
	var rows [][]string
	for _, s := range list {
		recheck := "not measured"
		if s.recheck.Class != "" {
			recheck = numbers(s.recheck) + ", " + string(s.recheck.Class)
		}
		rows = append(rows, []string{s.group, s.first.Pkg, s.first.Name, s.first.Metric,
			percent(cfg.group(s.group).Thresholds[s.first.Metric]), numbers(s.first), recheck})
	}
	table(b, []string{"Group", "Package", "Benchmark", "Metric", "Threshold", "First pass", "Re-check"}, rows)
}

// comparison writes every row of the first pass by group, judged groups first.
func comparison(b *strings.Builder, o options, cfg config, res result) {
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
			fmt.Fprintf(b, "### %s (%s)\n\n", g.Name, kind)
			if base, head := res.first.files(g.Name); base != "" {
				fmt.Fprintf(b, "Samples: `%s`, `%s`", o.inOut(base), o.inOut(head))
				if base, head := res.recheck.files(g.Name); base != "" {
					fmt.Fprintf(b, "; re-check: `%s`, `%s`", o.inOut(base), o.inOut(head))
				}
				b.WriteString(".\n\n")
			}
			var rows [][]string
			for _, r := range res.first.verdicts[g.Name].Rows {
				rows = append(rows, cells(r))
			}
			table(b, []string{"Package", "Benchmark", "Metric", "Base", "Head", "Delta", "p", "n", "Class", "Noisy"}, rows)
		}
	}
}

// cells are a compared row as the comparison shows it.
func cells(r verdict.Row) []string {
	delta, p := "", ""
	if r.Class != verdict.New && r.Class != verdict.Removed {
		delta, p = change(r.Delta), fmt.Sprintf("%.3f", r.P)
	}
	noisy := ""
	if r.Noisy {
		noisy = "noisy"
	}
	return []string{r.Pkg, r.Name, r.Metric, side(r.Base, r.Metric), side(r.Head, r.Metric),
		delta, p, fmt.Sprintf("%d+%d", r.Base.N, r.Head.N), string(r.Class), noisy}
}

func notMeasured(b *strings.Builder, res result) {
	b.WriteString("## Not measured\n\n")
	var rows [][]string
	for _, p := range []struct {
		name string
		pass pass
	}{{"first", res.first}, {"re-check", res.recheck}} {
		for _, g := range p.pass.Groups {
			for _, s := range g.Packages {
				if s.State != sample.Measured {
					rows = append(rows, []string{p.name, g.Name, s.Package, reasons[s.State]})
				}
			}
		}
	}
	table(b, []string{"Pass", "Group", "Package", "Reason"}, rows)
}

func loadPerRound(b *strings.Builder, res result) {
	b.WriteString("## Load per round\n\nThe 1, 5 and 15 minute load averages before each round.\n\n")
	first, recheck := res.first.Env.Loads, res.recheck.Env.Loads
	var rows [][]string
	for i := range max(len(first), len(recheck)) {
		row := []string{strconv.Itoa(i + 1), "", ""}
		if i < len(first) {
			row[1] = first[i]
		}
		if i < len(recheck) {
			row[2] = recheck[i]
		}
		rows = append(rows, row)
	}
	table(b, []string{"Round", "First pass", "Re-check"}, rows)
}

// numbers are a row's medians, change and p-value.
func numbers(r verdict.Row) string {
	return fmt.Sprintf("%s → %s, %s, p=%.3f", value(r.Base.Median, r.Metric), value(r.Head.Median, r.Metric), change(r.Delta), r.P)
}

// side is a side's median and interval, or empty when the row is absent there.
func side(s verdict.Side, metric string) string {
	if s.N == 0 {
		return ""
	}
	return value(s.Median, metric) + " " + interval(s)
}

// interval is how far the confidence interval reaches from the median, relative to it, as benchstat's ±.
func interval(s verdict.Side) string {
	d := max(s.Hi-s.Median, s.Median-s.Lo)
	if d == 0 {
		return "±0%"
	}
	if r := d / math.Abs(s.Median); !math.IsInf(r, 0) {
		return fmt.Sprintf("±%.0f%%", r*100)
	}
	return "±∞"
}

// unit is a prefixed unit of a metric.
type unit struct {
	size   float64
	suffix string
}

var (
	timeUnits  = []unit{{1, "s"}, {1e-3, "ms"}, {1e-6, "µs"}, {1e-9, "ns"}}
	byteUnits  = []unit{{1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}, {1, "B"}}
	countUnits = []unit{{1e9, "G"}, {1e6, "M"}, {1e3, "k"}, {1, ""}}
)

// value is v of metric in the largest unit it fills.
func value(v float64, metric string) string {
	units := countUnits
	switch metric {
	case "sec/op":
		units = timeUnits
	case "B/op":
		units = byteUnits
	}
	u := units[len(units)-1]
	for _, c := range units {
		if math.Abs(v) >= c.size {
			u = c
			break
		}
	}
	return fmt.Sprintf("%.4g%s", v/u.size, u.suffix)
}

// change is a relative change such as +12.34%, or +∞ from a zero base.
func change(d float64) string {
	if math.IsInf(d, 1) {
		return "+∞"
	}
	return fmt.Sprintf("%+.2f%%", d*100)
}

// percent is a threshold such as 0.1 as +10%.
func percent(t float64) string {
	return fmt.Sprintf("+%.4g%%", t*100)
}

// inOut is path relative to the output directory.
func (o options) inOut(path string) string {
	rel, _ := filepath.Rel(o.out, path)
	return filepath.ToSlash(rel)
}

// command is a command line of the built gate repeating this run, with placeholders for local paths.
func (o options) command() string {
	args := []string{"<benchgate> -base <base> -head <head>",
		"-base-label " + quote(o.baseLabel), "-head-label " + quote(o.headLabel), "-config " + o.configArg()}
	if o.groups != "" {
		args = append(args, "-groups "+quote(o.groups))
	}
	if o.measureIdentical != nil {
		args = append(args, "-measure-identical="+strconv.FormatBool(*o.measureIdentical))
	}
	return strings.Join(append(args, "-out <out>"), " ")
}

// configArg names the configuration file by its place in the head tree, or else by a placeholder.
func (o options) configArg() string {
	head, _ := filepath.Abs(o.head)
	config, _ := filepath.Abs(o.config)
	if rel, err := filepath.Rel(head, config); err == nil && filepath.IsLocal(rel) {
		return "<head>/" + filepath.ToSlash(rel)
	}
	return "<config>"
}

var plainWord = regexp.MustCompile(`^[\w@%+=:,./-]+$`)

// quote is s as one word of a POSIX shell command.
func quote(s string) string {
	if plainWord.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// table writes a Markdown table of header and rows, or "None." without rows.
func table(b *strings.Builder, header []string, rows [][]string) {
	if len(rows) == 0 {
		b.WriteString("None.\n\n")
		return
	}
	line := func(cells []string) {
		escaped := make([]string, len(cells))
		for i, c := range cells {
			escaped[i] = strings.ReplaceAll(c, "|", `\|`)
		}
		b.WriteString("| " + strings.Join(escaped, " | ") + " |\n")
	}
	line(header)
	b.WriteString(strings.Repeat("| --- ", len(header)) + "|\n")
	for _, r := range rows {
		line(r)
	}
	b.WriteString("\n")
}
