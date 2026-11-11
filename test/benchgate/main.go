// Command benchgate benchmarks a head source tree against a base tree on one machine and fails on a
// confirmed regression.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wzshiming/xet/test/benchgate/sample"
)

// Exit codes.
const (
	exitPass       = 0 // no confirmed regression
	exitRegression = 1 // a confirmed regression
	exitError      = 2 // nothing to conclude: a head build or run failed, or the configuration or usage is wrong
)

// sampler takes samples as sample.Run does.
type sampler func(context.Context, sample.Request) (sample.Result, error)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, sample.Run))
}

// options are the command line's values.
type options struct {
	base, head           string // source tree roots
	baseLabel, headLabel string // what the report calls the trees
	config               string // configuration file
	out                  string // output directory
	measureIdentical     *bool  // nil unless given
	groups               string // comma-separated names of the only groups to run, empty for all
}

// run is the command with args, sampling through sampleRun; it returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer, sampleRun sampler) int {
	var o options
	fs := flag.NewFlagSet("benchgate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.StringVar(&o.base, "base", "", "base source tree `directory`")
	fs.StringVar(&o.head, "head", "", "head source tree `directory`")
	fs.StringVar(&o.baseLabel, "base-label", "", "base tree's name in the report, such as a commit")
	fs.StringVar(&o.headLabel, "head-label", "", "head tree's name in the report, such as a commit")
	fs.StringVar(&o.config, "config", "", "configuration `file`")
	fs.StringVar(&o.out, "out", "", "output `directory`, new or empty")
	fs.BoolFunc("measure-identical", "measure packages whose test binaries are identical too, whatever the configuration says", func(s string) error {
		v, err := strconv.ParseBool(s)
		o.measureIdentical = &v
		return err
	})
	fs.StringVar(&o.groups, "groups", "", "comma-separated `names` of the only groups to run")
	if err := fs.Parse(args); err != nil {
		return exitError // the flag set said why
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "benchgate: unexpected argument %q\n", fs.Arg(0))
		return exitError
	}
	report, regressed, err := o.gate(ctx, sampleRun)
	if err != nil {
		fmt.Fprintln(stderr, "benchgate:", err)
		return exitError
	}
	stdout.Write(report)
	if regressed {
		return exitRegression
	}
	return exitPass
}

// gate runs the gate and renders its report; regressed tells whether a regression is confirmed.
func (o options) gate(ctx context.Context, sampleRun sampler) (report []byte, regressed bool, err error) {
	for _, f := range []struct{ name, value string }{
		{"-base", o.base}, {"-head", o.head}, {"-base-label", o.baseLabel}, {"-head-label", o.headLabel},
		{"-config", o.config}, {"-out", o.out},
	} {
		if f.value == "" {
			return nil, false, fmt.Errorf("missing %s", f.name)
		}
	}
	cfg, err := loadConfig(o.config)
	if err != nil {
		return nil, false, err
	}
	if o.measureIdentical != nil {
		cfg.MeasureIdentical = *o.measureIdentical
	}
	if o.groups != "" {
		if err := cfg.only(strings.Split(o.groups, ",")); err != nil {
			return nil, false, err
		}
	}
	if entries, err := os.ReadDir(o.out); err == nil && len(entries) > 0 {
		return nil, false, fmt.Errorf("-out %s is not empty", o.out)
	}
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return nil, false, err
	}
	res, err := gate(ctx, sampleRun, cfg, o.base, o.head, o.out)
	if err != nil {
		return nil, false, err
	}
	confirmed, _ := res.split()
	report = render(o, cfg, res)
	if err := os.WriteFile(filepath.Join(o.out, "report.md"), report, 0o644); err != nil {
		return nil, false, err
	}
	return report, len(confirmed) > 0, nil
}
