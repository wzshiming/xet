// Command benchgate benchmarks a head source tree against a base tree on one machine and fails on a
// confirmed regression.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

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
	measureIdentical     bool
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
	fs.StringVar(&o.out, "out", "", "output `directory`")
	fs.BoolVar(&o.measureIdentical, "measure-identical", false, "also measure packages whose test binaries are identical")
	if err := fs.Parse(args); err != nil {
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
	cfg, err := loadConfig(o.config)
	if err != nil {
		return nil, false, err
	}
	cfg.MeasureIdentical = cfg.MeasureIdentical || o.measureIdentical
	if err := os.MkdirAll(o.out, 0o755); err != nil {
		return nil, false, err
	}
	res, err := gate(ctx, sampleRun, cfg, o.base, o.head, o.out)
	if err != nil {
		return nil, false, err
	}
	return render(cfg, res), len(res.confirmed) > 0, nil
}
