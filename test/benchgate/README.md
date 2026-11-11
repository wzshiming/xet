# Benchmark gate

`benchgate` benchmarks a head source tree against a base tree on one machine
and fails when the head regresses. It builds every configured package's test
binary on both sides before any timing, leaves packages whose two binaries are
byte-identical unmeasured, and runs the two sides back to back in each round,
alternating which goes first. The statistics are benchstat's defaults: medians,
95% confidence intervals and the Mann-Whitney U test.

A row, one metric of one benchmark, regresses when its metric is judged, both
sides have at least `minSamples` samples, the difference is significant at
`alpha`, and the head median exceeds the base median by more than the
threshold. A regression in the first pass only makes the row a suspect: the
gate samples the suspect rows alone again with fresh rounds, and a regression
is confirmed only when those samples by themselves regress the same row in the
same metric.

| Exit code | Meaning |
| --- | --- |
| 0 | no confirmed regression |
| 1 | at least one confirmed regression |
| 2 | nothing to conclude: a head build or benchmark failed, a measured group has no benchmark row on the head side, the configuration or command line is wrong, or SIGINT or SIGTERM stopped the run |

A base tree that lacks a package, or whose package fails to build or run, is
not a failure: the report lists the package as not measured, and the command
prints why the base failed on standard error. A failure on both sides is the
head's and exits 2, whichever side ran first; so does a package without test
files, of which `go test` builds no test binary. A malformed line in the base
samples, such as one a base benchmark printed to standard output, is left out
with a warning on standard error; in the head samples it exits 2.

SIGINT and SIGTERM stop the run: the gate kills the benchmark or build in
progress, removes the test binaries it built and exits 2.

## Running it locally

Check out the two commits as worktrees, build the gate from the head tree's
copy of this module and run the binary, here with the configuration below
saved as `/tmp/gate.json`. Paths must be absolute, as `go -C` changes
directory, and the output directory must be new or empty:

```sh
git worktree add /tmp/xet-base origin/master
git worktree add /tmp/xet-head HEAD
go -C /tmp/xet-head/test/benchgate build -o /tmp/benchgate .
/tmp/benchgate \
  -base /tmp/xet-base -head /tmp/xet-head \
  -base-label "$(git -C /tmp/xet-base rev-parse --short HEAD)" \
  -head-label "$(git -C /tmp/xet-head rev-parse --short HEAD)" \
  -config /tmp/gate.json -out /tmp/xet-gate
```

Run the built binary rather than `go run`: `go run` exits 1 whenever the
program exits non-zero, so exit code 2 would read as 1. Both sides build with
the `go` command on `PATH`, whatever toolchain their `go.mod` names. The flags:

| Flag | Meaning |
| --- | --- |
| `-base`, `-head` | the source tree roots |
| `-base-label`, `-head-label` | what the report calls the trees, such as commits |
| `-config` | the configuration file |
| `-out` | the output directory |
| `-groups a,b` | run only these groups of the configuration |
| `-measure-identical`, `-measure-identical=false` | override the configuration's `measureIdentical` |

The output directory holds `report.md`, the Markdown report the command also
prints, and the raw samples: `first/<group>.base.txt` and
`first/<group>.head.txt` for the first pass, `recheck/` likewise for the
re-check. benchstat recomputes any table from them, for example
`go -C test/benchgate run golang.org/x/perf/cmd/benchstat /tmp/xet-gate/first/cpu.base.txt /tmp/xet-gate/first/cpu.head.txt`.
Every report ends with the command lines that build the gate and repeat its
run.

## Configuration

The configuration is JSON; an unknown field is an error.

```json
{
  "alpha": 0.05,
  "minSamples": 10,
  "measureIdentical": false,
  "groups": [
    {
      "name": "cpu",
      "packages": [".", "xorb"],
      "bench": ".",
      "benchtime": "100ms",
      "rounds": 10,
      "thresholds": {"sec/op": 0.1, "B/op": 0.1, "allocs/op": 0.1}
    },
    {
      "name": "storage",
      "packages": ["storage"],
      "bench": "^BenchmarkReconstructedFile$",
      "rounds": 10,
      "reportOnly": true
    }
  ]
}
```

| Field | Meaning |
| --- | --- |
| `alpha` | significance level, more than 0 and less than 1 |
| `minSamples` | samples each side needs before a row is judged, at least 1 |
| `measureIdentical` | also measure packages whose base and head test binaries are identical |
| `groups[].name` | letters, digits, `_`, `-` and `.`, starting with a letter, digit or `_`; names the group's sample files |
| `groups[].packages` | package directories relative to the tree roots |
| `groups[].bench` | the `go test -bench` regular expression, not empty |
| `groups[].benchtime` | the `go test -benchtime` value, a positive duration such as `100ms` or count such as `10x`; empty for Go's default |
| `groups[].rounds` | samples of each row on each side; at least 10 and at least `minSamples` in a judged group |
| `groups[].thresholds` | judged metric, `sec/op`, `B/op` or `allocs/op`, to the least relative increase that is a regression: 0.1 for 10% |
| `groups[].reportOnly` | `true` for a group compared without being judged, in place of `thresholds` |

Rows of metrics without a threshold, such as `B/s`, are only reported. A row of
a judged metric is flagged noisy when either side's confidence interval reaches
further from its median than the threshold, so that a regression of that size
may go unnoticed.
