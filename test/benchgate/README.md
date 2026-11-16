# Benchmark gate

`benchgate` benchmarks a head source tree against a base tree on one machine
and fails on a regression. It builds both trees' test binaries before timing
anything, skips packages whose two binaries are identical, and runs the rest in
alternating rounds: both sides back to back, switching which goes first.

A row, one metric of one benchmark, regresses when both sides have `minSamples`
samples, the difference is significant at `alpha` (Mann-Whitney U test, as in
benchstat), and the head median exceeds the base median by more than the
threshold. Regressed rows are sampled again in fresh rounds, and only a row
that regresses again is confirmed. The gate exits 0 without a confirmed
regression, 1 with one, and 2 with nothing to conclude: the head fails to build
or run, a group whose packages were all measured selects no benchmark, the
configuration or command line is wrong, or the run was interrupted. A package
the base lacks or fails to build or run is listed as not measured.

## Running it

Build the gate from the head tree and run it on the two trees; `go run` would
report exit code 2 as 1. The output directory must be new or empty:

```sh
go -C /tmp/head/test/benchgate build -o /tmp/benchgate .
/tmp/benchgate -base /tmp/base -head /tmp/head -base-label B -head-label H \
  -config /tmp/head/test/benchgate/bench-gate.json -out /tmp/gate
```

The labels name the trees in the report. `-groups a,b` runs only those groups;
`-measure-identical[=false]` overrides the configuration. The output directory
gets `report.md`, also printed, and the raw samples in `first/` and `recheck/`.

## Configuration

One JSON object; an unknown field is an error. `alpha` is the significance
level, `minSamples` the samples each side needs for a row to be judged, and
`measureIdentical` measures identical test binaries too. Each of `groups` has a
`name`, which names its sample files; `packages`, directories relative to the
tree roots; `bench` and `benchtime`, as for `go test`; `rounds`, the samples of
each row on each side, at least 10 when judged; and `thresholds`, from
`sec/op`, `B/op` or `allocs/op` to the relative increase a regression must
exceed (0.1 for 10%), or `"reportOnly": true` to compare without judging.

## Pull requests

The [workflow](../../.github/workflows/bench-gate.yml) runs the gate with
[bench-gate.json](bench-gate.json) on every pull request to master: the pull
request's merge result is the head, the master commit it merges into the base,
both on one runner. It measures the packages whose test binaries differ, or
every configured package if the pull request changes the gate itself (this
directory or the workflow), and nothing if neither holds. A new `xorb`
benchmark is judged as it is; elsewhere, add it to a group's `bench` pattern or
add a group.

When the check is red, the job summary is the report: the confirmed regressions
with both samplings' numbers, then every compared row, or the error if nothing
could be concluded. The artifact `bench-gate-<attempt>` holds the report and
the raw samples. To reproduce the run, use the commands that end the report on
worktrees of its base commit and of the pull request merged into it. There is
no override: the check stays red, and a maintainer decides whether to merge.
