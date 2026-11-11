# XET Protocol (Go Implementation)

Go implementation of the XET content-addressable storage protocol for large-file transfer with chunk-level deduplication.

This project tracks:

- [huggingface/xet-core](https://github.com/huggingface/xet-core) (reference behavior in production)
- [XET Protocol Draft](https://datatracker.ietf.org/doc/draft-denis-xet/05/) (public protocol baseline)

The Rust implementation in xet-core is currently ahead of the draft in several areas, so this repository prioritizes compatibility with real xet-core behavior where needed.

## Current Scope

Implemented in this repository:
- Core hash / gearhash / merkle primitives
- Shard and xorb encode/decode paths
- Upload and download client workflows with streaming wrappers (upload from any `io.ReadSeeker`, download to an `io.WriteSeeker` or as `io.ReadCloser` streams)
- Server storage backends: local filesystem, in-memory, or S3-compatible object stores, with optional presigned xorb download URLs
- Mirror mode: full-cache middle layer bridging xet and plain hubs
- Hugging Face token/LFS based integration helpers
- Conformance and unit tests for key protocol paths

## Compatibility Notes

- This project aims to follow the draft spec where possible.
- When draft and xet-core behavior diverge, practical interop with xet-core may take priority.
- Test and conformance coverage is evolving with protocol and upstream changes.
- Chunk boundaries, and therefore xet file hashes, follow the current xet-core chunker, which never cuts a chunk shorter than the 8 KiB minimum ([xet-core#487](https://github.com/huggingface/xet-core/pull/487), first shipped in hf-xet 1.1.10, September 2025). Earlier clients could occasionally cut a shorter chunk, so a file uploaded with hf-xet 1.1.9 or older — which includes many uploads made before 2026 — may be recorded upstream under a xet hash that differs from the one this implementation computes for the same bytes. The SHA-256 (LFS OID) is unaffected.

## Benchmark Gate

Every pull request to master runs the **Benchmark gate** check ([workflow](.github/workflows/bench-gate.yml)). On one runner, [benchgate](test/benchgate/README.md) builds the benchmarks' test binaries of the pull request merged into master and of the master commit it merges into, and runs the two sides in alternating rounds. Packages whose two test binaries are identical are not measured, so a pull request that changes no benchmarked code passes without running a benchmark, and its summary says nothing was measured.

[.github/bench-gate.json](.github/bench-gate.json) sorts the benchmarks into groups. Each group runs its benchmarks 10 times on each side, every run with the group's `-benchtime`, and judges time, bytes and allocations per operation (`sec/op`, `B/op`, `allocs/op`) against the group's thresholds, set from A/A runs on GitHub's runners:

| Group | Benchmarks | Benchtime | Judged |
| --- | --- | --- | --- |
| `root` | `BenchmarkComputeChunkHash` | 300ms | `sec/op` +5%, `B/op` +10%, `allocs/op` +10% |
| `filehash` | `BenchmarkComputeFileHash` | 20x | `sec/op` +5% |
| `xorb` | every benchmark in `xorb` | 300ms | `sec/op` +10%, `B/op` +10%, `allocs/op` +10% |
| `client` | the 1 MiB rows of `BenchmarkClient`, one per class, operation and protocol version | 50x | `sec/op` +10%, `B/op` +20%, `allocs/op` +10% |
| `serve` | the 1 MiB rows of `BenchmarkServe` | 100ms | only reported |
| `storage` | `BenchmarkReconstructedFile` | 100ms | only reported |

A row regresses when its change is statistically significant and larger than the threshold; a regression fails the check only when a re-check with fresh samples of the row regresses it again. Benchmarks that take milliseconds per operation run a fixed number of iterations, `20x` or `50x`, rather than for a duration, so that every sample spreads its slow first iterations and one-off allocations over the same count. The bytes and allocations of `BenchmarkComputeFileHash` vary between runs of the same binary, so the report shows them without judging them.

When the check is red:

- Open the run's summary, which is the gate's report: the confirmed regressions with the numbers of both passes, then every compared row. "Nothing to conclude" means the gate could not measure, for example because the pull request does not build, and the error follows.
- The run's artifact `bench-gate-<attempt>` holds the report and the raw samples, which benchstat reads.
- Reproduce the run locally as [test/benchgate/README.md](test/benchgate/README.md) shows, with the base commit the report names and the pull request merged into it as the head.
- There is no override: the check stays red, and a maintainer decides whether to merge anyway.

A new benchmark in `xorb` is judged as it is, in the `xorb` group. A new benchmark in the root package is judged once its name joins the pattern of the `root` group, or of a group of its own when it takes milliseconds per operation. Elsewhere, add its package and `-bench` pattern to a group of .github/bench-gate.json or add a group, in the format of [test/benchgate/README.md](test/benchgate/README.md#configuration): a group of benchmarks taking microseconds per operation runs for `300ms`, one taking milliseconds a fixed count such as `50x`. A judged group needs at least 10 rounds and thresholds that A/A runs on GitHub's runners pass, so a metric that varies between such runs gets no threshold; a group with `"reportOnly": true` is compared without being judged. A pull request that only changes the configuration builds identical test binaries, so check the pattern with a manual run: Actions, Benchmark gate, Run workflow, with the branch as head. A manual run compares master with itself by default and measures identical test binaries too; a group whose pattern selects no benchmark fails it.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/xet/blob/master/LICENSE) for the full license text.
