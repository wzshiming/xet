# xet-core conformance tests

This module tests the Go implementation directly against the Rust reference
implementation from [`huggingface/xet-core`](https://github.com/huggingface/xet-core).
The reference program `src/main.rs` is built against each pinned xet-core
release from that release's own locked manifest, `versions/<tag>/Cargo.toml`:

| Release | Commit | Date |
| --- | --- | --- |
| v1.7.1 (default) | `f4665087547e058adb73bec46ce400e1eff0ee61` | 2026-10-28 |
| v1.6.0 | `de71453d952bd8b806edaa997c72313051a49050` | 2026-08-03 |

The test adapter builds `xet-core-reference` automatically with Cargo. It covers
chunking and hashes, XORB interoperability, upload/download request behavior,
deduplication, reconstruction, and batch operations.

Run the complete suite against the default release with:

```sh
cd test/conformance
cargo build --release --locked --manifest-path versions/v1.7.1/Cargo.toml
go test ./...
```

Set `XET_CORE_VERSION` to another pinned tag to test that release instead, for
example `XET_CORE_VERSION=v1.6.0 go test ./...`; it runs
`versions/v1.6.0/target/release/xet-core-reference`. A tag that is not pinned
fails every test that needs the reference with the list of pinned tags. CI runs
one "Conformance Tests (xet-core <tag>)" job per pinned release.

Set `XET_CORE_REFERENCE_BIN` to an already-built reference executable to skip
the automatic Cargo build. It wins over `XET_CORE_VERSION`, which then only
labels that binary for `rustref.Version()`.

Cargo builds into `$CARGO_TARGET_DIR` instead of `versions/<tag>/target` when
it is set, so the tests would not run the binary just built: unset it, or point
`XET_CORE_REFERENCE_BIN` at `$CARGO_TARGET_DIR/release/xet-core-reference`
right after building the release you test.

## Adding or dropping a release

To add release `vX.Y.Z` at commit `<sha>`:

1. Create `versions/vX.Y.Z/` from copies of the newest
   `versions/<tag>/Cargo.toml` and `versions/<tag>/Cargo.lock`, set the five
   `rev =` values to `<sha>`, and update only the xet-core packages in the lock:
   `cargo update -p hf-xet -p xet-client -p xet-core-structures -p xet-data -p xet-runtime --manifest-path versions/vX.Y.Z/Cargo.toml`.
2. Add `"vX.Y.Z": "<sha>"` to `pinnedVersions` in `rustref/reference.go` and
   keep `DefaultVersion` at the newest release.
3. Add `vX.Y.Z` to the `version` matrix of the conformance job in
   `.github/workflows/test.yml` and to the table above.

To drop the oldest release, delete its `versions/<tag>/` directory, its
`pinnedVersions` entry, its workflow matrix entry and its table row. Either
change adds or removes a "Conformance Tests (xet-core <tag>)" check, so update
the required status checks to match.

## Mirror compatibility tests

`mirror/` tests the mirror against real public hubs with the official `hf`
CLI as the downstream client: huggingface.co (xet upstream, openai/gpt-oss-20b)
and modelscope.cn (plain HTTP upstream, openai-mirror/gpt-oss-20b). They skip
unless the `hf` CLI is on PATH and the upstream is reachable; huggingface.co
may need an HTTPS proxy:

```sh
https_proxy=http://127.0.0.1:1087 go test ./mirror/
```

