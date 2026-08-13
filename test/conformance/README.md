# xet-core conformance tests

This module tests the Go implementation directly against the Rust reference
implementation from [`huggingface/xet-core`](https://github.com/huggingface/xet-core).
The Rust dependencies are pinned to commit
`de71453d952bd8b806edaa997c72313051a49050`

The test adapter builds `xet-core-reference` automatically with Cargo. It covers
chunking and hashes, XORB interoperability, upload/download request behavior,
deduplication, reconstruction, and batch operations.

Run the complete suite with:

```sh
cd test/conformance
cargo build --release --locked
go test ./...
```

Set `XET_CORE_REFERENCE_BIN` to an already-built reference executable to skip
the automatic Cargo build.

## Mirror compatibility tests

`mirror/` tests the mirror against real public hubs with the official `hf`
CLI as the downstream client: huggingface.co (xet upstream, openai/gpt-oss-20b)
and modelscope.cn (plain HTTP upstream, openai-mirror/gpt-oss-20b). They skip
unless the `hf` CLI is on PATH and the upstream is reachable; huggingface.co
may need an HTTPS proxy:

```sh
https_proxy=http://127.0.0.1:1087 go test ./mirror/
```

