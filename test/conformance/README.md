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

