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
- Upload and download client workflows
- CAS server implementation for local or self-hosted usage
- Mirror mode: full-cache middle layer bridging xet and plain hubs
- Hugging Face token/LFS based integration helpers
- Conformance and unit tests for key protocol paths

## Internal Management Endpoints

Started with `-internal`, `xetd` serves unauthenticated management endpoints alongside the CAS API (use only on trusted networks):

- `GET /internal/files` — list stored files grouped by content SHA-256.
- `DELETE /internal/files/xet/{hash}` — unlink one file-index entry; its objects are reclaimed by the next sweep.
- `POST /internal/gc/sweep?dry_run=&grace=` — remove shards and xorbs no file-index entry keeps alive. `dry_run=true` only reports what would be removed. `grace` protects recently written objects: omitted defaults to 1h, `0` disables the window, malformed or negative values return `400`.
- `POST /internal/gc/compact?dry_run=` — aggregate live chunks into dense xorbs; superseded objects are reclaimed by the next sweep. Runs single-flight with sweep (`409` while one is running), and a pass should complete within the sweep grace window.

## Compatibility Notes

- This project aims to follow the draft spec where possible.
- When draft and xet-core behavior diverge, practical interop with xet-core may take priority.
- Test and conformance coverage is evolving with protocol and upstream changes.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/xet/blob/master/LICENSE) for the full license text.
