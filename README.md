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
- Server storage backends: local filesystem or S3-compatible object stores (MinIO etc.), with optional presigned xorb download URLs
- Mirror mode: full-cache middle layer bridging xet and plain hubs
- Hugging Face token/LFS based integration helpers
- Conformance and unit tests for key protocol paths

## Compatibility Notes

- This project aims to follow the draft spec where possible.
- When draft and xet-core behavior diverge, practical interop with xet-core may take priority.
- Test and conformance coverage is evolving with protocol and upstream changes.
- Chunk boundaries, and therefore xet file hashes, follow the current xet-core chunker, which never cuts a chunk shorter than the 8 KiB minimum ([xet-core#487](https://github.com/huggingface/xet-core/pull/487), first shipped in hf-xet 1.1.10, September 2025). Earlier clients could occasionally cut a shorter chunk, so a file uploaded with hf-xet 1.1.9 or older — which includes many uploads made before 2026 — may be recorded upstream under a xet hash that differs from the one this implementation computes for the same bytes. The SHA-256 (LFS OID) is unaffected.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/xet/blob/master/LICENSE) for the full license text.
