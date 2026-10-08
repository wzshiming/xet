# Agent Guide for xet

Go implementation of the Xet protocol: content-defined chunking, chunk-level deduplication, CAS
upload/download and Hugging Face Hub integration. The protocol is defined by the Rust reference
https://github.com/huggingface/xet-core, not here: wire formats, hashes and HTTP behaviour follow
xet-core bit for bit. Read the xet-core source relevant to the task instead of relying on memory.

## Protocol parity with xet-core

Sources of truth, in order; the first wins when they disagree:

1. xet-core at the revision pinned in `test/conformance/Cargo.toml` (`rev =`). Read files at that
   revision, never `main`: `https://raw.githubusercontent.com/huggingface/xet-core/<rev>/<path>`.
2. The protocol docs at https://huggingface.co/docs/xet and, in xet-core, `openapi/cas.openapi.yaml`
   and `api_changes/`.
3. The IETF draft https://datatracker.ietf.org/doc/draft-denis-xet/05/, for naming and intent only.

Where to read, by area (xet-core paths are relative to its repository root):

| Go | xet-core |
| --- | --- |
| `hash.go`, `merkle.go`, `gearhash.go`, `constants.go` | `xet_core_structures/src/merklehash/`, `xet_data/src/deduplication/chunking.rs` |
| `xorb/` | `xet_core_structures/src/xorb_object/` |
| `shard/` | `xet_core_structures/src/metadata_shard/` |
| CAS API: `client/`, `server/`, `download/types.go`, `upload/types.go` | `xet_client/src/cas_client/`, `xet_client/src/cas_types/`, `openapi/cas.openapi.yaml` |
| `upload/` | `xet_data/src/processing/`, `xet_data/src/deduplication/` |
| `download/` | `xet_data/src/file_reconstruction/`, `xet_client/src/chunk_cache/` |
| `client/hf/`, `lfs/` | `xet_client/src/hub_client/`, recorded hub traffic in `client/hf/hftest/testdata/` |

Rules:

- Never add a wire-visible element xet-core does not implement: route, path segment, query
  parameter, header, status-code meaning, JSON field, NDJSON frame, binary flag, hash or chunking
  constant. Behaviour an xet-core client cannot request is a server or library option.
- Protocol-touching changes cite the xet-core item (`<path>::<item>` at the pinned revision) in a
  one-line comment next to the Go symbol and in the PR description.
- When Go and xet-core disagree, keep xet-core's behaviour or open an issue; never fix the protocol
  locally.
- Bumping the pinned revision: update every `rev =` in `test/conformance/Cargo.toml` and the line in
  `test/conformance/README.md`, run `cargo update` there, read the `api_changes/` entries between
  the two revisions, run the conformance suite, list behaviour changes in the PR.

## Verification

CI is `.github/workflows/test.yml`; run what it runs for the packages you touched. Changes under the
protocol packages (root, `xorb/`, `shard/`, `upload/`, `download/`, `client/`, `server/`) also
require the conformance suite, which drives the Go code against the pinned xet-core:

```sh
cd test/conformance && cargo build --release --locked && go test ./...
```

`XET_CORE_REFERENCE_BIN=<path>` reuses an already built `xet-core-reference`; `hub/` and `mirror/`
skip without credentials, the `hf` CLI or network. Report which checks ran, failed or were skipped.

## Conventions

- Follow nearby code: functional options (`With*`), errors wrapped with `%w`, stdlib first.
- Tests use `httptest` and `t.TempDir()`; any `client.Client` in a test gets
  `client.WithCache(client.NewCache(t.TempDir(), 0, 0))`.
- Comments: one short line for what the code cannot show; keep xet-core citations current.
