# XET Protocol (Go Implementation)

Go implementation of the XET content-addressable storage protocol for large-file transfer with chunk-level deduplication.

This project tracks:
- [huggingface/xet-core](https://github.com/huggingface/xet-core) (reference behavior in production)
- [XET Protocol Draft](https://datatracker.ietf.org/doc/draft-denis-xet/03/) (public protocol baseline)

The Rust implementation in xet-core is currently ahead of the draft in several areas, so this repository prioritizes compatibility with real xet-core behavior where needed.

## Current Scope

Implemented in this repository:
- Core hash / gearhash / merkle primitives
- Shard and xorb encode/decode paths
- Upload and download client workflows
- CAS server implementation for local or self-hosted usage
- Hugging Face token/LFS based integration helpers
- Conformance and unit tests for key protocol paths

## Compatibility Notes

- This project aims to follow the draft spec where possible.
- When draft and xet-core behavior diverge, practical interop with xet-core may take priority.
- Test and conformance coverage is evolving with protocol and upstream changes.

## Hugging Face streaming mirror

`xetd` can proxy Hugging Face while asynchronously converting Xet files into
its local CAS:

```sh
xetd -addr :8080 -storage /var/lib/xet \
  -base-url https://hf.example.com \
  -mirror-upstream https://huggingface.co -hf-token "$HF_TOKEN"
```

When a file is cold, upstream Xet `Link` and hash headers are removed and the
mirror provides HTTP service only, even if the upstream supports Xet. The
upstream half of the fill prefers the Xet client when reconstruction metadata
is available and falls back to the ordinary HTTP client otherwise. In both
cases the bytes are streamed immediately to the cold downstream as HTTP while
being captured and converted into the mirror's own Xet data.
Only after the full file and its local shard/xorbs have
been atomically committed does the mirror return its own reconstruction and auth
links. The completed mapping is persisted below `<storage>/mirror/index.json`.
The HTTP metadata needed by Hugging Face clients is persisted alongside it in
`<storage>/mirror/metadata.json`, so completed resolve HEAD requests no longer
depend on the upstream. After promotion, the mirror can therefore serve HEAD,
ordinary/range HTTP GET, and Xet reconstruction entirely from local storage.
Incomplete bodies are stored below `<storage>/mirror/files/` using the SHA-256
of the canonical request path as the filename. An interrupted fill keeps that
stable file, allowing the next fill to continue instead of starting with a new
random temporary file; it is removed only after local Xet conversion succeeds.
Deleting a downstream client's partial file does not delete this server-side
progress. A new full HTTP request first replays the retained prefix and sends
`Range: bytes=<retained-size>-` upstream, so only the missing suffix is fetched;
these responses report `X-Cache-Status: RESUME`. If the SHA-256 file later disappears, the fill has been promoted:
the durable copy is then the shard/xorbs under the storage root, and subsequent
HTTP downloads are reconstructed from those objects rather than the raw file.
Xet clients and ordinary HTTP clients share that same local shard/xorb cache;
normal and range HTTP responses are reconstructed directly from Xet objects,
so the mirror does not retain a second raw-file copy.
Upstreams without Xet support (such as ModelScope) are also supported: their
HTTP body is streamed to the first client while being captured, but the mirror
does not advertise Xet links until that body has reached EOF and its local Xet
objects have been committed successfully.

| Upstream capability | Cold HTTP downstream | Cold Xet downstream | Warm HTTP/Xet downstreams |
| --- | --- | --- | --- |
| Xet | Proxied immediately | Not advertised | Both use the shared mirror Xet cache |
| HTTP only | Streamed and captured | Not advertised | Both use the shared mirror Xet cache |

Put TLS in front of `xetd` and forward `Host`/`X-Forwarded-Proto`; `-token` can
protect the local CAS and is returned by the mirror's Xet auth endpoint.

## License

Licensed under the MIT License. See [LICENSE](https://github.com/wzshiming/xet/blob/master/LICENSE) for the full license text.
