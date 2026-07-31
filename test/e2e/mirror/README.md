# Mirror end-to-end example

Run the complete mirror example with:

```sh
go test -v ./test/e2e/mirror
```

The test starts three in-process HTTP services: a fixed Hugging Face-compatible
route backed by a real Xet CAS server, the Xet mirror, and the clients. An
ordinary `net/http` client and an Xet client download the same file concurrently.

The simulated Xet-capable upstream limits HTTP response bandwidth. The test
asserts that a cold downstream sees HTTP only, waits for the mirror conversion,
and then verifies that a warm Xet download uses the mirror's local Xet objects.

When the official `hf` executable is available on `PATH`, the test additionally
runs these two commands concurrently against the warm mirror:

```sh
HF_HUB_DISABLE_XET=1 HF_ENDPOINT="$MIRROR" hf download acme/network-fixture model.bin --local-dir "$HTTP_OUTPUT" --force-download
HF_HUB_DISABLE_XET=0 HF_ENDPOINT="$MIRROR" hf download acme/network-fixture model.bin --local-dir "$XET_OUTPUT" --force-download
```

Each command receives a different `HF_HOME`, `HF_HUB_CACHE`, and `--local-dir`.
Consequently the HTTP-mode and Xet-mode clients cannot reuse one another's HF
cache, and the test compares both distinct `model.bin` files with the fixture.
Set `HF_E2E_REQUIRE_CLI=1` in an environment that installs
`huggingface_hub[hf_xet]` to make absence of the CLI a test failure.
