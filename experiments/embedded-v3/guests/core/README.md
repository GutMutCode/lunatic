# Embedded v3 core guests

This directory owns the candidate-neutral Wasm used by the embedded-runtime
comparison.  It contains no filesystem, network, credential, or candidate-host
code.

Four binaries are rendered from one WAT template:

- `tenant-a.wasm` and `tenant-b.wasm` are the two valid builds.
- `tenant-bad-a.wasm` and `tenant-bad-b.wasm` deliberately fail activation only
  when the host-provided tenant ID is `7`.

All candidates receive the exact same bytes for a given build.  Direct Wasmtime
and Extism invoke the zero-parameter exports once per command.  Lazy runtimes
first invoke the business-side-effect-free `activation_check` export to force
module activation before a build is allowed to serve commands.  Lunatic may run
the long-lived `run(i64)` export; its `next_command` import selects the same
internal operations.  Imports are scalar-only and live in the `comparison`
namespace, so no candidate receives privileged access to another candidate's
runtime internals.

Build and verify:

```text
cargo run --manifest-path experiments/embedded-v3/guests/core/Cargo.toml --locked --bin build-core-guests
cargo test --manifest-path experiments/embedded-v3/guests/core/Cargo.toml --locked
cargo clippy --manifest-path experiments/embedded-v3/guests/core/Cargo.toml --all-targets -- -D warnings
```

The builder writes deterministic binaries and `artifacts/manifest.json`.  A
second build must reproduce every SHA-256 digest byte-for-byte.
