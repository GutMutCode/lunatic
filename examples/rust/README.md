# Rust Examples

This directory contains two deliberately separate kinds of Rust code:

- [`guest-e2e/`](guest-e2e/) is the CI-certified, dependency-free Lunatic
  guest fixture.
- [`src/`](src/) contains legacy standalone and native host-side examples.
  The GenServer and Supervisor examples use native `lunatic-process` adapters;
  they are not Rust guest-Wasm OTP implementations.

## Verified guest fixture

The fixture is a separate `no_std` crate targeting
`wasm32-unknown-unknown`. It calls the low-level `lunatic::*` imports directly
and exports `_start()`, `parent(i64 observer_id)`, and `child(i64 parent_id)`.

It verifies:

- creation of an attenuated child configuration;
- same-module process spawn;
- a tagged payload round trip from `41` to `42`;
- bounded receive timeout status `9027`; and
- child spawn denial, a non-empty error resource, and explicit error-resource
  release.

CI pins Rust `1.94.0`. Build and run the same artifact locally from the
repository root:

```bash
rustup toolchain install 1.94.0 --target wasm32-unknown-unknown
cargo +1.94.0 build --locked --bin lunatic
make -C examples/rust guest-e2e-test
```

The artifact is written to:

```text
examples/rust/guest-e2e/target/wasm32-unknown-unknown/release/lunatic_rust_guest_e2e.wasm
```

The CLI calls `_start`, which runs the self-test without an observer. The
shared integration oracle calls `parent` and requires completion tag `4201`:

```bash
LUNATIC_MULTILANGUAGE_GUESTS_REQUIRED=1 \
  cargo +1.94.0 test --test multilanguage_guest_e2e \
  rust_guest_process_message_timeout_and_permission_e2e
```

## Legacy source scope

The top-level [`Cargo.toml`](Cargo.toml) is a native package used to compile and
exercise the older Rust examples. Run its supported check with:

```bash
cargo check --locked --manifest-path examples/rust/Cargo.toml
```

`gen_server_example.rs` and `supervisor_example.rs` are host-native adapter
examples. `counter.rs` and `echo_server.rs` show ordinary exports and local
state, but do not call Lunatic's process or message imports. None of these four
files is counted by the multi-language guest certification matrix.

This fixture covers only core Wasm and the small ABI slice named above. It is
not a stable Rust guest SDK and does not certify WASI, networking, distributed
actors, hot reload, guest threads, or OTP patterns. See
[`../MULTI_LANGUAGE_GUIDE.md`](../MULTI_LANGUAGE_GUIDE.md) for the exact shared
support boundary.
