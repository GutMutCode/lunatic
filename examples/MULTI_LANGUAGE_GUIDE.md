# Multi-Language Guests

Lunatic's host ABI is language-neutral. This repository verifies one small,
low-level guest adapter in Rust, Go/TinyGo, and AssemblyScript against the same
runtime behavior. The adapters are executable compatibility fixtures, not
published SDKs and not proof that the three languages expose equivalent
high-level APIs.

## CI-verified contract

The `multilanguage_guests` CI job builds every artifact from a clean checkout,
runs the language-specific `_start` export through `lunatic run`, and then runs
the strict integration suite. A missing artifact fails that job instead of
being treated as a skipped test.

Every verified mark below links to executable evidence. The integration suite
loads the compiler output, not a hand-written WAT substitute.
The corresponding sources are the [Rust fixture](rust/guest-e2e/),
[TinyGo fixture](go/guest_e2e.go), and
[AssemblyScript fixture](assemblyscript/assembly/guest_e2e.ts).

| Runtime check | Rust | Go/TinyGo | AssemblyScript |
| --- | --- | --- | --- |
| Clean compiler build | [✅ CI build](../.github/workflows/ci.yml) | [✅ CI build](../.github/workflows/ci.yml) | [✅ CI build](../.github/workflows/ci.yml) |
| Same-module process spawn | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) |
| Tagged `41 -> 42` message round trip | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) |
| Bounded receive timeout (`9027`) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) |
| Child spawn denied with an error handle | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) | [✅ E2E](../tests/multilanguage_guest_e2e.rs) |
| Current CLI invocation (`lunatic run`) | [✅ CI](../.github/workflows/ci.yml) | [✅ CI](../.github/workflows/ci.yml) | [✅ CI](../.github/workflows/ci.yml) |

The permission check is deliberate. The root fixture receives authority to
create a configuration and spawn a process. It creates an attenuated child
configuration, whose spawn capability is denied by default. The child confirms
that a grandchild spawn returns status `1`, exposes a non-empty error resource,
and can still use its mailbox.

## Reproducible toolchains

CI pins the following compatibility matrix:

| Language | Compiler/runtime used by CI | Wasm target | Important boundary |
| --- | --- | --- | --- |
| Rust | Rust `1.94.0` | `wasm32-unknown-unknown` | `no_std` core module with direct `lunatic::*` imports; no WASI dependency |
| Go | Go `1.25.5`, TinyGo `0.41.1` with bundled Binaryen `116` | `wasip1` command | TinyGo child processes must enter through `_start`; arbitrary reactor exports are not certified |
| AssemblyScript | Node `24.4.1`, npm lockfile, AssemblyScript `0.27.37` | core Wasm | Stub runtime and explicit Wasm traps; no JavaScript `env` shims at runtime |
| Lunatic | workspace lockfile | Wasmtime `46.0.1` | Core Wasm plus WASI Preview 1 host imports |

WASI Preview 2/components, guest threads, TinyGo goroutine equivalence, and
browser/JavaScript host bindings are outside this matrix. Rust and
AssemblyScript intentionally use core Wasm because their fixtures need only
the Lunatic ABI. TinyGo uses WASI Preview 1 so each spawned `_start` gets a
correctly initialized Go runtime and role-selecting `argv`.

## Build and run locally

Install the pinned toolchains, then run the same build targets as CI:

```bash
rustup toolchain install 1.94.0 --target wasm32-unknown-unknown

cargo +1.94.0 build --locked --release \
  --manifest-path examples/rust/guest-e2e/Cargo.toml \
  --target wasm32-unknown-unknown

make -C examples/go guest-e2e

npm ci --prefix examples/assemblyscript
npm run --prefix examples/assemblyscript build:guest-e2e
```

Run the compiler outputs through the public CLI:

```bash
cargo +1.94.0 build --locked --bin lunatic

./target/debug/lunatic run \
  examples/rust/guest-e2e/target/wasm32-unknown-unknown/release/lunatic_rust_guest_e2e.wasm
./target/debug/lunatic run examples/go/build/guest_e2e.wasm
./target/debug/lunatic run examples/assemblyscript/build/guest_e2e.wasm
```

Then run the host-side oracle in strict mode:

```bash
LUNATIC_MULTILANGUAGE_GUESTS_REQUIRED=1 \
  cargo +1.94.0 test --test multilanguage_guest_e2e
```

Without the environment variable, normal workspace tests skip an artifact that
has not been built locally. CI always enables strict mode.

## OTP scope

The language fixtures above validate actor primitives only. They do not claim
GenServer, Supervisor, GenStatem, or GenEvent support.

- [`rust/src/gen_server_example.rs`](rust/src/gen_server_example.rs) and
  [`rust/src/supervisor_example.rs`](rust/src/supervisor_example.rs) use native
  host-side adapters from `lunatic-otp-patterns`.
- [`go/gen_server_example.go`](go/gen_server_example.go),
  [`go/supervisor_example.go`](go/supervisor_example.go),
  [`assemblyscript/assembly/gen_server_example.ts`](assemblyscript/assembly/gen_server_example.ts),
  and
  [`assemblyscript/assembly/supervisor_example.ts`](assemblyscript/assembly/supervisor_example.ts)
  are educational in-module simulations. They are excluded from the runtime
  certification build.
- [`tests/otp_guest_wasm.rs`](../tests/otp_guest_wasm.rs) is the executable
  language-neutral OTP1 wire oracle. It verifies cast, correlated call/reply,
  timeout, and acknowledged stop using WAT, but it is not a packaged language
  SDK.

Until a language has an adapter test for the OTP1 contract, its OTP support
must remain classified as unsupported rather than inferred from a local object,
goroutine, channel, or native host process.

## Status codes used by the fixtures

The adapters intentionally stay close to `wat/all_imports.wat`:

- `0`: operation succeeded;
- `1`: operation returned an error resource (used for permission denial);
- `9027`: mailbox receive timed out.

The child function parameter is encoded using Lunatic's process-spawn value
format: one WebAssembly type byte (`0x7e` for `i64`) followed by a 16-byte
little-endian value slot. These details are fixture-level ABI coverage, not a
promise that applications should hand-code bindings indefinitely.

## Adding another language

A new language can be added to the verified matrix only when a clean-checkout
CI step:

1. pins its compiler and target;
2. builds a source artifact without committed generated Wasm;
3. runs the artifact through the current `lunatic run <path> [WASM_ARGS]...`
   interface;
4. adds a strict case to `tests/multilanguage_guest_e2e.rs`; and
5. documents unsupported WASI, runtime, concurrency, and SDK behavior.

Do not add performance ratings or nanosecond claims to this matrix. Performance
claims belong in a commit-bound benchmark report that names the workload,
hardware, sample distribution, and whether the path is a microbenchmark or a
production E2E.
