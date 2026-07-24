# Extism 1.30 feature-free build blocker and pre-measurement amendment

Date: 2026-07-24  
Repository: `f5ba0831ef757e2a134fbeafe622c0d0280a55dd`  
Host: `x86_64-pc-windows-msvc`  
Rust: `rustc 1.95.0 (59807616e 2026-04-14)`  
Cargo: `1.95.0 (f2d3ce0bd 2026-03-21)`

The original frozen Extism candidate dependency was:

```toml
extism = { version = "=1.30.0", default-features = false }
```

The restored featureless inputs after the controlled blocker experiments, before the owner
amendment, were:

- `Cargo.toml`: `65af22cc73c487f9a431c8a4be9cd7a97eecc2d85b40af76397ea611a7944b4b`
- `Cargo.lock`: `0fdea5993f6dbb707d77a04cf7bb7e0507df095cab1ee358d513c8e48d15c0c4`

## Frozen configuration

Command:

```text
cargo check --locked
```

Result: Extism itself fails before candidate code is checked. The compiler reports 41 errors in
`extism-1.30.0`, including missing conversions from `wasmtime::Error`, missing
`ToWasmtimeResult::to_wasmtime_result`, and missing `wasmtime::Error::from_anyhow`.

Cause: Extism 1.30 pins Wasmtime 43 with `default-features = false` and a long explicit feature
list, but that list omits Wasmtime's `anyhow` feature. Extism's implementation uses APIs that
require that feature. Extism exposes no feature that enables only `wasmtime/anyhow`.

The resolved featureless tree has 288 unique package/version lines, no `ureq`, and no
`wasmtime/anyhow` edge.

## Smallest compiling Extism feature

Command:

```text
cargo check --locked --features extism/wasmtime-default-features
```

Result: Extism and all dependencies compile. Compilation reaches candidate code and stops only on
the candidate's then-known missing Rust 2018 `TryFrom` import. This establishes that the dependency
blocker is removed.

`wasmtime-default-features` is the smallest feature exposed by Extism 1.30 that supplies the
missing `wasmtime/anyhow` edge. It enables Wasmtime's entire default feature set, not only
`anyhow`. The resolved tree has 317 unique package/version lines, no `ureq`, and keeps Extism's
`http`, `register-http`, and `register-filesystem` features disabled.

Consequently the genuine `extism:host/env::http_request` import still resolves, but invocation
uses Extism's `cfg(not(feature = "http"))` path and returns the explicit
`http_request is not enabled ... is not allowed` denial before an HTTP effect.

## Extism default features

Commands:

```text
cargo check --locked --features extism/default
cargo test --locked --test extism_public_api
```

Result: Extism compiles and the existing exact core-artifact public-API suite passes all three
tests, including genuine trap handling and `Plugin::cancel_handle` interruption after the guest's
first-action marker. The candidate check again reaches only the candidate's then-known `TryFrom`
import error.

The default set is `http`, `register-http`, `register-filesystem`, and
`wasmtime-default-features`. It resolves 338 unique package/version lines and adds `ureq`. It also
enables URL/file manifest registration and a functional HTTP client. An empty `allowed_hosts`
policy still denies the frozen HTTP canary, but this is a materially broader dependency and input
surface than the intended featureless candidate.

## Smallest technical feature-unification experiment

A controlled, temporary direct dependency was tested and then removed:

```toml
wasmtime = { version = "=43.0.2", default-features = false, features = ["anyhow"] }
```

With featureless Extism otherwise unchanged, `cargo check --offline` compiles Extism and reaches
the same candidate-only `TryFrom` error. This proves that `wasmtime/anyhow` is the exact technical
missing edge and does not require HTTP support.

This is not an eligible candidate configuration: the frozen dependency policy forbids a direct
Wasmtime dependency for the Extism candidate, and it would couple the adapter to Extism's private
Wasmtime major version. The manifest and lockfile were restored immediately afterward.

## Decision consequence

Under the original frozen policy, Extism could not produce an executable candidate, so no runtime
evidence or eligibility verdict may be synthesized. A freeze amendment must explicitly choose
between the broad but upstream-exposed `extism/wasmtime-default-features` feature and a narrowly
feature-unifying direct Wasmtime dependency. Leaving the policy unchanged makes the comparison
inconclusive rather than an Extism runtime failure.

## Pre-measurement freeze amendment

Before any candidate measurement, the comparison owner amended the Extism dependency policy to
the smallest upstream-exposed compiling configuration:

```toml
extism = { version = "=1.30.0", default-features = false, features = ["wasmtime-default-features"] }
```

The choice follows the evidence above: it fixes the missing Wasmtime `anyhow` edge without
enabling Extism HTTP execution, URL registration, filesystem registration, or `ureq`. Direct
Wasmtime feature unification and `extism/default` remain forbidden. Because the amendment precedes
all measured runs and applies to the dependency gate and candidate together, it is a declared
experiment correction rather than a post-result relaxation.

The final amended candidate inputs are:

- `Cargo.toml`: `55019e249cfb5c74648223fb25859744ec39e16fb5c0a73dc8ad6a09da2fbd5d`
- `Cargo.lock`: `ccb0a67aa86745b7cef014fce6fef898079a5ab5465bd4c8985a276be7a9f08b`
