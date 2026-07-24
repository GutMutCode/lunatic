# Embedded v3 SLOC accounting

`sloc-frozen.mjs` is the only canonical accounting entry point. It delegates to
`sloc-frozen-core.mjs`; `sloc-frozen-self-test.mjs` is its mandatory abuse
suite. `sloc.mjs`, `sloc-final.mjs`, and the older
`protocol/sloc-manifest.schema.json` are legacy, noncanonical artifacts and
must not be used for candidate comparison or threshold decisions.

The matching manifest schema is
`../protocol/sloc-manifest-frozen.schema.json`. Run the gate with:

```text
node tools/sloc-frozen.mjs --self-test
node tools/sloc-frozen.mjs candidates/<candidate>/sloc-manifest.json
```

## Candidate-intake freeze

The accounting mechanism is frozen, but candidate intake must remain closed
until the oracle protocol repair is green and
`protocol/candidate-wire-frozen.rs` contains every candidate-facing,
runtime-neutral control/event wire type. Run
`node tools/candidate-wire-frozen-check.mjs` after any Oracle protocol edit.
The check deterministically projects the production-only contract, compares the
frozen copy byte-for-byte, checks edition-2018 rustfmt, and runs its standalone
codec/strictness tests. Its LF-only SHA-256 is pinned in
`sloc-frozen-core.mjs` and the frozen schema. This prevents typed wire
boilerplate from biasing the percentage comparison or rewarding an untyped
`serde_json::Value` shortcut.

## Enforced policy

- Every present regular file is attributed exactly once. A root `target/`,
  symlink, junction/reparse redirect, or special filesystem entry rejects the
  candidate instead of becoming an unreported exclusion.
- Every candidate contains one root Cargo package, one root `Cargo.lock`, and
  the required exact `src/protocol.rs` canonical copy. The manifest itself has
  the path-locked `accounting` role; the lock has the path-locked `lockfile`
  role and is verified with `cargo metadata --locked`.
- `generated` is not author-selected. It means either the required
  byte-for-byte canonical copy with its embedded source hash, or a core-Wasm
  binary under `guest-artifacts/` whose header, declared hash, and separately
  attributed/frozen source provenance all verify.
- Candidate runtime, dispatch, adapter, build, configuration, WAT/WIT, and
  script inputs are production. The test role is limited to Rust beneath the
  top-level `tests/` tree. Cargo target metadata prevents a bin/lib/build target
  from pointing there, and production Rust may not use `#[path]`, `include!`,
  `include_str!`, or `include_bytes!` to pull excluded/test files back in.
- All attributed Rust is checked with exactly
  `rustfmt 1.9.0-stable (59807616e1 2026-04-14)`, edition 2018. Counted text
  uses nonblank physical lines and rejects lines over 120 UTF-8 bytes; this is
  the frozen anti-minification policy for Cargo/TOML, WAT/WIT, JSON/NDJSON,
  YAML, scripts, and other production text.
- README, license, notice, and `doc/` or `docs/` material is prohibited inside
  measured candidate roots. Keep experiment documentation beside the
  candidates instead of forcing prose into production SLOC.
- Every direct normal, build, and dev dependency of the one root package is
  checked against a candidate-specific frozen policy. Build/dev dependencies,
  crate renames, optional dependencies, target-conditioned dependencies, named
  registries, candidate-internal helper packages, and unlisted crates are
  rejected. Registry dependencies must use an exact `=version`, the canonical
  crates.io source, `default-features = false`, and the exact listed features.
- The common registry allowlist is deliberately small:
  `anyhow =1.0.100 [std]`, `serde =1.0.229 [derive,std]`,
  `serde_json =1.0.151 [std]`, `sha2 =0.10.9 [std]`, and
  `tokio =1.53.1 [macros,rt-multi-thread,sync,time]`. Bracketed items are the
  complete enabled feature set; no common dependency is required unless the
  implementation uses it.
- `extism` additionally requires crates.io `extism =1.30.0` with exactly
  `[wasmtime-default-features]`; Extism's own default features remain
  disabled, so `http`, `register-http`, `register-filesystem`, and
  `ureq` remain outside the adapter. This measured-before-run amendment is
  necessary because Extism 1.30's featureless Wasmtime 43 edge omits
  `wasmtime/anyhow` and does not compile on the frozen Windows/Rust
  toolchain. `direct-wasmtime` requires crates.io `wasmtime =46.0.1` with
  exactly `[cranelift,runtime]` and default features disabled.
- `lunatic` requires exactly the pinned product path
  `lunatic-process =0.14.0` at `crates/lunatic-process`, with default
  features disabled and no features. A direct crates.io `wasmtime =46.0.1`
  declaration with no features is allowed only because
  `ProcessState::register` exposes `wasmtime::Linker`; mandatory source
  review must reject constructing an Engine, Module, Store, or Instance through
  that declaration to bypass Lunatic.
- Any further common crate, product crate, or feature is an explicit freeze
  change: document the concrete adapter need, update the policy and abuse
  tests together, and rerun the self-test. Until then it remains rejected.
- The Lunatic product allowance is compared against pinned product commit
  `f5ba0831ef757e2a134fbeafe622c0d0280a55dd` for `Cargo.toml`, `Cargo.lock`,
  `src`, and `crates`, rejects tracked or untracked drift, and emits a binding
  SHA-256 for the run manifest.
