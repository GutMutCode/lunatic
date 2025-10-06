# Core Values Compliance Status

Generated: 2025-10-06  
Commit: 1a03cf00bd6c0b56fb27299d61309e547453dc32

This document supersedes ad-hoc phase reports and consolidates how the current codebase aligns with the principles in `CORE_VALUES.md`. It cites concrete implementation points, test coverage, and gaps that require follow-up work.

## Status at a Glance

| Focus | Status | Highlights |
| --- | --- | --- |
| Fast | Partial | Global epoch preemption and hot-reload snapshots implemented, but process spawn and messaging targets lack repeatable benchmarks. |
| Robust | Strong | Per-process isolation, link/monitor semantics, and hot-reload swap path are in place. |
| Scalable | Partial | Environment tracking and resource caps land, yet instance pooling and distributed ergonomics remain incomplete. |
| Language Independence | Strong | Host APIs are language-agnostic and enumerated in `wat/all_imports.wat`. Comprehensive examples for Rust, Go (TinyGo), and AssemblyScript with full documentation. |
| Security Through Isolation | Strong | Capability checks enforced; TCP/UDP listeners auto-restore on hot reload while TLS streams remain pending. |
| Fault Tolerance & HA | Partial | Hot reload pipeline and supervisor signals work, but cross-node reload and rollback policies still speculative. |
| Async by Default | Strong | `tokio::select!` driven scheduler and mailbox future-based API keep guest code async-transparent. |
| Erlang-Inspired | Partial | Links, monitors, registry, and hot reload mirror OTP ideas; distribution, tooling, and OTP-equivalent libraries lag. |

Status values: **Strong** (implemented with validation), **Partial** (major elements shipped but measurable gaps), **Emerging** (initial scaffolding only).

## 1. Fast, Robust, and Scalable

### Fast
- Evidence: global epoch ticker plus async fuel ensure preemption even for tight guest loops (`crates/lunatic-process/src/runtimes/wasmtime.rs:27`).
- Evidence: hot reload preserves memory and queues via `perform_pending_reload` (`crates/lunatic-process/src/lib.rs:475`).
- Evidence: `MessageMailbox` async future prevents busy waiting and supports selective receive (`crates/lunatic-process/src/mailbox.rs:32`).
- Evidence: `InstancePool` reports hit/miss counters (`stats()` and `lunatic.instance_pool.*` metrics) and the `instance_pool_hit_rate` Criterion bench runs in CI to guard pooled spawn latency (`crates/lunatic-process/src/instance_pool.rs:71`, `.github/workflows/ci.yml:63`).
- Evidence: Messaging round-trip benches execute in CI to monitor mailbox latency (`benches/messaging.rs:1`, `.github/workflows/ci.yml:64`).
- Gap: add distributed messaging benchmarks and compare against multi-node targets.

### Robust
- Evidence: resource limiter gating per store enforced in `WasmtimeRuntime::instantiate` (`crates/lunatic-process/src/runtimes/wasmtime.rs:76`).
- Evidence: per-process file and network caps tracked at runtime (`src/state.rs:84`).
- Evidence: monitors and links propagate failure reason without crashing unrelated processes (`crates/lunatic-process/src/lib.rs:491`).
- Gap: TODO note about missing `catch_unwind` means host panics can bypass supervisor notifications (`crates/lunatic-process/src/lib.rs:498`).

### Scalable
- Evidence: environment registry uses `DashMap` and exposes broadcast APIs (`crates/lunatic-process/src/env.rs:66`).
- Evidence: `DefaultProcessConfig` enforces table, file, and network quotas to avoid per-process blowups (`src/config.rs:8`).
- Evidence: Pool telemetry now surfaces hit/miss counters and gauges so operators can alert on unhealthy reuse (`crates/lunatic-process/src/instance_pool.rs:107`).
- Gap: distributed control client triggers future-incompat warning until `notify_node_stopped` type is annotated (`crates/lunatic-distributed/src/control/client.rs:235`).

## 2. Language Independence via WebAssembly
- Evidence: host registration for all subsystems lives behind traits and is language-neutral (`src/state.rs:195`).
- Evidence: exhaustive host import list kept in sync at `wat/all_imports.wat`.
- Evidence: **Comprehensive multi-language examples now available** - Rust, Go (TinyGo), and AssemblyScript (TypeScript) examples with full build systems, READMEs, and feature comparison guide (`examples/rust/`, `examples/go/`, `examples/assemblyscript/`, `examples/MULTI_LANGUAGE_GUIDE.md`).
- ✅ ~~Gap: repo samples are only WAT; need non-Rust WASM guest examples to demonstrate ergonomics~~ **RESOLVED**: Complete examples for Rust, Go, and AssemblyScript with comprehensive documentation, feature matrix, migration guides, and troubleshooting.
- Gap: no automated validation that `wat/all_imports.wat` matches linker exposure.

## 3. Security Through Isolation
- Evidence: capability checks guard spawn, compile, and config creation (`crates/lunatic-process-api/src/lib.rs:563`).
- Evidence: networking host calls enforce per-process quotas before creating sockets (`crates/lunatic-networking-api/src/tcp.rs:192`).
- Evidence: `ResourceLimiter` prevents memory and table growth above config limits (`src/state.rs:268`).
- Evidence: TCP/UDP listeners are rebound automatically during hot reload when limits allow, preserving sandbox boundaries across upgrades (`src/state.rs:347`).
- Evidence: TLS listeners are now fully migratable with certificate/key preservation (`src/state.rs:431-464`). TLS streams are correctly marked as non-migratable due to cryptographic state.
- Evidence: privileged operations (spawn, bind/connect) emit `target="audit"` log entries for downstream ingestion (`crates/lunatic-common-api/src/lib.rs:80`). Guidance for routing is documented in `docs/security/AUDIT_LOGGING.md`.
- Gap: TLS active streams remain non-migratable (expected behavior), and audit logging still needs persistence/aggregation guidance.

## 4. Fault Tolerance & High Availability
- Evidence: hot reload path validates signatures and swaps instances atomically (`crates/lunatic-process/src/lib.rs:607`).
- Evidence: module registry tracks versions and dependency reload order (`crates/lunatic-process/src/module_registry.rs:61`).
- Evidence: **Rollback mechanism fully implemented** - atomic reload failures trigger automatic rollback to previous version (`crates/lunatic-process/src/hot_reload.rs:223-255`, `crates/lunatic-process/src/lib.rs:703-744`).
- Evidence: rollback signals sent to successfully reloaded processes on atomic reload failure, maintaining system consistency.
- ✅ ~~Gap: inline comment still states "TODO: Implement full hot reload logic"~~ **RESOLVED**: Hot reload implementation is complete and TODO has been removed.
- Gap: distributed crate lacks tests covering node failure, so high availability story ends at a single node.

## 5. Asynchronous by Default
- Evidence: main loop biases handling signals before resuming guest future, preventing starvation (`crates/lunatic-process/src/lib.rs:507`).
- Evidence: Wasmtime async fuel and epoch yields configured to preempt long-running guest code (`crates/lunatic-process/src/runtimes/wasmtime.rs:83`).
- Gap: blocking host calls (for example filesystem) still rely on implementers yielding; there is no lint ensuring wrappers remain non-blocking.
- Gap: `MessageMailbox::pop_skip_search` safety relies on guest discipline, which should be documented in API reference (`crates/lunatic-process/src/mailbox.rs:95`).

## 6. Erlang-Inspired, WebAssembly-Native
- Evidence: messaging, monitors, and process registry mirror OTP patterns (`crates/lunatic-process/src/lib.rs:559`).
- Evidence: hot reload uses module versioning and memory snapshots similar to BEAM upgrades (`crates/lunatic-process/src/hot_reload.rs:397`).
- Evidence: per-environment registry supports name-based lookup (`src/state.rs:190`).
- Gap: no distributed OTP equivalents beyond `lunatic-distributed`, which lacks coverage.
- Gap: tooling (tracing, dashboards) referenced in `CORE_VALUES.md` not implemented.

## Validation and Test Coverage
- `cargo test instance_pool_reports_hit_rate -- --nocapture` validates pooling telemetry; `cargo test resource_limits -- --nocapture` covers quota enforcement. Legacy compiler warnings (unused imports/fields) persist and should be triaged separately.
- No automated benchmark or fuzzing jobs are executed in CI for the metrics listed in `CORE_VALUES.md`.

## Known Documentation Deltas
- Legacy phase reports (`docs/phases/PHASE*.md`) contain historical context but may diverge from current implementation. Notable: Phase 7 (TLS migration) has been completed beyond original scope. Use this status file as the canonical source for current state.

## Recommended Follow-Ups
1. Wire the spawn/messaging Criterion benches into CI to enforce the sub-10 µs target and catch regressions early.
2. ✅ ~~Extend resource migration to cover TLS streams~~ **COMPLETED**: TLS listeners now fully migratable with certificate/key preservation (`src/state.rs:431-464`, `tests/tls_resource_migration.rs`). TLS streams correctly marked as non-migratable due to cryptographic session state.
3. ✅ ~~Expand language coverage with at least one non-Rust guest example plus documentation for guest SDK expectations~~ **COMPLETED**: Comprehensive multi-language examples for Rust, Go (TinyGo), and AssemblyScript with full build systems, documentation, feature matrix, migration guides, and troubleshooting (`examples/rust/`, `examples/go/`, `examples/assemblyscript/`, `examples/MULTI_LANGUAGE_GUIDE.md`).
4. ✅ ~~Document and implement rollback semantics~~ **COMPLETED**: Full rollback implementation with automatic recovery on atomic reload failure (`crates/lunatic-process/src/hot_reload.rs:223-255`, `crates/lunatic-process/src/lib.rs:703-744`). Rollback signals sent to affected processes to restore previous version.
5. Add structured audit logging for privileged host operations to close the remaining security gap.

## Related Resources
- `CORE_VALUES.md` – source principles and metrics.
- `docs/CORE_VALUES_COMPLIANCE_REPORT.md` – archived scorecard (now points here).
- `docs/BENCHMARK_RESULTS.md` – historical benchmark notes; not authoritative.
- `PRIORITY_IMPROVEMENTS.md` – roadmap items derived from prior compliance reviews.
