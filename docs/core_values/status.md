# Core Values Compliance Status

Reviewed: 2026-07-20
Reviewed baseline: `bb6d9b51ef65780f2baf29fa508b5db0e12e5d68`

This document supersedes ad-hoc phase reports and consolidates how the current codebase aligns with the principles in `CORE_VALUES.md`. It cites concrete implementation points, test coverage, and gaps that require follow-up work.

An item is complete only when the production path is connected and an executable test exercises that path. Data structures, callback logic, serialization tests, loopback transport tests, and manually orchestrated handler tests are reported as partial evidence rather than end-to-end completion.

## Status at a Glance

| Focus | Status | Highlights |
| --- | --- | --- |
| Fast | Partial | Global epoch preemption and hot-reload snapshots are implemented; distributed serialization and a loopback QUIC transport benchmark exist, but the benchmark does not traverse Lunatic node message routing. |
| Robust | Strong | Per-process isolation, link/monitor semantics, and hot-reload swap path are in place. |
| Scalable | Partial | Environment tracking and resource caps land, yet instance pooling and distributed ergonomics remain incomplete. |
| Language Independence | Strong | Host APIs are language-agnostic and enumerated in `wat/all_imports.wat`. Comprehensive examples for Rust, Go (TinyGo), and AssemblyScript with full documentation. |
| Security Through Isolation | Strong | Capability checks and listener restoration are implemented; TLS stream snapshots preserve reconnection metadata, but the runtime does not reconnect active TLS streams. |
| Fault Tolerance & HA | Partial | Hot reload plus links/monitors work; OTP process supervision and cross-node registry coordination are not connected to live runtime paths. |
| Async by Default | Strong | `tokio::select!` driven scheduler and mailbox future-based API keep guest code async-transparent. |
| Erlang-Inspired | Partial | Links, monitors, local registry structures, and OTP callback traits mirror Erlang concepts; live OTP process integration and coordinated global registration remain open. |

Status values: **Strong** (implemented with validation), **Partial** (major elements shipped but measurable gaps), **Emerging** (initial scaffolding only).

## 1. Fast, Robust, and Scalable

### Fast
- Evidence: global epoch ticker plus async fuel ensure preemption even for tight guest loops (`crates/lunatic-process/src/runtimes/wasmtime.rs:27`).
- Evidence: hot reload preserves memory and queues via `perform_pending_reload` (`crates/lunatic-process/src/lib.rs:475`).
- Evidence: `MessageMailbox` async future prevents busy waiting and supports selective receive (`crates/lunatic-process/src/mailbox.rs:32`).
- Evidence: `InstancePool` reports hit/miss counters (`stats()` and `lunatic.instance_pool.*` metrics) and the `instance_pool_hit_rate` Criterion bench runs in CI to guard pooled spawn latency (`crates/lunatic-process/src/instance_pool.rs:71`, `.github/workflows/ci.yml:63`).
- Evidence: Messaging round-trip benches execute in CI to monitor mailbox latency (`benches/messaging.rs:1`, `.github/workflows/ci.yml:64`).
- Evidence: Distributed encode/decode costs are tracked via the new Criterion suite (`benches/distributed_messaging.rs:1`, `scripts/check_bench_thresholds.py:45`).
- Evidence: Control-plane node lookup latency is captured to baseline cross-node registration calls (`benches/distributed_latency.rs:1`, `scripts/check_bench_thresholds.py:52`).
- Evidence: `distributed_quic_round_trip` opens a real mTLS QUIC connection over `127.0.0.1`, creates bidirectional streams, and measures a 512-byte echo (`benches/distributed_messaging.rs:19-184`). CI builds/runs the benchmark and applies a latency ceiling (`.github/workflows/ci.yml:66`, `scripts/check_bench_thresholds.py:45-60`).
- Gap: the echo server is benchmark-local and does not exercise `distributed::Client`, message chunking, process delivery, registry lookup, control-plane discovery, multiple nodes, or network partitions. A full Lunatic distributed-message round trip remains unverified.

### Robust
- Evidence: resource limiter gating per store enforced in `WasmtimeRuntime::instantiate` (`crates/lunatic-process/src/runtimes/wasmtime.rs:76`).
- Evidence: per-process file and network caps tracked at runtime (`src/state.rs:84`).
- Evidence: monitors and links propagate failure reason without crashing unrelated processes (`crates/lunatic-process/src/lib.rs:491`).
- ✅ ~~Gap: TODO note about missing `catch_unwind` means host panics can bypass supervisor notifications (`crates/lunatic-process/src/lib.rs:498`)~~ **RESOLVED**: process runner wraps native futures in `AssertUnwindSafe(...).catch_unwind()` and reports panics before unwinding (`crates/lunatic-process/src/lib.rs:517-754`).

### Scalable
- Evidence: environment registry uses `DashMap` and exposes broadcast APIs (`crates/lunatic-process/src/env.rs:66`).
- Evidence: `DefaultProcessConfig` enforces table, file, and network quotas to avoid per-process blowups (`src/config.rs:8`).
- Evidence: Pool telemetry now surfaces hit/miss counters and gauges so operators can alert on unhealthy reuse (`crates/lunatic-process/src/instance_pool.rs:107`).
- Evidence: Control client HTTP wrappers propagate structured errors without leaking debug output (`crates/lunatic-distributed/src/control/client.rs:209`).
- ✅ ~~Gap: distributed scheduler still lacks automated stress runs to validate cluster-wide quotas across nodes~~ **RESOLVED**: Distributed stress test added (`crates/lunatic-distributed/tests/node_failure.rs::test_distributed_stress_message_throughput`) validates cross-node message throughput with 1000+ messages across 3 nodes, 10 concurrent senders, and >70% success rate requirement. Comprehensive documentation in `docs/testing/DISTRIBUTED_STRESS_TESTING.md`.

## 2. Language Independence via WebAssembly
- Evidence: host registration for all subsystems lives behind traits and is language-neutral (`src/state.rs:195`).
- Evidence: exhaustive host import list kept in sync at `wat/all_imports.wat`.
- Evidence: **Comprehensive multi-language examples now available** - Rust, Go (TinyGo), and AssemblyScript (TypeScript) examples with full build systems, READMEs, and feature comparison guide (`examples/rust/`, `examples/go/`, `examples/assemblyscript/`, `examples/MULTI_LANGUAGE_GUIDE.md`).
- ✅ ~~Gap: repo samples are only WAT; need non-Rust WASM guest examples to demonstrate ergonomics~~ **RESOLVED**: Complete examples for Rust, Go, and AssemblyScript with comprehensive documentation, feature matrix, migration guides, and troubleshooting.
- ✅ ~~Gap: no automated validation that `wat/all_imports.wat` matches linker exposure~~ **RESOLVED**: `tests/imports_match.rs` validates host registrations against the curated `wat/all_imports.wat` list each run.

## 3. Security Through Isolation
- Evidence: capability checks guard spawn, compile, and config creation (`crates/lunatic-process-api/src/lib.rs:563`).
- Evidence: networking host calls enforce per-process quotas before creating sockets (`crates/lunatic-networking-api/src/tcp.rs:192`).
- Evidence: `ResourceLimiter` prevents memory and table growth above config limits (`src/state.rs:268`).
- Evidence: TCP/UDP listeners are rebound automatically during hot reload when limits allow, preserving sandbox boundaries across upgrades (`src/state.rs:347`).
- Evidence: TLS listeners remain migratable with certificate/key preservation (`src/state.rs:455-488`).
- Evidence: TLS client stream snapshots capture server name, port, custom certificates, and timeouts (`src/state.rs:319-347`, `crates/lunatic-networking-api/src/tls_tcp.rs:425-438`).
- Evidence: TLS server stream snapshots record that the connection must close and be re-established by the client (`src/state.rs:338-346`, `crates/lunatic-process/src/resource_migration.rs:25-29`).
- Evidence: Comprehensive TLS migration documentation covers security rationale and operational behavior (`docs/tls/TLS_STREAM_MIGRATION.md`).
- Evidence: privileged operations (spawn, bind/connect) emit `target="audit"` log entries for downstream ingestion (`crates/lunatic-common-api/src/lib.rs:113`). Implementation documented in `docs/security/AUDIT_LOGGING.md`.
- Evidence: **Comprehensive audit logging persistence guide** - Production-ready documentation covering OpenTelemetry, syslog, and container logging architectures with storage recommendations, compliance checklists, and alerting strategies (`docs/security/AUDIT_LOGGING_PERSISTENCE.md`).
- Gap: `restore_resource_snapshot` only logs the saved client endpoint and explicitly reports that automatic reconnection is not implemented (`src/state.rs:490-510`). `tests/tls_stream_reconnection.rs` validates snapshot data and serialization, not a live TLS reconnect after hot reload.

## 4. Fault Tolerance & High Availability
- Evidence: hot reload path validates signatures and swaps instances atomically (`crates/lunatic-process/src/lib.rs:607`).
- Evidence: module registry tracks versions and dependency reload order (`crates/lunatic-process/src/module_registry.rs:61`).
- Evidence: **Rollback mechanism fully implemented** - atomic reload failures trigger automatic rollback to previous version (`crates/lunatic-process/src/hot_reload.rs:223-255`, `crates/lunatic-process/src/lib.rs:703-744`).
- Evidence: rollback signals sent to successfully reloaded processes on atomic reload failure, maintaining system consistency.
- Evidence: **Comprehensive distributed testing now in place** - Node failure scenarios, cross-node hot reload coordination, network partition handling, and rollback testing (`crates/lunatic-distributed/tests/node_failure.rs`, `crates/lunatic-distributed/tests/cross_node_hot_reload.rs`).
- ✅ ~~Gap: inline comment still states "TODO: Implement full hot reload logic"~~ **RESOLVED**: Hot reload implementation is complete and TODO has been removed.
- ✅ ~~Gap: distributed crate lacks tests covering node failure, so high availability story ends at a single node~~ **RESOLVED**: Comprehensive test suite covering node crashes, network partitions, coordinated hot reload, atomic reload failures, and rollback scenarios across multi-node clusters.

## 5. Asynchronous by Default
- Evidence: main loop biases handling signals before resuming guest future, preventing starvation (`crates/lunatic-process/src/lib.rs:507`).
- Evidence: Wasmtime async fuel and epoch yields configured to preempt long-running guest code (`crates/lunatic-process/src/runtimes/wasmtime.rs:83`).
- Gap: blocking host calls (for example filesystem) still rely on implementers yielding; there is no lint ensuring wrappers remain non-blocking.
- Gap: `MessageMailbox::pop_skip_search` safety relies on guest discipline, which should be documented in API reference (`crates/lunatic-process/src/mailbox.rs:95`).

## 6. Erlang-Inspired, WebAssembly-Native
- Evidence: messaging, monitors, and process registry mirror OTP patterns (`crates/lunatic-process/src/lib.rs:559`).
- Evidence: hot reload uses module versioning and memory snapshots similar to BEAM upgrades (`crates/lunatic-process/src/hot_reload.rs:397`).
- Evidence: per-environment registry supports name-based lookup (`src/state.rs:190`).
- Evidence: `GlobalProcessId` represents node/environment/process coordinates and supports compact encoding (`crates/lunatic-distributed/src/distributed/global_process_id.rs`).
- Evidence: `DistributedRegistry` provides concurrent in-memory local/global maps and reverse lookup (`crates/lunatic-distributed/src/distributed/registry.rs`).
- Evidence: `RegistryCoordinator` defines coordination messages and handler-side response aggregation (`crates/lunatic-distributed/src/distributed/registry_coordination.rs`).
- Evidence: registry tests validate map operations, serialization, and manually orchestrated handler transitions (`crates/lunatic-distributed/tests/distributed_registry.rs`, `crates/lunatic-distributed/tests/registry_coordination.rs`).
- Gap: multi-node `register_global_coordinated` stores a pending request and returns without sending a request or waiting for a quorum (`crates/lunatic-distributed/src/distributed/registry_coordination.rs:128-175`). Coordination messages are not carried by the existing distributed request/response transport, and the integration tests invoke handlers directly rather than running networked nodes.
- Gap: tooling (tracing, dashboards) referenced in `CORE_VALUES.md` not implemented.

## Validation and Test Coverage

| Area | Verified implementation | Not verified or not implemented | Executable evidence |
| --- | --- | --- | --- |
| OTP | Callback traits, message serialization, in-memory state transitions and restart bookkeeping | Process spawn, mailbox call/cast, real process termination/restart, runtime timeout behavior | `cargo test -p lunatic-otp-patterns` (21 tests pass; tests call callbacks or mock process IDs) |
| TLS streams | Snapshot variants, metadata capture, serialization | Live TCP+TLS reconnect and restored guest resource handle | `cargo test --test tls_stream_reconnection` (6 snapshot/serialization tests pass) |
| Global registry | In-memory registry operations and coordination handler transitions | Control/QUIC transport wiring, quorum wait, live concurrent registration, partition recovery | `cargo test -p lunatic-distributed --test registry_coordination` (8 manually orchestrated tests pass) |
| QUIC benchmark | Real loopback mTLS QUIC connection and stream echo | Lunatic distributed client/server routing and multi-node behavior | `cargo bench --bench distributed_messaging --no-run` builds the executable benchmark |

These commands were executed on Windows against the reviewed baseline on 2026-07-20. Passing scaffold-level tests must not be used as evidence that the missing production path is complete.

## Known Documentation Deltas
- Legacy phase reports (`docs/phases/PHASE*.md`) contain historical context but may diverge from current implementation. Notable: Phase 7 (TLS migration) has been completed beyond original scope. Use this status file as the canonical source for current state.

## OTP Patterns Implementation
- Evidence: `lunatic-otp-patterns` contains GenServer and GenStatem callback traits, serializable message envelopes, an in-memory GenEvent manager, and Supervisor strategy bookkeeping.
- Evidence: unit/integration tests exercise callbacks, serialization, and mock process IDs.
- Limitation: `GenServer::spawn`, `GenServerHandle::call`, and `GenServerHandle::cast` return explicit “not yet implemented” errors (`crates/lunatic-otp-patterns/src/gen_server.rs:115-126`, `:190-205`).
- Limitation: Supervisor stop/restart paths clear or replace stored IDs but do not invoke Lunatic process termination (`crates/lunatic-otp-patterns/src/supervisor.rs:371-396`).
- Limitation: the Rust example calls handlers directly and labels real process usage as pseudo-code (`examples/rust/src/gen_server_example.rs:68-113`).
- Gap: connect these abstractions to Lunatic process creation, mailboxes, replies, timeouts, exit notifications, and termination before claiming an OTP runtime implementation.

## Recommended Follow-Ups
1. Wire the spawn/messaging Criterion benches into CI to enforce the sub-10 µs target and catch regressions early.
2. TLS listeners are migratable with certificate/key preservation; implement and integration-test TLS client reconnection before marking active stream recovery complete.
3. ✅ ~~Expand language coverage with at least one non-Rust guest example plus documentation for guest SDK expectations~~ **COMPLETED**: Comprehensive multi-language examples for Rust, Go (TinyGo), and AssemblyScript with full build systems, documentation, feature matrix, migration guides, and troubleshooting (`examples/rust/`, `examples/go/`, `examples/assemblyscript/`, `examples/MULTI_LANGUAGE_GUIDE.md`).
4. ✅ ~~Document and implement rollback semantics~~ **COMPLETED**: Full rollback implementation with automatic recovery on atomic reload failure (`crates/lunatic-process/src/hot_reload.rs:223-255`, `crates/lunatic-process/src/lib.rs:703-744`). Rollback signals sent to affected processes to restore previous version.
5. Connect OTP traits and Supervisor strategies to real Lunatic processes and add process-level integration tests.
6. Carry registry coordination messages over the control/QUIC transport and test quorum behavior with real nodes.
7. Add structured audit logging for privileged host operations to close the remaining security gap.

## Related Resources
- `CORE_VALUES.md` – source principles and metrics.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` – archived scorecard (now points here).
- `docs/benchmarks/BENCHMARK_RESULTS.md` – historical benchmark notes; not authoritative.
- `PRIORITY_IMPROVEMENTS.md` – roadmap items derived from prior compliance reviews.
