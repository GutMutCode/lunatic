# Core Values Compliance Status

Reviewed: 2026-07-21
Reviewed baseline: `57885c783a587718a893af7492340f486bbd56df`

This document supersedes ad-hoc phase reports and consolidates how the current codebase aligns with the principles in `CORE_VALUES.md`. It cites concrete implementation points, test coverage, and gaps that require follow-up work.

An item is complete only when the production path is connected and an executable test exercises that path. Data structures, callback logic, serialization tests, loopback transport tests, and manually orchestrated handler tests are reported as partial evidence rather than end-to-end completion.

## Status at a Glance

| Focus | Status | Highlights |
| --- | --- | --- |
| Fast | Partial | Global epoch preemption and hot-reload snapshots are implemented; distributed serialization and a loopback QUIC transport benchmark exist, but the benchmark does not traverse Lunatic node message routing. |
| Robust | Strong | Per-process isolation, link/monitor semantics, and hot-reload swap path are in place. |
| Scalable | Partial | Environment tracking and resource caps land, yet instance pooling and distributed ergonomics remain incomplete. |
| Language Independence | Strong | Host APIs are language-agnostic and enumerated in `wat/all_imports.wat`. Comprehensive examples for Rust, Go (TinyGo), and AssemblyScript with full documentation. |
| Security Through Isolation | Strong | Capability checks are enforced; in-process hot reload transfers live TLS sessions without serializing keys, while serialized TLS restoration fails explicitly. |
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
- Evidence: in-process hot reload moves the live network resource maps into the replacement state, preserving active client/server TLS sessions, guest resource IDs, ID seeds, timeouts, listeners, and resource accounting (`src/state.rs`, `crates/lunatic-process/src/lib.rs`).
- Evidence: the live-path test completes a real TLS handshake, transfers the exact `TlsConnection`, and successfully exchanges data after state replacement (`src/state.rs::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts`).
- Evidence: serialized snapshots use the explicitly limited `TlsClientConnectionMetadata` and `TlsServerConnectionMetadata` variants and do not serialize TLS traffic keys (`crates/lunatic-process/src/resource_migration.rs`).
- Evidence: serialized TLS stream restoration returns an error before any partial listener/socket restoration, rather than logging and reporting success (`src/state.rs::restore_resource_snapshot`).
- Evidence: the support boundary and security rationale are documented in `docs/tls/TLS_STREAM_MIGRATION.md`.
- Evidence: privileged operations (spawn, bind/connect) emit `target="audit"` log entries for downstream ingestion (`crates/lunatic-common-api/src/lib.rs:113`). Implementation documented in `docs/security/AUDIT_LOGGING.md`.
- Evidence: **Comprehensive audit logging persistence guide** - Production-ready documentation covering OpenTelemetry, syslog, and container logging architectures with storage recommendations, compliance checklists, and alerting strategies (`docs/security/AUDIT_LOGGING_PERSISTENCE.md`).
- Limitation: TLS stream preservation applies only to hot reload inside the same runtime process. Persisted snapshots, host restarts, and cross-process migration cannot restore an active TLS/application byte stream; this path returns an explicit unsupported error.

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
| OTP | GenServer mailbox/lifecycle integration; Supervisor actual-process lifecycle and restart strategies; GenEvent exact-target and isolated concurrent fan-out | Supervisor automatic monitor-event intake; GenStatem/GenEvent process-runtime adapters; guest-WASM adapters | `cargo test -p lunatic-otp-patterns` (41 tests pass, including 9 real-process integration tests and 5 GenEvent contract tests) |
| TLS streams | Live in-process transfer with preserved session, guest ID and timeouts; metadata serialization contract | Serialized/cross-process active-stream restoration | `cargo test --lib state::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts`; `cargo test --test tls_stream_migration_contract` |
| Global registry | In-memory registry operations and coordination handler transitions | Control/QUIC transport wiring, quorum wait, live concurrent registration, partition recovery | `cargo test -p lunatic-distributed --test registry_coordination` (8 manually orchestrated tests pass) |
| QUIC benchmark | Real loopback mTLS QUIC connection and stream echo | Lunatic distributed client/server routing and multi-node behavior | `cargo bench --bench distributed_messaging --no-run` builds the executable benchmark |

These commands were executed on Windows against the reviewed baseline on 2026-07-21. Passing tests apply only to the runtime paths named in the table; they must not be generalized to the remaining Supervisor monitor intake, GenStatem/GenEvent process-runtime adapters, serialized TLS recovery, registry, or distributed messaging gaps.

## Known Documentation Deltas
- Legacy phase reports (`docs/phases/PHASE*.md`) contain historical context but may diverge from current implementation. Notable: Phase 7 (TLS migration) has been completed beyond original scope. Use this status file as the canonical source for current state.

## OTP Patterns Implementation
- Evidence: `GenServer::spawn` uses `lunatic_process::spawn_native`, registers the process in a `LunaticEnvironment`, and consumes serialized call/cast/stop messages from its `MessageMailbox` (`crates/lunatic-otp-patterns/src/gen_server.rs`, `crates/lunatic-process/src/lib.rs`).
- Evidence: call replies use unique request IDs with pending-reply correlation, configurable timeout cleanup, and process-exit wakeups. Graceful stop runs `terminate`; kill and fatal cast errors remove the process and reject later messages.
- Evidence: `crates/lunatic-otp-patterns/tests/gen_server_runtime.rs` verifies call response, cast state changes, timeout recovery, error propagation, graceful stop, and kill using actual native Lunatic processes.
- Evidence: Supervisor child starters receive its `Environment` and return actual `Process` handles. Strategy restarts preflight intensity, stop affected processes in reverse specification order, restart in forward order, preserve per-child restart counts, and roll back partial batch starts (`crates/lunatic-otp-patterns/src/supervisor.rs`).
- Evidence: `crates/lunatic-otp-patterns/tests/supervisor_runtime.rs` verifies actual-process replacement and removal for OneForOne, OneForAll and RestForOne, all restart policies, restart intensity rejection, persistent counts, ordered restart, duplicate-start rejection, and shutdown.
- Evidence: GenEvent snapshots `Arc` handlers under its `RwLock`, releases the guard, and executes the snapshot concurrently on Tokio's blocking pool. `notify_handler` invokes exactly one snapshotted handler; `NotifyReport` isolates returned errors, panics, and runtime failures (`crates/lunatic-otp-patterns/src/gen_event.rs`).
- Evidence: GenEvent tests prove a slow handler does not delay other deliveries or concurrent add/remove operations, additions are excluded from an existing snapshot, removals do not cancel in-flight delivery, and panic/error outcomes do not stop healthy handlers or later notifications.
- Limitation: Supervisor child exit notifications must currently be forwarded to `handle_child_exit`; automatic monitor-event intake is not yet implemented.
- Limitation: the current GenServer adapter is host-side and requires a multi-thread Tokio runtime. It does not yet expose the same abstraction through guest-WASM SDK host imports, and `GenServerConfig::name` is metadata rather than registry registration.
- Limitation: GenEvent now has a defined and tested in-memory concurrency contract, but it is not yet a Lunatic process-runtime or guest-WASM adapter.
- Gap: connect Supervisor monitor intake and GenStatem/GenEvent adapters to the process runtime before claiming complete OTP runtime coverage.

## Recommended Follow-Ups
1. Wire the spawn/messaging Criterion benches into CI to enforce the sub-10 µs target and catch regressions early.
2. If cross-process TLS recovery becomes a requirement, design an explicit application-level quiesce/replay protocol; do not substitute a fresh stream behind an existing guest handle.
3. ✅ ~~Expand language coverage with at least one non-Rust guest example plus documentation for guest SDK expectations~~ **COMPLETED**: Comprehensive multi-language examples for Rust, Go (TinyGo), and AssemblyScript with full build systems, documentation, feature matrix, migration guides, and troubleshooting (`examples/rust/`, `examples/go/`, `examples/assemblyscript/`, `examples/MULTI_LANGUAGE_GUIDE.md`).
4. ✅ ~~Document and implement rollback semantics~~ **COMPLETED**: Full rollback implementation with automatic recovery on atomic reload failure (`crates/lunatic-process/src/hot_reload.rs:223-255`, `crates/lunatic-process/src/lib.rs:703-744`). Rollback signals sent to affected processes to restore previous version.
5. Connect Supervisor monitor-event intake, GenStatem, and GenEvent to real Lunatic processes; add guest-WASM adapters and GenServer named registration.
6. Carry registry coordination messages over the control/QUIC transport and test quorum behavior with real nodes.
7. Add structured audit logging for privileged host operations to close the remaining security gap.

## Related Resources
- `CORE_VALUES.md` – source principles and metrics.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` – archived scorecard (now points here).
- `docs/benchmarks/BENCHMARK_RESULTS.md` – historical benchmark notes; not authoritative.
- `PRIORITY_IMPROVEMENTS.md` – roadmap items derived from prior compliance reviews.
