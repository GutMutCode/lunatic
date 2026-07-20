# Core Values Compliance Status

Reviewed: 2026-07-21

Reviewed baseline: `6ea4521f40cadaf36c610857ba67740c4bd767f8`

This is the canonical implementation-status document for `CORE_VALUES.md`. Design documents, phase reports, examples, and historical benchmark reports may describe intent or component work, but they do not override the status here.

An item is complete only when both conditions hold:

1. The production path is connected from its public/runtime entry point to the intended outcome.
2. An executable test exercises that same path and asserts the outcome, including relevant failure behavior.

Data structures, serialization tests, callback tests, loopback transport tests, manually orchestrated component harnesses, and documentation examples are useful evidence but are not end-to-end completion by themselves.

## Status at a Glance

| Focus | Status | Current evidence boundary |
| --- | --- | --- |
| Fast | Partial | Wasmtime preemption primitives exist; spawn, pooling, mailbox, and transport components are benchmarked. Live hot reload and end-to-end local/remote process delivery are not. |
| Robust | Partial | Wasm isolation and host-side supervision components exist. Wasm link death reasons and acknowledged reload/rollback semantics remain incomplete. |
| Scalable | Emerging | Concurrent registries and quotas exist, but mailboxes/signals are unbounded and cluster-scale process/resource behavior is unverified. |
| Language Independence | Partial | Host imports are language-neutral and multi-language source examples/build recipes exist. Equivalent guest APIs and a verified multi-language build/run CI matrix do not. |
| Security Through Isolation | Partial | Several syscall checks and resource limits are enforced. Least-privilege defaults, capability attenuation, complete accounting, and structured audit events remain open. |
| Fault Tolerance & HA | Partial | Links, monitors, host-side OTP components, snapshots, and registry quorum components exist. Critical production paths below are not complete. |
| Async by Default | Partial | Wasmtime preemption and several async host paths exist. Unbounded queues and unverified blocking host calls prevent a stronger claim. |
| Erlang-Inspired | Partial | Actor primitives, host-side GenServer/Supervisor, and coordinated registry components exist. Guest adapters and several BEAM-like guarantees remain open. |

Status values: **Strong** means production-path implementation plus executable validation; **Partial** means major connected elements with material gaps; **Emerging** means useful scaffolding without a demonstrated system-level guarantee. No focus currently meets the Strong threshold.

## P0 Production-Path Gaps

These gaps invalidate broad “production ready,” “all processes,” or “fully implemented” claims:

1. **Live Wasm hot reload is not connected.** The running guest future takes ownership of the instance from `ProcessContext`; the reload handler later attempts to take an instance from that same empty option and can return `No instance available for hot reload` (`crates/lunatic-process/src/wasm.rs`, `crates/lunatic-process/src/lib.rs`). Existing integration tests and `benches/hot_reload.rs` manually compose Wasmtime compilation, instantiation, snapshot, and restore instead of exercising the live `Signal::HotReload` path.
2. **Wasm link failure semantics lose the exit reason.** The Wasm runner calculates error, panic, kill, or normal outcomes, but its link-notification path currently sends `DeathReason::Normal`. Normal exits are ignored by the receiver, so linked failures are not demonstrated to propagate as intended (`crates/lunatic-process/src/lib.rs`).
3. **The default capability posture is not least privilege.** `DefaultProcessConfig` currently enables config creation, module compilation, and process spawning by default, while the process API documentation describes a newly created config as denying permissions. Arbitrary WASI preopen paths also require an explicit capability policy (`src/config.rs`, `crates/lunatic-process-api/src/lib.rs`, `crates/lunatic-wasi-api/src/lib.rs`).
4. **Queue and environment growth are not bounded end to end.** Process message and signal channels are unbounded, and the environment does not enforce a process-count limit. Existing file, network, memory, and table checks do not close mailbox/signal/process exhaustion paths (`crates/lunatic-process/src/mailbox.rs`, `crates/lunatic-process/src/env.rs`, `src/config.rs`).
5. **Reload coordination has no process acknowledgement protocol.** Successful signal delivery is treated as successful reload; atomic commit, version lifecycle, and rollback are not proven against actual process results. Rollback send failures are logged rather than recovered (`crates/lunatic-process/src/hot_reload.rs`).

## 1. Fast, Robust, and Scalable

### Verified components

- Wasmtime stores configure async fuel yielding and epoch deadlines (`crates/lunatic-process/src/runtimes/wasmtime.rs`).
- `MessageMailbox` implements asynchronous waiting and selective receive (`crates/lunatic-process/src/mailbox.rs`).
- Instance-pool and component microbenchmarks provide useful local regression signals.
- The distributed QUIC benchmark uses real loopback mTLS, production framing, reassembly, MessagePack decoding, and the request-dispatch boundary.

### Boundary

- The mailbox benchmark measures local mailbox operations, not a sender-to-live-receiver process round trip.
- The distributed benchmark stops at decoded request dispatch; it does not deliver into a live guest mailbox or cover discovery, multiple hosts, or partitions.
- Historical spawn, mailbox, and component-reload measurements do not establish current production latency guarantees.
- One ticker advances stores associated with the first-created Wasmtime engine. Later independently constructed engines are not advanced because the ticker-start guard is process-global while each runtime constructs a new engine.
- Sustained scheduler throughput, mailbox pressure, and cluster-wide memory/network limits are not covered by a scale or soak gate.

## 2. Language Independence via WebAssembly

### Verified components

- Host subsystems expose language-neutral Wasm imports, and `tests/imports_match.rs` checks registrations against `wat/all_imports.wat`.
- The repository contains Rust, TinyGo, AssemblyScript, and WAT source examples and build recipes.

### Boundary

- The example sources and recipes have not yet been validated as a complete multi-language build-and-run matrix; they do not demonstrate equivalent access to the complete Lunatic runtime API.
- Rust OTP examples use host-side `lunatic-otp-patterns`; they are not proof of a guest-Wasm OTP API.
- Go and AssemblyScript OTP samples are manual pattern simulations, not runtime adapters.
- CI does not yet build and execute a representative guest API scenario for every advertised language.

## 3. Security Through Isolation

### Verified components

- Spawn, compile, and config-creation operations have capability checks (`crates/lunatic-process-api/src/lib.rs`).
- Networking paths enforce several per-process limits, and Wasmtime uses memory/table resource limiting (`crates/lunatic-networking-api`, `src/state.rs`).
- A directly tested same-runtime helper can transfer supported live network resource maps between process states without serializing TLS traffic keys. Serialized active TLS restoration fails explicitly (`src/state.rs`, `crates/lunatic-process/src/resource_migration.rs`).

### Boundary

- The default privilege mismatch, path preopen policy, and complete capability attenuation model remain unresolved.
- Resource accounting is not yet closed across processes, messages, signals, and all handles.
- Selected successful process-spawn and network bind/accept/connect paths emit `target="audit"` formatted log records. Denial/failure coverage, a stable typed event schema, required fields, sink contract, redaction, and executable log assertions are not implemented. A persistence guide is operational guidance, not implementation evidence.
- The live-resource transfer helper is same-runtime only and is not currently proven reachable through running-Wasm reload. Serialized snapshots, host restarts, and cross-node migration cannot restore active TCP/TLS streams; listener restoration has separate support.

## 4. Fault Tolerance and High Availability

### Verified components

- Process links and monitors, snapshot/signature components, and reload coordination structures exist.
- Global registration crosses runtime mTLS QUIC control paths on localhost. Multi-endpoint tests cover one-winner contention, quorum waiting, partition recovery, and resynchronization (`crates/lunatic-distributed/tests/registry_coordination.rs`).
- Production QUIC framing has a multi-chunk transport test (`crates/lunatic-distributed/tests/quic_transport.rs`).

### Boundary

- The P0 link-death and live-reload gaps prevent production failure-propagation and zero-downtime claims.
- Registry coordination is real, and cleanup helpers exist, but guest name lookup, automatic process/node-lifecycle-triggered ownership cleanup, and live cross-node process-mailbox delivery are not connected and tested together.
- Cross-node process failure, reload, rollback, and message recovery have no production-path E2E test.

## 5. Asynchronous by Default

### Verified components

- The process loop prioritizes signals, and Wasmtime fuel yielding provides guest preemption points. Epoch deadlines are also configured, but the process-global ticker advances only stores belonging to the first-created engine.
- Mailbox waits and several networking operations use async futures rather than busy waiting.

### Boundary

- Unbounded message and signal channels provide no backpressure guarantee.
- No audit or test proves that every potentially blocking host call yields instead of occupying an executor worker.
- Fairness under sustained CPU, I/O, and mailbox load has not been established by a deterministic test.

## 6. Erlang-Inspired, WebAssembly-Native

### Verified components

- `GenServer::spawn` uses native Lunatic processes and mailbox call/cast/reply flows. Process-level integration tests cover its host-side lifecycle (`crates/lunatic-otp-patterns`).
- Supervisor strategies operate on native Lunatic process handles and have process-level integration tests.
- GenEvent has a tested in-memory concurrency contract.
- Coordinated registry messages travel over the production QUIC control transport.

### Boundary

- Supervisor exit events must be forwarded manually to `handle_child_exit`; automatic monitor intake is pending.
- GenStatem and GenEvent are not Lunatic process-runtime adapters.
- GenServer, Supervisor, GenStatem, and GenEvent do not yet expose equivalent guest-Wasm adapters; GenServer named registration is not connected.
- The Wasm link-death issue prevents claiming BEAM-like linked failure behavior.

## Validation Scope

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| `cargo test --all` | Current unit and integration contracts exercised by the repository | Untested production paths, performance, scale, or multi-host behavior |
| `cargo test -p lunatic-otp-patterns` | Host-side OTP component and process integration behavior | Guest-Wasm adapters, automatic Supervisor monitor intake, GenStatem/GenEvent runtime integration |
| Registry/QUIC integration tests | Real localhost mTLS transport, framing, and registry quorum behavior | Guest lookup-to-mailbox delivery, cross-host operations, distributed process recovery |
| Criterion mailbox benchmark | Local queue-operation cost for its configured workload | End-to-end process message latency or backpressure |
| Criterion hot-reload benchmark | Manual compile/instantiate/snapshot/restore component cost | A live running process receiving, acknowledging, committing, or rolling back a reload |
| Memory projections | Arithmetic estimates based on measured component sizes | Demonstrated million-process capacity or sustained workload stability |

Verification on 2026-07-21: `cargo test --all` completed with 137 passing tests, zero failures, and four ignored documentation tests. This confirms only the contracts exercised by those tests; it does not close the production-path gaps above.

The benchmark documents under `docs/benchmarks/` preserve an October 2025 measurement snapshot, but the original record omitted the tested commit and exact hardware/toolchain profile. It is therefore not a reproducible baseline. Future values must be quoted with commit, dirty state, hardware, toolchain, workload, and evidence boundary; they are not release certification by default.

## Documentation Policy

- `CORE_VALUES.md` defines goals and decision principles.
- This file is the canonical current status and evidence boundary.
- Historical phase/completion reports are archival context. Any unqualified completion language in them is superseded by this file.
- README feature claims must link here and use the same completion rule.
- A future completion change must name the production entry point, executable test, reviewed commit, and unsupported cases.

## Prioritized Follow-Ups

1. Enforce least-privilege defaults and explicit capability attenuation.
2. Correct Wasm link death-reason propagation and add production-path tests.
3. Connect and test live running-Wasm hot reload.
4. Add reload acknowledgement, atomic commit, rollback, and version lifecycle semantics.
5. Bound mailboxes/signals/process creation and validate resource accounting under pressure.
6. Define structured security audit events and test schema/redaction guarantees.
7. Connect guest registry lookup to live cross-node mailbox delivery and ownership cleanup.
8. Connect Supervisor monitor intake and OTP guest/runtime adapters.
9. Build and run representative Rust, TinyGo, and AssemblyScript guest E2E scenarios in CI.
10. Add scale, soak, and final production-readiness gates.

## Related Resources

- `CORE_VALUES.md` — design principles and target metrics.
- `docs/benchmarks/BENCHMARK_RESULTS.md` — historical component measurements with scope limitations.
- `examples/MULTI_LANGUAGE_GUIDE.md` — language example guide with support boundaries.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` — archived scorecard; this file supersedes its status claims.
