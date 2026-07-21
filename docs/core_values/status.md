# Core Values Compliance Status

Reviewed: 2026-07-21

Reviewed working tree based on: `66aaa0d8703bd99b50a488b693b1a42b279ab36f` plus the reviewed working-tree changes for Hanary #1659

This is the canonical implementation-status document for `CORE_VALUES.md`. Design documents, phase reports, examples, and historical benchmark reports may describe intent or component work, but they do not override the status here.

An item is complete only when both conditions hold:

1. The production path is connected from its public/runtime entry point to the intended outcome.
2. An executable test exercises that same path and asserts the outcome, including relevant failure behavior.

Data structures, serialization tests, callback tests, loopback transport tests, manually orchestrated component harnesses, and documentation examples are useful evidence but are not end-to-end completion by themselves.

## Status at a Glance

| Focus | Status | Current evidence boundary |
| --- | --- | --- |
| Fast | Partial | Wasmtime preemption primitives exist; spawn, pooling, mailbox, and transport components are benchmarked. Live hot reload and end-to-end local/remote process delivery are not. |
| Robust | Partial | Wasm isolation and actual-Wasm link/monitor exit semantics are production-path tested. Acknowledged reload/rollback semantics remain incomplete. |
| Scalable | Emerging | Concurrent registries and quotas exist, but mailboxes/signals are unbounded and cluster-scale process/resource behavior is unverified. |
| Language Independence | Partial | Host imports are language-neutral and multi-language source examples/build recipes exist. Equivalent guest APIs and a verified multi-language build/run CI matrix do not. |
| Security Through Isolation | Partial | Process configs now default to denied capabilities and enforce non-increasing child authority. Remote preopens fail closed; complete accounting, a usable receiver path policy, and structured audit events remain open. |
| Fault Tolerance & HA | Partial | Actual-Wasm links and monitors, host-side OTP components, snapshots, and registry quorum components exist. Live reload and distributed recovery paths remain incomplete. |
| Async by Default | Partial | Wasmtime preemption and several async host paths exist. Unbounded queues and unverified blocking host calls prevent a stronger claim. |
| Erlang-Inspired | Partial | Actor primitives, host-side GenServer/Supervisor, and coordinated registry components exist. Guest adapters and several BEAM-like guarantees remain open. |

Status values: **Strong** means production-path implementation plus executable validation; **Partial** means major connected elements with material gaps; **Emerging** means useful scaffolding without a demonstrated system-level guarantee. No focus currently meets the Strong threshold.

## P0 Production-Path Gaps

These gaps invalidate broad “production ready,” “all processes,” or “fully implemented” claims:

1. **Live Wasm hot reload is not connected.** The running guest future takes ownership of the instance from `ProcessContext`; the reload handler later attempts to take an instance from that same empty option and can return `No instance available for hot reload` (`crates/lunatic-process/src/wasm.rs`, `crates/lunatic-process/src/lib.rs`). Existing integration tests and `benches/hot_reload.rs` manually compose Wasmtime compilation, instantiation, snapshot, and restore instead of exercising the live `Signal::HotReload` path.
2. **Queue and environment growth are not bounded end to end.** Process message and signal channels are unbounded, and the environment does not enforce a process-count limit. Existing file, network, memory, and table checks do not close mailbox/signal/process exhaustion paths (`crates/lunatic-process/src/mailbox.rs`, `crates/lunatic-process/src/env.rs`, `src/config.rs`).
3. **Reload coordination has no process acknowledgement protocol.** Successful signal delivery is treated as successful reload; atomic commit, version lifecycle, and rollback are not proven against actual process results. Rollback send failures are logged rather than recovered (`crates/lunatic-process/src/hot_reload.rs`).

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
- `DefaultProcessConfig` denies compile/create/spawn and preopens by default. Child configs clear ambient arguments, environment values, preopens, and boolean capabilities while inheriting parent resource ceilings. Guest setters and local/lookup/distributed spawn boundaries enforce non-increasing capability, path, memory, fuel, table, FD, and network ceilings (`src/config.rs`, `crates/lunatic-process-api/src/lib.rs`, `crates/lunatic-wasi-api/src/lib.rs`, `crates/lunatic-distributed-api/src/lib.rs`).
- Distributed configs with filesystem preopens fail closed at both sender and receiver boundaries because sender-local paths are not remote authority (`crates/lunatic-distributed-api/src/lib.rs`, `crates/lunatic-distributed/src/distributed/server.rs`, `src/state.rs`).
- Production-import integration tests cover default compile/create/spawn/preopen denial, Windows-safe checked escalation errors and rollback, legacy void-setter no-op safety, checked and legacy delegation through actual child spawn, guest-readable returned error IDs, final selected-config validation through both local spawn imports, bounded error resources under repeated denial, sender-side remote-preopen rejection, and allowed/denied formatted audit emission (`tests/capability_attenuation.rs`). Receiver tests deserialize and validate configs through the production receive helper (`crates/lunatic-distributed/src/distributed/server.rs`).
- Networking paths enforce several per-process limits, and Wasmtime uses memory/table resource limiting (`crates/lunatic-networking-api`, `src/state.rs`).
- A directly tested same-runtime helper can transfer supported live network resource maps between process states without serializing TLS traffic keys. Serialized active TLS restoration fails explicitly (`src/state.rs`, `crates/lunatic-process/src/resource_migration.rs`).

### Boundary

- Filesystem preopens are canonicalized and attenuated locally, but path replacement still has a check/open race. Remote preopens are unavailable until a receiver-controlled path mapping/allowlist is implemented; this is a functionality gap, not an ambient-authority fallback.
- Authenticated cluster nodes are trusted for serialized non-filesystem capability flags and resource ceilings. A receiver-owned policy does not yet independently cap compile/create/spawn, memory, fuel, table, FD, or network authority from a compromised peer.
- FD and network ceilings are attenuated as configuration values, but FD accounting is not connected to every host file operation and network accounting does not yet cover all listener/TLS/UDP/DNS resources or destination policy.
- Resource accounting is not yet closed across processes, messages, signals, and all handles.
- Capability delegation emits tested allowed/denied `target="audit"` records. Selected successful process-spawn and network bind/accept/connect paths also emit formatted records, but their denial/failure coverage, a stable typed schema, required fields, sink contract, and redaction tests remain unimplemented. A persistence guide is operational guidance, not implementation evidence.
- The live-resource transfer helper is same-runtime only and is not currently proven reachable through running-Wasm reload. Serialized snapshots, host restarts, and cross-node migration cannot restore active TCP/TLS streams; listener restoration has separate support.

## 4. Fault Tolerance and High Availability

### Verified components

- Process links and monitors, snapshot/signature components, and reload coordination structures exist.
- `tests/wasm_link_death.rs` exercises the real `spawn_wasm` lifecycle for normal exit, guest trap, host panic, active receive/sleep cancellation, and missing-process link/monitor behavior. It verifies exact link reasons and tags, default peer termination, trapping-exit survival, one monitor notification, removal-before-notification ordering, and native-runner parity.
- Global registration crosses runtime mTLS QUIC control paths on localhost. Multi-endpoint tests cover one-winner contention, quorum waiting, partition recovery, and resynchronization (`crates/lunatic-distributed/tests/registry_coordination.rs`).
- Production QUIC framing has a multi-chunk transport test (`crates/lunatic-distributed/tests/quic_transport.rs`).

### Boundary

- The live-reload gap prevents zero-downtime claims; cross-node failure propagation remains outside the local link/monitor evidence.
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
- Linked failure behavior is verified locally for actual Wasm peers; automatic Supervisor monitor intake and distributed BEAM-like recovery remain unverified.

## Validation Scope

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| `cargo test --all` | Current unit and integration contracts exercised by the repository | Untested production paths, performance, scale, or multi-host behavior |
| `cargo test -p lunatic-runtime --test wasm_link_death` | Actual-Wasm and native normal/failure/panic/kill/missing-process link and monitor semantics | Cross-node links, Supervisor event intake, or every possible host-future suspension point |
| `cargo test -p lunatic-otp-patterns` | Host-side OTP component and process integration behavior | Guest-Wasm adapters, automatic Supervisor monitor intake, GenStatem/GenEvent runtime integration |
| Registry/QUIC integration tests | Real localhost mTLS transport, framing, and registry quorum behavior | Guest lookup-to-mailbox delivery, cross-host operations, distributed process recovery |
| Criterion mailbox benchmark | Local queue-operation cost for its configured workload | End-to-end process message latency or backpressure |
| Criterion hot-reload benchmark | Manual compile/instantiate/snapshot/restore component cost | A live running process receiving, acknowledging, committing, or rolling back a reload |
| Memory projections | Arithmetic estimates based on measured component sizes | Demonstrated million-process capacity or sustained workload stability |

Windows baseline verification on 2026-07-21, before the Hanary #1659 working-tree changes, completed `cargo test --all` across 57 suites with 158 passing tests, zero failures, and four ignored documentation examples. The #1659 changes add active Wasm `receive`/`sleep_ms` cancellation and host-panic coverage plus a narrowly vendored backport of the upstream Windows fiber-local-storage guard. Those new paths passed locally on macOS; they must also pass the existing Windows CI matrix before merge. Source parity with the upstream guard is not a substitute for that runtime evidence.

The benchmark documents under `docs/benchmarks/` preserve an October 2025 measurement snapshot, but the original record omitted the tested commit and exact hardware/toolchain profile. It is therefore not a reproducible baseline. Future values must be quoted with commit, dirty state, hardware, toolchain, workload, and evidence boundary; they are not release certification by default.

## Documentation Policy

- `CORE_VALUES.md` defines goals and decision principles.
- This file is the canonical current status and evidence boundary.
- Historical phase/completion reports are archival context. Any unqualified completion language in them is superseded by this file.
- README feature claims must link here and use the same completion rule.
- A future completion change must name the production entry point, executable test, reviewed commit, and unsupported cases.

## Prioritized Follow-Ups

1. Connect and test live running-Wasm hot reload.
2. Add reload acknowledgement, atomic commit, rollback, and version lifecycle semantics.
3. Bound mailboxes/signals/process creation and validate resource accounting under pressure.
4. Define structured security audit events and test schema/redaction guarantees.
5. Add receiver-owned distributed capability/ceiling policy, filesystem mapping/allowlists, and close the local path check/open authority boundary.
6. Connect guest registry lookup to live cross-node mailbox delivery and ownership cleanup.
7. Connect Supervisor monitor intake and OTP guest/runtime adapters.
8. Build and run representative Rust, TinyGo, and AssemblyScript guest E2E scenarios in CI.
9. Add scale, soak, and final production-readiness gates.

## Related Resources

- `CORE_VALUES.md` — design principles and target metrics.
- `docs/benchmarks/BENCHMARK_RESULTS.md` — historical component measurements with scope limitations.
- `docs/security/CAPABILITY_ATTENUATION.md` — process capability and resource-ceiling inheritance contract.
- `examples/MULTI_LANGUAGE_GUIDE.md` — language example guide with support boundaries.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` — archived scorecard; this file supersedes its status claims.
