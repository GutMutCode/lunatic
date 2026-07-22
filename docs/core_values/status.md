# Core Values Compliance Status

Reviewed: 2026-07-22

Reviewed working tree based on: `52101a2` plus the reviewed working-tree changes for Hanary #1665

This is the canonical implementation-status document for `CORE_VALUES.md`. Design documents, phase reports, examples, and historical benchmark reports may describe intent or component work, but they do not override the status here.

An item is complete only when both conditions hold:

1. The production path is connected from its public/runtime entry point to the intended outcome.
2. An executable test exercises that same path and asserts the outcome, including relevant failure behavior.

Data structures, serialization tests, callback tests, loopback transport tests, manually orchestrated component harnesses, and documentation examples are useful evidence but are not end-to-end completion by themselves.

## Status at a Glance

| Focus | Status | Current evidence boundary |
| --- | --- | --- |
| Fast | Partial | Wasmtime preemption primitives and live process-local hot reload are production-path tested. A two-node registry-to-live-native-mailbox round trip is benchmarked; hot-reload latency and guest-host-call messaging latency are not. |
| Robust | Partial | Wasm isolation, actual-Wasm link/monitor exit semantics, and acknowledged atomic reload/rollback/in-doubt behavior are production-path tested. Cross-node recovery remains incomplete. |
| Scalable | Partial | Process admission, mailboxes, signals, message allocations, and guest-visible network handles have finite quotas and pressure tests. Cluster-scale process/resource behavior is unverified. |
| Language Independence | Partial | Host imports are language-neutral and multi-language source examples/build recipes exist. Equivalent guest APIs and a verified multi-language build/run CI matrix do not. |
| Security Through Isolation | Partial | Process configs default to denied capabilities and enforce non-increasing child authority. Version-2 TLS listener snapshots exclude raw private-key bytes, Cloud CLI credentials use native protected stores, and typed/redacted V1 audit events cover major privileged boundaries, but the guest TLS ABI still accepts raw key input, complete resource accounting and receiver path policy remain open, and audit delivery is not durable/required. |
| Fault Tolerance & HA | Partial | Actual-Wasm links/monitors, process-local live reload, and automatic native Supervisor restart/escalation paths are production-path tested. Guest-side and distributed recovery paths remain incomplete. |
| Async by Default | Partial | Wasmtime preemption, bounded actor ingress, and several async host paths exist. Unverified blocking host calls and scheduler fairness prevent a stronger claim. |
| Erlang-Inspired | Partial | Actor primitives, process-backed GenServer/Supervisor/GenStatem/GenEvent adapters, a guest-Wasm OTP wire E2E, and coordinated registry components exist. High-level guest SDKs and several BEAM-like guarantees remain open. |

Status values: **Strong** means production-path implementation plus executable validation; **Partial** means major connected elements with material gaps; **Emerging** means useful scaffolding without a demonstrated system-level guarantee. No focus currently meets the Strong threshold.

## P0 Production-Path Gaps

No unresolved P0 gap was identified in this reviewed snapshot. The former
unbounded actor-ingress/process-admission gap is closed by finite, transactional
quotas, and the former reload-coordination gap is closed by process
acknowledgements, atomic commit, full-target rollback, and explicit in-doubt
state. The partial ratings below remain because durable audit delivery,
receiver-selected authority, cross-node recovery, and scale evidence are still
missing.

## 1. Fast, Robust, and Scalable

### Verified components

- Wasmtime stores configure async fuel yielding and epoch deadlines (`crates/lunatic-process/src/runtimes/wasmtime.rs`).
- The watch-equivalent compile/register/broadcast boundary reaches a live Wasm process through `Signal::HotReload`. Its execution driver preserves compatible linear memory, process identity, FIFO mailbox contents, and supported in-process host resources across successful replacement, and resumes the previous instance after signature rejection or instantiation failure (`tests/live_hot_reload.rs`).
- `MessageMailbox` implements asynchronous waiting and selective receive (`crates/lunatic-process/src/mailbox.rs`).
- Per-process mailbox slots, signal ingress, message bytes/resources, network
  handles, DNS iterators, and environment process admission are finite. Failed
  admission preserves ownership, and cancellation/drop releases reservations
  (`crates/lunatic-process/src/mailbox.rs`, `crates/lunatic-process/src/state.rs`,
  `crates/lunatic-process/src/env.rs`, `crates/lunatic-networking-api`,
  `src/state.rs`).
- `ReloadCoordinator` waits for process acknowledgements, commits only after
  the full target set applies the version, rolls the full set back after any
  apply failure, and blocks later updates when rollback is in doubt.
  `tests/live_hot_reload.rs` exercises this through running Wasm processes.
- Instance-pool and component microbenchmarks provide useful local regression signals.
- The distributed QUIC benchmark uses real loopback mTLS, production framing, reassembly, MessagePack decoding, and the request-dispatch boundary.
- The distributed latency suite also measures a persistent two-node production path from global-registry lookup through confirmed QUIC delivery into a live native-process mailbox and a confirmed live-mailbox reply (`benches/distributed_latency.rs`).

### Boundary

- The mailbox benchmark measures local mailbox operations, not a sender-to-live-receiver process round trip.
- The `distributed_messaging` transport benchmark stops at decoded request dispatch. The live-mailbox benchmark reaches real processes but does not include guest-Wasm host calls, multi-host deployment, or partitions in its timed fixture.
- Historical spawn, mailbox, and component-reload measurements do not establish current production latency guarantees.
- Each independently constructed Wasmtime engine has an epoch ticker, but live-reload latency and scheduler fairness have not been benchmarked under sustained load.
- Reload cancels the current Wasmtime call and re-enters the same export on the replacement module. It preserves compatible linear memory and host-owned state, not the interrupted instruction pointer, native stack, private globals, or tables; modules therefore need a compatible entrypoint-reentry contract.
- Sustained scheduler throughput, mailbox pressure, and cluster-wide memory/network limits are not covered by a scale or soak gate.

## 2. Language Independence via WebAssembly

### Verified components

- Host subsystems expose language-neutral Wasm imports, and `tests/imports_match.rs` checks registrations against `wat/all_imports.wat`.
- The repository contains Rust, TinyGo, AssemblyScript, and WAT source examples and build recipes.

### Boundary

- The example sources and recipes have not yet been validated as a complete multi-language build-and-run matrix; they do not demonstrate equivalent access to the complete Lunatic runtime API.
- Rust OTP examples still use host-side `lunatic-otp-patterns`. The actual-Wasm OTP1 fixture proves the low-level wire path, not a packaged Rust guest SDK.
- Go and AssemblyScript OTP samples are manual pattern simulations, not runtime adapters.
- CI does not yet build and execute a representative guest API scenario for every advertised language.

## 3. Security Through Isolation

### Verified components

- Spawn, compile, and config-creation operations have capability checks (`crates/lunatic-process-api/src/lib.rs`).
- `DefaultProcessConfig` denies compile/create/spawn and preopens by default. Child configs clear ambient arguments, environment values, preopens, and boolean capabilities while inheriting parent resource ceilings. Guest setters and local/lookup/distributed spawn boundaries enforce non-increasing capability, path, memory, fuel, table, FD, and network ceilings (`src/config.rs`, `crates/lunatic-process-api/src/lib.rs`, `crates/lunatic-wasi-api/src/lib.rs`, `crates/lunatic-distributed-api/src/lib.rs`).
- Distributed configs with filesystem preopens fail closed at both sender and receiver boundaries because sender-local paths are not remote authority (`crates/lunatic-distributed-api/src/lib.rs`, `crates/lunatic-distributed/src/distributed/server.rs`, `src/state.rs`).
- Production-import integration tests cover default compile/create/spawn/preopen denial, Windows-safe checked escalation errors and rollback, legacy void-setter no-op safety, checked and legacy delegation through actual child spawn, guest-readable returned error IDs, final selected-config validation through both local spawn imports, bounded error resources under repeated denial, sender-side remote-preopen rejection, and typed allowed/denied audit emission (`tests/capability_attenuation.rs`). Receiver tests deserialize and validate configs through the production receive helper (`crates/lunatic-distributed/src/distributed/server.rs`).
- `AuditEventV1` provides stable enum fields, available process/environment/node identity, typed targets, machine reason codes, and writer-ordered sequence numbers. String-free targets omit paths, endpoints, registry names, credentials, payloads, argv/env values, and raw errors before enqueue. Common tests cover exact JSON, redaction state, concurrent-producer FIFO, queue saturation, disabled/unavailable sinks, writer error/panic, counters, and bounded flush (`crates/lunatic-common-api/src/audit.rs`).
- Privileged operation boundaries now record compile/config/preopen/WASI directory access, local and distributed spawn, TCP/TLS/UDP/DNS operations, covered resource-limit denials, hot-reload terminal outcomes, distributed-registry changes/snapshots, and distributed authorization denials. Operation-level guards emit one terminal result on success, denial, failure, timeout, or early trap.
- Distributed mTLS certificates bind a control-plane-issued numeric node ID. Outbound connections verify the intended topology ID, inbound requests fence inactive members and reject mismatched source claims, response waiters require the expected authenticated peer, and registry leader/snapshot decisions consume only the certificate identity. Protocol-denial audit subjects use that verified peer (`crates/lunatic-distributed/src/quic/quin.rs`, `crates/lunatic-distributed/src/distributed/server.rs`, `crates/lunatic-distributed/src/distributed/client.rs`).
- Networking paths enforce several per-process limits, and Wasmtime uses memory/table resource limiting (`crates/lunatic-networking-api`, `src/state.rs`).
- Version-2 `ResourceMigrationSnapshot` TLS listener entries serialize only a local address and an opaque 128-bit handle, never a certificate or raw private key. Provider access is authorized against both environment and process identity. The default provider is process-local, five-minute, single-use, and capped at 1,024 unexpired entries; missing, expired, wrong-scope, capacity, and provider-error paths fail closed with stable secret-free errors. Unversioned, missing-version, unknown-version, and trailing-data snapshots are rejected without an automatic importer (`crates/lunatic-process/src/resource_migration.rs`, `src/tls_credentials.rs`, `src/state.rs`).
- The `tls_bind` host path zeroizes its temporary host PEM copy while preserving the const guest input required by SDK multi-address fallback. The production-import test checks that ABI behavior and audit-event redaction on an invalid-key path (`crates/lunatic-networking-api/src/tls_tcp.rs`, `tests/capability_attenuation.rs`).
- A directly tested same-runtime helper transfers supported live network resource maps, including the bound TLS listener/acceptor and active TLS sessions, without provider lookup or snapshot serialization. Serialized active TLS stream restoration fails explicitly (`src/state.rs`, `crates/lunatic-process/src/resource_migration.rs`).
- Lunatic Cloud CLI credentials are held in macOS Keychain, Windows Credential Manager, or Linux
  Secret Service behind an opaque config reference. The production config/login/request/logout
  paths fail closed without a backend, atomically migrate legacy plaintext after store/read-back
  verification, reject expired or partial credentials, send one sensitive cookie header without
  redirects, serialize native operations on one lifetime-owning worker, stage an opaque deletion
  tombstone before the first protected write, and retain it across logout failures so interrupted
  login or migration copies remain recoverable.
  Linked global config files are rejected and errors remain stable and secret-free
  (`src/mode/credential_store.rs`,
  `src/mode/config.rs`, `src/mode/login.rs`, `src/mode/logout.rs`).

### Boundary

- Filesystem preopens are canonicalized and attenuated locally, but path replacement still has a check/open race. Remote preopens are unavailable until a receiver-controlled path mapping/allowlist is implemented; this is a functionality gap, not an ambient-authority fallback.
- Distributed receivers apply the fixed default capability and resource ceiling, but operators cannot yet select a stricter per-node policy; fuel has no receiver-owned finite ceiling. Identity-bound certificates fail closed across legacy/new mixed data planes, so rollout requires the control plane first and coordinated node endpoint recreation. Membership removal is enforced after each node refreshes its topology view.
- Every guest-visible TCP/TLS listener or stream and UDP socket owns a paired FD/network lease, and DNS/address iterators are finite. General WASI file handles and network destination policy are not connected to those limits.
- Local actor ingress and resource transfer are bounded, but aggregate host/cluster memory, CPU, disk, and remote-transport budgets are not closed by one global policy.
- Audit delivery is best-effort and fail-open: a dedicated writer uses a bounded 1,024-record queue, drops newest on saturation, opens a health circuit on sink failure, exposes counters, and waits at most 250 ms at graceful shutdown. The default sink ends at the enabled Rust `log` facade; it does not prove disk/remote persistence, action-plus-audit atomicity, retention, or tamper evidence. An explicit `RUST_LOG` can still disable `audit=info`, and remaining non-privileged host calls are outside the event inventory.
- The current guest TLS bind ABI still receives raw private-key bytes through a const buffer that SDKs may reuse for multi-address fallback. The host zeroizes only its temporary copy and deliberately does not mutate guest memory, so the original or duplicates can appear in a `MemorySnapshot`. Intentional distributed credential-delivery APIs are also outside this resource-snapshot guarantee. Avoiding guest key bytes altogether requires a future provider-handle guest ABI.
- The running-Wasm reload transaction invokes the same-runtime live-resource transfer after its fallible preparation steps. Exact TLS session/ID preservation is tested at that transfer boundary, not yet by a guest-driven reload E2E. Serialized snapshots, host restarts, and cross-node migration cannot restore active TCP/TLS streams. A TLS listener can be rebound from a version-2 snapshot only while its scoped handle is resolvable; the default provider does not survive restart, a consumed or failed-attempt handle must not be replayed, and persisted/restart recovery requires an explicitly injected reprovisioning provider.
- Cloud CLI tests inject a fake credential store and loopback provider. CI compiles native backends
  on each supported OS, but availability, unlock prompts, desktop-session policy, and Linux session
  D-Bus behavior still require representative platform smoke tests. Logout deletes local material
  only because the provider API exposes no remote revocation endpoint.

## 4. Fault Tolerance and High Availability

### Verified components

- Process links and monitors, snapshot/signature components, and reload coordination structures exist.
- `tests/wasm_link_death.rs` exercises the real `spawn_wasm` lifecycle for normal exit, guest trap, host panic, active receive/sleep cancellation, and missing-process link/monitor behavior. It verifies exact link reasons and tags, default peer termination, trapping-exit survival, one reason-preserving monitor notification, removal-before-notification ordering, native-runner parity, and automatic Supervisor replacement after an immediate guest trap.
- Global registration crosses runtime mTLS QUIC control paths on localhost. Multi-endpoint tests cover one-winner contention, quorum waiting, partition recovery, resynchronization, rejection of a node-2 certificate claiming node 1 for leader notifications, and rejection of a node-2-authenticated synchronization snapshot while node 3 expects coordinator node 1. The latter proves processing with a same-stream response marker before a legitimate node-1 snapshot completes the preserved waiter (`crates/lunatic-distributed/tests/registry_coordination.rs`).
- `tests/distributed_registry_e2e.rs` runs actual Wasm guests on full node servers. It covers guest registration and replica lookup, confirmed request/reply through live cross-node mailboxes, missing environment/process errors, explicit removal, cross-environment fail-closed lookup, and owner-exit cleanup on both replicas. A three-node mTLS case also proves that a spawn waiter expecting node 2 rejects a matching response ID from node 3 without consuming the waiter, then accepts node 2's response.
- Confirmed sends correlate `Sent`/typed error responses before reporting success, preserve bounded waiter/outbound accounting on timeout or cancellation, and distinguish missing environments, missing processes, receiver backpressure, and oversized/rejected delivery. Owner cleanup retains a bounded retry record with backoff until quorum coordination succeeds.
- Production QUIC framing has a multi-chunk transport test (`crates/lunatic-distributed/tests/quic_transport.rs`).

### Boundary

- Process-local reload re-enters the guest entrypoint rather than continuing the interrupted instruction stream. Acknowledged commit/rollback is implemented for the local environment, but it does not establish instruction-level continuation, cross-node coordination, or broad zero-downtime guarantees. Cross-node failure propagation remains outside the local link/monitor evidence.
- Guest registry reads use the local replica and are eventually consistent during partitions. The legacy `(node, process)` ABI deliberately hides registrations from another environment; cross-environment delivery needs an environment-aware guest handle/API.
- Healthy owner exit is guest-E2E tested, and cleanup retry/partition mechanics are component-tested, but owner exit during a live partition has no combined guest E2E. Cross-node process failure propagation, reload, rollback, and message recovery remain unverified.

## 5. Asynchronous by Default

### Verified components

- The process loop prioritizes signals, Wasmtime fuel yielding provides guest preemption points, and each independently constructed engine advances its own epoch deadlines.
- Mailbox waits and several networking operations use async futures rather than busy waiting.

### Boundary

- Actor message and signal ingress is bounded and returns ownership-preserving pressure errors, but producer retry/fairness behavior is application-specific.
- No audit or test proves that every potentially blocking host call yields instead of occupying an executor worker.
- Fairness under sustained CPU, I/O, and mailbox load has not been established by a deterministic test.

## 6. Erlang-Inspired, WebAssembly-Native

### Verified components

- `GenServer::spawn` uses native Lunatic processes and mailbox call/cast/reply flows. `spawn_in` optionally registers a name in an injected bounded `DistributedRegistry` local namespace; tests cover collision rejection before `init`, owner-indexed cleanup, and immediate reuse after exit.
- `Supervisor::spawn_with_environment` runs the supervisor itself as a native Lunatic process. It registers acknowledged monitors before exposing startup, retains terminal reasons for late monitor registration, consumes child-death messages automatically, ignores stale intentional-kill events by process identity, and escalates restart exhaustion after reverse-order shutdown. Tests cover immediate normal/error/panic exits, kill, an actual guest-Wasm trap, ChildStart-panic orphan cleanup, Normal/Failure monitor output, OneForOne, OneForAll, and RestForOne.
- GenStatem and GenEvent have native Lunatic-process mailbox/lifecycle adapters in addition to their behavior APIs. Their synchronous handle waits, along with GenServer calls and lifecycle waits, are regression-tested on a one-worker multi-thread Tokio runtime.
- `tests/otp_guest_wasm.rs` runs an actual Wasm client and server using the language-neutral OTP1 envelope over existing bounded message imports. It verifies a Rust-encoded contract probe, cast, correlated reply envelopes, timeout status, and acknowledged graceful stop.
- Coordinated registry messages travel over the production QUIC control transport.

### Boundary

- The automatic Supervisor adapter is host-side and local. Guest-Wasm supervisor trees, cross-node supervision, and distributed BEAM-like recovery remain unverified.
- OTP1 is a low-level, language-neutral guest wire contract, not a complete high-level SDK. Rust, TinyGo, and AssemblyScript still need equivalent packaged guest APIs and CI build/run coverage; guest-side GenStatem, GenEvent, and Supervisor libraries remain follow-up work.
- Named GenServer registration is node-local and requires an explicitly supplied runtime context for embedding. Cluster-quorum/global naming and name-based reconstruction of a typed handle are outside the synchronous adapter.

## Validation Scope

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| `cargo test --all` | Current unit and integration contracts exercised by the repository | Untested production paths, performance, scale, or multi-host behavior |
| `cargo test -p lunatic-runtime --test wasm_link_death` | Actual-Wasm and native normal/failure/panic/kill/missing-process link and reason-preserving monitor semantics, plus Supervisor restart after an immediate guest trap | Cross-node links or every possible host-future suspension point |
| `cargo test -p lunatic-otp-patterns` | Native GenServer/GenStatem/GenEvent mailbox lifecycles, local named registration, and automatic Supervisor monitor/restart/escalation behavior | Guest-side supervisor trees, global naming, or distributed recovery |
| `cargo test -p lunatic-runtime --test otp_guest_wasm` | Actual-Wasm OTP1 cast, correlated call/reply, timeout, and acknowledged stop over production message imports | Packaged language SDK ergonomics, guest-side Supervisor/GenStatem/GenEvent libraries, or cross-node OTP |
| TLS listener credential tests | Version-2 address-plus-handle serialization, scoped/single-use/expiry/capacity behavior, stable fail-closed errors, provider-backed rebind, legacy rejection, and live listener transfer without provider lookup | Persistent-provider implementation, host restart, cross-node restoration, or elimination of guest-memory duplicates |
| TLS bind production-import test | The const guest key-input range remains reusable on the exercised invalid-input path, the temporary host copy is zeroized by implementation, and the audit record omits the marker | Guest-managed erasure, `MemorySnapshot` key absence, successful-bind crash-dump/remanence guarantees, or distributed credential delivery |
| Registry/QUIC + guest integration tests | Real localhost mTLS transport, framing, quorum behavior, two actual Wasm guest lookup-to-mailbox request/reply, typed missing-target errors, and owner cleanup | Cross-host deployment, cross-environment handles, partitioned guest owner exit, distributed process recovery |
| Criterion mailbox benchmark | Local queue-operation cost for its configured workload | End-to-end process message latency or backpressure |
| Criterion hot-reload benchmark | Manual compile/instantiate/snapshot/restore component cost | A live running process receiving, acknowledging, committing, or rolling back a reload |
| Memory projections | Arithmetic estimates based on measured component sizes | Demonstrated million-process capacity or sustained workload stability |
| Cloud CLI credential lifecycle tests | Production config/login/request/logout flow around an injected protected-store boundary, including plaintext migration and fail-closed errors | A configured/unlocked native store in a representative macOS, Windows, or Linux user session, or remote session revocation |

The reviewed working tree must pass the complete local Rust build/test/lint/format gate and the repository's Linux and Windows GitHub Actions checks. Historical green runs do not establish the current audit, quota, reload, or Windows behavior; the exact reviewed commit and CI run are the acceptance evidence.

The benchmark documents under `docs/benchmarks/` preserve an October 2025 measurement snapshot, but the original record omitted the tested commit and exact hardware/toolchain profile. It is therefore not a reproducible baseline. Future values must be quoted with commit, dirty state, hardware, toolchain, workload, and evidence boundary; they are not release certification by default.

## Documentation Policy

- `CORE_VALUES.md` defines goals and decision principles.
- This file is the canonical current status and evidence boundary.
- Historical phase/completion reports are archival context. Any unqualified completion language in them is superseded by this file.
- README feature claims must link here and use the same completion rule.
- A future completion change must name the production entry point, executable test, reviewed commit, and unsupported cases.

## Prioritized Follow-Ups

1. Add an operator-selectable required/fail-closed durable audit sink and health endpoint, then extend the typed inventory to any remaining privileged host boundaries.
2. Add a provider-handle TLS guest ABI so private-key bytes need not enter Wasm memory, and define equivalent contracts for intentional distributed credential delivery.
3. Add receiver-owned distributed capability/ceiling policy, filesystem mapping/allowlists, and close the local path check/open authority boundary.
4. Add an environment-aware global guest handle/send API, then exercise partitioned owner exit and node-failure recovery through the combined guest path.
5. Measure live hot-reload latency and formalize the guest entrypoint-reentry/checkpoint contract.
6. Package the OTP1 contract as equivalent Rust, TinyGo, and AssemblyScript guest APIs, including guest-side Supervisor/GenStatem/GenEvent libraries, and build/run representative scenarios in CI.
7. Add aggregate host/cluster budgets, scale/soak coverage, and final production-readiness gates.

## Related Resources

- `CORE_VALUES.md` — design principles and target metrics.
- `docs/benchmarks/BENCHMARK_RESULTS.md` — historical component measurements with scope limitations.
- `docs/security/CAPABILITY_ATTENUATION.md` — process capability and resource-ceiling inheritance contract.
- `docs/tls/TLS_LISTENER_CREDENTIALS.md` — version-2 TLS listener credential, legacy-artifact, and guest key-input boundary.
- `docs/tls/TLS_STREAM_MIGRATION.md` — live-transfer and serialized active-stream boundary.
- `examples/MULTI_LANGUAGE_GUIDE.md` — language example guide with support boundaries.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` — archived scorecard; this file supersedes its status claims.
