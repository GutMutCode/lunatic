# Core Values Compliance Status

Reviewed: 2026-07-23

Reviewed implementation baseline: `f62e5c5`; the Hanary #1667 evidence is bound to the current checkout SHA recorded by CI

This is the canonical implementation-status document for `CORE_VALUES.md`. Design documents, phase reports, examples, and historical benchmark reports may describe intent or component work, but they do not override the status here.

An item is complete only when both conditions hold:

1. The production path is connected from its public/runtime entry point to the intended outcome.
2. An executable test exercises that same path and asserts the outcome, including relevant failure behavior.

Data structures, serialization tests, callback tests, loopback transport tests, manually orchestrated component harnesses, and documentation examples are useful evidence but are not end-to-end completion by themselves.

## Status at a Glance

| Focus | Status | Current evidence boundary |
| --- | --- | --- |
| Fast | Partial | Wasmtime preemption primitives and live process-local hot reload are production-path tested. A bounded 16-process adversarial gate records live commit/rollback distributions, the native two-node path is benchmarked, and a sixteen-sample actual guest-Wasm loopback path records average/p50/p95/p99/rate; portable hot-reload and sub-µs messaging objectives remain unverified. |
| Robust | Partial | Wasm isolation, actual-Wasm link/monitor exit semantics, and acknowledged atomic reload/rollback/in-doubt behavior are production-path tested. Cross-node recovery remains incomplete. |
| Scalable | Partial | Process admission, mailboxes, signals, message allocations, and guest-visible network handles have finite quotas and pressure tests. An actual-Wasm `[1, 8, 32]` curve with one discarded warm-up plus five measured batches per population, 16 echo rounds per measured batch, committed 64KiB Wasm bytes per guest, and an 80-lifecycle bounded soak exercise those paths; large, longitudinal, and cluster-scale behavior remains unverified. |
| Language Independence | Partial | Rust, TinyGo, and AssemblyScript compiler output now passes one shared process/message/timeout/permission E2E in CI. These low-level fixtures are not equivalent published guest SDKs, and broader host APIs remain uncovered. |
| Security Through Isolation | Partial | Process configs default to denied capabilities and enforce non-increasing child authority. The provider-handle guest TLS ABI and version-2 listener snapshots exclude raw private-key bytes, Cloud CLI credentials use native protected stores, and typed/redacted V1 audit events cover major privileged boundaries, but deprecated raw TLS and distributed CA/signing compatibility imports remain, complete resource accounting and receiver path policy remain open, and audit delivery is not durable/required. |
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
receiver-selected authority, cross-node recovery, and large/longitudinal scale
calibration are still missing.

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
- `tests/wasm_scale_soak.rs` runs actual Wasm populations `[1, 8, 32]`, discards one timing warm-up and then measures five batches per population, performs 16 echo rounds per measured batch, records spawn-batch and mailbox throughput/rate plus spawn/readiness/echo p50/p95/p99, tracks 64KiB committed Wasm bytes per guest, and separately records one pre-guest Linux process-wide RSS baseline followed by cumulative live populations 1/8/32. Each delta divided by live guest count is explicitly non-allocator-attributable. It still proves exact N+1 admission denial, mailbox-eight saturation and reuse without sibling blockage, a two-page memory ceiling, and ten cycles/80 process lifecycles with zero registered-process leakage.
- `tests/live_hot_reload_scale.rs` runs ten alternating commit/rollback rounds across sixteen live Wasm processes with full bounded mailboxes. It preserves 1,280 FIFO messages, rejects 160 deliberate overflows, retains process/version ownership, rejects false success or `InDoubt`, and records separate commit/rollback p50/p95/p99 plus a workload summary. Its 250/500ms bounds are CI runaway guards, not portable product claims.
- The distributed QUIC benchmark uses real loopback mTLS, production framing, reassembly, MessagePack decoding, and the request-dispatch boundary.
- The distributed latency suite also measures a persistent two-node production path from global-registry lookup through confirmed QUIC delivery into a live native-process mailbox and a confirmed live-mailbox reply (`benches/distributed_latency.rs`), and the actual guest-Wasm production path now has 16 sequential registry lookup → loopback mTLS QUIC → remote live mailbox → reply round trips with average/p50/p95/p99/rate evidence.

### Boundary

- The mailbox benchmark measures local mailbox operations, not a sender-to-live-receiver process round trip.
- The `distributed_messaging` transport benchmark stops at decoded request dispatch. The Criterion live-mailbox benchmark reaches native processes but does not include guest-Wasm host calls; the separate sixteen-sample guest-Wasm timing fixture adds those calls but still excludes multi-host deployment and partitions.
- Historical spawn, mailbox, and component-reload measurements do not establish current production latency guarantees.
- Each independently constructed Wasmtime engine has an epoch ticker. The new fixed-round gates exercise local scheduler progress and live reload under bounded pressure, but they do not establish fairness or latency under sustained large-scale or multi-host load.
- Reload cancels the current Wasmtime call and re-enters the same export on the replacement module. It preserves compatible linear memory and host-owned state, not the interrupted instruction pointer, native stack, private globals, or tables; modules therefore need a compatible entrypoint-reentry contract.
- The bounded scale gate stops at 32 concurrent guests and 80 lifecycles; it measures process-wide RSS rather than allocator/reachable heap per process. Sustained large-scale scheduler throughput and cluster-wide memory/network limits remain uncovered.
- A Sunday/manual two-hour population-32 soak workflow now exists with exact cleanup and a 512MiB process-wide RSS-growth guard; version-tag publication depends on the same reusable workflow. Verified two-hour soak: [GitHub Actions run 29984715567](https://github.com/GutMutCode/lunatic/actions/runs/29984715567) at commit `30b0e1b11bada8bdefca848dee77b5b6aa180cc6`. The retained run completed its requested 7,200 seconds with four echo rounds per cycle, 164,745,248 process lifecycles, 658,980,992 mailbox echo samples, zero registrations after shutdown, and 5,881,856 bytes of process-wide RSS growth under the guard; it remains bounded single-host evidence rather than multi-day or cluster coverage.

## 2. Language Independence via WebAssembly

### Verified components

- Host subsystems expose language-neutral Wasm imports, and `tests/imports_match.rs` checks registrations against `wat/all_imports.wat`.
- Rust `1.94.0`, Go `1.25.5` with TinyGo `0.41.1`, and AssemblyScript `0.27.37` source fixtures compile from a clean checkout in the `multilanguage_guests` CI job.
- `tests/multilanguage_guest_e2e.rs` loads each compiler artifact through the production linker and verifies same-module process spawn, a tagged `41 -> 42` round trip, bounded timeout status `9027`, and an attenuated child's spawn denial with a guest-readable error handle. CI also invokes every fixture through the current `lunatic run` interface.
- `examples/MULTI_LANGUAGE_GUIDE.md` links each verified matrix cell to its fixture, integration oracle, and CI gate and records the exact Wasm/WASI boundary.

### Boundary

- The verified adapters intentionally cover a narrow raw host-ABI slice. They do not demonstrate equivalent access to the complete Lunatic runtime API or provide stable application-facing packages.
- Rust OTP examples still use host-side `lunatic-otp-patterns`. The actual-Wasm OTP1 fixture proves the low-level wire path, not a packaged Rust guest SDK.
- Go and AssemblyScript OTP samples are manual pattern simulations, not runtime adapters.
- TinyGo is certified only as a WASI Preview 1 command entered through `_start`; WASI Preview 2/components, arbitrary reactor child exports, guest threads, and goroutine equivalence are outside the matrix. Rust and AssemblyScript fixtures are core Wasm modules using direct Lunatic imports.

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
- Version-2 `ResourceMigrationSnapshot` TLS listener entries serialize only a local address and an opaque 128-bit handle, never a certificate or raw private key. Provider access is authorized against both environment and process identity. The default provider is process-local, five-minute, single-use, explicitly revocable, and capped at 1,024 unexpired entries; missing, revoked, expired, wrong-scope, capacity, and provider-error paths fail closed with stable secret-free errors. Provision/take/revoke unwinds are caught and reduced to provider failure, serialized restore uses that common boundary, and a failed rollback revoke during multi-listener snapshot capture promotes the overall error to provider failure. This unwind guarantee does not suppress the process-global panic hook. Unversioned, missing-version, unknown-version, and trailing-data snapshots are rejected without an automatic importer (`crates/lunatic-process/src/resource_migration.rs`, `src/tls_credentials.rs`, `src/state.rs`).
- `lunatic::networking::tls_bind_with_credential` accepts address fields, an output pointer, and low/high little-endian `u64` halves of the opaque handle, with no guest certificate or private-key range. It performs a non-consuming, default-denied `can_use_tls_credential_handles` check before quota reservation and revalidates capability plus exact host-derived environment/process scope at consumption; children do not inherit that capability and delegation/receiver validation is non-increasing. Capability denial audits as `denied`/`capability_denied`, unavailable or expired provider state as existence-hiding `denied`/`policy_denied`, and provider failure as `failed`/`runtime_failure`. Actual guest Wasm tests cover bind, accept, TLS traffic, stable secret-free error-resource results, capability-before-quota behavior, and key-marker absence from guest memory snapshots and audit output (`crates/lunatic-networking-api/src/tls_tcp.rs`, `crates/lunatic-process-api/src/lib.rs`, `src/config.rs`, `tests/tls_provider_guest.rs`).
- Deprecated raw-key `tls_bind` remains for compatibility. Its host path zeroizes its temporary host PEM copy while preserving the const guest input required by SDK multi-address fallback. The production-import test checks that ABI behavior and audit-event redaction on an invalid-key path; it does not establish key-free guest memory (`crates/lunatic-networking-api/src/tls_tcp.rs`, `tests/capability_attenuation.rs`).
- TLS credential handles are process/node-local and are not transferable authority through distributed spawn or messages. Copying their scalar bits to another process supplies neither the original scope nor provider entry and fails closed. The raw distributed CA/signing imports are a distinct privileged compatibility boundary rather than an extension of the listener provider.
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
- Node-control bearers use an explicit zeroizing wire envelope and a non-Clone/non-Serde runtime
  secret. The active Axum server stores only SHA-256 verifiers, compares them in constant time,
  uses generation-CAS two-phase refresh so response loss cannot strand a node, rejects duplicate
  live UUID registration, revokes on stop, removes stopped records, and expires crashed-node leases.
  Its HTTP listener is loopback-only; the client validates every endpoint against the registration
  origin, disables redirects and proxies, caps responses, and returns stable secret-free errors.
  Actual TCP tests cover Host poisoning, forged endpoints, redirect/authenticated-proxy refusal,
  lost refresh acknowledgement, cancelled shutdown, cache controls, and normal lifecycle
  (`crates/lunatic-control`, `crates/lunatic-control-axum`,
  `crates/lunatic-distributed/src/control/client.rs`, `tests/node_control_security.rs`).

### Boundary

- Filesystem preopens are canonicalized and attenuated locally, but path replacement still has a check/open race. Remote preopens are unavailable until a receiver-controlled path mapping/allowlist is implemented; this is a functionality gap, not an ambient-authority fallback.
- Distributed receivers apply the fixed default capability and resource ceiling, but operators cannot yet select a stricter per-node policy; fuel has no receiver-owned finite ceiling. Identity-bound certificates fail closed across legacy/new mixed data planes, so rollout requires the control plane first and coordinated node endpoint recreation. Membership removal is enforced after each node refreshes its topology view.
- Every guest-visible TCP/TLS listener or stream and UDP socket owns a paired FD/network lease, and DNS/address iterators are finite. General WASI file handles and network destination policy are not connected to those limits.
- Local actor ingress and resource transfer are bounded, but aggregate host/cluster memory, CPU, disk, and remote-transport budgets are not closed by one global policy.
- Audit delivery is best-effort and fail-open: a dedicated writer uses a bounded 1,024-record queue, drops newest on saturation, opens a health circuit on sink failure, exposes counters, and waits at most 250 ms at graceful shutdown. The default sink ends at the enabled Rust `log` facade; it does not prove disk/remote persistence, action-plus-audit atomicity, retention, or tamper evidence. An explicit `RUST_LOG` can still disable `audit=info`, and remaining non-privileged host calls are outside the event inventory.
- New workloads can keep listener private keys out of Wasm by using `tls_bind_with_credential`, but SDK/import migration is not complete and deprecated raw-key `tls_bind` still permits the original or duplicate PEM bytes to appear in `MemorySnapshot`. Removing that import requires a coordinated compatibility release, inventory and rotation of previously exposed keys, and secure retention handling for old memories/snapshots/backups. The production `lunatic::distributed::test_root_cert` import and raw `default_server_certificates`/`sign_node*` CA-key interfaces remain a separate unresolved privileged boundary; Hanary #1704 tracks removal or replacement with an explicit signer capability and a non-exportable host/HSM provider.
- TLS provider implementations must not panic and panic payloads must contain no handles, keys, certificates, credential material, or provider detail. `catch_unwind` stabilizes the runtime result but the process-global panic hook runs first and can emit to stderr/logger. Provider-owned logging and panic-hook output are outside the key-free log guarantee; embedders must configure/redact and protect those channels. Normally returned provider errors and Lunatic-generated guest errors, logs, and typed audit records remain secret-free.
- The running-Wasm reload transaction invokes the same-runtime live-resource transfer after its fallible preparation steps. Exact TLS session/ID preservation is tested at that transfer boundary, not yet by a guest-driven reload E2E. Serialized snapshots, host restarts, and cross-node migration cannot restore active TCP/TLS streams. A TLS listener can be rebound from a version-2 snapshot only while its scoped handle is resolvable; the default provider does not survive restart, a consumed or failed-attempt handle must not be replayed, and persisted/restart recovery requires an explicitly injected reprovisioning provider.
- Cloud CLI tests inject a fake credential store and loopback provider. CI compiles native backends
  on each supported OS, but availability, unlock prompts, desktop-session policy, and Linux session
  D-Bus behavior still require representative platform smoke tests. Logout deletes local material
  only because the provider API exposes no remote revocation endpoint.
- The built-in Axum node-control server deliberately provides loopback HTTP, not a remote HTTPS
  deployment, and enrollment has no bootstrap credential. It assumes a trusted single-user host;
  remote operators need both a trusted HTTPS implementation/termination contract and explicit
  registration admission authentication/authorization.
  `lunatic-control-submillisecond` is excluded, non-publishable, and runtime-quarantined until its
  plaintext SQLite credential model is migrated and independently tested; it is not covered by
  root workspace success.

## 4. Fault Tolerance and High Availability

### Verified components

- Process links and monitors, snapshot/signature components, and reload coordination structures exist.
- `tests/wasm_link_death.rs` exercises the real `spawn_wasm` lifecycle for normal exit, guest trap, host panic, active receive/sleep cancellation, and missing-process link/monitor behavior. It verifies exact link reasons and tags, default peer termination, trapping-exit survival, one reason-preserving monitor notification, removal-before-notification ordering, native-runner parity, and three crash-driven Supervisor replacements before a stable fourth actual-Wasm start.
- Global registration crosses runtime mTLS QUIC control paths on localhost. Multi-endpoint tests cover one-winner contention, quorum waiting, partition recovery repeated three times against fresh production-transport clusters, resynchronization, rejection of a node-2 certificate claiming node 1 for leader notifications, and rejection of a node-2-authenticated synchronization snapshot while node 3 expects coordinator node 1. The latter proves processing with a same-stream response marker before a legitimate node-1 snapshot completes the preserved waiter (`crates/lunatic-distributed/tests/registry_coordination.rs`).
- `tests/distributed_registry_e2e.rs` runs actual Wasm guests on full node servers. It covers guest registration and replica lookup, confirmed request/reply through live cross-node mailboxes, missing environment/process errors, explicit removal, cross-environment fail-closed lookup, and owner-exit cleanup on both replicas. A three-node mTLS case also proves that a spawn waiter expecting node 2 rejects a matching response ID from node 3 without consuming the waiter, then accepts node 2's response. The current production path also includes the 16 sequential registry lookup → loopback mTLS QUIC → remote live mailbox → reply round-trip evidence.
- Confirmed sends correlate `Sent`/typed error responses before reporting success, preserve bounded waiter/outbound accounting on timeout or cancellation, and distinguish missing environments, missing processes, receiver backpressure, and oversized/rejected delivery. Owner cleanup retains a bounded retry record with backoff until quorum coordination succeeds, and slow-consumer fairness now emits 16-sample evidence.
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
- `tests/otp_guest_wasm.rs` runs an actual Wasm client and server using the language-neutral OTP1 envelope over existing bounded message imports. It verifies a Rust-encoded contract probe, cast, correlated reply envelopes, timeout status, and acknowledged graceful stop. The live production fixture is the 16-sequential-round registry lookup → loopback mTLS QUIC → remote live mailbox → reply path with average/p50/p95/p99/rate evidence.
- Coordinated registry messages travel over the production QUIC control transport.

### Boundary

- The automatic Supervisor adapter is host-side and local. Guest-Wasm supervisor trees, cross-node supervision, and distributed BEAM-like recovery remain unverified.
- OTP1 is a low-level, language-neutral guest wire contract, not a complete high-level SDK. Rust, TinyGo, and AssemblyScript now have CI build/run coverage for primitive process and message imports, but they still need packaged OTP1 adapters; guest-side GenStatem, GenEvent, and Supervisor libraries remain follow-up work.
- Named GenServer registration is node-local and requires an explicitly supplied runtime context for embedding. Cluster-quorum/global naming and name-based reconstruction of a typed handle are outside the synchronous adapter.

## Validation Scope

| Evidence | What it establishes | What it does not establish |
| --- | --- | --- |
| `cargo test --all` | Current unit and integration contracts exercised by the repository | Untested production paths, performance, scale, or multi-host behavior |
| `cargo test -p lunatic-runtime --test wasm_link_death` | Actual-Wasm and native normal/failure/panic/kill/missing-process link and reason-preserving monitor semantics, plus three Supervisor restarts after immediate guest traps and clean stable-child teardown | Cross-node links, distributed supervision, or every possible host-future suspension point |
| `cargo test -p lunatic-otp-patterns` | Native GenServer/GenStatem/GenEvent mailbox lifecycles, local named registration, and automatic Supervisor monitor/restart/escalation behavior | Guest-side supervisor trees, global naming, or distributed recovery |
| `cargo test -p lunatic-runtime --test otp_guest_wasm` | Actual-Wasm OTP1 cast, correlated call/reply, timeout, and acknowledged stop over production message imports | Packaged language SDK ergonomics, guest-side Supervisor/GenStatem/GenEvent libraries, or cross-node OTP |
| `LUNATIC_MULTILANGUAGE_GUESTS_REQUIRED=1 cargo test --test multilanguage_guest_e2e` after the pinned compiler builds | Rust, TinyGo, and AssemblyScript artifacts all exercise process spawn, tagged message round trip, timeout, and child permission denial through registered production imports | Stable/public guest SDKs, full host-API parity, language-level concurrency equivalence, OTP adapters, WASI Preview 2/components, or distributed behavior |
| TLS listener/provider-handle tests | Version-2 address-plus-handle serialization, environment/process/capability scope, capability-before-quota ordering, existence-hiding audit classes, single-use/revocation/expiry/capacity behavior, provider unwind containment and rollback-failure promotion, stable fail-closed errors, actual guest Wasm bind/accept/TLS traffic, key-marker-free guest/resource snapshots and audit output, provider-backed rebind, legacy rejection, and live listener transfer without provider lookup | Panic-hook or provider-owned log redaction, persistent-provider implementation, host restart, cross-node restoration/reissue, host crash-dump/remanence guarantees, provider-specific reconciliation after a failed revoke, or distributed signer migration |
| Legacy TLS bind production-import test | The const guest key-input range remains reusable on the exercised invalid-input path, the temporary host copy is zeroized by implementation, and the audit record omits the marker | Guest-managed erasure, `MemorySnapshot` key absence, successful-bind crash-dump/remanence guarantees, or raw distributed CA/signing imports |
| Registry/QUIC + guest integration tests | Real localhost mTLS transport, framing, quorum behavior, sixteen sequential two-guest Wasm lookup-to-mailbox request/replies with average/p50/p95/p99/rate, typed missing-target errors, and owner cleanup | Cross-host deployment, cross-environment handles, partitioned guest owner exit, distributed process recovery |
| Criterion mailbox benchmark | Local queue-operation cost for its configured workload | End-to-end process message latency or backpressure |
| Criterion hot-reload benchmark | Manual compile/instantiate/snapshot/restore component cost | A live running process receiving, acknowledging, committing, or rolling back a reload |
| Actual-Wasm scale/pressure gate | Live populations 1/8/32, one discarded timing warm-up plus five measured batches per population, sixteen echo rounds per measured batch, host-to-guest-to-native-observer throughput and latency distributions, one pre-guest baseline plus cumulative live-population RSS delta/guest proxy, 64KiB committed Wasm bytes per guest, exact admission/mailbox/memory bounds, sibling progress, and 80 cleanup lifecycles | Allocator-attributable per-process heap, guest-to-guest serialization under pressure, large populations, multi-day/longitudinal stability, or cluster capacity |
| Live-reload scale/rollback gate | Sixteen running Wasm processes preserve full FIFO mailboxes across ten acknowledged commit/rollback operations and report per-run distributions | Portable `<100ms` proof, instruction-pointer continuation, cross-node reload, or multi-day/chaos stability |
| Memory projections | Arithmetic estimates based on measured component sizes | Demonstrated million-process capacity or sustained workload stability |
| Cloud CLI credential lifecycle tests | Production config/login/request/logout flow around an injected protected-store boundary, including plaintext migration and fail-closed errors | A configured/unlocked native store in a representative macOS, Windows, or Linux user session, or remote session revocation |

The reviewed working tree must pass the complete local Rust build/test/lint/format gate and the repository's Linux and Windows GitHub Actions checks. Historical green runs do not establish the current audit, quota, reload, or Windows behavior; the exact reviewed commit and CI run are the acceptance evidence.

The benchmark documents under `docs/benchmarks/` preserve an October 2025 measurement snapshot, but the original record omitted the tested commit and exact hardware/toolchain profile. It is therefore not a reproducible baseline. Future values must be quoted with commit, dirty state, hardware, toolchain, workload, and evidence boundary; they are not release certification by default.

## Documentation Policy

- `CORE_VALUES.md` defines goals and decision principles.
- This file is the canonical current status and evidence boundary.
- Historical phase/completion reports are archival context. Any unqualified completion language in them is superseded by this file.
- README feature claims must link here and use the same completion rule.
- A future completion change must name the production entry point, executable test, reviewed base commit, CI checkout SHA, and unsupported cases.
- `scripts/check_core_value_docs.py` enforces the protected checkboxes, fixed workload counts, evidence symbols, review dates, and stale-claim exclusions. CI feeds it the current Criterion, scale, live-reload, and resilience outputs; the scheduled workflow additionally requires a complete two-hour soak record, so component speed or an incomplete run cannot silently promote a production claim.

## Prioritized Follow-Ups

1. Add an operator-selectable required/fail-closed durable audit sink and health endpoint, then extend the typed inventory to any remaining privileged host boundaries.
2. Migrate supported SDKs/import manifests and deployments to `tls_bind_with_credential`, rotate keys exposed through legacy paths, then remove deprecated raw `tls_bind` in a coordinated release. Hanary #1704 separately tracks removal of production `test_root_cert` and replacement of raw distributed CA/signing imports with a signer capability plus non-exportable host/HSM provider and audit coverage.
3. Add receiver-owned distributed capability/ceiling policy, filesystem mapping/allowlists, and close the local path check/open authority boundary.
4. Add an environment-aware global guest handle/send API, then exercise partitioned owner exit and node-failure recovery through the combined guest path.
5. Measure live hot-reload latency and formalize the guest entrypoint-reentry/checkpoint contract.
6. Build on the now-verified primitive Rust, TinyGo, and AssemblyScript matrix by packaging equivalent OTP1 guest APIs, including guest-side Supervisor/GenStatem/GenEvent libraries.
7. Maintain fresh retained two-hour soak evidence, then expand the bounded local gates into aggregate host/cluster budgets, larger populations, longitudinal CPU/queue telemetry, sustained chaos coverage, and calibrated final production-readiness policies.

## Related Resources

- `CORE_VALUES.md` — design principles and target metrics.
- `docs/benchmarks/BENCHMARK_RESULTS.md` — historical component measurements with scope limitations.
- `docs/security/CAPABILITY_ATTENUATION.md` — process capability and resource-ceiling inheritance contract.
- `docs/tls/TLS_LISTENER_CREDENTIALS.md` — provider-handle guest ABI, version-2 listener credential,
  legacy migration, and distributed credential boundary.
- `docs/tls/TLS_STREAM_MIGRATION.md` — live-transfer and serialized active-stream boundary.
- `examples/MULTI_LANGUAGE_GUIDE.md` — language example guide with support boundaries.
- `docs/core_values/CORE_VALUES_COMPLIANCE_REPORT.md` — archived scorecard; this file supersedes its status claims.
