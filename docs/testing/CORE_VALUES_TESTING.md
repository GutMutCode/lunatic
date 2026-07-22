# Core Values Verification

**Status**: Active
**Last reviewed**: 2026-07-22

This guide maps core-value claims to executable production-path evidence. A passing
test is evidence only for the path it actually runs. Source inspection, printed
checklists, fixed delays, and in-memory simulations are not compliance tests.

## Required Quality Gate

Run these commands from the repository root:

```bash
cargo fmt -- --check
cargo clippy --examples --tests --benches -- -D warnings
cargo test --all
```

GitHub Actions runs the same formatter, Clippy, and workspace test commands on
Linux, macOS, and Windows. Linux CI also executes Criterion benchmarks and
enforces the configured thresholds in `scripts/check_bench_thresholds.py`.

## Executable Evidence

| Area | Production path exercised | Command |
| --- | --- | --- |
| Wasm failure propagation | Actual `spawn_wasm` normal/trap/host-panic/active-kill/missing-process exits, default and trapping-exit peers, monitor deduplication, native-runner parity, and Supervisor replacement after an immediate guest trap | `cargo test -p lunatic-runtime --test wasm_link_death` |
| OTP runtime adapters | Native GenServer/GenStatem/GenEvent mailbox lifecycles including one-worker synchronous handles, local named GenServer registration/cleanup, and automatic Supervisor monitoring of immediate/ordinary exits, restart, shutdown, and escalation | `cargo test -p lunatic-otp-patterns` |
| Guest-Wasm OTP wire contract | Rust-encoded probe plus actual Wasm client/server cast, correlated reply envelope, timeout, and acknowledged stop over the production message imports | `cargo test --test otp_guest_wasm` |
| Hot reload component harness | Two manually orchestrated Wasmtime modules, memory snapshot, replacement module behavior and restored state | `cargo test -p lunatic-process --test hot_reload_integration` |
| Live Wasm hot reload | Watch-equivalent compile/register/broadcast through `Signal::HotReload` and the execution driver; process identity, compatible memory, FIFO mailbox state, signature rejection, instantiation fallback, and rollback | `cargo test -p lunatic-runtime --test live_hot_reload` |
| Live TLS hot reload | Real TLS handshake, live resource transfer, preserved resource ID/timeouts, post-transfer traffic | `cargo test -p lunatic-runtime --lib hot_reload_transfers_live_tls_stream_with_id_and_timeouts` |
| Live TLS listener hot reload | Bound listener and configured acceptor move through the production same-process resource-transfer path without provider access | `cargo test -p lunatic-runtime --lib hot_reload_transfers_live_tls_listener_without_provider_lookup` |
| Serialized TLS listener credentials | Environment/process-scoped handles, five-minute default, single-use/expiry/capacity behavior, stable secret-free failures, private-key-free snapshot bytes, multi-listener preflight rollback, and real TLS traffic after provider-backed rebind | `cargo test -p lunatic-runtime --lib tls_credentials::tests`<br>`cargo test -p lunatic-runtime --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes`<br>`cargo test -p lunatic-runtime --lib state::tests::missing_tls_credential_fails_before_any_resource_mutation`<br>`cargo test -p lunatic-runtime --lib state::tests::later_tls_preflight_failure_rolls_back_prepared_listener_and_lease`<br>`cargo test -p lunatic-runtime --lib state::tests::expired_tls_credential_fails_closed_with_stable_error`<br>`cargo test -p lunatic-runtime --lib state::tests::tls_provider_failure_is_stable_and_secret_free` |
| Versioned TLS listener snapshot | Version-2 local-address-plus-opaque-handle structure, absence of a raw-key marker, fail-closed unversioned/unknown-version decoding, and redacted debug output | `cargo test -p lunatic-runtime --test tls_resource_migration`<br>`cargo test -p lunatic-process --lib resource_migration::tests` |
| TLS bind key-input handling | Production import preserves the const guest buffer needed for address fallback, while the implementation zeroizes its temporary host PEM copy and emits a marker-free typed audit event | `cargo test -p lunatic-runtime --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once` |
| Serialized TLS boundary | Explicit refusal to restore active serialized TLS streams and metadata-only contracts | `cargo test -p lunatic-runtime --test tls_stream_migration_contract` |
| Global registry and peer identity | Real localhost mTLS QUIC nodes, concurrent registration, quorum blocking, partition recovery, resynchronization, and rejection of forged leader notifications and snapshots without state mutation | `cargo test -p lunatic-distributed --test registry_coordination` |
| Distributed response origin | Full node servers retain the expected authenticated node on a spawn waiter, reject a matching response ID from another certificate without consuming the waiter, and accept the intended peer | `cargo test --test distributed_registry_e2e spawn_response_waiter_accepts_only_the_intended_mtls_peer` |
| QUIC framing | Production chunk framing, multi-chunk reassembly, MessagePack decode and dispatch callback | `cargo test -p lunatic-distributed --test quic_transport` |
| Host import inventory | Runtime linker exports compared with `wat/all_imports.wat` | `cargo test -p lunatic-runtime --test imports_match` |
| Resource limits | Runtime memory, table, file and network limit contracts | `cargo test -p lunatic-runtime --test resource_limits` |
| Cloud CLI credential lifecycle | Injected protected-store boundary plus production CLI/config/request paths for login, sanitized cookie reuse, local expiry, redirect refusal, persistent worker affinity, verified tombstone logout/retry, linked-config rejection, unavailable-store failure, legacy plaintext migration, and secret-marker absence | `cargo test --bin lunatic mode::credential_store::tests`<br>`cargo test --bin lunatic mode::config::tests`<br>`cargo test --bin lunatic mode::login::tests`<br>`cargo test --bin lunatic mode::execution::tests::logout_subcommand_parses` |

The full workspace command is the release gate because it also catches
cross-crate compilation and integration failures that targeted commands can miss.

## Performance Evidence

Correctness tests do not assert wall-clock performance. Criterion owns performance
measurement:

```bash
cargo bench --bench spawn
cargo bench --bench messaging
cargo bench --bench distributed_messaging
cargo bench --bench distributed_latency
python scripts/check_bench_thresholds.py
```

`distributed_messaging` opens a real mTLS QUIC connection and measures the
production framing/dispatch boundary. It does not prove delivery into a live
remote process mailbox.

## Removed False Signals

The former `tests/core_values_compliance.rs` and
`tests/core_values_performance.rs` files were removed. They mainly asserted
constants, printed implementation claims, or timed sleeps and in-memory stand-ins;
they did not exercise the production behavior they claimed to certify.

The obsolete `node_failure.rs` and `cross_node_hot_reload.rs` distributed tests
were also removed because they targeted APIs that no longer compile and described
mock scenarios as a live cluster. Current distributed evidence is documented in
`DISTRIBUTED_TESTING.md`.

No numeric “core values score” is generated. The canonical claim inventory,
including known gaps, is `docs/core_values/status.md`.

## Known Gaps

- `distributed_messaging` and the QUIC transport E2E stop at decoded request
  dispatch. `distributed_latency` separately measures a persistent two-node
  path from global-registry lookup through confirmed QUIC delivery into a live
  native-process mailbox and a confirmed reply; it excludes guest-Wasm host
  calls, multi-host deployment, and partitions.
- Cluster-wide process, memory, and network quota stress tests are not present.
- Cross-node hot reload and node-crash recovery do not yet have production-path
  E2E coverage.
- New guests can keep listener private-key bytes out of Wasm by using
  `tls_bind_with_credential`. Actual-Wasm tests cover bind, accept, TLS traffic,
  fail-closed capability and scope checks, and marker-free guest memory and
  audit output. Deprecated raw-key `tls_bind` remains and cannot establish
  key-free guest memory; raw distributed CA/signing imports also remain an
  unresolved privileged boundary.
- Version-2 listener snapshots contain the local address plus an opaque handle
  and can rebind while that scoped handle remains resolvable. The default
  provider is process-local and does not survive restart; persisted recovery
  requires an injected reprovisioning provider. Same-runtime listener and
  stream transfer is tested, while serialized active streams, restart, and
  cross-node restoration remain unsupported.
- Cloud CLI lifecycle tests inject a deterministic fake credential store. CI compiles each native
  provider, but it does not prove that Keychain, Credential Manager, or Secret Service is available
  and unlocked in a representative user session; release validation needs platform smoke tests.
- `tests/multilanguage_guest_e2e.rs` verifies pinned Rust, TinyGo, and
  AssemblyScript compiler artifacts against process spawn, tagged messaging,
  timeout, and permission-denial imports. These are low-level compatibility
  fixtures, not stable SDK packages.
- OTP1 remains a low-level wire contract. Packaged Rust, TinyGo, and
  AssemblyScript OTP adapters, guest-side Supervisor/GenStatem/GenEvent
  libraries, global named GenServers, and distributed supervision remain
  follow-up work.

## Adding Evidence

A new completion claim should include:

1. The production entry point being exercised.
2. Deterministic setup and bounded waits for asynchronous behavior.
3. A failure assertion that would catch a disconnected implementation.
4. A command that CI actually runs.
5. A documented limitation when the test stops before the user-visible outcome.
