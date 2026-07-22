# TLS stream migration implementation summary

**Implemented**: 2026-07-22

**Status**: Provider-handle guests and version-2 snapshots avoid guest key bytes; in-process
listeners/streams use live transfer and serialized active-stream restoration remains unsupported

The authoritative contracts are [TLS stream migration](./TLS_STREAM_MIGRATION.md) and
[TLS listener credentials](./TLS_LISTENER_CREDENTIALS.md).

## Implementation

- `crates/lunatic-process/src/lib.rs`
  - hot reload calls `transfer_runtime_resources_to` after restoring Wasm memory and mailbox state;
  - the replacement instance is installed only after transfer succeeds;
  - an error restores the old instance instead of swallowing resource restoration failure.
- `crates/lunatic-process/src/state.rs`
  - `ProcessState` exposes the live-resource transfer contract and a snapshot-based default.
- `src/state.rs`
  - `DefaultProcessState` moves live network `HashMapId` maps into the replacement state;
  - resource IDs, next-ID seeds, TLS sessions, TCP sockets, timeouts, and resource accounting survive;
  - production in-process listener transfer moves the bound listener and `TlsAcceptor` without a
    provider lookup;
  - serialized TLS listeners are preflighted through an environment/process-scoped provider before
    resource-table mutation, and guest handle binding adds the process capability check;
  - provider provision/take/revoke calls catch unwinds and reduce them to stable provider failure
    (including the guest error/audit contract where applicable), without allowing the unwind across
    the state boundary;
  - partial snapshot provisioning revokes earlier handles, and any rollback-revoke failure is
    promoted to stable provider failure instead of reporting successful cleanup;
  - the default provider is process-local, five-minute, single-use, explicitly revocable, and
    capped at 1,024 entries, while `new_with_tls_credential_provider` allows explicit
    restart/persistence reprovisioning;
  - serialized TLS stream restoration returns an explicit error before partial restoration.
- `src/tls_credentials.rs`
  - defines the non-serializable credential material, scoped provider boundary, stable secret-free
    errors, explicit revocation, and default ephemeral provider.
- `crates/lunatic-networking-api/src/tls_tcp.rs`
  - registers `lunatic::networking::tls_bind_with_credential` with address fields, an output
    pointer, and low/high little-endian `u64` handle halves;
  - performs a non-consuming `can_use_tls_credential_handles` precheck before quota reservation,
    then derives environment/process scope from the caller and performs the scoped single-use take;
  - audits capability as `denied`/`capability_denied`, unavailable or expired provider state as
    existence-hiding `denied`/`policy_denied`, provider/OS failure as
    `failed`/`runtime_failure`, and quota as `denied`/`resource_limit`, without reading guest key
    bytes;
  - builds the listener's rustls configuration at `tls_bind` time;
  - retains deprecated raw `tls_bind` for compatibility, preserving the const guest private-key PEM
    input needed for SDK address fallback and zeroizing its temporary host byte copy.
- `crates/lunatic-process-api/src/lib.rs` and `src/config.rs`
  - expose the default-denied `can_use_tls_credential_handles` configuration capability;
  - create children without that authority and reject parent or distributed-receiver escalation.
- `crates/lunatic-networking-api`
  - misleading reconnect names were replaced by metadata names:
    `TlsClientConnectionMetadata`, `client_metadata`, and `with_client_metadata`.
- `crates/lunatic-process/src/resource_migration.rs`
  - version-2 TLS listener entries contain only a local address and opaque 128-bit credential
    handle;
  - unversioned, missing-version, and unknown-version snapshots fail closed without an automatic
    legacy importer;
  - serialized variants are named `TlsClientConnectionMetadata` and
    `TlsServerConnectionMetadata`;
  - `ResourceTransferReport` records live-resource transfer counts.

## Deliberate non-feature

Lunatic does not establish a fresh TCP/TLS connection and attach it to the old guest handle.
Doing so would substitute a new application byte stream without knowing request, response,
authentication, multiplexing, or replay state.

Serialized stream metadata remains available for diagnostics, but it is not accepted as sufficient
active-stream restoration state. Serialized listener recovery is a rebind with provider-supplied
credentials, not preservation of the old socket or resource ID. Cross-process active-stream
recovery still needs a separately designed application-level protocol.

The key-free guest-memory claim applies to `tls_bind_with_credential`: its Wasm signature contains
no certificate or key pointer, and the runtime receives only the handle's little-endian scalar
halves. It does not prove absence of an independently delivered key or host-side key material.

Legacy `tls_bind` remains deprecated for compatibility. It preserves its const guest input for SDK
address fallback and zeroizes only its temporary host raw-byte copy, so its original buffer and
duplicates remain outside the key-free guarantee. SDK/import manifests must move to the new ABI,
operators must rotate keys previously exposed to Wasm, and legacy removal requires a coordinated
compatibility release.

Provider handles are non-delegable process/node-local locators. Distributed spawn/message copying
fails closed because the destination lacks the source process scope and provider entry. The raw
`lunatic::distributed` CA/signing imports are a different privileged compatibility boundary;
production `test_root_cert` removal and migration to an explicit signer capability backed by a
non-exportable provider/HSM are tracked in Hanary #1704.

The unwind boundary is not a logging sandbox. Rust's process-global panic hook executes before
`catch_unwind` returns and can emit a provider panic payload to stderr or another logger. Providers
must not panic, panic payloads must omit handles/keys/credential detail, and embedders must protect
and configure panic-hook output. Provider-owned logging and panic-hook output are outside the
key-free log claim; normally returned provider errors and Lunatic-generated guest errors, logs, and
typed audit records remain secret-free.

## Validation

```bash
cargo test --lib state::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts
cargo test --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes
cargo test --lib state::tests::missing_tls_credential_fails_before_any_resource_mutation
cargo test --lib state::tests::panicking_tls_provider_take_is_contained_during_serialized_restore
cargo test --lib state::tests::snapshot_rollback_failure_returns_stable_provider_failure
cargo test --lib tls_credentials::tests
cargo test --test tls_stream_migration_contract
cargo test --test tls_resource_migration
cargo test --test tls_provider_guest
cargo test --test imports_match
cargo test -p lunatic-process --lib
cargo test --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

The live test performs a real TLS handshake and proves continued I/O after moving the exact
stream into a replacement process state while preserving its guest-visible ID and timeout values.
The listener evidence separately proves live transfer without provider access, provider-backed
serialized rebind without private-key bytes, scoped/single-use/revocation/expiry/capacity failure
behavior, provider unwind containment and rollback-failure promotion, version rejection,
provider-handle Wasm bind/accept/TLS traffic, capability-before-quota ordering, key-free guest
memory, stable existence-hiding audit classification, legacy const-input preservation, and
audit-marker absence. The panic test establishes stable failure and containment of the unwind, not
redaction or suppression of process-global panic-hook output.
