# TLS stream migration implementation summary

**Implemented**: 2026-07-22

**Status**: In-process listeners/streams use live transfer; version-2 snapshots support provider-backed listener rebind and explicitly reject active-stream restoration

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
    resource-table mutation;
  - the default provider is process-local, five-minute, single-use, and capped at 1,024 entries, while
    `new_with_tls_credential_provider` allows explicit restart/persistence reprovisioning;
  - serialized TLS stream restoration returns an explicit error before partial restoration.
- `src/tls_credentials.rs`
  - defines the non-serializable credential material, scoped provider boundary, stable secret-free
    errors, and default ephemeral provider.
- `crates/lunatic-networking-api/src/tls_tcp.rs`
  - builds the listener's rustls configuration at `tls_bind` time;
  - preserves the const guest private-key PEM input needed for SDK address fallback and zeroizes
    its temporary host byte copy.
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

The listener snapshot contract does not claim that the guest never held a key. `tls_bind` must
preserve its const guest input for SDK address fallback and zeroizes only its temporary host
raw-byte copy; the original guest buffer, guest duplicates, and intentional distributed credential
delivery remain outside this guarantee. Eliminating raw key input from Wasm requires a future
provider-handle guest ABI.

## Validation

```bash
cargo test --lib state::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts
cargo test --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes
cargo test --lib state::tests::missing_tls_credential_fails_before_any_resource_mutation
cargo test --lib tls_credentials::tests
cargo test --test tls_stream_migration_contract
cargo test --test tls_resource_migration
cargo test -p lunatic-process --lib
cargo test --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

The live test performs a real TLS handshake and proves continued I/O after moving the exact
stream into a replacement process state while preserving its guest-visible ID and timeout values.
The listener evidence separately proves live transfer without provider access, provider-backed
serialized rebind without private-key bytes, scoped/single-use/expiry/capacity failure behavior,
version rejection, const guest-input preservation, and audit-marker absence.
