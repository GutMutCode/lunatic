# TLS stream migration contract

**Status**: Live TLS listeners and streams are supported for in-process Wasm hot reload;
provider-handle guests and version-2 serialized snapshots can bind/rebind listeners without guest
key bytes, but serialized snapshots cannot restore active streams

**Reviewed**: 2026-07-22

**Canonical status**: `docs/core_values/status.md`

The TLS listener credential and legacy-snapshot contract is documented separately in
[TLS listener credentials](./TLS_LISTENER_CREDENTIALS.md).

## Contract

Lunatic has two distinct resource migration paths:

| Path | TLS listener | Active TLS client/server stream | Resource identity |
| --- | --- | --- | --- |
| In-process Wasm hot reload | Live bound listener and acceptor transferred | Live host object transferred | IDs, next-ID seeds, and stream timeouts preserved |
| Version-2 serialized restoration | Rebound from local address plus a scoped opaque credential handle | Explicitly unsupported; metadata only | Listener ID may be reallocated; TLS stream is not restored |

The in-process path is transparent to the guest. The replacement Wasm instance receives the same
host-owned `TlsConnection`, including its TCP socket, rustls session, split reader/writer, timeout
configuration, and `HashMapId` resource ID. No reconnect or new TLS handshake occurs.

The serialized path never claims that descriptive metadata can recreate an active TLS byte stream.
`restore_resource_snapshot` returns an error if a snapshot contains a TLS stream. Serialized TLS
listener restoration is a distinct rebind operation: the snapshot contains no certificate or
private-key bytes and the host provider must authorize and consume its handle.

## In-process hot reload

`perform_pending_reload` performs the following operations:

1. Validate old/new Wasm module compatibility.
2. Snapshot Wasm memory and the message mailbox.
3. Instantiate the replacement Wasm module and restore memory/mailbox state.
4. Call `ProcessState::transfer_runtime_resources_to`.
5. Install the replacement instance only after resource transfer succeeds.

`DefaultProcessState` overrides the transfer method and moves the complete network resource maps.
Moving each `HashMapId` preserves both existing IDs and its next-ID seed. The transfer includes TCP
listeners/streams, TLS listeners/streams, UDP sockets, DNS iterators, and the open-network-resource
counter.

The target state is checked before any move. If replacement creation or transfer fails,
`perform_pending_reload` puts the old instance back instead of leaving the process without an
instance. A custom `ProcessState` implementation must likewise leave its source unchanged when its
transfer method returns an error.

## Serialized snapshot boundary

A version-2 TLS listener entry contains only `local_addr` and an opaque 128-bit
`credential_handle`. The configured provider resolves the handle under the exact process and
environment scope, then the runtime rebinds the listener. The built-in process-local provider uses
five-minute, single-use, revocable handles with a 1,024-entry ceiling; persisted or restart
restoration requires an explicitly injected provider. A failed attempt can consume handles, so
callers must capture a fresh snapshot or securely reprovision fresh handles instead of replaying
it. See
[TLS listener credentials](./TLS_LISTENER_CREDENTIALS.md) for the complete authorization,
fail-closed error, legacy-artifact, and guest/host key-input boundary.

All state-side provider provision/take/revoke calls catch provider unwinds and return stable
provider failure. If capture provisions several listener handles and a later provision fails, it
revokes the earlier handles; any failed or panicking rollback revoke promotes the overall result to
provider failure. Serialized restore uses the same unwind-catching take helper before publishing
any resource-table mutation.

This catches the unwind, not its process-global panic hook. Providers must not panic, and panic
payloads must contain no handle, key, certificate, credential, or provider detail. Provider-owned
logging and panic-hook stderr/logger output are outside the key-free log guarantee and require
embedder controls. Normally returned provider errors and Lunatic-generated guest errors/logs/audit
records remain secret-free.

Active TLS stream entries use names that state their limitation:

```rust
ResourceSnapshot::TlsClientConnectionMetadata {
    server_name,
    port,
    peer_addr,
    local_addr,
    custom_root_certs,
    read_timeout_ms,
    write_timeout_ms,
}

ResourceSnapshot::TlsServerConnectionMetadata {
    requires_peer_reconnect,
    reason,
}
```

The networking-side fields use the same terminology:

- `TlsClientConnectionMetadata`
- `TlsConnection::client_metadata`
- `TlsConnection::with_client_metadata`

This metadata is useful for diagnostics and snapshot inspection. It does not contain TCP sequence
state, TLS traffic keys, record sequence numbers, unread application bytes, or application protocol
state. Consequently, restoring it as though it were the original guest handle would be incorrect.

The failure is returned before any listener/socket restoration begins, preventing a misleading
partial success. Client and server metadata produce endpoint/resource-specific errors.

## Why Lunatic does not automatically reconnect

A fresh TCP connection and TLS handshake create a new byte stream. Reusing the old guest resource
ID for that stream could silently corrupt higher-level protocol state:

- requests may have been written but not acknowledged;
- unread response bytes may exist in the old stream;
- authentication or multiplexing state may be connection-bound;
- replay safety depends on the application protocol.

Lunatic therefore transfers the exact live object when that is possible and reports unsupported
restoration when it is not. It does not disguise a new connection as the old one.

## Security properties

- TLS traffic keys are not serialized into `ResourceMigrationSnapshot`.
- A version-2 TLS listener entry serializes its local address and an opaque 128-bit provider handle,
  never its certificate or raw private key.
- The handle is authorized against both environment and process identity and is single-use in the
  default five-minute, 1,024-entry process-local provider. The guest bind import additionally
  performs a non-consuming, default-denied `can_use_tls_credential_handles` precheck before quota
  reservation, then rechecks authority at scoped consumption.
- In-process transfer does not copy or expose key material; ownership of the opaque rustls object
  moves within the same runtime process.
- `tls_bind_with_credential` accepts address fields, an output pointer, and the low/high
  little-endian `u64` halves of the handle. It accepts no certificate or private-key memory range,
  so a guest using only that path does not add key bytes to `MemorySnapshot`.
- Legacy raw-key `tls_bind` remains deprecated compatibility behavior. It preserves its const guest
  PEM input for SDK address fallback and zeroizes only its temporary host copy; no key-free guest
  memory claim applies to that path.
- A process/node-local handle cannot be delegated through distributed spawn or message transport.
  Copying its scalar bits supplies neither the source scope nor provider backing and fails closed at
  the destination. The raw distributed CA/signing imports remain a separate privileged legacy
  boundary pending production `test_root_cert` removal and signer-capability/provider migration in
  Hanary #1704.
- Audit keeps capability denial distinct from provider state: capability is
  `denied`/`capability_denied`; missing, expired, revoked, consumed, and wrong-scope are uniformly
  `denied`/`policy_denied`; provider error/panic is `failed`/`runtime_failure`.
- Serialized restoration fails explicitly instead of silently weakening continuity guarantees.
- Cross-process migration remains unsupported until a separate protocol defines application-level
  quiescing, replay, acknowledgement, and credential handling.

## Executable evidence

The live-path test creates a local CA-signed TLS client/server pair, completes a handshake, moves the
client resource into a replacement `DefaultProcessState`, and verifies:

- the original resource ID still resolves;
- the exact `Arc<TlsConnection>` is retained;
- read/write timeouts are unchanged;
- the transferred stream can still exchange `ping`/`pong`.

```bash
cargo test --lib state::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts
cargo test --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes
cargo test --lib state::tests::panicking_tls_provider_take_is_contained_during_serialized_restore
cargo test --lib state::tests::snapshot_rollback_failure_returns_stable_provider_failure
cargo test --test tls_stream_migration_contract
cargo test --test tls_resource_migration
cargo test --test tls_provider_guest
cargo test --test imports_match
cargo test -p lunatic-process --lib
cargo test --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

`tests/tls_stream_migration_contract.rs` separately verifies the metadata-only serialization
contract and transfer-report accounting. The listener tests verify provider-backed rebind,
single-use failure, absence of private-key markers in version-2 bytes, and the production
live-transfer path without a provider lookup. Provider-handle guest tests establish actual Wasm
bind/accept/TLS traffic, default-denied capability and provider failure behavior, and key-marker
absence from guest memory and audit output. State tests cover unwind-contained restore and
rollback-revoke failure promotion; they do not establish panic-hook output redaction. The legacy
capability test only verifies that raw
`tls_bind` preserves its const guest key range while redacting audit output; it does not prove
guest-memory erasure.

## Operational boundary

Live-object preservation applies to Wasm hot reload inside the same Lunatic runtime process. A
runtime crash, host restart, persisted snapshot restore, or migration to another OS process cannot
preserve an active TLS stream under this contract. A listener can be securely rebound in those
scenarios only when an explicitly injected provider can resolve and authorize its version-2 handle;
the default provider does not survive restart. Applications still need an application-level
reconnect and replay protocol for active streams. A process-local handle is not valid migration
authority on another node; cross-node use requires authenticated target-scoped reissue, not copying
the handle or provider object.
