# TLS stream migration contract

**Status**: Supported for in-process Wasm hot reload; unsupported for serialized or cross-process restoration

**Reviewed**: 2026-07-21

**Canonical status**: `docs/core_values/status.md`

## Contract

Lunatic has two distinct resource migration paths:

| Path | TLS listener | Active TLS client/server stream | Resource ID and timeouts |
| --- | --- | --- | --- |
| In-process Wasm hot reload | Live host object transferred | Live host object transferred | Preserved |
| Serialized/cross-process snapshot restoration | Recreated where supported | Explicitly unsupported; metadata only | Not restored for TLS streams |

The in-process path is transparent to the guest. The replacement Wasm instance receives the same
host-owned `TlsConnection`, including its TCP socket, rustls session, split reader/writer, timeout
configuration, and `HashMapId` resource ID. No reconnect or new TLS handshake occurs.

The serialized path never claims that descriptive metadata can recreate an active TLS byte stream.
`restore_resource_snapshot` returns an error if a snapshot contains a TLS stream.

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

Serialized snapshots use names that state their limitation:

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
- In-process transfer does not copy or expose key material; ownership of the opaque rustls object
  moves within the same runtime process.
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
cargo test --test tls_stream_migration_contract
cargo test --test tls_resource_migration
cargo test -p lunatic-process --lib
```

`tests/tls_stream_migration_contract.rs` separately verifies the metadata-only serialization
contract and transfer-report accounting.

## Operational boundary

This support applies to Wasm hot reload inside the same Lunatic runtime process. A runtime crash,
host restart, persisted snapshot restore, or migration to another OS process cannot preserve an
active TLS stream under this contract. Applications that require those scenarios must define an
application-level reconnect and replay protocol.
