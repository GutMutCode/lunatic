# TLS stream migration implementation summary

**Implemented**: 2026-07-21

**Status**: In-process hot reload supported; serialized/cross-process restoration explicitly unsupported

The authoritative contract is documented in `docs/tls/TLS_STREAM_MIGRATION.md`.

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
  - serialized TLS stream restoration returns an explicit error before partial restoration.
- `crates/lunatic-networking-api`
  - misleading reconnect names were replaced by metadata names:
    `TlsClientConnectionMetadata`, `client_metadata`, and `with_client_metadata`.
- `crates/lunatic-process/src/resource_migration.rs`
  - serialized variants are named `TlsClientConnectionMetadata` and
    `TlsServerConnectionMetadata`;
  - `ResourceTransferReport` records live-resource transfer counts.

## Deliberate non-feature

Lunatic does not establish a fresh TCP/TLS connection and attach it to the old guest handle.
Doing so would substitute a new application byte stream without knowing request, response,
authentication, multiplexing, or replay state.

Serialized metadata remains available for diagnostics, but it is not accepted as sufficient
restoration state. Cross-process recovery needs a separately designed application-level protocol.

## Validation

```bash
cargo test --lib state::tests::hot_reload_transfers_live_tls_stream_with_id_and_timeouts
cargo test --test tls_stream_migration_contract
cargo test --test tls_resource_migration
cargo test -p lunatic-process --lib
```

The live test performs a real TLS handshake and proves continued I/O after moving the exact
resource into a replacement process state while preserving its guest-visible ID and timeout values.
