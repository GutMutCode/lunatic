# TLS listener credential snapshot contract

**Status**: Version-2 serialized listener restoration is supported through a scoped host credential provider; production in-process hot reload transfers the live listener instead

**Reviewed**: 2026-07-22

This document is the security and operations contract for TLS listener entries in
`ResourceMigrationSnapshot`. The separate active-stream contract is documented in
[TLS stream migration](./TLS_STREAM_MIGRATION.md).

## Version-2 wire contract

`ResourceMigrationSnapshot::to_bytes` writes the `LUNRSNP\0` magic followed by version byte `2`
and a bincode payload. The payload of each `ResourceSnapshot::TlsListener` entry has exactly two
fields:

```rust
ResourceSnapshot::TlsListener {
    local_addr: String,
    credential_handle: TlsCredentialHandle, // opaque [u8; 16]
}
```

The enclosing `tls_listeners` map still carries the snapshot resource ID. The listener entry does
not contain a certificate, private-key PEM/DER bytes, or another serializable form of the rustls
server configuration. Version 2 therefore does not preserve a raw listener private key in the
snapshot. The restorer rebinds `local_addr` and obtains a host-owned `TlsAcceptor` by presenting the
opaque 128-bit handle to the configured provider. Serialized restoration can allocate a new guest
resource ID; ID preservation belongs to the live-transfer path described below.

`TlsCredentialHandle` is a locator, not sufficient authority by itself. Its `Debug` output is
redacted, but operators should still avoid logging or exposing its serialized value.

## Authorization and provider lifecycle

Every provider operation receives a `TlsCredentialScope` containing the exact
`(environment_id, process_id)` pair. A provider must authorize both values. The built-in provider
returns the same secret-free `Unavailable` result for an unknown handle and a handle presented from
the wrong scope, so the handle cannot be replayed across process or environment authority
boundaries.

`DefaultProcessState::new` creates an `EphemeralTlsCredentialProvider` with these properties:

- storage exists only in the current runtime process;
- handles are opaque 128-bit UUID v4 values;
- the default time-to-live is five minutes;
- at most 1,024 unexpired credentials are retained;
- successful `take` is single-use and removes the credential;
- an expired handle is removed and denied.

This default is intended for short-lived, process-local snapshot/rebind workflows. It cannot
resolve a handle after a runtime restart. An embedder that persists snapshots or restores them in a
new runtime process must construct state with `new_with_tls_credential_provider` and inject a
provider that can securely reprovision the same handle, credential material, and authorization
scope. Lunatic supplies the provider boundary, not a persistent credential store, HSM adapter, key
rotation system, or cross-node authorization protocol.

## Fail-closed restoration

The provider exposes three stable, secret-free failures:

| Condition | Error text |
| --- | --- |
| Unknown, already consumed, or wrong-scope handle | `TLS listener credential is unavailable` |
| Expired handle | `TLS listener credential has expired` |
| Provider/internal failure | `TLS credential provider failed` |

The messages intentionally omit the handle, address, credential material, and provider detail.
Before changing a resource table, `restore_resource_snapshot` resolves, authorizes, reserves, and
rebinds every TLS listener into temporary host objects. A credential error, invalid address, quota
denial, or bind failure aborts the TLS listener restore; prepared sockets and leases are dropped
instead of publishing a partial listener set.

Provider `take` happens before the corresponding socket bind completes. Consequently, a successful
lookup consumes that handle even if its bind or a later listener's preflight fails. A serialized
snapshot is therefore a single restore attempt, not a replayable recovery token:

1. Capture a fresh snapshot immediately before each restore attempt.
2. After any failed or abandoned attempt, do not retry the same snapshot.
3. Capture fresh handles from the still-live source, or securely reprovision fresh handles through
   the injected provider.
4. Treat successful restore as consumption of every listener handle in that snapshot.

The default provider makes unused handles unusable after five minutes, cleans expired entries
lazily on provider access, and fails closed when its 1,024-entry ceiling is full. Persistent
providers need their own capacity, expiry, revocation, audit, and abandoned-snapshot cleanup
policy.

## Production in-process hot reload

The running-Wasm hot-reload transaction does not serialize and restore TLS listeners. After the
replacement module, memory, and mailbox have been prepared, it calls
`ProcessState::transfer_runtime_resources_to`. `DefaultProcessState` moves the complete live
resource maps, including each bound `TcpListener` and configured `TlsAcceptor`, into the replacement
state. This preserves listener resource IDs and the next-ID seed and performs no provider lookup,
key reserialization, socket rebind, or TLS configuration rebuild.

This is the preferred process-local hot-reload path. The version-2 provider contract exists for an
explicit serialized listener rebind; it is not how production in-process hot reload preserves the
listener.

## Guest key-input boundary

The current `lunatic::networking::tls_bind` guest ABI accepts raw private-key PEM bytes through a
const input buffer. Guest SDKs may reuse that buffer while trying multiple resolved addresses, so
the host must not mutate it. The host copies the exact declared range, parses the temporary PEM
copy, and zeroizes that temporary host byte copy. The configured rustls server object necessarily
retains derived key material so it can accept future TLS connections.

This host-copy guarantee is deliberately narrow:

- the original guest buffer and any duplicate elsewhere in Wasm memory are not erased by the host;
- the guest buffer can therefore remain in a later `MemorySnapshot` unless the guest SDK manages
  its lifetime;
- another host copy outside the temporary input copy is not erased;
- intentional distributed credential-delivery APIs have their own security contract and are
  outside this resource-snapshot guarantee;
- memory allocators, crash dumps, swap, and platform memory-remanence controls remain deployment
  concerns.

Version 2 ensures that listener snapshots do not add another raw private-key copy. Avoiding raw key
bytes in guest memory altogether requires a future provider-handle guest ABI in which `tls_bind`
receives an authorized host credential reference instead of a PEM buffer.

## Legacy snapshot incident and upgrade policy

Older unversioned snapshots could serialize TLS listener certificate and private-key bytes. Treat
every such artifact as potentially containing a live private key. The runtime has no automatic
legacy importer and must not gain one that silently deserializes or rewrites those values.

For an upgrade to version 2:

1. Stop accepting all unversioned snapshots, snapshots with a missing or unknown version, and v2
   payloads with trailing data. `from_bytes` fails closed for each case rather than accepting an
   otherwise valid payload with appended legacy bytes.
2. Inventory and purge old snapshots, staging copies, caches, exports, and backups under the
   applicable secure-deletion and retention controls. Do not log or inspect their decoded payloads.
3. Rotate or revoke every certificate/private key that may have appeared in one of those artifacts;
   absence from the current primary store is not evidence that no backup retained it.
4. Provision the replacement identity through the host credential provider and create a fresh
   version-2 snapshot under the intended process/environment scope.
5. If a legacy workload cannot be securely reprovisioned, leave restoration disabled rather than
   importing the old snapshot.

## Executable evidence

These targeted commands exercise the contract; they do not broaden it beyond the assertions named
above:

```bash
cargo test -p lunatic-process --lib legacy_unversioned_snapshot_is_rejected_without_echoing_secret_bytes
cargo test -p lunatic-process --lib unknown_and_truncated_versions_are_rejected_without_payload_details
cargo test -p lunatic-process --lib trailing_payload_is_rejected_without_echoing_secret_bytes
cargo test -p lunatic-runtime --lib tls_credentials::tests
cargo test -p lunatic-runtime --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes
cargo test -p lunatic-runtime --lib state::tests::missing_tls_credential_fails_before_any_resource_mutation
cargo test -p lunatic-runtime --lib state::tests::later_tls_preflight_failure_rolls_back_prepared_listener_and_lease
cargo test -p lunatic-runtime --lib state::tests::expired_tls_credential_fails_closed_with_stable_error
cargo test -p lunatic-runtime --lib state::tests::tls_provider_failure_is_stable_and_secret_free
cargo test -p lunatic-runtime --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test -p lunatic-runtime --test tls_resource_migration
cargo test -p lunatic-runtime --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

The capability test proves that `tls_bind` preserves its const guest key-input buffer on the
exercised failure path while omitting its marker from the audit event. The serialized-listener test
performs a real post-restore TLS exchange and checks that neither a DER marker nor the PEM input
appears in the resource-snapshot bytes.

For the audit record redaction boundary, see [Audit logging](../security/AUDIT_LOGGING.md).
