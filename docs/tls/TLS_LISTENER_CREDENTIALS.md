# TLS listener credential contract

**Status**: The provider-handle guest bind and version-2 serialized listener restoration keep raw
private-key bytes out of Wasm memory; production in-process hot reload transfers the live listener
instead

**Reviewed**: 2026-07-22

This document is the security and operations contract for provider-handle guest binding and TLS
listener entries in `ResourceMigrationSnapshot`. The separate active-stream contract is documented
in [TLS stream migration](./TLS_STREAM_MIGRATION.md).

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

## Guest provider-handle ABI

New guests bind a listener through this import:

```text
lunatic::networking::tls_bind_with_credential(
    addr_type: i32,
    addr_ptr: i32,
    port: i32,
    flow_info: i32,
    scope_id: i32,
    out_ptr: i32,
    handle_low: i64,
    handle_high: i64,
) -> i32
```

The first six arguments have the same address and output meanings as `tls_bind`. Status `0` writes
the listener resource ID to `out_ptr`; status `1` writes a bounded error-resource ID there. The
import has no certificate or private-key pointer. The only guest-memory ranges it reads are the
address bytes, and the only output it writes is the result ID.

`TlsCredentialHandle` is an opaque 16-byte value. Its ABI encoding is intentionally exact:

```text
handle_low  = u64::from_le_bytes(handle[0..8])
handle_high = u64::from_le_bytes(handle[8..16])
handle      = handle_low.to_le_bytes() || handle_high.to_le_bytes()
```

SDKs must preserve that low-then-high little-endian ordering and must not reinterpret the value as
a UUID string or host-endian integer. The two scalar arguments are still a sensitive locator and
must not be placed in logs, diagnostics, metrics labels, or error text.

An embedder or credential control plane provisions the host-owned `TlsAcceptor` and gives the guest
only the opaque handle. Calling the import does not create authority: the runtime derives the
environment and process identities from the caller and reads the caller's
`can_use_tls_credential_handles` capability from its process configuration.

## Authorization and provider lifecycle

Every provider operation receives a `TlsCredentialScope` containing the exact
`(environment_id, process_id)` pair. A provider must authorize both values, and the guest bind path
must additionally require `can_use_tls_credential_handles`. The import performs a non-consuming
capability precheck before reserving network quota, and the scoped provider-consumption boundary
checks the capability again. The capability defaults to denied, new child configurations do not
inherit it, parent-to-child delegation cannot increase it, and a distributed configuration cannot
request authority above the receiver's local ceiling. The guest does not supply any of these three
authorization inputs.

The built-in provider returns the same secret-free `Unavailable` result for an unknown, revoked,
already consumed, or wrong-scope handle. Possession of the 128 bits is therefore necessary but not
sufficient authority, and copying the value across a process or environment boundary does not make
it usable.

`DefaultProcessState::new` creates an `EphemeralTlsCredentialProvider` with these properties:

- storage exists only in the current runtime process;
- handles are opaque 128-bit UUID v4 values;
- the default time-to-live is five minutes;
- at most 1,024 unexpired credentials are retained;
- successful `take` is single-use and removes the credential;
- explicit `revoke` removes an unconsumed credential in the matching scope;
- an expired handle is removed and denied.

This default is intended for short-lived, process-local snapshot/rebind workflows. It cannot
resolve a handle after a runtime restart. An embedder that persists snapshots or restores them in a
new runtime process must construct state with `new_with_tls_credential_provider` and inject a
provider that can securely reprovision the same handle, credential material, and authorization
scope. Lunatic supplies the provider boundary and explicit consumption/revocation operations, not
a persistent credential store, HSM adapter, key-rotation system, or cross-node authorization
protocol.

Runtime calls to `provision`, `take`, and `revoke` catch provider unwinds at the state boundary. A
normally returned provider error or a caught panic is reduced to the same stable
`ProviderFailure` result; an unwind does not cross that boundary. Guest bind converts the result to
its stable error and typed audit event, while snapshot capture/restore returns the stable runtime
error.

This is not panic-output containment. Rust invokes the process-global panic hook before
`catch_unwind` returns, and that hook can write the provider's panic payload to stderr or another
logger. Provider implementations must not panic. As defense in depth, every panic payload must omit
handles, certificates, keys, credential material, addresses, and provider detail. Provider-owned
logging and panic-hook output are outside Lunatic's key-free runtime-log guarantee; embedders must
configure and protect the hook and stderr/log collection accordingly. Normally returned provider
errors and Lunatic-generated guest errors, normal logs, and audit records remain stable and
secret-free.

## Fail-closed errors and consumption

The guest access boundary exposes these stable, secret-free failures:

| Condition | Error text |
| --- | --- |
| Caller lacks `can_use_tls_credential_handles` | `TLS credential handle capability is denied` |
| Unknown, revoked, already consumed, or wrong-scope handle | `TLS listener credential is unavailable` |
| Expired handle | `TLS listener credential has expired` |
| Provider error or panic | `TLS credential provider failed` |

Capability denial also fails before provider consumption or socket creation and is returned through
the same bounded error-resource channel. All messages intentionally omit the handle, address,
credential material, and provider detail. Invalid guest-memory ranges retain the usual trapping ABI
contract; they are not converted into provider errors.

The provider-handle bind audit classification is also stable and existence-hiding:

| Outcome | Audit result / reason |
| --- | --- |
| Capability precheck or consumption-boundary capability denial | `denied` / `capability_denied` |
| Unknown, revoked, consumed, wrong-scope, or expired handle | `denied` / `policy_denied` |
| Provider error or panic | `failed` / `runtime_failure` |
| Network quota denial | `denied` / `resource_limit` |
| OS bind failure | `failed` / `runtime_failure` |

Using one `policy_denied` classification for unavailable and expired provider states prevents the
audit channel from revealing whether a credential entry exists. The stable guest error can still
distinguish expiry from generic unavailability without placing handle or provider detail in the
event.

Both `tls_bind_with_credential` and serialized restoration validate the host-derived scope before
publishing a listener. After validating the address/output range, the guest bind performs the
non-consuming capability precheck, reserves its finite network-handle lease, performs the scoped
single-use `take`, and only then attempts the OS bind. Address, capability, or quota failure
therefore leaves the handle unconsumed. Once `take` succeeds it is irreversible, so a subsequent OS
bind failure requires a freshly provisioned handle.
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

Snapshot capture can provision more than one listener handle. If a later provision fails, the
runtime attempts to revoke every handle provisioned earlier in that capture. If any rollback revoke
fails or panics, the capture returns stable `TLS credential provider failed` rather than reporting
the original error as though cleanup had succeeded. The provider must still expose operational
reconciliation for a persistent store; the runtime cannot prove that a provider completed a failed
revocation.

Revocation prevents future consumption of an unconsumed handle; it does not reach into an already
bound listener and replace the key inside its live rustls acceptor. Certificate rotation therefore
requires provisioning a new handle, binding the replacement listener, draining or stopping the old
listener, revoking any unused old handles, and rotating the underlying identity according to the
operator's certificate policy.

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

## Key-free guest-memory and serialization boundary

`tls_bind_with_credential` never accepts certificate or private-key bytes. A listener created only
through this import therefore cannot add those bytes to `MemorySnapshot` or
`ResourceMigrationSnapshot`. Runtime-owned Serde, `Debug`, normal diagnostics, and typed audit
records do not serialize provider material. This is a scoped claim: it does not prove that a guest
did not independently receive or generate the same key, and the live host-owned rustls acceptor
necessarily retains derived key material for future handshakes. It also does not cover
provider-owned logging or process-global panic-hook output described above.

The handle itself can appear in guest state or in a version-2 listener snapshot. Its custom
`Debug` representation is redacted, credential material is not serializable, and audit events must
emit only typed numeric metadata without either handle half, provider detail, certificate, key, or
raw error. Host crash dumps, swap, allocator behavior, and memory remanence remain deployment
concerns.

## Deprecated raw-key ABI

The legacy `lunatic::networking::tls_bind` guest ABI remains available for compatibility and is
deprecated. It accepts raw private-key PEM bytes through a const input buffer. Guest SDKs may reuse
that buffer while trying multiple resolved addresses, so the host must not mutate it. The host
copies the exact declared range, parses the temporary PEM copy, and zeroizes that temporary host
byte copy. The configured rustls server object necessarily retains derived key material so it can
accept future TLS connections.

This host-copy guarantee is deliberately narrow:

- the original guest buffer and any duplicate elsewhere in Wasm memory are not erased by the host;
- the guest buffer can therefore remain in a later `MemorySnapshot` unless the guest SDK manages
  its lifetime;
- another host copy outside the temporary input copy is not erased;
- memory allocators, crash dumps, swap, and platform memory-remanence controls remain deployment
  concerns.

Version 2 ensures that listener snapshots do not add another raw private-key copy, but it cannot
erase bytes that entered guest memory through this legacy ABI. The key-free `MemorySnapshot` claim
therefore applies only to a guest that uses `tls_bind_with_credential` and did not obtain the key
through another path.

### SDK, import, and operational migration

There is no safe automatic adapter from raw guest key bytes to a durable provider identity. SDK and
application owners must migrate deliberately:

1. Inventory modules and SDKs importing `tls_bind`, and treat their Wasm memories, old snapshots,
   crash dumps, caches, and backups as potentially key-bearing.
2. Provision the certificate and key on the host side under the intended environment, process, and
   `can_use_tls_credential_handles` authority. Give the guest only the 16-byte handle.
3. Add the `tls_bind_with_credential` declaration to each language SDK and import manifest,
   including `wat/all_imports.wat`, using the exact little-endian split above. Do not silently fall
   back to `tls_bind` when handle binding is denied or unavailable.
4. Exercise real guest Wasm bind, accept, and TLS traffic plus denied-capability, missing, expired,
   revoked, wrong-scope, and provider-failure paths. Verify that known key markers are absent from
   guest memory snapshots, resource snapshots, Serde/`Debug`, runtime-owned diagnostics, and audit
   output. Independently enforce the provider no-panic/payload policy and review the configured
   panic hook and stderr/log collector.
5. Rotate every key that previously entered guest memory, revoke unused old handles, drain old live
   listeners, and apply retention/secure-deletion policy to historical artifacts.
6. Remove the legacy import only in a coordinated compatibility release after supported SDKs and
   deployed modules have moved. Until then, production policy should reject or inventory legacy
   use rather than describing it as key-free.

## Distributed credential boundary

The listener provider is process- and node-local. A handle is not a distributable resource and is
not delegated by distributed spawn or message transport. Message resource attachments are rejected
at that boundary, and a distributed spawn configuration requesting the capability above the
receiver's default-denied ceiling is rejected. Copying the low/high integers as ordinary message
data or a `v128` copies only a locator. Even an independently authorized destination has a different
process scope and normally a different provider instance, so lookup fails closed as unavailable.
Cross-node support would require an authenticated provider namespace and an explicit target-scoped
reissue protocol. It must not serialize a provider `Arc`, raw private key, or reusable source
handle.

The legacy `lunatic::distributed` imports `test_root_cert`,
`default_server_certificates`, `sign_node`, `sign_node_for_name`, and `sign_node_for_id` form a
separate privileged compatibility boundary. They are not backed by the TLS listener provider and
are not covered by this document's key-free guest-memory guarantee: the current raw CA APIs accept
or return CA/private-key material in Wasm memory. The supported Axum control path already keeps its
CA authority host-side; generic production registration of these guest imports is still an
unresolved compatibility exposure. Removing the production `test_root_cert` import and replacing
the raw signing imports with an explicit signer capability plus a non-exportable host/HSM signer
provider are tracked as Hanary #1704. That provider should accept only CSR and authorized identity
inputs and return certificates, never CA key bytes.

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
cargo test -p lunatic-runtime --lib state::tests::panicking_tls_provider_take_is_contained_during_serialized_restore
cargo test -p lunatic-runtime --lib state::tests::snapshot_rollback_failure_returns_stable_provider_failure
cargo test -p lunatic-runtime --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test -p lunatic-runtime --test tls_resource_migration
cargo test -p lunatic-runtime --test tls_provider_guest
cargo test -p lunatic-runtime --test imports_match
cargo test -p lunatic-runtime --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

`tls_provider_guest` runs an actual Wasm module through provider-handle bind, accept, and a TLS
`ping`/`pong`. It checks single-use, missing, expired, revoked, wrong-process, wrong-environment,
denied-capability, and provider-failure paths, stable error resources, one typed audit event per
attempt, and absence of DER/PEM markers from guest memory, resource snapshots, `Debug`,
runtime-generated logs, and audit JSON under non-panicking provider behavior. Provider unit tests
separately cover lifecycle and capacity. The legacy capability test
proves only that raw `tls_bind` preserves its const guest key-input buffer on the exercised failure
path while omitting its marker from the audit event. The serialized-listener test performs a real
post-restore TLS exchange and checks that neither a DER marker nor the PEM input appears in the
resource-snapshot bytes. `imports_match` keeps the production linker and `wat/all_imports.wat`
declarations synchronized. Dedicated state tests verify that a panicking provider `take` cannot
unwind across serialized restore and that rollback-revoke failure is reported as stable provider
failure without mutating live resource tables. They do not prove that the process-global panic hook
emitted no payload before the unwind was caught.

For the audit record redaction boundary, see [Audit logging](../security/AUDIT_LOGGING.md).
