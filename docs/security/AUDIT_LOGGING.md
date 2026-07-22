# Audit Logging

Lunatic emits security-relevant runtime decisions as versioned JSON records on the Rust `log` target `audit` at `INFO` level.

This is an application-level event and delivery contract. It is not a durability, retention, tamper-evidence, or compliance guarantee. The built-in sink ends at the Rust `log` facade; an operator must configure and validate the downstream logger and storage path.

## V1 event contract

Every record has the same envelope:

```json
{
  "schema_version": 1,
  "sequence": 42,
  "event": "network_connect",
  "action": "connect",
  "result": "succeeded",
  "reason": "completed",
  "subject": {
    "node_id": null,
    "environment_id": 7,
    "process_id": 11
  },
  "target": {
    "kind": "network_endpoint",
    "resource_id": null,
    "node_id": null,
    "environment_id": null,
    "process_id": null,
    "port": 443,
    "sensitive_data": "redacted"
  }
}
```

The enums in `lunatic_common_api::audit` define the stable event, action, result, reason, and target values. Optional identity keys are always present and serialize as `null` when that layer does not know the value. The dedicated writer assigns `sequence` in dequeue order, so concurrent producers cannot create out-of-order records. Queue rejections occur before sequence assignment and must be detected from delivery counters; a gap among records accepted by a custom sink can indicate a sink write failure.

Results have distinct meanings:

- `allowed` / `denied`: an authorization or delegation decision.
- `succeeded` / `failed`: an attempted operation's terminal result.
- `cancelled`: the operation did not reach a normal terminal branch.

Reasons are stable machine codes. Raw `anyhow` messages, OS errors, payloads, or user-controlled detail strings are not part of the schema.

## Current event coverage

The implemented boundary records:

- module compile permission, success, and failure;
- child configuration creation, mutation, and final delegated-config validation;
- filesystem preopen delegation and WASI directory operations (`open`, create, remove, link, rename, metadata, and time changes);
- local spawn, get-or-spawn, distributed spawn authorization, receiver authorization, and receiver spawn results;
- TCP/TLS bind, accept, and connect, including provider-handle TLS bind capability/provider
  outcomes; UDP bind, connect, and send-to; DNS resolution;
- operation-level resource denials reached by the covered spawn and network paths;
- hot-reload transaction commit, rollback, in-doubt, and failure results;
- local/global distributed-registry changes, coordinator protocol denials, and snapshot application.

Events are emitted at one operation boundary. Replica application does not repeat the coordinator's logical registry event, and quota helpers do not emit a second event when their owning network/spawn operation already records the denial.

Distributed request-authorization and protocol-denial events record the remote
node ID extracted from the CA-signed peer certificate as the subject. The
requested environment remains a typed target where applicable. A redundant
payload claim is compared with that identity but is never emitted as the audit
subject, even when the claim is the reason for rejection. Subsequent spawn or
mailbox execution events can still use the receiving local node as their
subject because those events describe local work, not peer authentication.

This coverage is intentionally narrower than "every host call." Routine reads/writes, message contents, and non-security diagnostics are not audit events.

## Redaction contract

`AuditTarget` accepts enum values, numeric IDs, and a port only. It has no arbitrary string field. Callers therefore cannot place raw paths, IP addresses, hostnames, registry names, guest function names, or error messages in V1 records. When such input exists, `sensitive_data` is `redacted`.

The event schema never accepts:

- TLS certificates, private keys, or session material;
- cookies, bearer tokens, credentials, or authorization headers;
- environment values or command-line arguments;
- filesystem paths or registry names;
- message, SQL, HTTP, or Wasm payload bytes;
- raw diagnostic errors.

Related runtime `Debug` implementations redact credential-bearing configuration and migration data. Lunatic Cloud CLI login material is stored behind an opaque reference in the native OS credential store; its authentication path does not emit an `audit` event or put response bodies, response headers, login identifiers, or cookies into diagnostic errors. See [CLI Credential Storage](./CLI_CREDENTIAL_STORAGE.md). This does not turn general diagnostic logging into an audit sink; operators must still protect all runtime logs.

Audit redaction is separate from the TLS listener snapshot contract. Version-2 listener entries
contain only a local address and an opaque provider handle, while the certificate and raw private
key remain outside the serialized snapshot. `tls_bind_with_credential` likewise accepts only
address fields, an output pointer, and the low/high little-endian scalar halves of that handle. It
does not read a guest certificate or private-key range, so a guest that uses only this path does not
add those bytes to `MemorySnapshot`.

The handle halves are not audit metadata. The provider-handle bind emits the same typed
`network_bind`/`bind` boundary and redacted TLS-listener target as other TLS binds. It includes the
host-derived environment/process subject and numeric port where available, but never a handle,
certificate, key, provider message, socket address, hostname, or raw error. This guarantee applies
to the typed audit record, not separate provider-owned or panic-hook output. A missing
`can_use_tls_credential_handles` capability is checked without consuming the handle and before
network quota reservation; it is always `denied`/`capability_denied`, even when the process has no
remaining network quota. Missing, expired, revoked, consumed, and wrong-scope outcomes are all
`denied`/`policy_denied`, preserving existence hiding across provider states. A provider error or
panic is `failed`/`runtime_failure`, quota denial is `denied`/`resource_limit`, and an OS bind error
is `failed`/`runtime_failure`. Each path returns a stable secret-free guest error and emits one
terminal event.

Provider `provision`, `take`, and `revoke` unwinds are caught at the state boundary and reduced to
stable provider failure. A guest bind surfaces that as a stable error and typed
failed/runtime-failure audit event; snapshot capture/restore surfaces a stable runtime error.
During listener snapshot capture, failure to revoke any already-provisioned handle while rolling
back a later provision failure is promoted to stable provider failure. This prevents
Lunatic-generated audit or diagnostics from claiming clean rollback when the provider could retain
a credential.

`catch_unwind` does not suppress Rust's process-global panic hook: the hook runs first and may write
the provider panic payload to stderr or a logger outside the `AuditEventV1` path. Providers must not
panic, and panic payloads must never contain handles, keys, certificates, credential material, or
provider detail. Panic-hook output and provider-owned logging are outside this audit-redaction and
key-free-log contract. Embedders must configure/redact the hook and protect stderr/log collectors.
Normally returned provider errors and Lunatic-generated logs/audit records retain the secret-free
contract.

Legacy `tls_bind` remains deprecated compatibility behavior. It zeroizes its temporary host PEM
copy but preserves the const guest input needed for SDK address fallback, so it cannot establish a
key-free `MemorySnapshot`. Its audit event is still redacted, but redacted logging does not erase
the guest buffer.

The raw `lunatic::distributed` CA/signing imports are a separate privileged compatibility boundary,
not a listener-provider path. They do not inherit the provider-handle key-free or audit guarantee.
Removing production `test_root_cert` and replacing raw signing imports with an explicit signer
capability, non-exportable host/HSM provider, and typed audit coverage are tracked in Hanary #1704.
See [TLS Listener Credential Snapshots](../tls/TLS_LISTENER_CREDENTIALS.md) for the full scope,
revocation, migration, and distributed-handle restrictions.

## Delivery and backpressure

The default `AuditDispatcher` has a dedicated writer thread and a bounded queue of 1,024 records. Guest/runtime operations call `try_send` and never wait for queue capacity.

The policy is:

- disabled: drop the event and continue the operation (`fail-open`);
- queue full: drop the newest event and continue (`fail-open`);
- writer closed: drop and continue (`fail-open`);
- logger/sink unavailable, returned error, or panic: open the health circuit, count the failure, suppress queued and later writes, and continue (`fail-open`); a bounded flush is still attempted once so records buffered before the failure can drain;
- shutdown: atomically stop new event admission, wait at most 250 ms for already-admitted producers, prior events, and sink flush, then report the outcome without hanging shutdown; background producers after that boundary receive `DroppedClosed`.

Concurrent flush callers may share a pending barrier only when that barrier is
ordered after every event accepted before the caller began. A later caller
whose required event epoch is newer waits for a subsequent barrier. A timed-out
caller does not consume event capacity; the writer can still complete and
release the one reserved control slot.

The observable counters are returned by `audit_stats()`: attempted, enqueued, written, each drop category, sink/flush failures, flush timeouts/pending state, queue depth, and sink health. Counters are lock-free operational metrics read independently, not a transactional snapshot; consumers must not require cross-field equality from one sample. `written` means the configured `AuditSink::write` returned success. For the default `LogAuditSink`, that proves only handoff to the enabled Rust logger, not disk persistence or remote ingestion.

The runtime enables `audit=info` in its default filter. An explicit `RUST_LOG` value replaces that default; operators who override it must include `audit=info` if events are required.

## Embedding API

Embedders may install an `AuditDispatcher` with their own `AuditSink` before the first event:

```rust
use lunatic_common_api::{
    install_global_audit_dispatcher, AuditConfig, AuditDispatcher, AuditSink,
};

let dispatcher = AuditDispatcher::new(AuditConfig::default(), MySink::new()?);
install_global_audit_dispatcher(dispatcher)
    .map_err(|_| anyhow::anyhow!("audit dispatcher was already initialized"))?;
```

The sink runs on the dedicated writer thread. It must emit one record per event and must not reintroduce redacted data. A custom sink defines its own persistence semantics; Lunatic does not infer durability from successful construction.

## Verification

The common API has deterministic tests for the exact V1 JSON shape, redaction state, concurrent-producer/FIFO sequencing, disabled delivery, full-queue drop-newest, sink error and panic circuit opening, counters, and bounded flush. Host API tests assert typed fields for the representative production paths listed below rather than treating substring-formatted diagnostic messages as audit evidence.

Production-import integration tests capture exact terminal records for process
capability and process-quota decisions, configuration/preopen mutations, local
registry changes, TCP/UDP bind, DNS resolution, and TLS/port validation. They
cover successful, denied, failed, and redacted outcomes. The invalid TLS bind
case also verifies that the const guest key-input range remains unchanged and
that its marker is absent from the typed audit event; the host-copy zeroization
is an implementation property, not guest-memory erasure. Provider-handle TLS
tests separately exercise actual guest Wasm bind/accept/TLS traffic and denial
paths, assert one typed terminal event, and check that neither handle halves nor
known key markers appear in guest memory snapshots or captured audit JSON. The capability case
uses a zero network quota to prove that authorization is classified before resource pressure; the
provider-state cases remain existence-hiding policy denials. The provider-panic restore test proves
stable failure and unwind containment only; it does not prove panic-hook output redaction. The live-Wasm reload
test captures one record for each commit, rollback, in-doubt, and blocked
attempt. WASI directory tests cover successful access and cancellation-guard
behavior. Distributed receiver decode/authorization and atomic-admission
classification are tested at their production helpers. Identity tests assert
that the certificate-derived remote ID is the subject and the unverified
payload claim is not promoted into the target. The mTLS registry integration
suite also proves that a certificate/payload ID mismatch is rejected before
registry state changes; a full remote-spawn audit capture is not claimed as a
standalone audit-delivery E2E.

For deployment routing and the precise external boundary, see [Audit Logging Persistence](./AUDIT_LOGGING_PERSISTENCE.md).

For certificate issuance and rotation, see
[Distributed Node Identity](../distributed/NODE_IDENTITY.md).

**Last reviewed:** 2026-07-22
