# Node-Control Bearer Security

**Status:** Enforced by the active Axum control plane and distributed client
**Last reviewed:** 2026-07-22

This contract covers the short-lived bearer used between `lunatic node` and
the node-control HTTP API. It is separate from Lunatic Cloud CLI cookies and
from the mTLS identity used between distributed nodes.

## Supported transport and origin

The active `lunatic control` server is clear-text HTTP only and therefore
refuses every non-loopback listener. Remote control-plane deployments must use
an HTTPS implementation or trusted TLS termination; binding the built-in HTTP
server to `0.0.0.0` is not a supported substitute.

The client accepts:

- an `https://` root origin; or
- `http://` with a literal IPv4/IPv6 loopback address.

User information, non-root paths, queries, fragments, other schemes, and
non-loopback HTTP fail before a request is sent. The request Host header is not
used to construct response endpoints. The server derives its advertised origin
from the verified loopback listener address.

Every returned endpoint must use the registration scheme, host, and effective
port and the expected fixed path. The client validates all endpoints, then
discards their authority and derives `/started`, `/refreshed`, `/stopped`,
`/nodes`, and `/module` from the trusted origin. Generic authenticated
arbitrary-URL request methods are not public.

The dedicated control client disables redirects and all implicit/system
proxies, marks the Authorization header sensitive, applies bounded connect and
request deadlines, and streams responses into endpoint-appropriate finite byte
limits. A redirect, forged endpoint, scheme downgrade, or proxy environment
therefore cannot move the bearer to another request target.
Transport, status, parsing, and validation failures return stable operation
codes without response bodies, target URLs, headers, or bearer values.

## Secret and storage boundary

Registration responses and client-proposed rotation requests use
`WireBearerToken`, the explicit Serde envelope at which the raw token crosses
the wire. It is non-cloneable, Debug-redacted, and zeroizes its allocation on
drop. The client immediately converts an issued token to runtime
`BearerToken`; runtime `Registration` and
`BearerToken` implement neither general Serde nor `Clone`. `Client::reg()`
returns certificate/topology metadata only and cannot copy the bearer.

Every accepted bearer is canonical unpadded Base64URL for exactly 32 random
bytes. Non-canonical, weak-length, padded, empty, or oversized bearer values
fail before entering runtime state, and rejected input is held in zeroizing
storage while it is checked.

The active Axum server never stores the raw token. It hashes the 256-bit random
bearer with SHA-256 and retains only the fixed-size verifier. Authentication
hashes the presented value and compares verifier bytes with constant-time
equality. Registration/verifier Debug output is redacted. No token is written
to configuration, persistence, audit events, returned errors, or logs. Raw
wire responses carry `Cache-Control: no-store, max-age=0` and `Pragma:
no-cache`.

## Issuance, rotation, and revocation

One live registration lease exists per node UUID:

1. Registration issues a fresh 256-bit bearer and a five-minute inactivity
   lease. A duplicate live UUID registration is rejected, so an unauthenticated
   local caller cannot replace an existing UUID registration.
2. A successful `/started` call allocates the identity-bound certificate. It
   retains the current bearer, so a lost start response can still be cleaned up
   with the authenticated `/stopped` path.
3. The client calls `/refreshed` halfway through the advertised lease. It
   generates the next bearer, retains it as a pending zeroizing secret, and
   submits it with the current generation. The server stores only its digest as
   pending and acknowledges the next generation without replacing the current
   verifier. The client promotes its retained pending bearer after the
   acknowledgement; the server promotes the pending digest only when that
   bearer is first presented. A lost response retries the same pending secret,
   competing rotations fail a generation/digest CAS, and there is no
   full-authority previous-token grace period.
4. Authenticated activity renews the inactivity lease. `/stopped` immediately
   removes active node records and the registration verifier. Repeated
   start/stop/crash churn does not retain stopped node tombstones.
5. A crashed node sends no stop request. Once it stops refreshing/using the
   control plane, the lease expires and the server sweep retires its node and
   removes the verifier. Recreating the in-memory Axum server also recreates its
   CA and invalidates every prior registration.

Client refresh workers retain only a weak reference. All public client clones
share a separate lifetime guard: its final drop wakes and cancels both workers
and zeroizes current and pending local tokens even if the workers temporarily
hold strong internal references during requests. A session mutex serializes
refresh and shutdown. Explicit shutdown performs the same local revocation
before waiting for a rotation or network response, so cancellation or a
missing server acknowledgement cannot resurrect local authority; an
unacknowledged server record remains bounded by the lease. Node bootstrap also
runs its post-start setup inside a cleanup boundary so ordinary setup errors
still attempt authenticated shutdown, and abort-on-drop task guards prevent
outer-future cancellation from detaching node or Wasm tasks that own client
handles.

## Enrollment threat boundary

The built-in server has no bootstrap enrollment credential and currently marks
new registrations privileged. Loopback limits exposure to the host, but is not
an OS-user or process-isolation boundary. The supported built-in deployment
therefore assumes a trusted single-user/development host. HTTPS termination
alone does not make it safe to expose remotely: a remote deployment must add
admission authentication and authorization before registration as well as TLS.

## Quarantined Submillisecond implementation

`crates/lunatic-control-submillisecond` is not an alternative implementation
of this contract. It is excluded from the root workspace, marked
`publish = false`, and its binary exits without opening a listener. Its legacy
source persists recoverable bearer strings in SQLite, derives token-bearing
Clone/Debug/Serde records, trusts Host-derived HTTP URLs, and has no compatible
rotation or expiry acknowledgement path.

The source is retained only for a separate migration. Reactivation requires
opaque/verifier-only persistence, migration or revocation of existing
plaintext rows, trusted-origin transport, acknowledged storage and lifecycle
operations, matching rotation/revocation semantics, and an independently
gated security E2E suite.

## Verification

Unit tests cover canonical bearer parsing, verifier mutation, duplicate-UUID
rejection, two-phase generation-CAS rotation, every advertised URL field,
direct deterministic lease expiry cleanup, stopped-record removal, IPv4/IPv6
loopback validation, delayed-upload generation revalidation, final-public-handle
revocation, parent-task cancellation, and Debug/compile-time non-Serde
boundaries. Actual-TCP tests cover Host poisoning, forged targets, redirect
refusal, authenticated traffic with hostile proxy variables, lost rotation
acknowledgement and retry, cancelled shutdown local revocation, cache headers,
and normal register → start → refresh → topology refresh → stop behavior. The
tests do not claim operating-system crash-dump erasure or capture every
possible external logging/audit subscriber; production code emits only stable
value-free control errors and never submits bearer material to those
interfaces.

```bash
cargo test -p lunatic-control --lib
cargo test -p lunatic-control-axum --lib
cargo test -p lunatic-distributed --lib control::client::tests
cargo test --test node_control_security
```

See also [CLI Credential Storage](CLI_CREDENTIAL_STORAGE.md) and
[Distributed Node Identity](../distributed/NODE_IDENTITY.md).
