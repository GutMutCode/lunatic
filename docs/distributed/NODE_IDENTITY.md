# Distributed Node Identity

**Status**: Enforced for node-to-node QUIC
**Last reviewed**: 2026-07-22

Lunatic binds every distributed numeric node ID to the peer's CA-signed leaf
certificate. A `node_id` carried inside a MessagePack request is redundant
protocol input; it is never promoted to an authenticated identity.

## Trust boundary

A distributed connection or request is accepted only when all applicable
checks succeed:

1. rustls validates the certificate chain and the target DNS name used by the
   QUIC client;
2. the leaf certificate contains the Lunatic `CertAttrs` extension with a
   non-zero `node_id`;
3. an outbound connection's certificate identity equals the numeric ID of the
   intended topology entry;
4. the authenticated ID is still in the receiver's active topology; and
5. every redundant source field in a spawn, mailbox, or registry request
   equals the certificate identity.

Response waiters also retain the expected remote node ID. A response with the
right message ID but a different authenticated peer is rejected without
completing the waiter. Registry leader decisions, commit notifications,
unregistrations, heartbeats, and snapshots are evaluated against the verified
peer identity, not the payload claim.

Protocol-denial audit events use only that verified peer ID as their subject.
The mismatched claim, registry name, payload, and certificate material are not
copied into the audit record.

## Issuance lifecycle

Registration and node start are separate phases because a numeric ID does not
exist when a node first submits its CSR.

1. `/register` verifies the CSR signature and requires exactly one non-wildcard
   DNS SAN equal to the registered node UUID. It emits a controlled leaf
   profile without a `node_id`; this provisional certificate is bootstrap
   state and is not valid for node-to-node QUIC.
2. `/started` allocates the numeric ID, ignores caller-requested CA, key-usage,
   extended-key-usage, and custom extensions, rechecks the registered UUID,
   and signs a new leaf certificate containing `node_id: <allocated ID>`.
   The chain is returned with `NodeStarted`. Registration issues a separate
   short-lived node-control bearer; periodic generation-CAS refresh rotates it
   through an acknowledgement-safe pending verifier. That bearer is not a
   distributed-node identity credential.
3. The distributed control client installs the returned chain before the node
   creates its QUIC client and server endpoints. An empty chain fails closed
   with an upgrade error.

Control implementations that sign certificates from a guest use the strict
`lunatic::distributed::sign_node_for_name` and `sign_node_for_id` imports. The
legacy `sign_node` import remains only for privileged guest compatibility and
creates an unbound certificate, which the distributed transport rejects.

Static or embedded clusters do not call `/started`; they must provision each
certificate with `CertAttrs.node_id = Some(topology_node_id)` themselves.

## Restart, rotation, and membership changes

Every successful `/started` call retires the registration's prior active node,
allocates a new node ID, and issues a new certificate for that ID. The old
certificate remains cryptographically attributable to the old ID, but active
membership fencing prevents it from acting as the replacement node after
topology refresh.

The in-memory Axum control plane generates a fresh CA whenever its in-memory
registration and node-ID state is recreated. Its `/register` response returns
that generated trust root, so leaves from a previous control-plane lifetime
cannot authenticate after a restart even if numeric counters restart. The
legacy persistent submillisecond implementation is security-quarantined and is
not a supported control plane; its prior persistence behavior is not evidence
for the active contract.
Any custom control implementation that persists a CA must likewise persist a
never-reused node-ID/issuance epoch; CA state and identity-allocation state must
not be restored independently.

QUIC endpoint configuration captures its certificate when the endpoint is
created, and existing connections keep the identity established during the
handshake. Certificate rotation therefore requires endpoint recreation:

1. drain or close existing distributed traffic;
2. stop the old QUIC endpoints and connections;
3. call `/started` and install the returned certificate chain;
4. recreate both client and server endpoints; and
5. advertise and use the newly allocated topology ID.

Do not copy an old certificate onto a node with a new topology ID. For a static
cluster, changing an ID or removing and re-adding a member likewise requires a
new bound certificate and endpoint recreation.

Membership is checked when a connection is accepted and again when requests
are handled. Removal takes effect on each node when its control-plane topology
view refreshes; operators should close connections to a removed member as part
of the membership change instead of relying only on the refresh interval.

## Legacy certificate migration

Certificates whose `CertAttrs` JSON has no `node_id`, or has ID zero, are
parsed only far enough to return a clear migration error and are then rejected.
There is no payload-claim compatibility fallback.

Upgrade the control plane first so `/started` returns `cert_pem_chain`, then
restart or replace the distributed nodes so every endpoint uses an ID-bound
certificate. New clients fail closed against an old control server, while new
transports reject old nodes still using provisional or legacy certificates.
Consequently, plan the data-plane transition as a coordinated maintenance
window (or a separate replacement cluster) and restore registry quorum only
from identity-bound nodes.

After the rollout, revoke or remove legacy certificates from deployment
stores. CA trust alone is not evidence that a leaf certificate is usable as a
distributed node identity.

## Verification

The mTLS integration suites provision distinct ID-bound certificates for every
node. They cover normal 2/3/5-node registry quorum and partition recovery plus
three origin attacks:

- a node-2 certificate sends a leader-only notification while its wire field
  claims node 1;
- a node-2 certificate, with a matching node-2 wire field, submits a snapshot
  for node 3's pending synchronization request that expects node 1; a
  same-stream response marker proves the rejection ran, and the real node-1
  response subsequently completes the still-pending synchronization; and
- a node-3 certificate sends a matching spawn response ID to a waiter expecting
  node 2, after which the real node-2 response still completes that waiter.

The forged registry states remain absent, and the forged response neither
completes nor removes the pending waiter.

See also [Distributed Runtime Testing](../testing/DISTRIBUTED_TESTING.md) and
[Audit Logging](../security/AUDIT_LOGGING.md), plus
[Node-Control Bearer Security](../security/NODE_CONTROL_BEARER.md) for the
separate HTTP control credential.
