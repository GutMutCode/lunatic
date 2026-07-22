# Distributed Runtime Testing

**Status**: Active
**Last reviewed**: 2026-07-22

Distributed completion claims must cross the production network boundary. Tests
that invoke coordination handlers directly are useful unit tests but are not
reported as multi-node E2E evidence.

## Test Inventory

| File | Scope |
| --- | --- |
| `crates/lunatic-distributed/src/distributed/registry.rs` | Local/global registry unit behavior |
| `crates/lunatic-distributed/src/distributed/registry_coordination.rs` | Coordinator state-machine and snapshot unit behavior |
| `crates/lunatic-distributed/tests/distributed_registry.rs` | Public registry and identifier contracts |
| `crates/lunatic-distributed/tests/registry_coordination.rs` | Real 2/3/5-node mTLS QUIC coordination and authenticated node-ID spoof rejection |
| `crates/lunatic-distributed/tests/quic_transport.rs` | Production QUIC framing, reassembly, decode and dispatch |
| `tests/distributed_registry_e2e.rs` | Live Wasm registry/mailbox flow and authenticated response-origin enforcement on full node servers |

## Networked E2E Coverage

`registry_coordination.rs` binds a separate QUIC endpoint for every node, creates
node certificates signed by the test root and bound to distinct numeric node
IDs, uses the production distributed client, serializes registry messages as
`Request::Registry`, and sends them through the production QUIC transport.

It verifies:

- simultaneous same-name registration has one winner in 2-, 3-, and 5-node
  clusters;
- request IDs from different nodes do not collide at the coordinator;
- a registration does not report success before a majority responds;
- a node that missed a commit during a partition resynchronizes after recovery;
- a connection authenticated by node 2 cannot send a leader-only commit by
  claiming node 1 in the redundant registry source field, and the target
  registry remains unchanged;
- a node-2-authenticated snapshot cannot satisfy a pending node-3
  synchronization that expects coordinator node 1; a same-stream observable
  response proves processing order, and the later node-1 snapshot completes
  the preserved waiter.

`distributed_registry_e2e.rs` exercises two actual Wasm guests through the
production node server. It covers global lookup, a confirmed remote mailbox
request/reply, typed missing-environment and missing-process failures, explicit
unregistration, and owner-exit cleanup. Its three-node response-origin test
creates a spawn waiter for node 2, proves a matching response ID authenticated
as node 3 leaves the waiter pending, then proves the same response from node 2
completes it.

`quic_transport.rs` sends a 4,097-byte `Request::Message`, forcing multiple
production chunks, then checks the message ID and byte-for-byte decoded request at
the dispatch boundary.

## Commands

```bash
cargo test -p lunatic-distributed
cargo test -p lunatic-distributed --test registry_coordination
cargo test -p lunatic-distributed --test quic_transport
cargo test --test distributed_registry_e2e
```

The repository-wide gate is:

```bash
cargo test --all
```

All asynchronous waits are bounded and checked against observable state. The
snapshot-origin regression sends a second request on the same input stream and
waits until its decision is observed at node 2, proving the preceding snapshot
was processed before checking that state and the pending waiter are unchanged.
A valid node-1 response must then complete that same waiter.

## Performance

```bash
cargo bench --bench distributed_messaging
cargo bench --bench distributed_latency
python scripts/check_bench_thresholds.py distributed_messaging distributed_latency
```

The QUIC benchmark uses real mutual TLS and production message framing. It
measures through dispatch, not delivery into a live process mailbox.

## Not Yet Covered

- distributed spawn and process failure recovery;
- cross-node hot reload and rollback;
- cluster-wide process, memory, file, or network quota enforcement;
- sustained throughput, soak, packet-loss, and adversarial partition testing.

The removed `node_failure.rs` and `cross_node_hot_reload.rs` files are not valid
historical evidence: they used obsolete APIs and mock behavior and did not compile
in the current workspace.

See [Distributed Node Identity](../distributed/NODE_IDENTITY.md) for the
certificate and active-membership invariants these tests rely on.
