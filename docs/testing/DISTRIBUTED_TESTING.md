# Distributed Runtime Testing

**Status**: Active
**Last reviewed**: 2026-07-21

Distributed completion claims must cross the production network boundary. Tests
that invoke coordination handlers directly are useful unit tests but are not
reported as multi-node E2E evidence.

## Test Inventory

| File | Scope |
| --- | --- |
| `crates/lunatic-distributed/src/distributed/registry.rs` | Local/global registry unit behavior |
| `crates/lunatic-distributed/src/distributed/registry_coordination.rs` | Coordinator state-machine and snapshot unit behavior |
| `crates/lunatic-distributed/tests/distributed_registry.rs` | Public registry and identifier contracts |
| `crates/lunatic-distributed/tests/registry_coordination.rs` | Real 2/3/5-node mTLS QUIC coordination |
| `crates/lunatic-distributed/tests/quic_transport.rs` | Production QUIC framing, reassembly, decode and dispatch |

## Networked E2E Coverage

`registry_coordination.rs` binds a separate QUIC endpoint for every node, creates
node certificates signed by the test root, uses the production distributed
client, serializes registry messages as `Request::Registry`, and sends them
through the production QUIC transport.

It verifies:

- simultaneous same-name registration has one winner in 2-, 3-, and 5-node
  clusters;
- request IDs from different nodes do not collide at the coordinator;
- a registration does not report success before a majority responds;
- a node that missed a commit during a partition resynchronizes after recovery.

`quic_transport.rs` sends a 4,097-byte `Request::Message`, forcing multiple
production chunks, then checks the message ID and byte-for-byte decoded request at
the dispatch boundary.

## Commands

```bash
cargo test -p lunatic-distributed
cargo test -p lunatic-distributed --test registry_coordination
cargo test -p lunatic-distributed --test quic_transport
```

The repository-wide gate is:

```bash
cargo test --all
```

All asynchronous waits are bounded. Short polling intervals are used only to
observe eventual convergence; a sleep by itself is never treated as proof of
success.

## Performance

```bash
cargo bench --bench distributed_messaging
cargo bench --bench distributed_latency
python scripts/check_bench_thresholds.py distributed_messaging distributed_latency
```

The QUIC benchmark uses real mutual TLS and production message framing. It
measures through dispatch, not delivery into a live process mailbox.

## Not Yet Covered

- live cross-node process-mailbox round-trip correctness and latency;
- distributed spawn and process failure recovery;
- cross-node hot reload and rollback;
- cluster-wide process, memory, file, or network quota enforcement;
- sustained throughput, soak, packet-loss, and adversarial partition testing.

The removed `node_failure.rs` and `cross_node_hot_reload.rs` files are not valid
historical evidence: they used obsolete APIs and mock behavior and did not compile
in the current workspace.
