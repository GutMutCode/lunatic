# Global Process Registry Cross-Node Coordination

**Status**: ⚠️ Protocol state machine implemented; network transport integration pending
**Date**: 2025-10-07
**Purpose**: Define the state and messages required for cluster-wide global name registration with conflict resolution

---

## Overview

This document describes the **cross-node coordination protocol** for the global process registry, building on top of the basic distributed registry implementation.

**Previous Gap** (`docs/distributed/GLOBAL_PROCESS_REGISTRY.md`):
> "Global registration: Functional (local coordination only)"
> "Cross-node global coordination: Future enhancement"

**Current Boundary**: The message types and handler-side state transitions exist. Multi-node `register_global_coordinated` only stores a pending request and returns; it does not send the request, wait for responses, or broadcast the resulting notification. The existing tests call handlers directly without a control-plane or QUIC connection.

---

## Architecture

### Coordination Protocol

The `RegistryCoordinator` contains the local state machine for an intended **majority-based coordination protocol**:

1. **Request Phase**: The transport must send a registration request to all other nodes — not wired
2. **Vote Phase**: Handler methods can check for conflicts and create accept/reject responses
3. **Commit Phase**: Response aggregation can create a notification after enough successes, but no caller drives this over the network
4. **Apply Phase**: Handler methods can apply a notification to a node-local registry

```
Node 1 (initiator)    Node 2               Node 3
     |                  |                    |
     |--- RegisterReq ->|                    |
     |--- RegisterReq ---------------->      |
     |                  |                    |
     |<-- Success ------                     |
     |<-- Success -----------------------    |
     |                  |                    |
     |--- Notify ------>|                    |
     |--- Notify ---------------------->     |
     |                  |                    |
   [Apply]            [Apply]             [Apply]
```

### Message Types

```rust
pub enum RegistryCoordinationMessage {
    // Registration workflow
    GlobalRegisterRequest { request_id, requesting_node_id, name, global_pid },
    GlobalRegisterResponse { request_id, result },
    GlobalRegisterNotify { name, global_pid, registered_at },

    // Unregistration workflow
    GlobalUnregisterRequest { request_id, requesting_node_id, name },
    GlobalUnregisterResponse { request_id, result },
    GlobalUnregisterNotify { name },

    // Synchronization for new nodes
    RegistrySyncRequest { request_id, requesting_node_id },
    RegistrySyncResponse { request_id, global_entries },

    // Failure detection
    RegistryHeartbeat { node_id, timestamp },
}
```

---

## Usage

### Basic Global Registration (Single Node)

For single-node setups, registration is immediate (no coordination needed):

```rust
let registry = Arc::new(DistributedRegistry::new(node_id));
let coordinator = Arc::new(RegistryCoordinator::new(registry.clone(), node_id));

let gpid = GlobalProcessId::new(node_id, env_id, pid);

// Single node - registers immediately
coordinator.register_global_coordinated("service", gpid, 1).await?;

assert!(registry.lookup_global("service").is_some());
```

### Multi-Node Global Registration

The following is an API-level orchestration sketch, not a currently connected runtime flow:

```rust
// Node 1: Initiate registration
let gpid = GlobalProcessId::new(1, 1, 100);
coordinator1.register_global_coordinated("database", gpid, 3).await?;
// Current behavior: creates pending state and returns immediately.
// It does not return a request message or wait for a majority.

// Node 2 & 3: Handle request
let response = coordinator2.handle_register_request(
    request_id,
    1, // requesting_node_id
    "database".to_string(),
    gpid
).await;

// If no conflict, response is Success
match response {
    GlobalRegisterResponse { result: Success, .. } => {
        // Send response back to Node 1
    }
    _ => {}
}

// Node 1: Collect responses
// A future transport integration must serialize, send, receive, and dispatch
// RegistryCoordinationMessage values. The current implementation does not.

// Node 1: Broadcast notification after majority
coordinator1.handle_register_notify("database".to_string(), gpid, timestamp).await?;

// Node 2 & 3: Apply notification
coordinator2.handle_register_notify("database".to_string(), gpid, timestamp).await?;
coordinator3.handle_register_notify("database".to_string(), gpid, timestamp).await?;

// All nodes now have the registration
```

### Conflict Resolution

The current request handler detects an existing-name conflict and reports it. It does not select a winner, compare timestamps, or converge conflicting registries:

```rust
// Node 2 already has "cache_manager" registered
let gpid2 = GlobalProcessId::new(2, 1, 200);
registry2.register_global("cache_manager", gpid2)?;

// Node 1 tries to register same name
let gpid1 = GlobalProcessId::new(1, 1, 100);
let response = coordinator2.handle_register_request(
    request_id,
    1,
    "cache_manager".to_string(),
    gpid1
).await;

// Response indicates conflict
match response {
    GlobalRegisterResponse { result: AlreadyRegistered { existing_gpid, registered_at }, .. } => {
        // The caller can retry with a different name. Automatic resolution is
        // not implemented.
        assert_eq!(existing_gpid, gpid2);
    }
    _ => {}
}
```

### New Node Joining Cluster

Synchronization handlers can export and apply registry state when an external caller invokes them. Node-join detection and message delivery are not connected:

```rust
// Node 4 (new) joins cluster
let registry4 = Arc::new(DistributedRegistry::new(4));
let coordinator4 = Arc::new(RegistryCoordinator::new(registry4.clone(), 4));

// A future transport layer must send a request to an existing node.

// Node 1: Handle sync request
let response = coordinator1.handle_sync_request(request_id, 4).await;

match response {
    RegistrySyncResponse { global_entries, .. } => {
        // Node 4: Apply all global entries
        coordinator4.handle_sync_response(global_entries).await?;
    }
    _ => {}
}

// Node 4 now has all global registrations
assert_eq!(registry4.global_count(), registry1.global_count());
```

### Node Failure Cleanup

The cleanup helper removes entries for a supplied node ID. Failure detection and cluster-wide invocation are not implemented:

```rust
// Node 2 fails
// An external failure detector would need to call this on each node.
let cleaned_names = coordinator1.cleanup_node_registrations(2).await;

println!("Cleaned up {} services from failed node 2", cleaned_names.len());

// Global registrations from Node 2 are removed
for name in cleaned_names {
    assert!(registry1.lookup_global(name.as_str()).is_none());
}
```

---

## API Reference

### RegistryCoordinator

```rust
impl RegistryCoordinator {
    /// Create coordinator for a node
    pub fn new(registry: Arc<DistributedRegistry>, node_id: u64) -> Self

    /// Request global registration with cross-node coordination
    pub async fn register_global_coordinated(
        &self,
        name: impl Into<ProcessName>,
        global_pid: GlobalProcessId,
        node_count: usize,
    ) -> Result<()>

    /// Handle registration request from another node
    pub async fn handle_register_request(
        &self,
        request_id: u64,
        requesting_node_id: u64,
        name: String,
        global_pid: GlobalProcessId,
    ) -> RegistryCoordinationMessage

    /// Handle registration response
    pub async fn handle_register_response(
        &self,
        request_id: u64,
        result: GlobalRegisterResult,
    ) -> Result<Option<RegistryCoordinationMessage>>

    /// Handle registration notification (commit)
    pub async fn handle_register_notify(
        &self,
        name: String,
        global_pid: GlobalProcessId,
        registered_at: u64,
    ) -> Result<()>

    /// Handle unregistration request
    pub async fn handle_unregister_request(
        &self,
        request_id: u64,
        requesting_node_id: u64,
        name: String,
    ) -> RegistryCoordinationMessage

    /// Handle unregistration notification
    pub async fn handle_unregister_notify(&self, name: String) -> Result<()>

    /// Handle sync request from new node
    pub async fn handle_sync_request(
        &self,
        request_id: u64,
        requesting_node_id: u64,
    ) -> RegistryCoordinationMessage

    /// Handle sync response (apply to local registry)
    pub async fn handle_sync_response(
        &self,
        global_entries: Vec<(String, GlobalProcessId, u64)>,
    ) -> Result<()>

    /// Clean up registrations for failed node
    pub async fn cleanup_node_registrations(
        &self,
        failed_node_id: u64,
    ) -> Vec<ProcessName>
}
```

---

## Integration with Distributed Client

The distributed client constructs and exposes a coordinator, but does not dispatch coordination messages or call it from the message transport:

```rust
pub struct Client {
    pub node_id: NodeId,
    pub inner: Arc<Inner>,
}

pub struct Inner {
    // ... existing fields
    pub registry: Arc<DistributedRegistry>,
    pub coordinator: Arc<RegistryCoordinator>, // ✅ Built-in coordinator
}

impl Client {
    /// Get coordinator reference
    pub fn coordinator(&self) -> &RegistryCoordinator {
        &self.inner.coordinator
    }
}
```

**Usage:**
```rust
let client = Client::new(node_id, control_client, quic_client);

// Access coordinator
let coordinator = client.coordinator();

// Multi-node calls currently create pending state and return Ok immediately.
// They neither register the name nor contact peers.
let gpid = GlobalProcessId::new(node_id, env_id, pid);
coordinator.register_global_coordinated("service", gpid, node_count).await?;
```

---

## Test Coverage

### Unit Tests (8 tests in `registry_coordination.rs`)

```bash
cargo test --package lunatic-distributed --lib registry_coordination
```

Tests:
- ✅ `test_coordinator_creation`
- ✅ `test_single_node_registration`
- ✅ `test_handle_register_request_no_conflict`
- ✅ `test_handle_register_request_with_conflict`
- ✅ `test_handle_register_notify`
- ✅ `test_cleanup_node_registrations`
- ✅ `test_handle_sync_request`
- ✅ `test_handle_sync_response`

### Handler Integration Tests (8 tests in `tests/registry_coordination.rs`)

```bash
cargo test --test registry_coordination --package lunatic-distributed
```

Tests:
- ✅ `test_cross_node_registration_workflow`
- ✅ `test_global_registration_conflict`
- ✅ `test_registry_sync_for_new_node`
- ✅ `test_node_failure_cleanup`
- ✅ `test_single_node_fast_path`
- ✅ `test_idempotent_notifications`
- ✅ `test_unregistration_workflow`
- ✅ `test_three_node_cluster_simulation`

These tests instantiate multiple registry/coordinator objects in one process and call handler methods directly. They do not open sockets, dispatch `RegistryCoordinationMessage`, wait for a real quorum, or run independent Lunatic nodes.

**Results:**
```
running 8 tests
........
test result: ok. 8 passed; 0 failed
```

---

## Performance Characteristics

### Registration Latency

The following values are design estimates. No multi-node registry transport benchmark currently measures them.

| Scenario | Latency | Notes |
|----------|---------|-------|
| Single node | O(1) | ~1μs (DashMap insert) |
| 3-node cluster | O(n) | ~10-50ms (network RTT × 2) |
| 10-node cluster | O(n) | ~30-100ms (network RTT × 2) |

### Majority Calculation

For `n` nodes:
- Majority = `(n / 2) + 1`
- Minimum responses needed: `⌈n/2⌉ + 1`

Examples:
- 1 node: immediate (no coordination)
- 3 nodes: need 2 responses
- 5 nodes: need 3 responses
- 7 nodes: need 4 responses

### Network Messages

Per global registration:
- **Requests**: `n - 1` (to all other nodes)
- **Responses**: `n - 1` (from all nodes)
- **Notifications**: `n - 1` (broadcast to all)
- **Total**: `3(n - 1)` messages

For 5-node cluster: **12 messages** per registration

---

## Failure Scenarios

### Split-Brain Prevention

**Problem**: Network partition creates two separate clusters

**Intended solution**: a connected majority protocol could prevent both sides from accepting registrations. The current runtime cannot provide this guarantee because coordination messages are not transported.

Example (5-node cluster splits 2-3):
```
Partition A (2 nodes): Cannot achieve majority (need 3)
Partition B (3 nodes): Can achieve majority ✅

Only Partition B can register global names
Partition A rejects all requests (not enough nodes)
```

### Conflict Resolution Strategy

Conflict convergence remains a design item:
1. Existing-name requests can return `AlreadyRegistered`.
2. `handle_register_notify` currently ignores the supplied timestamp and ignores duplicate-registration errors.
3. No timestamp comparison, node-ID tiebreaker, or broadcast convergence path is implemented.

### Node Failure Detection

**Heartbeat Mechanism** (future enhancement):
```rust
// Each node sends periodic heartbeats
RegistryHeartbeat { node_id, timestamp }

// If node doesn't respond within timeout:
coordinator.cleanup_node_registrations(failed_node_id).await;
```

---

## Future Enhancements

### 1. Persistent Storage

Currently, registry state is in-memory only.

**Planned:**
- Save global registry to disk/database
- Restore on node restart
- Avoid re-synchronization overhead

### 2. Optimistic Registration

For low-latency use cases:

```rust
// Register locally first, then confirm with cluster
registry.register_global_optimistic("service", gpid)?;

// Background: coordinate with other nodes
// If conflict detected, rollback local registration
```

### 3. Leases & TTL

Auto-expire registrations after timeout:

```rust
coordinator.register_global_with_lease(
    "temp_service",
    gpid,
    Duration::from_secs(300) // 5-minute lease
).await?;
```

### 4. Raft Consensus Integration

Replace custom majority protocol with Raft for stronger guarantees:
- Guaranteed single leader
- Log replication
- Better partition handling

---

## Comparison with Erlang

### Erlang `global` module

```erlang
% Erlang global registration
global:register_name(database, Pid).
global:whereis_name(database).
global:unregister_name(database).
```

### Lunatic Equivalent

```rust
// Lunatic global registration
coordinator.register_global_coordinated("database", gpid, node_count).await?;
registry.lookup_global("database");
coordinator.handle_unregister_notify("database".to_string()).await?;
```

### Key Differences

| Feature | Erlang `global` | Lunatic `RegistryCoordinator` |
|---------|----------------|-------------------------------|
| Coordination | Built-in Erlang distribution | Local coordination state machine; transport pending |
| Conflict resolution | Last writer wins | Conflict response type only |
| Network protocol | Erlang term format | Coordination messages are serializable but not dispatched |
| Failure detection | `net_kernel` monitoring | Heartbeat (planned) |
| Persistence | In-memory only | In-memory (disk planned) |

---

## Troubleshooting

### Issue: Multi-Node Registration Returns Before Registration Exists

**Symptom**: `register_global_coordinated(..., node_count > 1)` returns `Ok(())`, but `lookup_global` still returns `None`.

**Cause:** This is the current implementation boundary. The method stores private pending state but does not expose/send a request or wait for responses.

**Required fix:** Return or dispatch a request message, route responses back to `handle_register_response`, await a terminal result with timeout/cancellation, and broadcast/apply the commit notification.

### Issue: Conflicts Not Detected

**Symptom**: Same name registered on multiple nodes

**Possible Causes:**
1. Notifications not broadcasted
2. Network partition during registration
3. Race condition between concurrent registrations

**Debug:**
```rust
// Check all nodes have same global count
for node_id in node_ids {
    let count = registry.global_count();
    println!("Node {} global count: {}", node_id, count);
}
```

### Issue: Stale Registrations After Node Failure

**Symptom**: Failed node's registrations still present

**Solution:**
Ensure cleanup is called:
```rust
// Detect node failure (e.g., via heartbeat timeout)
if !node.is_alive() {
    coordinator.cleanup_node_registrations(node.id()).await;
}
```

---

## References

### Internal
- `crates/lunatic-distributed/src/distributed/registry_coordination.rs`: Implementation
- `crates/lunatic-distributed/tests/registry_coordination.rs`: Integration tests
- `docs/distributed/GLOBAL_PROCESS_REGISTRY.md`: Basic registry documentation
- `docs/core_values/status.md`: CORE_VALUES compliance

### External
- [Erlang global module](https://www.erlang.org/doc/man/global.html)
- [Raft Consensus Algorithm](https://raft.github.io/)
- [Distributed Consensus Patterns](https://martinfowler.com/articles/patterns-of-distributed-systems/)

---

## Conclusion

The repository contains the registry data model and coordination state-machine building blocks:

1. ✅ **Response aggregation logic**: counts successful handler responses
2. ✅ **Conflict response types**: represents existing-name conflicts
3. ✅ **Cleanup helper**: removes registrations when explicitly given a failed node ID
4. ✅ **Synchronization handlers**: export and apply an in-memory snapshot
5. ✅ **Idempotent notification handlers**: tolerate repeated local application
6. ⚠️ **Test coverage**: unit and integration tests manually orchestrate these handlers; they do not run networked nodes

### Status Summary

- ✅ Basic distributed registry (local coordination)
- ✅ `GlobalProcessId` address representation
- ⚠️ Cross-node coordination state machine
- ❌ Control/QUIC serialization and dispatch
- ❌ Quorum wait in the public registration call
- ❌ Live split-brain/conflict-resolution guarantee
- ⚠️ Node failure cleanup and new-node synchronization helpers require an external caller
- ⚠️  Persistent storage (future)
- ⚠️  Raft integration (future)
- ⚠️  Heartbeat monitoring (future)

**Gap Status**: ❌ **OPEN** — connect coordination messages to the distributed transport and validate concurrent registration, quorum, partition, recovery, and convergence with real nodes.
