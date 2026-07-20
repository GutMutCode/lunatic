# Global Process Registry & Location Transparency

**Status**: ⚠️ In-memory registry and global address types implemented; runtime routing and coordination pending
**Date**: 2025-10-07
**Purpose**: Provide the data model and lookup APIs needed for future Erlang-style location-transparent process addressing

---

## Overview

The Global Process Registry provides the **address representation and in-memory lookup foundation** for distributed processes. It does not yet close the runtime location-transparency gap identified in `docs/core_values/status.md`:

> "no distributed OTP equivalents beyond `lunatic-distributed`, which lacks coverage"

and CORE_VALUES.md requirements:

> - Transparent distribution (CORE_VALUES.md:207)
> - Send messages to remote processes (CORE_VALUES.md:208)
> - Location transparency (process IDs work across nodes) (CORE_VALUES.md:209)
> - Distributed process registry (CORE_VALUES.md:214)
> - Global name registration (CORE_VALUES.md:215)

---

## Architecture

### GlobalProcessId

A globally unique process identifier with location transparency:

```rust
pub struct GlobalProcessId {
    node_id: u64,         // Which node the process is on
    environment_id: u64,  // Environment within the node
    process_id: u64,      // Local process ID
}
```

**Key Features:**
- **Location-Aware Addressing**: Represent the node, environment, and process in one value
- **Compact Encoding**: u128 representation for efficient storage
- **Erlang Compatibility**: Matches Erlang's `{Node, Pid}` semantics

**Example:**
```rust
use lunatic_distributed::distributed::GlobalProcessId;

// Create global process ID
let gpid = GlobalProcessId::new(
    node_id: 1,
    environment_id: 1,
    process_id: 100
);

// Check if local
if gpid.is_local(current_node_id) {
    // Send locally
} else {
    // A caller must route to the remote node; registry lookup is not wired
    // into the Lunatic messaging path yet.
}

// Compact encoding for network transfer
let compact = gpid.to_compact(); // u128
let decoded = GlobalProcessId::from_compact(compact);
```

---

## Distributed Registry

### Design

```rust
pub struct DistributedRegistry {
    local: Arc<DashMap<ProcessName, RegistryEntry>>,   // Node-local names
    global: Arc<DashMap<ProcessName, RegistryEntry>>,  // Intended global scope; local map
    reverse: Arc<DashMap<GlobalProcessId, Vec<ProcessName>>>, // Reverse lookup
}
```

### Registration Scopes

#### Local Registration (Node-Scoped)

Names are unique **within the node only**:

```rust
let registry = client.registry();

let gpid = GlobalProcessId::new(node_id, env_id, pid);
registry.register_local("logger", gpid)?;

// Lookup on same node
if let Some(entry) = registry.lookup("logger") {
    println!("Found: {}", entry.global_pid);
}
```

**Use Cases:**
- Node-specific services (local file logger, cache manager)
- Per-node resource managers
- Node-local coordinators

#### Intended Global Registration Scope

Names passed to `register_global` are unique only within that `DistributedRegistry` instance. No cluster synchronization occurs:

```rust
registry.register_global("database_manager", gpid)?;

// Only this registry instance can look up the entry until an external
// coordination path propagates it.
if let Some(entry) = registry.lookup_global("database_manager") {
    println!("Database manager address: {}", entry.global_pid);
}
```

**Intended use cases after coordination is connected:**
- Singleton services (database connection pool, metrics aggregator)
- Cluster-wide coordinators
- Global state managers

---

## API Reference

### Registration

```rust
/// Register locally (node-scoped)
fn register_local(
    &self,
    name: impl Into<ProcessName>,
    global_pid: GlobalProcessId,
) -> Result<()>

/// Register in this registry instance's global namespace
fn register_global(
    &self,
    name: impl Into<ProcessName>,
    global_pid: GlobalProcessId,
) -> Result<()>

/// Unregister a name
fn unregister(&self, name: impl Into<ProcessName>) -> Result<()>

/// Unregister all names for a process (called on process death)
fn unregister_process(&self, global_pid: GlobalProcessId)
```

### Lookup

```rust
/// Lookup in any scope (checks local first, then global)
fn lookup(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry>

/// Lookup local only
fn lookup_local(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry>

/// Lookup global only
fn lookup_global(&self, name: impl Into<ProcessName>) -> Option<RegistryEntry>

/// Reverse lookup: get all names for a process
fn get_names(&self, global_pid: GlobalProcessId) -> Vec<ProcessName>
```

### Introspection

```rust
/// Get all local names
fn local_names(&self) -> Vec<ProcessName>

/// Get all global names
fn global_names(&self) -> Vec<ProcessName>

/// Get counts
fn local_count(&self) -> usize
fn global_count(&self) -> usize
```

---

## Usage Examples

### Example 1: Erlang-Style Process Registration

```rust
use lunatic_distributed::distributed::{GlobalProcessId, Client};

// Create client with registry
let client = Client::new(node_id, control_client, quic_client);
let registry = client.registry();

// Spawn processes and register with meaningful names
let logger_pid = GlobalProcessId::new(1, 1, 101);
let db_pid = GlobalProcessId::new(1, 1, 102);

registry.register_global("logger", logger_pid)?;
registry.register_global("database", db_pid)?;

// Lookup by name in this client's local registry object
if let Some(entry) = registry.lookup("logger") {
    println!("Logger at {}", entry.global_pid);
}

// Cleanup is explicit; process-exit handling does not call this automatically.
registry.unregister_process(logger_pid);
assert!(registry.lookup("logger").is_none());
```

### Example 2: Multi-Alias Registration

```rust
let worker_pid = GlobalProcessId::new(1, 1, 200);

// Register same process with multiple names
registry.register_local("worker", worker_pid)?;
registry.register_local("background_job", worker_pid)?;
registry.register_global("primary_worker", worker_pid)?;

// Reverse lookup shows all names
let names = registry.get_names(worker_pid);
assert_eq!(names.len(), 3);

// Unregister one name
registry.unregister("background_job")?;

// Other names still active
assert!(registry.lookup("worker").is_some());
assert!(registry.lookup("primary_worker").is_some());
```

### Example 3: Cross-Node Routing After External Synchronization

```rust
// Node 1: Register service
let node1_registry = client1.registry();
let service_pid = GlobalProcessId::new(1, 1, 300);
node1_registry.register_global("auth_service", service_pid)?;

// Node 2 does not receive the entry automatically.
let node2_registry = client2.registry();
assert!(node2_registry.lookup_global("auth_service").is_none());

// After an external coordination layer applies the entry to node 2:
node2_registry.register_global("auth_service", service_pid)?;
if let Some(entry) = node2_registry.lookup_global("auth_service") {
    // Found service on node 1
    assert_eq!(entry.global_pid.node_id(), 1);

    // Send message to auth service (cross-node)
    client2.send(SendParams {
        env: EnvironmentId(entry.global_pid.environment_id()),
        src: ProcessId(my_pid),
        node: NodeId(entry.global_pid.node_id()),
        dest: ProcessId(entry.global_pid.process_id()),
        tag: None,
        data: request_bytes,
    }).await?;
}
```

---

## Integration with Distributed Client

Each distributed client owns a registry object and exposes it through `registry()`. Message sending does not perform registry lookup automatically:

```rust
pub struct Client {
    pub node_id: NodeId,
    pub inner: Arc<Inner>,
}

pub struct Inner {
    // ... existing fields
    pub registry: Arc<DistributedRegistry>, // ✅ Built-in registry
}

impl Client {
    /// Get a reference to the distributed process registry
    pub fn registry(&self) -> &DistributedRegistry {
        &self.inner.registry
    }
}
```

**Usage:**
```rust
let client = Client::new(node_id, control_client, quic_client);

// Access registry
let registry = client.registry();

// Register process
let gpid = GlobalProcessId::new(node_id, env_id, pid);
registry.register_local("my_service", gpid)?;

// The caller performs lookup and translates the GlobalProcessId into SendParams.
if let Some(entry) = registry.lookup("target_service") {
    client.send(SendParams {
        node: NodeId(entry.global_pid.node_id()),
        // ... other params
    }).await?;
}
```

---

## Testing

### Unit Tests

The registry includes comprehensive unit tests:

```bash
# Run all distributed registry tests
cargo test --test distributed_registry --package lunatic-distributed

# Example tests:
# - test_global_process_id_basics
# - test_compact_encoding_preserves_data
# - test_registry_local_registration
# - test_registry_global_registration
# - test_erlang_style_workflow
# - test_cross_node_identification
```

**Test Coverage:**
- ✅ GlobalProcessId creation and accessors
- ✅ Compact u128 encoding/decoding
- ✅ Local registration and lookup
- ✅ Global registration and lookup
- ✅ Duplicate registration prevention
- ✅ Unregister by name
- ✅ Unregister-by-process helper (cleanup must be invoked explicitly)
- ✅ Reverse lookup (process → names)
- ✅ Scoped lookups (local-only, global-only)
- ✅ Registration timestamps
- ✅ Erlang-style workflows
- ✅ Node ID distinguishes `GlobalProcessId` values

**Results:**
```
running 14 tests
test test_compact_encoding_preserves_data ... ok
test test_global_process_id_basics ... ok
test test_process_name_conversions ... ok
test test_erlang_style_workflow ... ok
test test_registry_clear ... ok
test test_registry_counters_and_lists ... ok
test test_cross_node_identification ... ok
test test_registry_global_registration ... ok
test test_registry_local_registration ... ok
test test_registry_local_vs_global_scope ... ok
test test_registry_reverse_lookup ... ok
test test_registry_unregister ... ok
test test_registry_unregister_process ... ok
test test_registration_timestamp ... ok

test result: ok. 14 passed; 0 failed; 0 ignored
```

---

## Erlang Compatibility

### Erlang Process Registration

```erlang
% Erlang: Register locally
register(logger, Pid).

% Erlang: Register globally
global:register_name(database, Pid).

% Erlang: Lookup
Pid = whereis(logger).
Pid = global:whereis_name(database).

% Erlang: Unregister
unregister(logger).
global:unregister_name(database).
```

### Lunatic Equivalent

```rust
// Lunatic: Register locally
registry.register_local("logger", gpid)?;

// Lunatic: Register in the current registry's global namespace
registry.register_global("database", gpid)?;

// Lunatic: Lookup
let entry = registry.lookup("logger");
let entry = registry.lookup_global("database");

// Lunatic: Unregister
registry.unregister("logger")?;
registry.unregister("database")?;
```

### Key Differences

| Feature | Erlang | Lunatic |
|---------|--------|---------|
| Local registration | `register/2` | `register_local` |
| Global registration | `global:register_name/2` | `register_global` |
| Lookup | `whereis/1` | `lookup` |
| Process ID format | `Pid` (opaque) | `GlobalProcessId` (explicit node/env/pid) |
| Automatic cleanup | On process exit | Manual `unregister_process(gpid)` helper; not wired to process exit |
| Multi-alias | Not supported | ✅ Supported via reverse map |

---

## Future Enhancements

### 1. Cross-Node Coordination for Global Names

**Current State**: Global names are registered locally without cluster coordination.

**Planned**:
- Control plane integration for global name uniqueness
- Distributed consensus (Raft or Paxos) for global registry
- Automatic conflict resolution on network partition healing

**Implementation:**
```rust
// Future API
registry.register_global_coordinated("database", gpid).await?;
// Returns error if name already registered on another node
```

### 2. Process Groups (pg module equivalent)

```rust
// Register process in a group
registry.join_group("workers", gpid)?;

// Get all processes in a group
let workers = registry.get_group("workers"); // Vec<GlobalProcessId>

// Leave group
registry.leave_group("workers", gpid)?;
```

### 3. Monitoring & Notifications

```rust
// Monitor name registration
registry.monitor_name("database", callback)?;
// Callback fires when name is registered/unregistered

// Watch for process registration
registry.watch_process(gpid, callback)?;
// Callback fires when process registers a new name
```

### 4. TTL-Based Expiration

```rust
// Register with TTL
registry.register_local_with_ttl("temp_cache", gpid, Duration::from_secs(300))?;
// Automatically unregisters after 5 minutes
```

---

## Performance Characteristics

### Lookup Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `lookup` | O(1) | DashMap hash lookup |
| `lookup_local` | O(1) | Single map lookup |
| `lookup_global` | O(1) | Single map lookup |
| `get_names` | O(1) | Reverse map lookup |
| `register_local` | O(1) amortized | DashMap insert + reverse map update |
| `register_global` | O(1) amortized | DashMap insert + reverse map update |
| `unregister` | O(k) | k = number of names for the process |
| `unregister_process` | O(k) | k = number of names for the process |

### Memory Overhead

**Per Registration:**
- ProcessName: ~24 bytes (String + overhead)
- RegistryEntry: ~32 bytes (GlobalProcessId + metadata)
- Reverse map entry: ~32 bytes (Vec allocation)
- **Total**: ~88 bytes per registration

**For 1M registered processes:**
- Memory: ~84 MB (assuming average 1 name per process)
- Lookup latency: < 1μs (DashMap is highly concurrent)

---

## Troubleshooting

### Issue: Duplicate Name Registration

**Symptom:**
```
Error: Name 'logger' already registered locally
```

**Solution:**
Check if name is already in use:
```rust
if registry.lookup("logger").is_some() {
    // Name already taken, choose different name or unregister first
    registry.unregister("logger")?;
}
registry.register_local("logger", gpid)?;
```

### Issue: Process Not Found After Registration

**Symptom:**
Lookup returns `None` immediately after registration.

**Possible Causes:**
1. Typo in name
2. Wrong scope (looking in global when registered local)
3. Process was unregistered by another thread

**Debug:**
```rust
// Check both scopes
if let Some(entry) = registry.lookup_local("myservice") {
    println!("Found in local: {:?}", entry);
}
if let Some(entry) = registry.lookup_global("myservice") {
    println!("Found in global: {:?}", entry);
}

// List all names
println!("Local names: {:?}", registry.local_names());
println!("Global names: {:?}", registry.global_names());
```

### Issue: Memory Leak from Uncleared Registrations

**Symptom:**
Registry growing unbounded after many process spawns/deaths.

**Solution:**
Ensure `unregister_process` is called when processes terminate:

```rust
// In process cleanup handler
registry.unregister_process(gpid);
```

---

## References

### Internal
- `crates/lunatic-distributed/src/distributed/global_process_id.rs`: GlobalProcessId implementation
- `crates/lunatic-distributed/src/distributed/registry.rs`: DistributedRegistry implementation
- `crates/lunatic-distributed/tests/distributed_registry.rs`: Comprehensive test suite
- `docs/core_values/status.md`: CORE_VALUES compliance tracking

### External
- [Erlang Process Registration](https://www.erlang.org/doc/reference_manual/processes.html#process-registration)
- [Erlang global module](https://www.erlang.org/doc/man/global.html)
- [Erlang pg module (Process Groups)](https://www.erlang.org/doc/man/pg.html)
- [Distributed Erlang](https://learnyousomeerlang.com/distribunomicon)

---

## Conclusion

The Global Process Registry establishes part of the foundation for Erlang-style location transparency:

1. ⚠️ **Global address representation**: Identify a process by node, environment, and process ID; transparent routing is pending
2. ⚠️ **Dual namespace maps**: Local and global maps exist within one registry instance; cluster-wide synchronization is pending
3. ✅ **Reverse Lookup**: Find all names for a given process
4. ⚠️ **Cleanup helper**: Unregister all names for a process when explicitly called; process-exit integration is pending
5. ⚠️ **Erlang-inspired APIs**: Similar map operations exist, but cluster-wide semantics are not yet provided

### Status

- ✅ GlobalProcessId: Implemented with compact u128 encoding
- ✅ DistributedRegistry: Implemented with DashMap for concurrency
- ✅ Local registration: Fully functional
- ✅ Global registration: Functional (local coordination only)
- ✅ Test suite: 14 tests covering all major features
- ❌ Registry lookup is not connected to message routing
- ❌ Cross-node global coordination transport is not implemented

### Next Steps

1. Integrate with control plane for true global coordination
2. Add process groups (pg module equivalent)
3. Implement name monitoring and notifications
4. Add TTL-based registration expiration

**Gap Status**: ⚠️ **FOUNDATION ONLY** — data structures and local operations are implemented; transport integration and real multi-node tests remain required.
