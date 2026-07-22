# Phase 7: Resource Migration Infrastructure (historical report)

**Date**: October 5, 2025
**Branch**: feature/phase4-state-preservation
**Status**: Historical phase status; current claims are governed by `docs/core_values/status.md`

> **SECURITY CORRECTION (July 22, 2026)**: The October 6, 2025 claim that serialized
> TLS listener migration preserved certificate/key bytes is obsolete. Version-2
> `ResourceMigrationSnapshot` listener entries contain only the local address and an opaque
> 128-bit credential-provider handle; they never contain a certificate or raw private key.
> Production in-process hot reload moves the live listener and rustls acceptor without
> serialization. See [TLS listener credentials](../tls/TLS_LISTENER_CREDENTIALS.md) and
> [TLS stream migration](../tls/TLS_STREAM_MIGRATION.md).

Unversioned and unknown-version snapshots are rejected. There is no automatic legacy importer.
Operators upgrading from the old unversioned format must purge old copies and backups, rotate or
revoke any key that may have appeared in them, and securely reprovision fresh version-2 handles.

---

## Executive Summary

Phase 7 implements the **infrastructure** for resource migration during hot reload. Rather than attempting automatic migration of all resource types (which is complex and error-prone), we provide:

1. ✅ **ResourceMigrationSnapshot** data structure
2. ✅ **HotReloadContext** integration for resource snapshots  
3. ✅ **Pattern documentation** for application-level migration
4. ✅ **Test infrastructure** for validation

This approach follows the "simplicity scales" principle: provide the mechanism, let applications implement policy.

---

## What We Implemented

### 1. ResourceMigrationSnapshot (`crates/lunatic-process/src/resource_migration.rs`)

**Current core data structure (abridged)**:
```rust
pub enum ResourceSnapshot {
    TcpConnection { peer_addr: String, local_addr: String },
    TcpListener { local_addr: String },
    TlsClientConnectionMetadata { /* descriptive fields only */ },
    TlsServerConnectionMetadata { /* descriptive fields only */ },
    TlsListener {
        local_addr: String,
        credential_handle: TlsCredentialHandle, // opaque [u8; 16]
    },
    UdpSocket { local_addr: String },
    NonMigratable { resource_type: String, reason: String },
}

pub struct ResourceMigrationSnapshot {
    pub tcp_listeners: HashMap<u64, ResourceSnapshot>,
    pub tcp_streams: HashMap<u64, ResourceSnapshot>,
    pub tls_listeners: HashMap<u64, ResourceSnapshot>,
    pub tls_streams: HashMap<u64, ResourceSnapshot>,
    pub udp_sockets: HashMap<u64, ResourceSnapshot>,
}
```

**Features**:
- Versioned (`LUNRSNP\0` plus version `2`) bincode payload for storage/transfer
- Type-safe resource identification
- Extensible for new resource types
- Count and empty checks
- TLS listener snapshots contain an address and scoped provider handle, not certificate/private-key
  bytes
- Active TLS stream entries are descriptive metadata and cannot recreate the byte stream
- Unversioned, missing-version, and unknown-version payloads fail closed

**Example Usage**:
```rust
let mut snapshot = ResourceMigrationSnapshot::new();

snapshot.add_tcp_stream(
    1,
    ResourceSnapshot::TcpConnection {
        peer_addr: "127.0.0.1:8080".to_string(),
        local_addr: "127.0.0.1:12345".to_string(),
    },
);

// Serialize
let bytes = snapshot.to_bytes()?;

// Deserialize
let restored = ResourceMigrationSnapshot::from_bytes(&bytes)?;
```

The default listener credential provider is process-local, five-minute, and single-use. Persisted
or restart restoration requires an explicitly injected provider that can securely reprovision the
same handle and `(environment_id, process_id)` scope. Because lookup consumes a handle before bind
completion, every retry requires a fresh snapshot or freshly provisioned handles.

---

### 2. HotReloadContext Integration

**Extended Context**:
```rust
pub struct HotReloadContext<S: ProcessState + Send> {
    pub module_id: u64,
    pub old_version: u32,
    pub new_version: u32,
    pub memory_snapshot: MemorySnapshot,
    pub resource_snapshot: Option<ResourceMigrationSnapshot>,  // ← NEW
    _phantom: std::marker::PhantomData<S>,
}
```

**New Methods**:
```rust
impl<S: ProcessState + Send> HotReloadContext<S> {
    pub fn set_resource_snapshot(&mut self, snapshot: ResourceMigrationSnapshot) {
        if !snapshot.is_empty() {
            info!("Captured {} resources for migration", snapshot.count());
            self.resource_snapshot = Some(snapshot);
        }
    }

    pub fn get_resource_snapshot(&self) -> Option<&ResourceMigrationSnapshot> {
        self.resource_snapshot.as_ref()
    }
}
```

---

### 3. MigratableResource Trait

**Design**:
```rust
pub trait MigratableResource {
    fn snapshot(&self) -> Result<ResourceSnapshot>;
    fn restore(snapshot: &ResourceSnapshot) -> Result<Self> where Self: Sized;
}
```

**Purpose**: Future-proof extension point for resource types that can implement automatic migration.

**Current Status**: Trait defined, implementations are application responsibility.

---

## Migration Patterns (Application-Level)

### Pattern 1: Reconnection on Reload

**When to Use**: TCP connections, database connections

**Example**:
```rust
use lunatic_process::reloadable_state::ReloadableState;

struct MyState {
    server_addr: Option<String>,
    tcp: Option<TcpConnection>,
    // ... other state
}

impl ReloadableState for MyState {
    fn code_change(&mut self, old_version: u32, new_version: u32) -> Result<()> {
        // After reload, reconnect using saved address
        if let Some(addr) = &self.server_addr {
            self.tcp = Some(TcpConnection::connect(addr)?);
            log::info!("Reconnected to {} after reload", addr);
        }
        Ok(())
    }

    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Save connection metadata, not the connection itself
        let metadata = StateMetadata {
            server_addr: self.server_addr.clone(),
        };
        Ok(bincode::serialize(&metadata)?)
    }

    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let metadata: StateMetadata = bincode::deserialize(bytes)?;
        Ok(Self {
            server_addr: metadata.server_addr,
            tcp: None,  // Will reconnect in code_change()
        })
    }
}
```

**Pros**:
- Simple and reliable
- Works across versions
- No resource handle complexity

**Cons**:
- Brief connection interruption
- TCP handshake overhead

---

### Pattern 2: Listener Rebinding

**When to Use**: TCP/TLS listeners that need to maintain bound port

**Example**:
```rust
struct ServerState {
    listen_addr: String,
    listener: Option<TcpListener>,
}

impl ReloadableState for ServerState {
    fn code_change(&mut self, old: u32, new: u32) -> Result<()> {
        // Rebind to same address
        self.listener = Some(TcpListener::bind(&self.listen_addr)?);
        log::info!("Rebound listener on {}", self.listen_addr);
        Ok(())
    }

    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Only save the address
        Ok(bincode::serialize(&self.listen_addr)?)
    }

    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let listen_addr: String = bincode::deserialize(bytes)?;
        Ok(Self {
            listen_addr,
            listener: None,  // Will rebind in code_change()
        })
    }
}
```

**Pros**:
- Maintains port binding
- Clean state management

**Cons**:
- Brief unavailability during reload
- Port must not be SO_REUSEADDR

---

### Pattern 3: Resource Pooling

**When to Use**: Database connections, reusable resources

**Example**:
```rust
struct PooledState {
    pool_config: PoolConfig,
    pool: Option<ConnectionPool>,
}

impl ReloadableState for PooledState {
    fn code_change(&mut self, old: u32, new: u32) -> Result<()> {
        // Recreate pool with same config
        self.pool = Some(ConnectionPool::new(&self.pool_config)?);
        log::info!("Recreated connection pool after reload");
        Ok(())
    }

    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Save config, not connections
        Ok(bincode::serialize(&self.pool_config)?)
    }

    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let pool_config: PoolConfig = bincode::deserialize(bytes)?;
        Ok(Self {
            pool_config,
            pool: None,
        })
    }
}
```

**Pros**:
- Maintains connection pool size
- Fresh connections after reload

**Cons**:
- Connection setup cost
- Brief pool unavailability

---

### Pattern 4: Stateless Resources (Future File Descriptors)

**Current Status**: Not implemented due to WASI limitations

**When Available**: WASI preview2 with resource handle transfer

**Concept**:
```rust
// Future API (not yet available)
struct FileState {
    file_handles: HashMap<String, FileHandle>,
}

impl MigratableResource for FileHandle {
    fn snapshot(&self) -> Result<ResourceSnapshot> {
        Ok(ResourceSnapshot::FileDescriptor {
            path: self.path.clone(),
            flags: self.flags,
            position: self.position,
        })
    }

    fn restore(snapshot: &ResourceSnapshot) -> Result<Self> {
        if let ResourceSnapshot::FileDescriptor { path, flags, position } = snapshot {
            let mut file = File::open_with_flags(path, flags)?;
            file.seek(SeekFrom::Start(position))?;
            Ok(Self::from(file))
        } else {
            Err(anyhow!("Invalid snapshot type"))
        }
    }
}
```

---

## Why Application-Level Migration?

### Design Rationale

**Problem**: Automatic resource migration is complex because:
1. Different resources have different semantics (TCP vs UDP vs files)
2. Migration strategies depend on application requirements
3. Some migrations need application-specific logic (auth, state sync)
4. Error handling varies by use case

**Solution**: Provide infrastructure, let applications decide policy

**Benefits**:
- ✅ Simple runtime implementation
- ✅ Flexible application control
- ✅ Clear responsibility boundaries
- ✅ Easier to reason about failures

**Tradeoffs**:
- ⚠️ Applications must implement migration logic
- ⚠️ Not "automatic" like Erlang (but Erlang's is also limited)
- ✅ More explicit than implicit (aligned with Rust philosophy)

---

## Comparison with Erlang

### Erlang's Approach

**Process Dictionary**:
- Erlang stores process-local data in process dictionary
- Not directly migrated, must be handled in `code_change/3`

**Open Ports**:
- File descriptors and ports are **not** automatically migrated
- Application must close old and reopen new

**gen_server State**:
- State term is passed to `code_change/3`
- Application transforms old state to new state

**Verdict**: Lunatic's approach is **similar to Erlang** (application responsibility)

### What Lunatic Adds

**Better than Erlang**:
- ✅ Strong typing (ResourceSnapshot enum)
- ✅ Serialization infrastructure (bincode)
- ✅ Explicit migration hooks (ReloadableState trait)

**Same as Erlang**:
- ⚠️ Application implements migration logic
- ⚠️ Network connections require reconnection
- ⚠️ File descriptors require reopen

**Not Yet Implemented** (future):
- ❌ Process registry migration (Phase 9)
- ❌ Distributed resource migration (Phase 10)

---

## Testing

### Unit Tests

**Snapshot Serialization**:
```rust
#[test]
fn test_resource_snapshot_serialization() {
    let mut snapshot = ResourceMigrationSnapshot::new();
    
    snapshot.add_tcp_stream(1, ResourceSnapshot::TcpConnection {
        peer_addr: "127.0.0.1:8080".to_string(),
        local_addr: "127.0.0.1:12345".to_string(),
    });

    let bytes = snapshot.to_bytes().unwrap();
    let restored = ResourceMigrationSnapshot::from_bytes(&bytes).unwrap();
    
    assert_eq!(restored.tcp_streams.len(), 1);
}
```

**Current targeted evidence**:

```bash
cargo test -p lunatic-process --lib test_resource_snapshot_serialization
cargo test -p lunatic-process --lib legacy_unversioned_snapshot_is_rejected_without_echoing_secret_bytes
cargo test -p lunatic-process --lib unknown_and_truncated_versions_are_rejected_without_payload_details
cargo test -p lunatic-runtime --test tls_resource_migration
cargo test -p lunatic-runtime --lib state::tests::serialized_tls_listener_reinjects_without_private_key_bytes
cargo test -p lunatic-runtime --lib state::tests::hot_reload_transfers_live_tls_listener_without_provider_lookup
cargo test -p lunatic-runtime --test capability_attenuation tcp_dns_and_tls_host_paths_emit_typed_terminal_events_once
```

These tests separate versioned serialization, fail-closed legacy rejection, provider-backed
listener rebind, production live-object transfer, const guest-input preservation, and temporary
host key-copy zeroization. They do not make active serialized TLS streams restorable.

### Integration Pattern (Application Responsibility)

Applications should test their migration logic:
```rust
#[tokio::test]
async fn test_tcp_reconnection_pattern() {
    // 1. Start server
    let server = TcpListener::bind("127.0.0.1:0").await?;
    let addr = server.local_addr()?;

    // 2. Create state with connection
    let mut state = MyState {
        server_addr: Some(addr.to_string()),
        tcp: Some(TcpStream::connect(&addr).await?),
    };

    // 3. Simulate hot reload
    let serialized = state.serialize_state()?;
    let mut new_state = MyState::deserialize_state(&serialized)?;
    new_state.code_change(1, 2)?;

    // 4. Verify reconnection
    assert!(new_state.tcp.is_some());
    assert_eq!(new_state.server_addr, Some(addr.to_string()));
}
```

---

## Files Changed

### New Files (3)

1. **`crates/lunatic-process/src/resource_migration.rs`** (~140 lines)
   - ResourceSnapshot enum
   - ResourceMigrationSnapshot struct
   - MigratableResource trait
   - Serialization helpers
   - Unit tests

2. **`docs/PHASE7_RESOURCE_MIGRATION.md`** (this file)
   - Implementation details
   - Migration patterns
   - Application guide

3. **Tests** (integrated into resource_migration.rs)
   - Snapshot serialization test
   - Empty snapshot test

### Modified Files (2)

1. **`crates/lunatic-process/src/hot_reload.rs`** (~15 lines)
   - Added resource_snapshot field to HotReloadContext
   - Added set_resource_snapshot() method
   - Added get_resource_snapshot() method

2. **`crates/lunatic-process/Cargo.toml`** (~1 line)
   - Added bincode dependency

3. **`crates/lunatic-process/src/lib.rs`** (~1 line)
   - Added pub mod resource_migration

---

## Impact Assessment

### ✅ What's Complete

1. **Infrastructure**: Resource snapshot data structures ✅
2. **Integration**: HotReloadContext can store/retrieve snapshots ✅
3. **Patterns**: Documented 4 migration patterns ✅
4. **Tests**: Serialization validated ✅

### ⏳ What's Application Responsibility

1. **Migration Logic**: Apps implement ReloadableState::code_change()
2. **Reconnection**: Apps handle network reconnection
3. **Resource Cleanup**: Apps close old resources
4. **Error Handling**: Apps decide on migration failure policy

### ❌ What's Not Possible Yet

1. **File Descriptors**: WASI limitation (deferred to WASI preview2)
2. **Automatic Migration**: Too complex, error-prone (design choice)
3. **Distributed Resources**: Requires Phase 10 (distributed registry)

---

## Future Work

### Phase 7.5: Helper Libraries (Optional)

**Idea**: Provide common migration helpers in guest libraries

```rust
// lunatic-std/src/hot_reload/helpers.rs
pub fn reconnect_tcp(
    state: &mut impl HasTcpConnection,
    snapshot: &ResourceSnapshot
) -> Result<()> {
    if let ResourceSnapshot::TcpConnection { peer_addr, .. } = snapshot {
        state.set_tcp(TcpStream::connect(peer_addr)?);
    }
    Ok(())
}
```

**Benefit**: Reduce boilerplate in applications  
**Timeline**: After Phase 11 (OTP patterns)

### Phase 10: Distributed Resource Migration

**Scope**: Migrate resources across nodes
- Process registry entries
- Distributed connections
- Cluster state

**Complexity**: High  
**Timeline**: Phase 10 (4 weeks)

### WASI Preview2: File Descriptor Migration

**Blocker**: Waiting for WASI preview2 resource handle API

**When Available**: 
- Can implement MigratableResource for file handles
- Transfer FDs across module instances
- Maintain file position, flags

**Timeline**: TBD (dependent on Wasmtime/WASI)

---

## Lessons Learned

### 1. Simplicity Over Automation

**Attempted**: Automatic resource migration for all types  
**Realized**: Too many edge cases, application-specific requirements  
**Solution**: Provide mechanism (infrastructure), let apps implement policy

**Benefit**: Clear boundaries, easier to reason about

### 2. Erlang Parity Achieved (Application Level)

**Discovery**: Erlang also doesn't auto-migrate resources  
**Validation**: Our approach matches Erlang's proven model  
**Advantage**: Strong typing + explicit serialization (Rust benefit)

### 3. Infrastructure First, Convenience Later

**Phase 7**: Core infrastructure (snapshots, hooks)  
**Phase 7.5** (future): Convenience helpers  
**Phase 10** (future): Distributed extensions

**Rationale**: Get the foundation right first

---

## Documentation for Application Developers

### Quick Start Guide

**1. Identify Resources to Migrate**

List all resources your process holds:
- TCP/TLS connections
- File handles
- Database connections
- Custom resources

**2. Choose Migration Pattern**

- **Reconnection**: Network connections (Pattern 1)
- **Rebinding**: Listeners (Pattern 2)
- **Pooling**: Database connections (Pattern 3)
- **Manual**: Application-specific logic

**3. Implement ReloadableState**

```rust
impl ReloadableState for YourState {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Save metadata, not resources
        Ok(bincode::serialize(&YourMetadata::from(self))?)
    }

    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        let metadata = bincode::deserialize(bytes)?;
        Ok(Self::from_metadata(metadata))
    }

    fn code_change(&mut self, old: u32, new: u32) -> Result<()> {
        // Recreate resources using saved metadata
        self.reconnect_resources()?;
        Ok(())
    }
}
```

**4. Test Migration**

Write integration tests for your migration logic (see examples above).

**5. Handle Errors**

Decide what happens if migration fails:
- Retry connection?
- Fall back to default?
- Crash and restart?

---

## Alignment with CORE_VALUES.md

### ✅ Fast, Robust, Scalable

**Fast**: Minimal overhead (just snapshot storage)  
**Robust**: Application controls error handling  
**Scalable**: No runtime bottlenecks

### ✅ Security Through Isolation

**Listener snapshots**: Version 2 stores only a local address and an opaque provider handle, not a
certificate or raw private key. Provider lookup is scoped to environment and process identity.

**Live reload**: Ownership of the configured listener/acceptor moves in-process; it is not
serialized or rebuilt.

**Guest ABI boundary**: `tls_bind` preserves the const guest key-input range required by SDK
multi-address fallback and zeroizes its temporary host PEM byte copy. The original guest buffer and
duplicates elsewhere can remain in Wasm memory, and distributed credential delivery is outside
this snapshot contract. Total avoidance of raw key bytes in Wasm needs a future provider-handle
guest ABI.

### ✅ Fault Tolerance & HA

**Improved**: In-process live transfer can maintain supported listeners and connections across
reloads.

**Flexible**: Choose availability vs consistency per resource

### ✅ Erlang-Inspired

**Matches Erlang**: Application-level migration (like `code_change/3`)  
**Improves on Erlang**: Strong typing, explicit serialization

---

## Compliance Score Update

The numeric values below are retained as the 2025 report's historical self-assessment. They are not
the current repository score or acceptance status; `docs/core_values/status.md` supersedes them.

**Before Phase 7**: 78/100

**After Phase 7**: 80/100 (+2)

**Improvements**:
- ✅ Fault Tolerance & HA: 70 → 73 (+3)
  - Resource migration infrastructure complete
  - Application patterns documented
  
- ✅ Erlang-Inspired: 65 → 67 (+2)
  - Matches Erlang's migration model
  - Better typing than Erlang

**Still Needed** (for 90/100):
- Phase 8: Observability (production debugging)
- Phase 9: Cluster Management (operations)
- Phase 10: Transparent Distribution (scalability)
- Phase 11: OTP Patterns (developer experience)

---

## Conclusion

Phase 7 delivers **practical resource migration** by:

1. ✅ Providing infrastructure (ResourceMigrationSnapshot)
2. ✅ Documenting patterns (4 migration strategies)
3. ✅ Maintaining simplicity (application responsibility)
4. ✅ Matching Erlang's proven approach

**Key Insight**: Automatic migration is a false goal. Applications know their requirements best. Our job is to provide the tools, not dictate the policy.

**Current result boundary**: Production process-local reload transfers supported live resources.
Version-2 serialized snapshots can rebind TLS listeners only through scoped, single-use provider
handles; they cannot restore active TLS streams.

**Next**: Phase 8 (Observability) for production debugging capabilities.

---

**Historical status**: Phase 7 infrastructure report complete

**Current production status**: See `docs/core_values/status.md`

**Current TLS contracts**: See `docs/tls/TLS_LISTENER_CREDENTIALS.md` and
`docs/tls/TLS_STREAM_MIGRATION.md`
