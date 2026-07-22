# Hot Reload Phase 6: Integration & Future Roadmap - COMPLETE

> **Historical phase record (2025-10-05).** “Complete” refers to the Phase 6
> documentation checklist. Readiness, performance, and limitation statements in
> the original record are not current guarantees. Unsupported latency and
> zero-interruption claims are retracted below. See
> [Core Values Status](../core_values/status.md) for current evidence and
> [Watch-Mode Hot Reload](HOT_RELOAD_MVP.md) for current usage.

## Summary

Phase 6 documented the implementation state and roadmap as understood at the
time. Later integration work and tests supersede its readiness assessment.

## Historical State Assessment

### ✅ What Works (Phases 4-5)

**State Preservation:**
- Memory snapshots with stack/heap pointers
- Mailbox message preservation
- Process ID stability
- Atomic instance swapping

**Safety & Validation:**
- Function signature validation
- Memory size compatibility checks
- Export presence verification
- Type-safe signal broadcasting

**Performance:**
- No reproducible production-path end-to-end reload-latency bound was attached
  to this phase record.
- The former reload and validation timing estimates are retracted.

### 📋 Known Limitations

#### 1. Resource Migration
**Status:** Not implemented

**Resources Affected:**
- TCP connections (`TcpConnection`, `TlsConnection`)
- File handles (WASI file descriptors)
- UDP sockets (`UdpSocket`)
- Database connections (SQLite)

**Workaround:**
Applications should implement reconnection logic:

```rust
// Example: Reconnect pattern
impl MyState {
    fn after_reload(&mut self) -> Result<()> {
        if let Some(addr) = &self.server_addr {
            // Reconnect to saved address
            self.tcp = TcpConnection::connect(addr)?;
        }
        Ok(())
    }
}
```

**Future (Phase 7):**
- Generic `ResourceMigration` trait
- Per-resource migration hooks
- Automatic handle transfer where possible

#### 2. Link & Monitor Preservation
**Status:** Signal-based, not persisted

**Current Behavior:**
- Links: Established via signals, not stored
- Monitors: Created on-demand, ephemeral
- Process death notifications work as expected

**Impact:** Low
- Most applications re-establish links at startup
- Monitoring can be re-registered after reload

**Future (Phase 8):**
- Optional link table snapshot
- Monitor relationship preservation
- Configurable persistence policy

#### 3. Watch Mode Integration (current correction)
**Status:** Integrated for the local CLI path

**Current CLI contract:**
```rust
// --watch is the sole file-triggered reload mode flag.
lunatic run --watch app.wasm
```

The original manual-registry blocker was closed by later integration work. The
current local watch path creates the registry and reload coordinator, compiles
the changed module, and applies the coordinated update. See
[Watch-Mode Hot Reload](HOT_RELOAD_MVP.md) for its tested limits.

#### 4. Distributed Hot Reload
**Status:** Local processes only

**Current correction:** Production-path tests cover the local reload transaction;
distributed and cross-node behavior remains unverified.

**Future (Phase 9):**
- Distributed module registry
- Cross-node version coordination
- Network-aware reload timing
- Partial rollout (canary deployments)

## Architecture Summary

### Core Components

```
┌─────────────────────────────────────────────┐
│          Hot Reload System                  │
├─────────────────────────────────────────────┤
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   ModuleRegistry                    │   │
│  │  - Version tracking                 │   │
│  │  - Module storage                   │   │
│  │  - Dependency management            │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   SignatureValidator                │   │
│  │  - Function signature check         │   │
│  │  - Memory compatibility             │   │
│  │  - Export verification              │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   MemorySnapshot                    │   │
│  │  - Linear memory                    │   │
│  │  - Stack pointer (__stack_pointer)  │   │
│  │  - Heap pointer (__heap_base)       │   │
│  │  - Custom metadata                  │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   MessageMailbox                    │   │
│  │  - snapshot() - capture messages    │   │
│  │  - restore() - restore messages     │   │
│  └─────────────────────────────────────┘   │
│                                             │
│  ┌─────────────────────────────────────┐   │
│  │   ProcessContext                    │   │
│  │  - Arc<RwLock<Instance>>            │   │
│  │  - Atomic swap support              │   │
│  │  - Reload state management          │   │
│  └─────────────────────────────────────┘   │
│                                             │
└─────────────────────────────────────────────┘
```

### Hot Reload Flow

```
1. File Change Detected (watch mode)
          ↓
2. Compile New Module
          ↓
3. Register in ModuleRegistry
          ↓
4. Broadcast HotReload Signal
          ↓
5. Validate Signatures ←─────┐
          ↓                  │
6. ✅ Compatible?            │
    │         │              │
    YES       NO             │
    ↓         ↓              │
7. Snapshot  Reject ─────────┘
   State     (log error)
    ↓
8. Create New Instance
    ↓
9. Restore State
    ↓
10. Atomic Swap
    ↓
11. ✅ Hot Reload Complete
```

## API Usage

### For Application Developers

**1. Make State Reloadable:**
```rust
use lunatic_process::reloadable_state::ReloadableState;

struct MyAppState {
    counter: i32,
    data: Vec<String>,
}

impl ReloadableState for MyAppState {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Serialize state
        bincode::serialize(self)
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
    }
    
    fn code_change(&mut self, old_ver: u32, new_ver: u32) -> Result<()> {
        // Optional: migrate state between versions
        match (old_ver, new_ver) {
            (1, 2) => {
                // Initialize new fields
            }
            _ => {}
        }
        Ok(())
    }
}
```

**2. Test Hot Reload:**
```bash
# Compile your WAT/WASM
wat2wasm app_v1.wat -o app.wasm

# Run with watch mode
lunatic run --watch app.wasm

# Modify app_v1.wat → app_v2.wat. The existing --watch mode is the only
# file-triggered reload entry point.
```

### For Runtime Developers

**1. Trigger Hot Reload Programmatically:**
```rust
use lunatic_process::{Signal, ModuleRegistry};

// Register new module version
let new_module = runtime.compile_module(wasm_bytes)?;
let registry = Arc::new(ModuleRegistry::new());
let version = registry.add_version(module_id, Arc::new(new_module))?;

// Send signal to all processes
env.send_to_all(Signal::HotReload {
    module_id,
    new_version: version,
});
```

**2. Validate Before Reload:**
```rust
use lunatic_process::signature_validation::SignatureValidator;

let errors = SignatureValidator::validate_compatibility(
    &old_module,
    &new_module,
)?;

if !errors.is_empty() {
    for error in errors {
        eprintln!("Incompatibility: {}", error);
    }
    return Err(anyhow!("Cannot hot reload"));
}
```

## Testing

### Unit Tests
- ✅ ReloadableState implementations (7 tests)
- ✅ Memory snapshot/restore
- ✅ Mailbox preservation (11 tests)
- ✅ Signature validation (1 test)
- ✅ Module registry (3 tests)

**Total: 21+ tests passing**

### Manual Testing

Use the current [demo instructions](../../examples/DEMO_INSTRUCTIONS.md), which
exercise the supported `lunatic run --watch` spelling.

## Retracted Performance Estimates

The original phase document listed component timings, total reload latency, and
per-process/per-version memory estimates without a reproducible
production-path benchmark. Those figures are removed and must not be used as
current performance or capacity claims.

## Future Roadmap

### Phase 7: Resource Migration (4-6 weeks)
**Priority: Medium**

- [ ] `ResourceMigration` trait
- [ ] TCP connection transfer
- [ ] File descriptor migration
- [ ] Database connection handling
- [ ] Custom resource hooks

**Key Features:**
```rust
pub trait ResourceMigration {
    fn can_migrate(&self) -> bool;
    fn snapshot(&self) -> Result<Vec<u8>>;
    fn restore(bytes: &[u8]) -> Result<Self>;
}
```

### Phase 8: Link & Monitor Preservation (2-3 weeks)
**Priority: Low**

- [ ] Link table snapshot
- [ ] Monitor relationship tracking
- [ ] Atomic link update
- [ ] Death notification replay

### Phase 9: Watch Mode Full Integration (historical roadmap item)

Later work integrated the registry and reload coordinator under the existing
`--watch` flag. The implementation sketch below is retained only as historical
design context.

**Implementation:**
```rust
// In run_with_watch()
let module_registry = Arc::new(ModuleRegistry::new());
env.set_module_registry(module_registry.clone());

// On file change
let new_module = runtime.compile_module(bytes)?;
let version = module_registry.add_version(0, Arc::new(new_module))?;
env.send_to_all(Signal::HotReload { module_id: 0, new_version: version });
```

### Phase 10: Distributed Hot Reload (6-8 weeks)
**Priority: Low**

- [ ] Distributed module registry
- [ ] Cross-node version sync
- [ ] Network-aware reload coordination
- [ ] Partial rollout support

### Phase 11: Advanced Features (4-6 weeks)
**Priority: Low**

- [ ] Canary deployments
- [ ] A/B testing support
- [ ] Reload analytics
- [ ] Performance profiling

## Migration Guide

### From MVP (Process Restart) to Hot Reload

**Before (MVP):**
```rust
// Watch mode restarts process
// All state lost on file change
lunatic run --watch app.wasm
```

**After (Phases 4-5):**
```rust
// State preserved, manual signal required
// Programmatic hot reload via ModuleRegistry
```

**Integrated CLI spelling:**
```rust
// Full watch mode integration uses the existing flag.
lunatic run --watch app.wasm
```

### Preparing Apps for Hot Reload

**1. Minimize Resources:**
```rust
// ❌ Hard to migrate
struct State {
    tcp: TcpConnection,  // Active connection
    file: FileHandle,     // Open file
}

// ✅ Easy to reload
struct State {
    tcp_addr: String,     // Reconnect after reload
    file_path: PathBuf,   // Reopen after reload
    data: Vec<u8>,        // Preserved in memory
}
```

**2. Implement ReloadableState:**
```rust
impl ReloadableState for State {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(Into::into)
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(Into::into)
    }
}
```

**3. Version Migration:**
```rust
fn code_change(&mut self, old: u32, new: u32) -> Result<()> {
    match (old, new) {
        (1, 2) => {
            // v1 → v2: Added 'timeout' field
            self.timeout = Duration::from_secs(30);
        }
        (2, 3) => {
            // v2 → v3: Renamed 'count' to 'counter'
            // (handled by deserialization)
        }
        _ => {}
    }
    Ok(())
}
```

## Conclusion

### Achievements (Phases 4-6)

✅ Local hot-reload infrastructure milestones recorded by the phase
✅ State-preservation and validation paths recorded by the phase
📋 No end-to-end latency or zero-interruption guarantee established
✅ **Type-safe** signature validation
✅ **Comprehensive** test coverage (21+ tests)
✅ **Well-documented** API and usage patterns

### Known Limitations

📋 Resource migration (workaround: reconnect pattern)
📋 Link/monitor persistence (low impact)
✅ Watch mode is integrated under the existing `--watch` flag
📋 Local processes only (distributed in Phase 10)

### Recommendation

Use the current [watch-mode guide](HOT_RELOAD_MVP.md) and
[Core Values Status](../core_values/status.md) when deciding whether the
documented local behavior is suitable for a workload.

**Phase 6 Status: COMPLETE** 🎉

This phase record does not establish production readiness; the current status
document defines the verified boundary.

---

**Date:** 2025-10-05  
**Status:** Phase 6 COMPLETE - Documentation & roadmap finalized  
**Next:** Phase 7+ as needed (resource migration, distributed reload)
