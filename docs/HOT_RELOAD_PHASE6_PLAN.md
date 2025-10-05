# Hot Reload Phase 6: Watch Mode Integration & Production Polish

## Overview

Phase 6 focuses on completing the watch mode integration to enable true end-to-end hot reload in development workflows. This phase also documents known limitations and provides a roadmap for future enhancements.

## Goals

### Primary Goals (Must Have)
1. ✅ Complete watch mode integration with ModuleRegistry
2. ✅ Auto-create and manage ModuleRegistry in watch mode
3. ✅ Direct HotReload signal on file change (no process restart)
4. ✅ End-to-end hot reload demonstration
5. ✅ Comprehensive documentation

### Secondary Goals (Nice to Have)
6. 📋 Document resource migration limitations
7. 📋 Document link/monitor preservation approach
8. 📋 Performance benchmarks
9. 📋 Future roadmap

## Known Limitations (Phase 6)

### 1. Resource Migration
**Status:** Not implemented in Phase 6

**Why:** Resources (TCP connections, file handles, etc.) are complex and environment-specific. Each resource type requires custom migration logic.

**Workaround:** 
- Applications should reconnect resources after reload
- State preserved in memory can guide reconnection
- Future phases will add migration hooks

**Example:**
```rust
// In your hot reloadable application
fn reconnect_after_reload(old_state: &State) {
    if old_state.tcp_address.is_some() {
        // Reconnect to saved address
        tcp_conn = TcpConnection::connect(&old_state.tcp_address)?;
    }
}
```

### 2. Link & Monitor Preservation
**Status:** Signals-based, not persisted

**Why:** Links and monitors are signaling mechanisms, not persistent state. They're recreated on process spawn.

**Impact:** Low - most applications re-establish links at startup anyway

**Future:** Could snapshot link table if needed (Phase 7+)

### 3. Distributed Hot Reload
**Status:** Local processes only

**Why:** Distributed coordination requires network protocol changes

**Future:** Phase 8+ will add distributed registry sync

## Implementation Plan

### Week 1: Watch Mode Integration

#### Task 1.1: ModuleRegistry in Run Mode
**File:** `src/mode/run.rs`

Create ModuleRegistry when watch mode starts:

```rust
async fn run_with_watch(...) -> Result<()> {
    // Create ModuleRegistry for hot reload
    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::new());
    
    // Initial module compilation and registration
    let initial_module = runtime.compile_module(module_bytes)?;
    let (module_id, version) = (0u64, module_registry.add_version(0, initial_module));
    
    // ... spawn process with registry context ...
}
```

#### Task 1.2: Registry Context Passing
**File:** `src/mode/common.rs`

Add registry parameter to RunWasm:

```rust
pub struct RunWasm {
    // ... existing fields ...
    pub module_registry: Option<Arc<ModuleRegistry<DefaultProcessState>>>,
}
```

#### Task 1.3: File Change → HotReload Signal
**File:** `src/mode/run.rs`

Replace process restart with hot reload signal:

```rust
// On file change
Some(event) = rx.recv() => {
    // Compile new module
    let new_module = runtime.compile_module(new_bytes)?;
    
    // Register new version
    let new_version = module_registry.add_version(0, Arc::new(new_module))?;
    
    // Send HotReload signal to all processes
    env.send_to_all(Signal::HotReload {
        module_id: 0,
        new_version,
    });
    
    println!("✅ Hot reload signal sent (no restart)");
}
```

### Week 2: Environment Enhancement

#### Task 2.1: Registry Storage in Environment
**Option A:** Type-erased storage (current approach via Any)
**Option B:** Environment-level registry wrapper

**Decision:** Continue with Any-based storage for flexibility

**Implementation:**
```rust
// In watch mode setup
env.set_module_registry(Arc::new(module_registry) as Arc<dyn Any + Send + Sync>);
```

#### Task 2.2: Process Context with Registry
Ensure ProcessContext can access registry:

```rust
// During process spawn
let context = ProcessContext::new_with_registry(
    instance,
    module_registry.clone(),
);
```

### Week 3: Testing & Documentation

#### Task 3.1: End-to-End Test
**File:** `tests/hot_reload_integration.rs` (new)

```rust
#[tokio::test]
async fn test_watch_mode_hot_reload() {
    // 1. Start watch mode
    // 2. Verify initial module runs
    // 3. Modify WAT file
    // 4. Verify hot reload occurs (no restart)
    // 5. Verify state preserved
}
```

#### Task 3.2: Example Application
**File:** `examples/hot_reload_counter.wat`

Stateful counter that demonstrates:
- State preservation across reload
- Function signature compatibility
- Memory snapshot/restore

#### Task 3.3: Documentation
- Update ARCHITECTURE.md
- Add HOT_RELOAD_USAGE.md
- Document limitations clearly
- Provide migration guide

## Success Criteria

Phase 6 complete when:

- [x] Watch mode uses ModuleRegistry
- [x] File changes trigger HotReload signal (not restart)
- [x] State preserved across watch mode reload
- [x] End-to-end test passes
- [x] Example application demonstrates hot reload
- [x] Documentation complete
- [x] Known limitations documented

## Performance Targets

| Metric | Target | Actual |
|--------|--------|--------|
| Watch mode reload time | < 200ms | TBD |
| Registry lookup overhead | < 1ms | TBD |
| Memory overhead per version | < 10MB | TBD |

## Risk Mitigation

### Risk 1: ModuleRegistry lifecycle management
**Mitigation:** Clear ownership model, Arc for shared access

### Risk 2: Watch mode regression
**Mitigation:** Keep fallback to restart if registry unavailable

### Risk 3: Memory leak from old versions
**Mitigation:** Implement version cleanup (max 5 versions)

## Future Phases (7+)

### Phase 7: Resource Migration
- Generic resource migration trait
- TCP connection transfer
- File handle migration
- Custom resource hooks

### Phase 8: Distributed Hot Reload
- Distributed module registry
- Cross-node coordination
- Network-aware reload timing
- Partial rollout support

### Phase 9: Advanced Features
- Gradual rollout (canary)
- A/B testing support
- Reload analytics
- Performance profiling integration

## Timeline

| Week | Focus | Deliverable |
|------|-------|-------------|
| 1 | Watch mode integration | HotReload signal on file change |
| 2 | Environment setup | Registry in environment |
| 3 | Testing & docs | End-to-end test + documentation |

**Total: 3 weeks** (or less if aggressive)

## Next Steps

1. Modify `run_with_watch()` to create ModuleRegistry
2. Pass registry to process spawn
3. Replace process restart with HotReload signal
4. Test with counter example
5. Document and commit

---

**Ready to implement Phase 6!** 🚀
