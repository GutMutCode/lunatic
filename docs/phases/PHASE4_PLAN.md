# Phase 4: True Hot Reload - Implementation Plan

## Overview

Transform the current MVP (process restart) into **true Erlang-style hot reload** where:
- Process ID remains the same
- State is preserved across code changes
- No downtime during reload
- Mailbox messages are retained

## Current vs Target Architecture

### Current (MVP)
```
File change → Kill process → New process with new env → Lost state
```

### Target (Phase 4)
```
File change → HotReload signal → Serialize state → Swap WASM instance → Restore state → Continue
```

## Implementation Breakdown

### Week 1-2: State Serialization Framework

#### 4.1.1 ReloadableState Trait
**File:** `crates/lunatic-process/src/reloadable_state.rs` (new)

```rust
pub trait ReloadableState: Sized {
    /// Serialize process state to bytes
    fn serialize_state(&self) -> Result<Vec<u8>>;
    
    /// Deserialize state from bytes
    fn deserialize_state(bytes: &[u8]) -> Result<Self>;
    
    /// Optional: Transform state between versions
    fn code_change(&mut self, old_version: u32, new_version: u32) -> Result<()> {
        Ok(()) // Default: no transformation
    }
}
```

**Tasks:**
- [ ] Create `reloadable_state.rs` module
- [ ] Define `ReloadableState` trait
- [ ] Implement for basic types (Vec, HashMap, primitives)
- [ ] Add derive macro support (future enhancement)

#### 4.1.2 Memory Snapshot Enhancement
**File:** `crates/lunatic-process/src/runtimes/wasmtime.rs`

Current `snapshot_memory()` captures entire linear memory. Enhance it to:

```rust
pub struct MemorySnapshot {
    /// Raw linear memory contents
    pub memory: Vec<u8>,
    
    /// Stack pointer (if available)
    pub stack_ptr: Option<u32>,
    
    /// Heap pointer (if available)  
    pub heap_ptr: Option<u32>,
    
    /// Custom metadata
    pub metadata: HashMap<String, Vec<u8>>,
}
```

**Tasks:**
- [ ] Extend `MemorySnapshot` struct
- [ ] Capture stack/heap pointers
- [ ] Add metadata support
- [ ] Update `restore_memory()` to handle new fields

#### 4.1.3 Mailbox Preservation
**File:** `crates/lunatic-process/src/mailbox.rs`

```rust
impl MessageMailbox {
    /// Extract all messages without consuming mailbox
    pub fn snapshot(&self) -> Vec<Message> {
        // Clone all pending messages
    }
    
    /// Restore messages to mailbox
    pub fn restore(&mut self, messages: Vec<Message>) {
        // Prepend messages to maintain order
    }
}
```

**Tasks:**
- [ ] Add `snapshot()` method
- [ ] Add `restore()` method
- [ ] Ensure message order preservation
- [ ] Handle message priorities (if any)

### Week 2-3: In-Place Instance Swap

#### 4.2.1 Process Context Swapping
**File:** `crates/lunatic-process/src/lib.rs`

Modify the process execution loop to support hot reload:

```rust
// Current: Process owns single WasmtimeInstance
struct ProcessContext<S> {
    instance: WasmtimeInstance<S>,
}

// New: Process can swap instances
struct ProcessContext<S> {
    instance: Arc<RwLock<WasmtimeInstance<S>>>,
    reload_in_progress: Arc<AtomicBool>,
}
```

**Key Changes:**
- [ ] Wrap instance in `Arc<RwLock<>>` for atomic swap
- [ ] Add reload flag to prevent concurrent reloads
- [ ] Modify execution loop to check reload flag
- [ ] Implement `swap_instance()` method

#### 4.2.2 Signal Handling Enhancement
**File:** `crates/lunatic-process/src/lib.rs` (execution loop)

```rust
// In the process execution loop (around line 356)
Ok(Signal::HotReload { module_id, new_version }) => {
    log::info!("Processing HotReload signal for module {}", module_id);
    
    // 1. Set reload flag
    reload_in_progress.store(true, Ordering::SeqCst);
    
    // 2. Snapshot current state
    let snapshot = capture_process_state(&instance).await?;
    
    // 3. Get new module version
    let new_module = env.get_module_version(module_id, new_version)?;
    
    // 4. Create new instance with restored state
    let new_instance = runtime
        .instantiate(&new_module, snapshot)
        .await?;
    
    // 5. Swap instances atomically
    *instance.write().await = new_instance;
    
    // 6. Clear reload flag
    reload_in_progress.store(false, Ordering::SeqCst);
    
    log::info!("Hot reload completed successfully");
}
```

**Tasks:**
- [ ] Implement state capture
- [ ] Implement instance swap logic
- [ ] Add error recovery (rollback on failure)
- [ ] Add comprehensive logging

#### 4.2.3 Function Signature Compatibility
**Challenge:** What if function signatures change?

**Strategy:**
- Start with **same-signature only** (Phase 4)
- Validate signatures before reload
- Reject reload if incompatible
- Future: Support signature changes (Phase 5)

```rust
fn validate_reload_compatibility(
    old_module: &WasmtimeCompiledModule,
    new_module: &WasmtimeCompiledModule,
) -> Result<()> {
    // Check exports match
    // Check memory size compatible
    // Check table size compatible
}
```

**Tasks:**
- [ ] Implement compatibility check
- [ ] Add signature comparison
- [ ] Create clear error messages
- [ ] Document compatibility rules

### Week 3-4: Integration with Existing Code

#### 4.3.1 Wire ModuleRegistry to Environment
**File:** `crates/lunatic-process/src/env.rs`

```rust
pub struct LunaticEnvironment {
    environment_id: u64,
    next_process_id: Arc<AtomicU64>,
    processes: Arc<DashMap<u64, Arc<dyn Process>>>,
    
    // NEW: Module registry
    module_registry: Arc<ModuleRegistry>,
}

impl LunaticEnvironment {
    pub fn get_module_version(&self, module_id: u64, version: u32) 
        -> Option<Arc<WasmtimeCompiledModule>> 
    {
        self.module_registry.get_version(module_id, version)
    }
}
```

**Tasks:**
- [ ] Add `module_registry` field to `LunaticEnvironment`
- [ ] Update constructor
- [ ] Add getter methods
- [ ] Update all existing tests

#### 4.3.2 Trigger Hot Reload from Watch Mode
**File:** `src/mode/run.rs`

Instead of killing the process, send `HotReload` signal:

```rust
// Current MVP approach (Week 3.5):
if let Some(env) = envs.get(info.env_id).await {
    env.kill_all_processes();  // ← Replace this
}

// New Phase 4 approach:
if let Some(env) = envs.get(info.env_id).await {
    // 1. Compile new module
    let new_module = runtime.compile_module(module_bytes)?;
    
    // 2. Register in registry
    let (module_id, version) = env.register_module_version(new_module)?;
    
    // 3. Send HotReload signal to all processes
    for process_id in env.get_all_process_ids() {
        env.send(process_id, Signal::HotReload {
            module_id,
            new_version: version,
        });
    }
}
```

**Tasks:**
- [ ] Modify `run_with_watch()` to use HotReload signal
- [ ] Add module compilation on file change
- [ ] Register new versions in registry
- [ ] Broadcast to all relevant processes

#### 4.3.3 Update DefaultProcessState
**File:** `src/state.rs`

```rust
impl ReloadableState for DefaultProcessState {
    fn serialize_state(&self) -> Result<Vec<u8>> {
        // Serialize process-specific state
        // - module reference
        // - runtime handle  
        // - distributed state (if any)
        bincode::serialize(self)
    }
    
    fn deserialize_state(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
    }
}
```

**Tasks:**
- [ ] Implement `ReloadableState` for `DefaultProcessState`
- [ ] Handle non-serializable fields (e.g., runtime)
- [ ] Test serialization round-trip
- [ ] Add version compatibility checks

## Testing Strategy

### Unit Tests
- [ ] `ReloadableState` trait implementations
- [ ] Memory snapshot/restore
- [ ] Mailbox preservation
- [ ] Instance swapping

### Integration Tests
- [ ] Simple reload (counter example)
- [ ] Reload with pending messages
- [ ] Reload under load (100+ messages/sec)
- [ ] Multiple processes reload simultaneously

### Example Applications
- [ ] Counter with hot reload
- [ ] Chat server with hot reload
- [ ] State machine with hot reload

## Success Criteria

**Phase 4 is complete when:**

1. ✅ Process ID remains stable across reload
2. ✅ Simple state (integers, strings) is preserved
3. ✅ Mailbox messages are not lost
4. ✅ No crashes during reload
5. ✅ < 100ms reload time for simple cases
6. ✅ Clear error messages on incompatible changes
7. ✅ At least 3 working example applications
8. ✅ Integration tests passing

## Risk Mitigation

### Risk: State corruption during swap
**Mitigation:** Atomic swap with RwLock, rollback on failure

### Risk: Performance degradation
**Mitigation:** Benchmark before/after, optimize hot paths

### Risk: Complex state serialization fails
**Mitigation:** Start with simple types, expand gradually

### Risk: Breaking existing functionality
**Mitigation:** Comprehensive test suite, feature flag for hot reload

## Feature Flag

Add feature flag to allow gradual rollout:

```toml
[features]
default = ["metrics"]
hot-reload = []  # NEW
```

This allows:
- MVP users continue working
- Phase 4 development in parallel
- Easy testing/benchmarking

## Timeline

| Week | Focus | Deliverable |
|------|-------|-------------|
| 1 | ReloadableState trait | Trait + basic impls |
| 2 | Memory snapshot enhancement | Enhanced snapshot API |
| 2-3 | Mailbox preservation | Message retention |
| 3 | Instance swap mechanism | Atomic swap working |
| 3-4 | Signal handling | HotReload signal processed |
| 4 | Integration | Watch mode uses hot reload |
| 4 | Testing | Examples working |

**Total:** 4 weeks

## Next Steps

1. Create `feature/phase4-state-preservation` branch
2. Implement `ReloadableState` trait
3. Enhance memory snapshot
4. Write unit tests
5. Integrate with existing code
6. Test with example applications

---

**Ready to begin Phase 4!** 🚀
