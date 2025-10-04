# Hot Reload Phase 2: Basic Hot Reload - IN PROGRESS

## Summary

Phase 2 aims to implement basic hot reload functionality with state preservation. This phase is currently in progress.

## Completed Work

### 1. Memory Snapshot/Restore ✅
**File:** `crates/lunatic-process/src/runtimes/wasmtime.rs`

Added methods to `WasmtimeInstance<T>`:

```rust
pub fn snapshot_memory(&mut self) -> Result<Vec<u8>>
pub fn restore_memory(&mut self, snapshot: &[u8]) -> Result<()>
pub fn store(&self) -> &wasmtime::Store<T>
pub fn store_mut(&mut self) -> &mut wasmtime::Store<T>
pub fn instance(&self) -> &wasmtime::Instance
```

**Purpose:**
- `snapshot_memory()`: Captures the entire linear memory of a WASM instance
- `restore_memory()`: Restores memory snapshot to a new instance
- Additional accessor methods for low-level operations

**Approach:**
- Simple memory dump/restore (no selective serialization)
- Captures entire WASM linear memory as `Vec<u8>`
- Works for stateless or simple stateful processes

## Remaining Work (Phase 2)

### High Priority

1. **ModuleRegistry Integration**
   - Add ModuleRegistry to Environment
   - Track module versions across processes
   - Enable version lookup during reload

2. **HotReload Signal Handler**
   - Implement actual reload logic in signal handler
   - Currently just logs, needs to:
     - Snapshot current state
     - Create new instance with new module
     - Restore state
     - Resume execution

3. **Reload Coordination**
   - Function to coordinate reload process
   - Handle failures and rollback
   - Preserve mailbox and links

4. **Testing**
   - Simple test with stateful counter
   - Verify state preservation across reload
   - Test error cases

### Medium Priority

5. **Host Function for Reload**
   - Add `lunatic::process::reload()` host function
   - Allow guest code to trigger reload
   - Useful for development workflow

6. **Documentation**
   - Usage examples
   - Limitations and caveats
   - Best practices

## Technical Challenges

### Challenge 1: Execution State
**Problem:** When to snapshot? Process is running asynchronously.

**Options:**
- A: Cooperative - Process yields for snapshot (requires guest cooperation)
- B: Preemptive - Use fuel exhaustion to pause (may lose call stack)

**Current Approach:** Will implement Option B first (simpler)

### Challenge 2: Resource Handles
**Problem:** File descriptors, TCP connections, etc. in ProcessState

**Current Approach:** 
- Phase 2: Simple memory dump (ignores resources)
- Phase 3: Will add proper state preservation

### Challenge 3: Mailbox Preservation
**Problem:** Pending messages should survive reload

**Current Approach:**
- Phase 2: May lose messages (documented limitation)
- Phase 3: Will preserve mailbox

## What Works Now

✅ Memory snapshot/restore infrastructure
✅ Phase 1 infrastructure (ModuleRegistry, Signals)
✅ Basic building blocks in place

## What Doesn't Work Yet

❌ Actual hot reload (end-to-end)
❌ State preservation beyond memory
❌ Automatic reload triggers
❌ Resource handle migration

## Next Immediate Steps

1. Integrate ModuleRegistry into Environment
2. Implement actual HotReload signal handler
3. Create minimal test case
4. Iterate based on test results

## Architecture Notes

```
Current Flow (Phase 1):
--watch flag → file change → process restart (full)

Target Flow (Phase 2):
HotReload signal → snapshot memory → swap module → restore memory → resume

Target Flow (Phase 3):
HotReload signal → serialize state → swap module → deserialize state → preserve mailbox/links → resume
```

## Files Modified

- ✅ `crates/lunatic-process/src/runtimes/wasmtime.rs` (snapshot/restore methods)

---

**Date:** 2025-10-05
**Status:** Phase 2 In Progress - Memory operations complete, integration pending
