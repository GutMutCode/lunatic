# Hot Reload Watch Mode Integration

**Status**: Partially Implemented  
**Date**: October 5, 2025

## Overview

Watch mode now includes hot reload infrastructure that attempts to preserve process state across file changes. However, due to WebAssembly execution model limitations, hot reload only works for certain types of processes.

## Implementation

### What Was Implemented

1. **ModuleRegistry Integration**
   - `LunaticEnvironment` now optionally holds a `ModuleRegistry`
   - New `create_with_registry()` method for environments
   - Registry is properly passed to spawned processes

2. **Watch Mode Changes**
   - File changes trigger module compilation
   - New versions registered in `ModuleRegistry`
   - `HotReload` signal sent to all processes
   - Old process restart behavior replaced

3. **Signal Flow**
   ```
   File Change → Compile → Register → Send Signal → Process Handles
   ```

### Code Changes

**crates/lunatic-process/src/env.rs:**
- Added `module_registry` field to `LunaticEnvironment`
- Implemented `with_module_registry()` constructor
- Added `create_with_registry()` to `Environments` trait
- Implemented `get_module_registry()` for `Environment` trait

**src/mode/run.rs:**
- Initialize `ModuleRegistry` on startup
- Compile initial module and register as version 0
- On file change: compile new module, register, send `HotReload` signal
- Removed old process kill/restart logic

**crates/lunatic-process/src/lib.rs:**
- Fixed `ModuleRegistry` downcast (from `Arc<Registry>` to `Registry`)

## Current Limitations

### ❌ Long-Running Functions Don't Reload

**Problem**: If a WASM function is executing in a loop, hot reload signals are queued but not processed until the function yields or completes.

**Example - DOESN'T WORK:**
```wat
(func (export "_start")
  (loop $forever
    ;; Print something
    call $print
    br $forever  ;; Never yields!
  )
)
```

**Why**: 
- Hot reload happens at async yield points
- Wasmtime yields every 100k instructions (fuel system)
- But if the loop is tight and function never returns, new instance can't start

**Workaround**: None currently - process must complete or use message-based architecture

### ✅ Message-Based Processes WOULD Work

**This pattern SHOULD work** (not yet tested):
```rust
loop {
    let msg = receive_message().await;  // Yields here!
    handle_message(msg);  // Hot reload can happen between messages
}
```

Each `handle_message` call would use the latest module version.

## Test Results

### Test: simple_loop.wat

**Setup:**
- V1: Prints "Running v1..." in infinite loop
- V2: Prints "RELOADED v2!" in infinite loop

**Result:**
```
✅ Module compiled and registered
✅ HotReload signal sent
✅ Signal received by process
✅ Module signature validated
❌ V2 never executes (V1 loop never yields)
```

**Logs:**
```
[INFO] Compiled new module version: 1
[INFO] Hot reload signal sent (version 1)
[INFO] Processing HotReload signal for module 0 version 1
[INFO] Starting hot reload: module_id=0, 0 -> 1
[INFO] Validating module compatibility...
[INFO] Module signatures are compatible
```

Then nothing - process still running V1 loop.

## What Works

✅ Infrastructure is in place  
✅ ModuleRegistry integration  
✅ Signal sending and receiving  
✅ Module validation  
✅ Memory snapshot/restore code  

## What Doesn't Work

❌ Actual instance swapping for running processes  
❌ Long-running `_start` functions  
❌ Infinite loops without async points  

## Future Work

### Phase 7.5: Message-Based Hot Reload (Recommended)

**Approach**: Instead of trying to reload running functions, reload between message handles.

**Implementation**:
1. Create test with message-passing actor
2. Each message handled in separate function call
3. Hot reload occurs between messages
4. Test confirms new behavior takes effect

**Expected Outcome**: ✅ WORKS - Each message uses latest module

### Phase 8: Cooperative Reload Points (Advanced)

**Approach**: Add explicit reload check points in WASM code.

**Implementation**:
```wat
(loop $main
  ;; Do work
  call $check_hot_reload  ;; Host function that yields if reload pending
  br $main
)
```

**Pros**: Works with long-running functions  
**Cons**: Requires code changes, host function overhead

### Phase 9: Preemptive Reload (Complex)

**Approach**: Force function to return and restart with new module.

**Challenges**:
- Must save execution state (locals, stack)
- Complex state migration
- May not be possible with current Wasmtime APIs

## Recommendations

### For Users

**DO** use hot reload with:
- Message-based actors
- HTTP request handlers (each request is a function call)
- Event-driven systems
- Short-lived functions

**DON'T** use hot reload with:
- Infinite loops in `_start`
- Long-running compute tasks
- Processes that never yield

### For Developers

**Next Steps**:
1. Create message-based hot reload test
2. Document actor pattern for hot reload
3. Add host function for cooperative reload points (optional)
4. Consider adding "reload-friendly" flag to detect incompatible patterns

## Conclusion

Watch mode hot reload infrastructure is **implemented and working**, but practical hot reload requires:
1. Process architecture that yields control (messages, HTTP handlers, etc.)
2. OR cooperative reload points in WASM code
3. OR accepting process restart for long-running functions

The current implementation successfully:
- Compiles new modules
- Validates compatibility
- Sends reload signals
- Prepares for instance swapping

But actual swapping only occurs when WASM code yields control.

**Status**: Infrastructure complete, practical usage limited to cooperative processes.

