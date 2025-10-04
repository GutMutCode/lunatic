# Hot Reload API Usage Example

## Overview

This document demonstrates how to use the hot reload API once fully implemented in Phase 3.

## Basic Usage

```rust
use lunatic_process::{
    hot_reload::{HotReloadContext, perform_hot_reload, send_hot_reload_signal},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::WasmtimeRuntime,
};

// 1. Setup module registry
let registry = ModuleRegistry::new();

// 2. Compile and add module versions
let module_v1 = runtime.compile_module(wasm_bytes_v1)?;
let version1 = registry.add_version(module_id, module_v1);

let module_v2 = runtime.compile_module(wasm_bytes_v2)?;
let version2 = registry.add_version(module_id, module_v2);

// 3. Trigger hot reload
send_hot_reload_signal(process_id, module_id, version2, &env)?;
```

## Low-Level API

```rust
// Manual hot reload process
async fn manual_hot_reload() {
    // Create context
    let mut ctx = HotReloadContext::new(module_id, 0, 1);
    
    // Capture current state
    ctx.capture_memory(&mut current_instance)?;
    
    // Get new module version
    let new_module = registry.get_version(module_id, 1)?;
    
    // Create new instance
    let mut new_instance = runtime.instantiate(&new_module, state).await?;
    
    // Restore state
    ctx.restore_memory(&mut new_instance)?;
}
```

## Integration with --watch Mode

The hot reload system integrates with the existing `--watch` flag:

```bash
# Current behavior: Process restart
lunatic run --watch app.wasm

# Future (Phase 3): Hot reload with state preservation
lunatic run --watch --hot-reload app.wasm
```

## Memory Snapshot Details

The hot reload system captures the entire WASM linear memory:

```rust
// Automatic memory capture
let memory_size = ctx.memory_snapshot.len();
println!("Captured {} bytes", memory_size);

// Memory is automatically restored to new instance
ctx.restore_memory(&mut new_instance)?;
```

## Limitations (Phase 2)

Current limitations that will be addressed in Phase 3:

1. **No automatic trigger**: Must manually send HotReload signal
2. **Memory only**: Only WASM linear memory is preserved
3. **No resource migration**: File handles, TCP connections not preserved
4. **No mailbox preservation**: Pending messages may be lost
5. **No call stack**: Execution starts from entry point

## Future Enhancements (Phase 3+)

- Full state serialization with custom `serialize_state()` trait
- Mailbox and link preservation
- Resource handle migration
- Automatic reload on file change with state preservation
- Version transformation callbacks
- Rollback on reload failure

## Example: Counter with Hot Reload

```rust
// counter_v1.wat - Initial version
(module
  (memory (export "memory") 1)
  (global $counter (mut i32) (i32.const 0))
  
  (func (export "increment")
    global.get $counter
    i32.const 1
    i32.add
    global.set $counter
  )
  
  (func (export "get_count") (result i32)
    global.get $counter
  )
)

// counter_v2.wat - Updated version with logging
(module
  (memory (export "memory") 1)
  (global $counter (mut i32) (i32.const 0))
  
  (func (export "increment")
    global.get $counter
    i32.const 1
    i32.add
    global.set $counter
    ;; New: log the increment
    call $log_increment
  )
  
  (func (export "get_count") (result i32)
    global.get $counter
  )
  
  (func $log_increment
    ;; Implementation
  )
)
```

After hot reload from v1 to v2, the counter value is preserved in memory.

## Testing

```rust
#[tokio::test]
async fn test_hot_reload_preserves_state() {
    let runtime = WasmtimeRuntime::new(&config)?;
    let registry = ModuleRegistry::new();
    
    // Run v1 and increment counter
    let mut instance_v1 = runtime.instantiate(&module_v1, state).await?;
    call_function(&mut instance_v1, "increment").await?;
    let count_before = call_function(&mut instance_v1, "get_count").await?;
    
    // Perform hot reload
    let instance_v2 = perform_hot_reload(
        &runtime,
        &registry,
        module_id,
        2,
        &mut instance_v1,
        state,
    ).await?;
    
    // Verify state preserved
    let count_after = call_function(&mut instance_v2, "get_count").await?;
    assert_eq!(count_before, count_after);
}
```

## API Reference

### `HotReloadContext<S>`

Manages state capture and restoration during hot reload.

**Methods:**
- `new(module_id, old_version, new_version) -> Self`
- `capture_memory(&mut self, instance) -> Result<()>`
- `restore_memory(&self, instance) -> Result<()>`

### `perform_hot_reload<S>(...)` 

High-level function to perform complete hot reload.

**Parameters:**
- `runtime`: WasmtimeRuntime reference
- `registry`: ModuleRegistry with available versions
- `module_id`: ID of module to reload
- `new_version`: Target version number
- `current_instance`: Mutable reference to current instance
- `state`: ProcessState for new instance

**Returns:** New instance with restored state

### `send_hot_reload_signal(...)`

Sends HotReload signal to a process.

**Parameters:**
- `process_id`: Target process ID
- `module_id`: Module to reload
- `new_version`: Version to load
- `env`: Environment containing the process

**Returns:** Result indicating if signal was sent

## See Also

- `HOT_RELOAD_ARCHITECTURE.md` - Full architecture design
- `HOT_RELOAD_PHASE2_PROGRESS.md` - Implementation progress
- `docs/HOT_RELOAD_MVP.md` - Current --watch implementation
