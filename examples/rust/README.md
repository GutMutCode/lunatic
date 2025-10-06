# Rust Examples for Lunatic

This directory contains Rust examples demonstrating best practices for developing Lunatic applications. Rust is the **recommended language** for Lunatic due to first-class support and access to all runtime features.

## Prerequisites

- Rust 1.70 or later
- `wasm32-wasi` target installed
- Lunatic runtime (built from this repo)

## Setup

### Install Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Add WASM Target

```bash
rustup target add wasm32-wasi
```

## Building Examples

### Build All Examples

```bash
cd examples/rust
make
```

### Build Individual Examples

```bash
make counter      # Build counter.wasm
make echo         # Build echo_server.wasm
```

### Manual Build

```bash
cargo build --target wasm32-wasi --release --bin counter
```

## Running Examples

### Counter Example

```bash
# From lunatic repo root
./target/debug/lunatic run examples/rust/build/counter.wasm
```

### Echo Server Example

```bash
./target/debug/lunatic run examples/rust/build/echo_server.wasm
```

### With Hot Reload

```bash
# Terminal 1
./target/debug/lunatic run --watch examples/rust/build/counter.wasm

# Terminal 2
cd examples/rust
# Modify src/counter.rs
make counter
# Watch mode automatically reloads!
```

## Examples Overview

### 1. Counter (`src/counter.rs`)

**Demonstrates:**
- Basic WASM exports with `#[no_mangle]`
- State management with atomic operations
- Simple function interfaces
- Type compatibility between Rust and WASM

**Key Functions:**
```rust
#[no_mangle]
pub extern "C" fn _start()              // Initialization

#[no_mangle]
pub extern "C" fn increment() -> i32    // Returns counter++

#[no_mangle]
pub extern "C" fn get_count() -> i32    // Returns current value

#[no_mangle]
pub extern "C" fn multiply_count(factor: i32) -> i32
```

### 2. Echo Server (`src/echo_server.rs`)

**Demonstrates:**
- Working with strings and raw pointers
- Using Rust standard library collections (HashMap)
- Memory management in WASM
- Processing complex data structures

**Key Functions:**
```rust
#[no_mangle]
pub extern "C" fn store_message(ptr: *const u8, len: usize) -> u32

#[no_mangle]
pub extern "C" fn get_message_count() -> u32

#[no_mangle]
pub extern "C" fn process_messages() -> u32
```

## Key Concepts

### 1. Function Exports

```rust
#[no_mangle]           // Preserve function name in WASM
pub extern "C"         // Use C ABI
fn function_name() {}  // Your function
```

### 2. State Management

```rust
use std::sync::atomic::{AtomicI32, Ordering};

static COUNTER: AtomicI32 = AtomicI32::new(0);

#[no_mangle]
pub extern "C" fn increment() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst) + 1
}
```

### 3. Memory Safety

Rust's ownership system works in WASM:
```rust
// Safe: Rust manages memory
let message = String::from("Hello");

// Unsafe: When interfacing with raw pointers
unsafe {
    let bytes = std::slice::from_raw_parts(ptr, len);
}
```

### 4. Type Mapping

| Rust Type | WASM Type | Notes |
|-----------|-----------|-------|
| `i32` | `i32` | Direct mapping |
| `i64` | `i64` | Direct mapping |
| `f32` | `f32` | Direct mapping |
| `f64` | `f64` | Direct mapping |
| `bool` | `i32` | 0 = false, 1 = true |
| `*const T` | `i32` | Pointer (linear memory offset) |
| `String` | `(*const u8, usize)` | Pointer + length |

## Optimization

### Size Optimization

The `Cargo.toml` includes size optimizations:

```toml
[profile.release]
opt-level = "z"   # Optimize for size
lto = true        # Link Time Optimization
strip = true      # Remove debug symbols
```

### Further Optimization

```bash
# Install wasm-opt
cargo install wasm-opt

# Optimize after build
wasm-opt -Oz -o optimized.wasm original.wasm
```

**Results:**
- Typical reduction: 30-50% size
- No performance loss
- Essential for production

## Advanced Patterns

### Pattern 1: State Machines

```rust
enum State {
    Idle,
    Processing(u32),
    Done,
}

static mut CURRENT_STATE: State = State::Idle;

#[no_mangle]
pub extern "C" fn transition(event: u32) {
    unsafe {
        CURRENT_STATE = match CURRENT_STATE {
            State::Idle => State::Processing(event),
            State::Processing(_) => State::Done,
            State::Done => State::Idle,
        };
    }
}
```

### Pattern 2: Error Handling

```rust
#[repr(C)]
pub struct Result {
    success: bool,
    value: i32,
    error_code: u32,
}

#[no_mangle]
pub extern "C" fn safe_divide(a: i32, b: i32) -> Result {
    if b == 0 {
        Result { success: false, value: 0, error_code: 1 }
    } else {
        Result { success: true, value: a / b, error_code: 0 }
    }
}
```

### Pattern 3: Callbacks

```rust
type Callback = extern "C" fn(i32) -> i32;

static mut CALLBACK: Option<Callback> = None;

#[no_mangle]
pub extern "C" fn register_callback(cb: Callback) {
    unsafe { CALLBACK = Some(cb); }
}

#[no_mangle]
pub extern "C" fn trigger(value: i32) -> i32 {
    unsafe {
        if let Some(cb) = CALLBACK {
            cb(value)
        } else {
            0
        }
    }
}
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment() {
        _start();
        assert_eq!(increment(), 1);
        assert_eq!(increment(), 2);
    }
}
```

Run tests:
```bash
cargo test
```

### Integration Tests

```bash
# Build and validate
make test
```

## Debugging

### Print Debugging

```rust
#[no_mangle]
pub extern "C" fn debug_function() {
    println!("Debug: function called");  // Works in Lunatic!
}
```

### Inspect WASM

```bash
# View exports
wasm-objdump -x build/counter.wasm | grep export

# View imports
wasm-objdump -x build/counter.wasm | grep import

# Disassemble
wasm-objdump -d build/counter.wasm
```

## Common Patterns for Lunatic

### Pattern: Hot Reload Compatible State

```rust
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
struct AppState {
    counter: i32,
    messages: Vec<String>,
}

// Serialize state before reload
#[no_mangle]
pub extern "C" fn save_state() -> Vec<u8> {
    // Implementation
}

// Restore state after reload
#[no_mangle]
pub extern "C" fn restore_state(data: &[u8]) {
    // Implementation
}
```

## Comparison with Other Languages

### Rust vs AssemblyScript

**Rust Advantages:**
- ✅ Full Lunatic API access
- ✅ Better performance
- ✅ Smaller WASM size
- ✅ Memory safety guarantees
- ✅ Rich ecosystem

**When to use AssemblyScript:**
- TypeScript developers
- Simple computational modules
- Rapid prototyping

### Rust vs Go/TinyGo

**Rust Advantages:**
- ✅ Smaller binary size
- ✅ More control over memory
- ✅ Better WASM tooling
- ✅ No garbage collector

**When to use Go:**
- Go developers
- Simpler syntax preference
- Existing Go codebase

## Production Checklist

- [ ] Add `opt-level = "z"` to Cargo.toml
- [ ] Enable LTO (`lto = true`)
- [ ] Strip debug symbols (`strip = true`)
- [ ] Run `wasm-opt -Oz` on output
- [ ] Test WASM module with `wasm-validate`
- [ ] Verify exports with `wasm-objdump`
- [ ] Test hot reload compatibility
- [ ] Add proper error handling
- [ ] Document all exported functions

## Resources

- [Rust WASM Book](https://rustwasm.github.io/docs/book/)
- [Lunatic Documentation](https://lunatic.solutions/)
- [WASI Documentation](https://wasi.dev/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)

## Next Steps

1. Explore the Lunatic crate for full runtime API
2. Check `examples/assemblyscript/` for TypeScript approach
3. Check `examples/go/` for Go/TinyGo examples
4. Read main Lunatic docs for process spawning and networking
