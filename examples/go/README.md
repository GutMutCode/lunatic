# Go (TinyGo) Examples for Lunatic

This directory contains examples demonstrating how to use Lunatic runtime with Go, compiled to WebAssembly using TinyGo.

## Prerequisites

- [TinyGo](https://tinygo.org/) 0.30.0 or later
- Go 1.21 or later (for module management)
- Lunatic runtime (built from this repo)
- Optional: `wasm-validate` from [wabt](https://github.com/WebAssembly/wabt) for testing

## Why TinyGo?

Standard Go compiler doesn't support WASI target yet. TinyGo is a Go compiler designed for embedded systems and WebAssembly, with excellent WASM support.

### Installing TinyGo

**macOS:**
```bash
brew install tinygo
```

**Linux:**
```bash
wget https://github.com/tinygo-org/tinygo/releases/download/v0.30.0/tinygo_0.30.0_amd64.deb
sudo dpkg -i tinygo_0.30.0_amd64.deb
```

**Verify Installation:**
```bash
tinygo version
# Should output: tinygo version 0.30.0 ...
```

## Building Examples

### Using Make (Recommended)

```bash
cd examples/go
make build
```

### Manual Build

```bash
tinygo build -o build/counter.wasm -target=wasi counter.go
```

## Running Examples

### Basic Counter

```bash
# From the lunatic repo root
./target/debug/lunatic run examples/go/build/counter.wasm
```

### With Watch Mode

```bash
# Terminal 1: Run with watch mode
./target/debug/lunatic run --watch examples/go/build/counter.wasm

# Terminal 2: Modify and rebuild
# Edit counter.go (e.g., change increment logic)
cd examples/go
make build
# Watch mode will automatically reload!
```

## Example Structure

- `counter.go` - Simple counter with various operations
- `go.mod` - Go module definition
- `Makefile` - Build automation
- `build/` - Compiled WASM output (generated)

## Key Concepts Demonstrated

### 1. Export Functions to WASM

```go
//export increment
func increment() int32 {
    counter++
    return counter
}
```

The `//export` comment directive tells TinyGo to export this function to WebAssembly.

### 2. Module Initialization

```go
//export _start
func _start() {
    counter = 0
    fmt.Println("Go Counter initialized!")
}
```

`_start` is called when Lunatic loads the module.

### 3. State Management

```go
var counter int32 = 0  // Module-level state

//export increment
func increment() int32 {
    counter++  // State persists across function calls
    return counter
}
```

### 4. Type Compatibility

TinyGo types map to WASM types:
- `int32` → `i32`
- `int64` → `i64`
- `float32` → `f32`
- `float64` → `f64`
- `bool` → `i32` (0 or 1)

## Language-Specific Considerations

### Go/TinyGo vs Rust

**Pros:**
- Familiar to Go developers
- Simple syntax
- Good standard library subset
- Easier concurrency concepts

**Cons:**
- Larger WASM binary size (vs Rust)
- Not all Go standard library available in TinyGo
- Limited Lunatic API support (manual bindings needed)
- Slower compilation than standard Go

### TinyGo Limitations

Not all Go features are available:
- ❌ Full `reflect` package
- ❌ Some `syscall` functions
- ❌ CGo
- ✅ Most of `fmt`, `math`, `strings`
- ✅ Basic concurrency (goroutines, channels)
- ✅ Structs, interfaces, methods

### Recommended Use Cases

- ✅ Business logic modules
- ✅ Data processing
- ✅ Algorithms and computation
- ⚠️ Heavy I/O (better in Rust with full Lunatic API)
- ⚠️ Process management (better in Rust)

## Example Functions

### Counter Operations

```go
//export increment
func increment() int32          // Returns counter++

//export get_count
func get_count() int32           // Returns current value

//export reset
func reset()                     // Sets counter to 0

//export set_count
func set_count(value int32)      // Sets counter to value

//export multiply_count
func multiply_count(factor int32) int32  // Multiplies counter

//export is_even
func is_even() bool              // Checks if counter is even
```

### Calling from Lunatic

In the future, you could call these from a Rust process:

```rust
// Example (not yet implemented in Lunatic)
let module = lunatic::spawn_module("counter.wasm")?;
module.call("increment", &[])?;
let count = module.call("get_count", &[])?;
```

## Building Optimized WASM

For production, add optimization flags:

```bash
tinygo build -o build/counter.wasm \
  -target=wasi \
  -opt=2 \
  -no-debug \
  counter.go
```

**Flags:**
- `-opt=2`: Optimization level (0-2, or z/s for size)
- `-no-debug`: Remove debug info
- `-scheduler=none`: Disable scheduler (if no goroutines)

## Testing WASM Output

```bash
# Validate WASM
make test

# Or manually
wasm-validate build/counter.wasm

# Inspect WASM
wasm-objdump -x build/counter.wasm
```

## Common Issues

### Issue: "tinygo: command not found"
**Solution:** Install TinyGo (see Prerequisites)

### Issue: Large WASM file size
**Solution:** Use optimization flags (`-opt=2`)

### Issue: Function not exported
**Solution:** Ensure `//export` comment is immediately above function

### Issue: Import errors
**Solution:** Some Go packages aren't available in TinyGo. Check [TinyGo packages](https://tinygo.org/docs/reference/lang-support/)

## Next Steps

1. Explore Rust examples for full Lunatic API access
2. See AssemblyScript examples for TypeScript developers
3. Check main Lunatic documentation for process spawning
4. Look at `examples/wat/` for low-level WASM examples

## Resources

- [TinyGo Documentation](https://tinygo.org/docs/)
- [TinyGo WASM Guide](https://tinygo.org/docs/guides/webassembly/)
- [Lunatic Documentation](https://lunatic.solutions/)
- [Go WebAssembly Wiki](https://github.com/golang/go/wiki/WebAssembly)
