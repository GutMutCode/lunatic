# Multi-Language Examples for Lunatic

Lunatic exposes language-neutral WebAssembly host imports. A language can target Lunatic when its toolchain emits a compatible Wasm/WASI module and it has the bindings required for the host APIs it uses.

This guide helps you choose the right language for your use case and get started quickly.

> **Evidence boundary:** Source examples and build recipes are present, but the repository does not yet validate them as a complete build-and-run matrix. They do not prove equivalent Lunatic API coverage across languages. Rust OTP examples use host-side native-process adapters; the Go and AssemblyScript OTP examples are manual pattern simulations. TinyGo and AssemblyScript guest E2E execution are not enforced in CI, and some example recipes still require repair. See [`docs/core_values/status.md`](../docs/core_values/status.md) for canonical status.

## Quick Start by Language

| Language | Directory | Best For | Difficulty |
|----------|-----------|----------|------------|
| **Rust** | [`rust/`](rust/) | Broadest available bindings and host-side OTP examples | ⭐⭐ |
| **Go** | [`go/`](go/) | Go developers, simple services | ⭐⭐ |
| **AssemblyScript** | [`assemblyscript/`](assemblyscript/) | TypeScript devs, quick prototypes | ⭐ |
| **WAT** | [`*.wat`](./) | Learning, low-level control | ⭐⭐⭐ |

## OTP Pattern Examples — Experimental and Scope-Limited

The `lunatic-otp-patterns` crate contains host-side Erlang/OTP-inspired components. GenServer and Supervisor have native Lunatic-process integration tests, but guest-Wasm adapters, automatic Supervisor monitor intake, and process-runtime GenStatem/GenEvent adapters remain pending.

### GenServer - Generic Server Pattern

**GenServer** provides synchronous and asynchronous message handling with state management.

```rust
use lunatic_otp_patterns::{GenServer, GenServerConfig};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter { count: i64 }

#[derive(Debug, Serialize, Deserialize)]
enum Request { Increment, Get }

#[derive(Debug, Serialize, Deserialize)]
enum Response { Ok, Value(i64) }

impl GenServer for Counter {
    type State = Self;
    type Call = Request;
    type CallReply = Response;
    type Cast = Request;

    fn init() -> Self::State { Counter { count: 0 } }

    fn handle_call(&mut self, request: Self::Call) -> anyhow::Result<Self::CallReply> {
        match request {
            Request::Increment => {
                self.count += 1;
                Ok(Response::Ok)
            }
            Request::Get => Ok(Response::Value(self.count)),
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) -> anyhow::Result<()> {
        match request {
            Request::Increment => {
                self.count += 1;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
```

### Supervisor - Process Supervision

**Supervisor** manages real child process handles and applies restart strategies when an exit reason is forwarded to `handle_child_exit`.

```rust
use lunatic_otp_patterns::{
    ChildSpec, RestartPolicy, RestartStrategy, ShutdownPolicy, Supervisor, SupervisorSpec,
};
use lunatic_process::{env::Environment, spawn_native, Process};
use std::{future, sync::Arc};

fn start_worker(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    let (_join, process) = spawn_native(environment, |_process, _mailbox| async move {
        future::pending::<anyhow::Result<()>>().await
    });
    Ok(Arc::new(process))
}

let spec = SupervisorSpec {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 3,
    max_seconds: 5,
    children: vec![
        ChildSpec {
            id: "worker1".to_string(),
            start: start_worker,
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Brutal,
            child_type: Default::default(),
        }
    ],
};

let mut supervisor = Supervisor::new(spec);
supervisor.start_children()?;
```

### Key Features

- ✅ **Fault Tolerance**: Ordered process replacement with bounded restart intensity after exit notification
- ✅ **Concurrency**: Message-passing between processes
- ✅ **State Management**: Safe mutable state handling
- ✅ **Error Propagation**: Structured error handling with `OtpError`
- ⚠️ **Language boundary**: Rust examples call host-side adapters; Go and AssemblyScript show manual designs rather than the same runtime API
- ⚠️ **Performance boundary**: Historical mailbox microbenchmarks measure local queue operations, not OTP or end-to-end process latency

### Running a Wasm Example

```bash
# Build a compatible Wasm module
cargo build --target wasm32-wasip1 --release

# Run it with the current CLI
lunatic run target/wasm32-wasip1/release/my_app.wasm
```

This command demonstrates module execution only; it does not turn the host-side OTP crate into a guest-Wasm API or certify a production deployment.

**→ [OTP Examples](rust/src/gen_server_example.rs)** | **→ [Supervisor Examples](go/supervisor_example.go)**

## 🐛 Troubleshooting OTP Patterns

### Common Issues

#### 1. **GenServer handle_call/handle_cast Not Returning Result**

**Error:** `expected Result<T>, found T`

**Solution:** Update your GenServer implementation to return `Result<T>`:

```rust
// ❌ Wrong
fn handle_call(&mut self, request: Self::Call) -> Self::CallReply {
    // ... logic
    MyReply::Ok  // Missing Ok()
}

// ✅ Correct
fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
    // ... logic
    Ok(MyReply::Ok)
}
```

#### 2. **Supervisor Restart Intensity Exceeded**

**Error:** `Restart intensity limit exceeded`

**Solution:** Check your supervisor configuration:

```rust
SupervisorSpec {
    max_restarts: 10,  // Increase if needed
    max_seconds: 60,   // Increase window
    // ... other config
}
```

#### 3. **Message Serialization Errors**

**Error:** `InvalidMessage` or deserialization failures

**Solution:** Ensure all message types implement `Serialize + Deserialize`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]  // Add these derives
pub enum MyMessage {
    // ...
}
```

#### 4. **Process Spawn Failures**

**Error:** `ChildStartFailure`

**Solution:** Check your child start function uses the supplied environment and returns a registered process handle:

```rust
ChildSpec {
    start: |environment| {
        // Ensure this returns an Arc<dyn Process> registered in `environment`.
        spawn_my_process(environment)
    },
    // ...
}
```

### Performance Tuning

- **GenServer Calls**: Keep `handle_call` fast (< 1ms) to avoid blocking
- **Message Size**: Keep messages small for better throughput
- **Supervisor Trees**: Limit depth to 3-4 levels for maintainability
- **Restart Windows**: Tune `max_seconds` based on your failure patterns

### Monitoring

The current Supervisor does not automatically emit restart telemetry. Applications must add their own logging around forwarded exit events and `handle_child_exit`; a structured OTP monitoring contract remains future work.

## Language Comparison

### 🦀 Rust — Broadest Current Ecosystem

**Pros:**
- ✅ Most mature available Lunatic binding ecosystem
- ✅ Strong type safety and memory safety
- ✅ Host-side OTP component examples in this repository

**Cons:**
- ⚠️ Steeper learning curve
- ⚠️ Longer compile times than scripting languages

**When to Use:**
- When the required host imports are available in the selected Rust binding
- When you can validate the complete workload against the runtime
- Complex state management

**Example:**
```rust
#[no_mangle]
pub extern "C" fn increment() -> i32 {
    COUNTER.fetch_add(1, Ordering::SeqCst) + 1
}
```

[**→ Rust Examples**](rust/)

---

### 🔵 Go (via TinyGo)

**Pros:**
- ✅ Familiar syntax for Go developers
- ✅ Simpler than Rust
- ✅ Good standard library support
- ✅ Easy concurrency concepts

**Cons:**
- ⚠️ Larger WASM size than Rust
- ⚠️ Limited Lunatic API (manual bindings needed)
- ⚠️ Not all Go stdlib available in TinyGo
- ⚠️ Requires TinyGo compiler

**When to Use:**
- Existing Go codebase
- Team familiar with Go
- Business logic modules
- Data processing tasks

**Example:**
```go
//export increment
func increment() int32 {
    counter++
    return counter
}
```

[**→ Go Examples**](go/)

---

### 📘 AssemblyScript (TypeScript)

**Pros:**
- ✅ TypeScript syntax (familiar to JS/TS devs)
- ✅ Easiest for web developers
- ✅ Fast compilation
- ✅ Good WASM tooling

**Cons:**
- ⚠️ Limited Lunatic API support
- ⚠️ Smaller ecosystem than Rust
- ⚠️ Manual bindings for host functions
- ⚠️ Moderate WASM binary size

**When to Use:**
- TypeScript/JavaScript developers
- Rapid prototyping
- Computational modules
- Simple state machines

**Example:**
```typescript
export function increment(): i32 {
  counter += 1;
  return counter;
}
```

[**→ AssemblyScript Examples**](assemblyscript/)

---

### 📝 WebAssembly Text (WAT)

**Pros:**
- ✅ Full control over WASM
- ✅ No compilation step
- ✅ Educational value
- ✅ Direct control over emitted Wasm instructions

**Cons:**
- ⚠️ Very verbose
- ⚠️ No high-level abstractions
- ⚠️ Error-prone
- ⚠️ Not suitable for large projects

**When to Use:**
- Learning WASM internals
- Tiny modules (<100 lines)
- Performance debugging
- Educational purposes

**Example:**
```wasm
(func (export "increment") (result i32)
  (global.set $counter
    (i32.add (global.get $counter) (i32.const 1)))
  (global.get $counter))
```

[**→ WAT Examples**](./)

---

## Feature Support Matrix

| Feature | Rust | Go | AssemblyScript | WAT |
|---------|------|-----|----------------|-----|
| **Basic Functions** | ✅ | ✅ | ✅ | ✅ |
| **State Management** | ✅ | ✅ | ✅ | ✅ |
| **Strings** | ✅ | ✅ | ✅ | ⚠️ Manual |
| **Collections** | ✅ HashMap, Vec | ✅ map, slice | ✅ Array, Map | ❌ |
| **Error Handling** | ✅ Result, Option | ✅ error | ⚠️ Limited | ❌ |
| **OTP Patterns** | ⚠️ Host-side GenServer/Supervisor examples | ⚠️ Manual simulation | ⚠️ Manual simulation | ❌ |
| **Live Hot Reload** | ⚠️ Production path unverified | ⚠️ Production path unverified | ⚠️ Production path unverified | ⚠️ Production path unverified |
| **Process Spawning** | ⚠️ Binding-dependent | ⚠️ Manual bindings | ⚠️ Manual bindings | ⚠️ Manual bindings |
| **Networking** | ⚠️ Binding-dependent | ⚠️ Manual bindings | ❌ | ❌ |
| **Binary Size** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Compile Speed** | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Developer Experience** | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ |

---

## Getting Started

### 1. Choose Your Language

Pick based on:
- **Your experience**: Use what you know
- **Project requirements**: Choose only after validating the exact host imports and failure behavior your workload needs
- **Team skills**: Match team expertise
- **Feature needs**: Rust currently has the broadest ecosystem; verify every required API against its binding and an executable test

### 2. Install Prerequisites

**All Languages:**
```bash
# Build Lunatic runtime
cargo build

# Optional: after installing WABT with your platform's supported package,
# confirm that `wasm-validate` is available on PATH.
wasm-validate --version
```

**Rust:**
```bash
rustup target add wasm32-wasip1
```

**Go:**
```bash
# Install TinyGo
brew install tinygo  # macOS
# or download from https://tinygo.org/getting-started/install/
```

**AssemblyScript:**
```bash
npm install --save-dev assemblyscript
```

### 3. Inspect and Validate an Example

Choose a language directory and inspect its README and build files before running it. The current recipes are not a CI-verified cross-language matrix: the Rust package mixes host-only OTP dependencies with guest targets, TinyGo is not built in CI, and the AssemblyScript test script references missing test material. Treat successful compilation, Wasm validation, and execution through the current `lunatic run` CLI as separate required checks.

### 4. Experiment with Watch Mode

Watch mode can detect a rebuilt file and request a reload, but the provided counter exits instead of remaining alive and the running-Wasm replacement path is not connected end to end. There is no verified runnable live-reload example yet; do not use this guide as a zero-downtime deployment procedure.

---

## Common Patterns Across Languages

### Pattern 1: Module Initialization

Every language needs a `_start` function:

**Rust:**
```rust
#[no_mangle]
pub extern "C" fn _start() {
    // Initialize
}
```

**Go:**
```go
//export _start
func _start() {
    // Initialize
}
```

**AssemblyScript:**
```typescript
export function _start(): void {
    // Initialize
}
```

### Pattern 2: Exporting Functions

**Rust:**
```rust
#[no_mangle]
pub extern "C" fn my_function(x: i32) -> i32 {
    x * 2
}
```

**Go:**
```go
//export my_function
func my_function(x int32) int32 {
    return x * 2
}
```

**AssemblyScript:**
```typescript
export function my_function(x: i32): i32 {
    return x * 2;
}
```

### Pattern 3: State Management

**Rust:**
```rust
use std::sync::atomic::{AtomicI32, Ordering};
static COUNTER: AtomicI32 = AtomicI32::new(0);
```

**Go:**
```go
var counter int32 = 0
```

**AssemblyScript:**
```typescript
let counter: i32 = 0;
```

---

## OTP Patterns in Lunatic

The repository provides host-side Rust adapters for selected Erlang/OTP-inspired patterns and manual Go/AssemblyScript sketches. These examples are learning material, not equivalent guest-Wasm runtime integrations or a general fault-tolerance guarantee.

### GenServer Pattern

GenServer provides a generic server process that handles synchronous and asynchronous requests.

**Rust (with lunatic-otp-patterns):**
```rust
use lunatic_otp_patterns::{GenServer, SupervisorSpec, RestartStrategy};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
    count: i64,
}

impl GenServer for Counter {
    type State = Self;
    type Call = CounterRequest;
    type CallReply = CounterResponse;
    type Cast = CounterRequest;

    fn init() -> Self::State {
        Counter { count: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> anyhow::Result<Self::CallReply> {
        Ok(match request {
            CounterRequest::Increment => {
                self.count += 1;
                CounterResponse::Ok
            }
            CounterRequest::Get => CounterResponse::Value(self.count),
            // ... other handlers
        })
    }
}
```

**Go (manual implementation):**
```go
type CounterServer struct {
    count int
}

func (c *CounterServer) handleCall(request CounterRequest) CounterResponse {
    switch request.Type {
    case "increment":
        c.count++
        return CounterResponse{Type: "ok", Value: c.count}
    case "get":
        return CounterResponse{Type: "value", Value: c.count}
    }
    return CounterResponse{Type: "error", Value: 0}
}
```

**AssemblyScript (manual implementation):**
```typescript
export class CounterServer {
    private count: i32 = 0;

    handleCall(request: CounterRequest): CounterResponse {
        switch (request.type) {
            case "increment":
                this.count++;
                return new CounterResponse("ok", this.count);
            case "get":
                return new CounterResponse("value", this.count);
        }
        return new CounterResponse("error", 0);
    }
}
```

### Supervisor Pattern

Supervisors manage child processes with configurable restart strategies.

**Rust (with lunatic-otp-patterns):**
```rust
let spec = SupervisorSpec {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 3,
    max_seconds: 5,
    children: vec![
        ChildSpec {
            id: "worker1".to_string(),
            start: start_worker,
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Timeout(5000),
            child_type: ChildType::Worker,
        },
    ],
};

let mut supervisor = Supervisor::new(spec);
supervisor.start_children()?;
```

**Go (manual implementation):**
```go
type SupervisorSpec struct {
    Strategy     RestartStrategy
    MaxRestarts  int
    MaxSeconds   int
    Children     []ChildSpec
}

type Supervisor struct {
    spec         SupervisorSpec
    children     map[string]*WorkerHandle
    restartCount int
    lastRestart  time.Time
}
```

**AssemblyScript (manual implementation):**
```typescript
export class SupervisorSpec {
    strategy: RestartStrategy;
    maxRestarts: i32;
    maxSeconds: i32;
    children: ChildSpec[];
}

export class Supervisor {
    private spec: SupervisorSpec;
    private children: Map<string, WorkerHandle>;

    handleChildExit(childID: string, reason: string): boolean {
        // Implement restart logic based on strategy
        return true;
    }
}
```

### When to Use OTP Patterns

- **Use GenServer** for stateful processes that need to handle requests
- **Use Supervisor** for managing groups of related processes
- **Use both together** for building fault-tolerant application architectures

**Examples:**
- [`rust/gen_server_example.rs`](rust/src/gen_server_example.rs) - Rust host-side GenServer
- [`go/gen_server_example.go`](go/gen_server_example.go) - Go manual pattern sketch
- [`assemblyscript/gen_server_example.ts`](assemblyscript/assembly/gen_server_example.ts) - AssemblyScript manual pattern sketch
- [`go/supervisor_example.go`](go/supervisor_example.go) - Go manual Supervisor sketch
- [`assemblyscript/supervisor_example.ts`](assemblyscript/assembly/supervisor_example.ts) - AssemblyScript manual Supervisor sketch

---

## Migration Guide

### From Node.js/TypeScript → AssemblyScript

1. Learn TypeScript strict mode
2. Understand WASM limitations (no DOM, limited stdlib)
3. Start with `assemblyscript/` examples
4. Gradually adopt Lunatic patterns

### From Go → TinyGo

1. Check [TinyGo supported packages](https://tinygo.org/docs/reference/lang-support/)
2. Start with `go/` examples
3. Test compilation with the repository's documented TinyGo WASI target and validate the emitted imports
4. Replace unsupported packages

### From Any Language → Rust

1. Complete [Rust Book](https://doc.rust-lang.org/book/)
2. Read [Rust WASM Book](https://rustwasm.github.io/docs/book/)
3. Start with `rust/` examples
4. Leverage Rust's type system for safety

---

## Troubleshooting

### Issue: WASM Module Won't Load

**Symptoms:** "Failed to instantiate module" error

**Solutions:**
1. Verify WASM validity: `wasm-validate module.wasm`
2. Check exports: `wasm-objdump -x module.wasm | grep export`
3. Ensure `_start` function exists
4. For current Rust toolchains, check that the target is `wasm32-wasip1`, not `wasm32-unknown-unknown`

### Issue: Function Not Found

**Symptoms:** "Unknown function" when calling from Lunatic

**Solutions:**
1. Verify export directive (`#[no_mangle]`, `//export`, `export`)
2. Check function signature matches expected types
3. Inspect WASM: `wasm-objdump -x module.wasm`

### Issue: Large WASM Binary

**Symptoms:** WASM file is several MB

**Solutions:**
1. **Rust:** Add optimization to `Cargo.toml`:
   ```toml
   [profile.release]
   opt-level = "z"
   lto = true
   strip = true
   ```
2. **Go:** Use TinyGo instead of standard Go
3. **All:** Run `wasm-opt -Oz input.wasm -o output.wasm`

### Issue: Memory Access Errors

**Symptoms:** Crashes when accessing strings/arrays

**Solutions:**
1. Ensure proper pointer passing
2. Export memory: `export { memory }`
3. Check alignment requirements
4. Verify string encoding (UTF-8)

---

## Best Practices

### 1. Start Simple

Begin with basic counter examples before complex features.

### 2. Use Version Control

Track your WASM modules and source separately:
```gitignore
*.wasm
build/
target/
```

### 3. Test Incrementally

Build and test each function before adding more:
```bash
# Build
make

# Test validity
wasm-validate build/module.wasm

# Test with Lunatic
./target/debug/lunatic run build/module.wasm
```

### 4. Prepare a Release Build

- Enable all optimizations
- Run `wasm-opt`
- Strip debug symbols
- Validate output

### 5. Document Exports

Clearly document all exported functions:
```rust
/// Increments the global counter
/// Returns: New counter value
#[no_mangle]
pub extern "C" fn increment() -> i32 {
    // ...
}
```

---

## Contributing Examples

Want to add examples in other languages (Zig, C, C++, etc.)?

1. Create `examples/language/` directory
2. Add counter example demonstrating basic features
3. Include comprehensive README with:
   - Prerequisites
   - Build instructions
   - Language-specific considerations
   - Comparison with Rust
4. Add to this guide's comparison matrix
5. Submit PR!

---

## Resources

### General
- [Lunatic Documentation](https://lunatic.solutions/)
- [WebAssembly Specification](https://webassembly.org/)
- [WASI Documentation](https://wasi.dev/)

### Language-Specific
- [Rust WASM Book](https://rustwasm.github.io/docs/book/)
- [TinyGo WASM Guide](https://tinygo.org/docs/guides/webassembly/)
- [AssemblyScript Docs](https://www.assemblyscript.org/)
- [WAT Specification](https://webassembly.github.io/spec/core/text/index.html)

### Tools
- [wabt](https://github.com/WebAssembly/wabt) - WASM Binary Toolkit
- [wasm-opt](https://github.com/WebAssembly/binaryen) - WASM Optimizer
- [wasm-pack](https://rustwasm.github.io/wasm-pack/) - Rust WASM workflow

---

## Summary

| If you want... | Use... | Because... |
|----------------|--------|------------|
| Broadest current Lunatic bindings | **Rust** | Most mature available ecosystem; verify required APIs individually |
| Familiar Go syntax | **Go/TinyGo** | Easy for Go developers |
| Quick TypeScript prototypes | **AssemblyScript** | Familiar syntax, fast iteration |
| Learn WASM internals | **WAT** | Direct WASM control |
| Production evaluation | **Rust** | Broadest tooling, while production-path gaps still require workload-specific validation |

**Still unsure?** Start with the language you know best, then migrate to Rust when you need advanced features.

Happy coding! 🚀
