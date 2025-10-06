# AssemblyScript Examples for Lunatic

This directory contains examples demonstrating how to use Lunatic runtime with AssemblyScript/TypeScript compiled to WebAssembly.

## Prerequisites

- Node.js (v16 or later)
- npm or yarn
- Lunatic runtime (built from this repo)

## Setup

```bash
cd examples/assemblyscript
npm install
```

## Building Examples

### Counter Example

A simple counter demonstrating state management in WASM:

```bash
npm run build
```

This will generate `build/counter.wasm`.

## Running Examples

### Basic Counter

```bash
# From the lunatic repo root
./target/debug/lunatic run examples/assemblyscript/build/counter.wasm
```

### With Watch Mode (Hot Reload)

```bash
# Terminal 1: Run with watch mode
./target/debug/lunatic run --watch examples/assemblyscript/build/counter.wasm

# Terminal 2: Modify and rebuild
# Edit assembly/counter.ts
npm run build
# Watch mode will automatically reload!
```

## Example Structure

- `assembly/counter.ts` - Simple counter with increment/reset operations
- `package.json` - NPM dependencies and build scripts
- `build/` - Compiled WASM output (generated)

## Key Concepts Demonstrated

### 1. WebAssembly Exports

```typescript
export function _start(): void {
  // Initialization function called by Lunatic
}

export function increment(): i32 {
  // Function exported to be called from Lunatic
}
```

### 2. State Management

```typescript
let counter: i32 = 0;  // Module-level state

export function increment(): i32 {
  counter += 1;
  return counter;
}
```

### 3. Memory Exports

```typescript
export { memory };  // Required for Lunatic to access WASM memory
```

## Language-Specific Considerations

### AssemblyScript vs Rust

**Pros:**
- Familiar TypeScript syntax
- Easy for JavaScript/TypeScript developers
- Good WebAssembly support

**Cons:**
- Limited access to Lunatic-specific APIs
- Smaller ecosystem for WASM than Rust
- Manual bindings needed for host functions

### Recommended Use Cases

- ✅ Computational modules
- ✅ State machines
- ✅ Simple services
- ⚠️ Complex networking (better in Rust)
- ⚠️ Process spawning (better in Rust)

## Testing

AssemblyScript code can be tested with standard JavaScript testing tools before compilation:

```bash
# Add to package.json
{
  "scripts": {
    "test": "node --experimental-wasm-modules tests/counter.test.js"
  }
}
```

## Next Steps

1. Explore more complex examples in other languages
2. See `examples/rust/` for full-featured Lunatic applications
3. Read `examples/go/` for Go/TinyGo examples
4. Check main documentation for Lunatic APIs

## Resources

- [AssemblyScript Documentation](https://www.assemblyscript.org/)
- [Lunatic Documentation](https://lunatic.solutions/)
- [WebAssembly Specification](https://webassembly.org/)
