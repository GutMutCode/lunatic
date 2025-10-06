# Zig Examples for Lunatic

This directory contains Zig examples demonstrating Lunatic's multi-language support.

## Prerequisites

- [Zig](https://ziglang.org/) 0.11+ with WASM support
- Lunatic runtime

## Building Examples

```bash
# Build counter example
zig build -Dtarget=wasm32-freestanding

# The WASM files will be in zig-out/bin/
```

## Examples

### Counter
A simple counter demonstrating basic WASM functionality with exported functions.

### GenServer Example (Planned)
OTP GenServer pattern implementation in Zig (work in progress).

## Current Status

- ✅ Basic WASM compilation setup
- ✅ Counter example with exported functions
- 🔄 GenServer pattern implementation (in progress)
- 🔄 Full OTP integration (planned)

## Notes

Zig support is experimental. The build system may need updates based on Zig version changes.