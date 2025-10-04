# Agent Guidelines for Lunatic Runtime

## Build & Test Commands
- Build: `cargo build` (release: `cargo build --release`)
- Test all: `cargo test --all`
- Test single: `cargo test test_name`
- Test specific crate: `cargo test -p crate-name`
- Lint: `cargo clippy --examples --tests --benches -- -D warnings`
- Format check: `cargo fmt -- --check`
- Format: `cargo fmt`

## Code Style
- Language: Rust (edition 2018)
- Imports: Group std, external crates, then internal modules with `use` statements
- Error handling: Use `anyhow::Result<T>` for public APIs, propagate with `?`
- Async: Use `tokio` runtime, async functions where needed for I/O
- Naming: snake_case for functions/variables, PascalCase for types, SCREAMING_SNAKE_CASE for constants
- Modules: Prefer `mod.rs` or separate files in subdirectories
- Types: Use explicit types for public APIs, inference for internal code
- Host function changes require updates to `wat/all_imports.wat`
