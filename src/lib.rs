/*!
The [lunatic vm](https://lunatic.solutions/) is a system for creating actors from WebAssembly
modules. This `lunatic-runtime` library allows you to embed the `lunatic vm` inside your Rust
code.

> _The actor model in computer science is a mathematical model of concurrent computation that
> treats actor as the universal primitive of concurrent computation. In response to a message it
> receives, an actor can: make local decisions, create more actors, send more messages, and
> determine how to respond to the next message received. Actors may modify their own private
> state, but can only affect each other indirectly through messaging (removing the need for
> lock-based synchronization)._
>
> Source: <https://en.wikipedia.org/wiki/Actor_model>

_**Note:** If you are looking to build actors in Rust and compile them to `lunatic` compatible
Wasm modules, checkout out the [lunatic crate](https://crates.io/crates/lunatic)_.

## Core Concepts

* [`Environment`] - defines the characteristics of Processes that are spawned into it. An
  [`Environment`] is created with an [`EnvConfig`] to tweak various settings, like maximum
  memory and compute usage.

* [`WasmProcess`](process::WasmProcess) - a handle to send signals and messages to spawned
  Wasm processes. It implements the [`Process`](process::Process) trait.


## WebAssembly module requirements

Lunatic expects guest modules to target the `wasm32-wasi` ABI and export their linear memory as
`memory`. Public entry points (for example `main`, `init` or any function that will be spawned as a
process) must use the C calling convention and be marked with `#[no_mangle]` so that Wasmtime can
locate them. Runtime features are accessed through the host imports defined in
`wat/all_imports.wat`; using the high-level `lunatic` crate automatically links the required
`lunatic::*` namespaces.

At runtime the module is spawned inside a sandboxed process configuration that defines resource
limits (memory, table slots, file descriptors, network handles, fuel). Modules should therefore
avoid relying on globals or ambient authority and communicate exclusively through the message API
provided by Lunatic.
*/

mod config;
pub mod hot_reload;
pub mod state;

pub use config::DefaultProcessConfig;
pub use lunatic_process::{Finished, Process, Signal, WasmProcess};
pub use state::DefaultProcessState;
