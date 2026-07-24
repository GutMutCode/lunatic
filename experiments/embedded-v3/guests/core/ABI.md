# Core guest ABI v2

The module imports these scalar functions from module `comparison`:

| Function | Signature | Contract |
| --- | --- | --- |
| `tenant_id` | `() -> i32` | Tenant selected by the independent runner. |
| `restore_counter` | `() -> i64` | Initial counter for a fresh instance. |
| `command_handle` | `() -> i64` | Oracle-owned correlation handle for a one-shot call. |
| `activation_started` | `(i32, i32, i64) -> ()` | Guest-originated tenant, version, and build marker. |
| `emit_result` | `(i64, i64, i32, i32, i64) -> ()` | Handle, counter, result kind, version, and build marker. |
| `execution_started` | `(i64) -> ()` | First externally observable action of `cpu_loop`. |
| `bind_observer` | `(i64) -> ()` | Binds Lunatic's observer argument; a no-op is valid elsewhere. |
| `next_command` | `() -> i32` | Lunatic loop opcode; also updates the host's current handle. |

Exports shared by all candidates are `memory`, `activation_check`, `increment`,
`probe`, `snapshot`, `trap`, and `cpu_loop`.  The six callable exports have no
parameters and no results, matching Extism's public `Plugin::call` ABI.
`activation_check` has an empty body: invoking it forces lazy runtimes to run
module activation without emitting a business result or mutating command state.
The activation-started marker remains intentional.  `run(i64) -> ()` is the
long-lived Lunatic entry point.  Extism and direct
Wasmtime register its unused imports so the same module instantiates unchanged.

Counter state is the little-endian `i64` at linear-memory offset `0`.  Version
and build markers are code constants, deliberately outside restored memory.
Consequently a Lunatic memory restore preserves state without overwriting the
identity of the newly activated build.

Result kinds are `1=increment`, `2=probe`, and `3=snapshot`.  `next_command`
opcodes are `0=stop`, `1=increment`, `2=probe`, `3=snapshot`, `4=trap`, and
`5=cpu_loop`.

`cpu_loop` calls `execution_started(command_handle())` and then executes an
infinite host-call-free Wasm loop.  Cancellation, epoch interruption, or fuel
exhaustion must therefore be supplied by the candidate runtime.
