# AssemblyScript guest example

This directory contains a minimal AssemblyScript module that is compiled and
executed against Lunatic's production guest imports. It is an ABI fixture, not a
complete AssemblyScript SDK.

## Supported toolchain

- Node.js 18 or newer
- npm 10 or newer
- AssemblyScript 0.27.37, pinned by `package-lock.json`
- the Lunatic runtime built from this repository

The generated module is core WebAssembly. The fixture imports
`lunatic::process`, `lunatic::message`, and `lunatic::error` directly and does
not import WASI. Lunatic registers WASI preview0 (`wasi_unstable`) and preview1
(`wasi_snapshot_preview1`) for languages that emit those ABIs; component-model
WASI and arbitrary JavaScript `env` imports are outside this example's contract.

AssemblyScript normally emits imports such as `env.abort`, and `console.log`
emits `env.console.log`. Lunatic does not provide those JavaScript host objects.
The E2E fixture therefore uses fixed raw-memory buffers, the stub runtime, and
explicit traps, and it does not generate ESM bindings.

## Build and run the E2E

From this directory:

```bash
npm ci
npm test
```

`npm test` builds `build/guest_e2e.wasm` and executes it through the repository's
`lunatic run` command. A contract mismatch traps the guest and fails the command.
The module verifies all of the following over real host imports:

- `create_config` returns a least-privilege child configuration;
- the parent spawns the exported `child(i64)` function from the same module;
- the child is denied a nested spawn, receives a guest-visible error handle,
  inspects it, and releases it;
- a tagged request carrying `41` receives the correlated tag/value `42`;
- both send/receive and direct receive timeout paths return status `9027`;
- `parent(i64)` sends observer completion tag `4203` when an observer is supplied.

The exported `_start()` runs the same parent/child scenario without an external
observer, so it can also be invoked directly:

```bash
npm run build
cargo run --locked --manifest-path ../../Cargo.toml --bin lunatic -- \
  run build/guest_e2e.wasm
```

The fixture exports linear memory under the standard `memory` name by default;
an explicit `export { memory }` declaration is neither required nor supported by
current AssemblyScript as a normal value export.

## Other source files

- `assembly/counter.ts` is a computation-only example. It has no Lunatic host
  API coverage and can be compiled separately with `npm run build:counter`.
- `assembly/gen_server_example.ts` is an educational direct-object simulation.
  Its handle calls methods on an in-memory object; it is not a Lunatic process or
  OTP1 guest adapter.
- `assembly/supervisor_example.ts` is an educational `WorkerHandle` simulation.
  It manually invokes failure handling and does not spawn, link, or monitor
  Lunatic processes.

Those simulations are intentionally excluded from `npm run build` and
`npm test`. High-level AssemblyScript GenServer/Supervisor bindings, networking,
WASI filesystem use, distributed OTP, and live hot reload remain unsupported by
this fixture. The finite E2E module exits after validation, so it is not a useful
`--watch` demonstration.
