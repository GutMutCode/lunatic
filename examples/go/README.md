# Go (TinyGo) guest examples

This directory contains one executable Lunatic guest-API scenario and several
older Go/TinyGo source examples. Only `guest_e2e.go` is runtime-verified: the
counter is a generic Wasm export example, while the GenServer and Supervisor
files simulate those patterns with in-module Go state, goroutines, and channels.

## Verified guest scenario

`guest_e2e.go` uses `//go:wasmimport` declarations for the production
`lunatic::process`, `lunatic::wasi`, `lunatic::message`, and `lunatic::error`
namespaces. A successful run proves that a TinyGo WASI Preview 1 guest can:

1. Create an attenuated child configuration and pass its role through WASI
   command-line arguments.
2. Spawn a fresh instance of the same module through its `_start` entry point.
3. Observe that the child cannot spawn another process and read the returned
   permission error through an error-resource handle.
4. Receive timeout status `9027` from an empty mailbox.
5. Notify its root process with tag `99`, then complete a tagged `41 -> 42`
   message round trip.
6. Notify an external observer with completion tag `4202`.

The host integration harness supplies the observer process ID as root
`argv[0]`. When launched directly from the CLI, the filename in that position
does not parse as an ID, so the guest uses observer ID zero, skips the external
notification, and prints `GO_GUEST_E2E_OK` after all internal assertions pass.

## Toolchain

The reproducible target requires exactly TinyGo `0.41.1` and emits a core Wasm
module for WASI Preview 1 (`wasip1`). The Makefile rejects a different TinyGo
version instead of silently producing an untested artifact. CI also verifies
the Binaryen `116` optimizer bundled in TinyGo's checksum-verified Linux package.

- Go 1.25.5 and TinyGo 0.41.1 (the versions pinned by CI)
- Lunatic runtime built from this repository
- GNU Make or an equivalent Make implementation
- Optional: `wasm-validate` from WABT for an additional structural check

Go has had its own `wasip1/wasm` port since Go 1.21, but this example pins
TinyGo because TinyGo is the compiler whose generated guest is covered by this
scenario. The `go 1.21` line in `go.mod` is a module language-version floor; it
does not pin TinyGo.

## Build and run

From this directory:

```bash
make guest-e2e
```

The equivalent compiler invocation is:

```bash
tinygo build \
  -o build/guest_e2e.wasm \
  -target=wasip1 \
  -scheduler=none \
  -opt=z \
  -no-debug \
  guest_e2e.go
```

TinyGo invokes `wasm-opt` during this build. The official Linux package used by
CI bundles Binaryen 116; installations that do not bundle the optimizer must
provide a compatible `wasm-opt` themselves.

Build the runtime from the repository root, then execute the guest:

```bash
cargo build --locked --bin lunatic
make -C examples/go run-guest-e2e
```

`make -C examples/go test` performs the same Lunatic execution first and only
then runs `wasm-validate` when that optional tool is installed. Missing
`wasm-validate` never substitutes for the runtime E2E.

## Why `_start` and `-scheduler=none`?

Every Lunatic process receives a fresh Wasm instance. TinyGo command modules
initialize their language runtime through `_start`, so the parent spawns that
entry point and uses child `argv[0] == "child"` for role selection. Calling an
arbitrary exported Go function in a fresh instance would bypass this
initialization contract. A TinyGo `-buildmode=c-shared` reactor exports
`_initialize` instead of the `_start` entry point expected by `lunatic run`, so
it is not used here.

The E2E intentionally compiles with `-scheduler=none`. Its concurrency comes
from separate Lunatic processes and bounded host mailboxes, not TinyGo
goroutines or channels.

## Boundary of the other files

- `counter.go` demonstrates Wasm exports and module-local state only. It does
  not call a Lunatic host API.
- `gen_server_example.go` uses a goroutine and Go channels inside one module.
  It is an in-module pattern simulation, not a Lunatic GenServer adapter.
- `supervisor_example.go` models workers and restart strategies as Go objects.
  It does not spawn, link, or monitor Lunatic processes and is not runtime E2E
  evidence.
- The local imports in `guest_e2e.go` are a minimal executable adapter, not a
  published or versioned Go SDK. Guest Supervisor/GenStatem/GenEvent libraries,
  distributed OTP, resource-transfer wrappers, WASI Preview 2 components, and
  cross-language API parity remain outside this example.

The legacy files remain available through individual Make targets or
`make simulations`, but they are intentionally excluded from the default
`make build` and `make test` paths.

## ABI maintenance

The import declarations must stay aligned with `wat/all_imports.wat`. The E2E
provides an additional link-time and execution-time guard: a namespace, name,
or signature mismatch prevents `guest_e2e.wasm` from starting against Lunatic.
