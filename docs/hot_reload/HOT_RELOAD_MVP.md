# Watch-Mode Hot Reload

## Current status

Lunatic exposes one file-triggered reload command:

```bash
lunatic run --watch app.wasm
```

When the watched WebAssembly file changes, Lunatic compiles and registers the
new module, then applies the update to the running local process through the
reload coordinator. A successful compatible update preserves process identity,
linear memory, FIFO mailbox contents, and the supported in-process host
resources exercised by the production-path tests. If signature validation or
new-instance preparation fails, the previous instance keeps running.

The authoritative evidence boundary is documented in
[Core Values Status](../core_values/status.md). The main executable regression
test is [`tests/live_hot_reload.rs`](../../tests/live_hot_reload.rs).

## Usage

```bash
# Run a module and watch its .wasm file.
lunatic run --watch app.wasm

# Pass guest arguments after the module path.
lunatic run --watch app.wasm arg1 arg2

# Grant directory access while watching the module.
lunatic run --watch --dir /path/to/dir app.wasm
```

Lunatic watches the compiled `.wasm` file; it does not rebuild guest source.
Recompile the guest in another terminal, for example:

```bash
cargo build --target wasm32-wasip1
```

## Reload flow

```text
.wasm change
    -> compile candidate module
    -> validate and register candidate version
    -> prepare affected local processes
    -> commit all prepared replacements, or resume the previous instances
```

The watcher debounces rapid file-system events. An invalid candidate is
reported as a reload failure and is not committed.

## Current limitations

- The validated path is process-local; distributed and cross-node reload are
  not established.
- The replacement module must satisfy the runtime's compatibility checks.
- Compatible linear memory, mailbox contents, process identity, and selected
  in-process resources are covered. This is not a guarantee that every WASI or
  application-managed resource can migrate.
- Live TCP/TLS transfer is tested at the runtime resource-transfer boundary,
  not by a guest-driven reload E2E. Persisted snapshots, host restarts, and
  cross-node migration cannot restore active TCP/TLS streams.
- Source recompilation remains external to Lunatic.
- End-to-end hot-reload latency and production scale are not established by the
  repository's current benchmarks.

## Testing

Run the production-path integration test:

```bash
cargo test --test live_hot_reload
```

For a manual file-watcher demonstration, follow
[the demo instructions](../../examples/DEMO_INSTRUCTIONS.md).

## References

- [Hot reload architecture history](HOT_RELOAD_ARCHITECTURE.md)
- [Core Values Status](../core_values/status.md)
- [Lunatic Process Model](../../README.md#architecture)
