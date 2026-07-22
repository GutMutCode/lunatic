# Watch-Mode Hot Reload Usage

## Run a watched module

The supported file-triggered reload interface is:

```bash
lunatic run --watch app.wasm
```

Guest arguments follow the module path:

```bash
lunatic run --watch app.wasm arg1 arg2
```

The command watches the compiled `.wasm` file. Rebuild the guest separately
whenever its source changes.

## What happens on a file change

Lunatic compiles the candidate module, registers a new version, and coordinates
an atomic update of affected local processes. On a compatible successful
replacement, the production-path tests cover:

- stable process identity;
- compatible linear-memory preservation;
- FIFO mailbox preservation;
- selected in-process host-resource transfer; and
- rollback to the previous running instance when signature validation or
  candidate instantiation fails.

An unsuccessful candidate is logged and is not committed.

## Evidence and limitations

[`tests/live_hot_reload.rs`](../../tests/live_hot_reload.rs) is the main
running-Wasm integration test. The exact current evidence boundary is maintained
in [Core Values Status](../core_values/status.md).

Important limits:

- The demonstrated reload transaction is local to one runtime environment;
  distributed and cross-node reload are not verified.
- The new module must remain compatible with the running process.
- Resource transfer coverage is not universal. Live TCP/TLS preservation is
  tested at the runtime transfer boundary rather than through a guest-driven
  reload E2E.
- Persisted snapshots, host restarts, and cross-node migration cannot restore
  active TCP/TLS streams.
- Lunatic watches Wasm output but does not invoke Cargo or another compiler.
- Current benchmarks do not establish end-to-end reload latency, zero
  interruption, or cluster-scale behavior.

## Related documents

- [Current watch-mode overview](HOT_RELOAD_MVP.md)
- [Historical architecture proposal](HOT_RELOAD_ARCHITECTURE.md)
- [Historical development log](HOT_RELOAD_DEVELOPMENT.md)
- [Phase 2 historical progress](HOT_RELOAD_PHASE2_PROGRESS.md)
