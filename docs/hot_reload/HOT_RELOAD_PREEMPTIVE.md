# Preemptive Hot Reload — Archived 2025 Design Note

Original date: 2025-10-05

Evidence review: 2026-07-21

Status: **design and component history; live production path unverified**

This document formerly claimed that preemptive hot reload was implemented for all processes and production-ready for local use. Those claims are withdrawn. The canonical current assessment is [`docs/core_values/status.md`](../core_values/status.md).

## Original Design Goal

The intended flow combined Wasmtime async execution, fuel/epoch yield points, a file watcher, process signals, memory snapshots, signature validation, and replacement-instance construction:

```text
file change
  -> compile candidate module
  -> send Signal::HotReload
  -> interrupt/yield running guest
  -> validate signatures
  -> snapshot state/resources
  -> instantiate replacement
  -> restore state/resources
  -> acknowledge success or failure
  -> commit version or roll back
```

This remains a useful target architecture, but the current code does not connect and verify the entire sequence.

## Components Present

- Wasmtime stores configure asynchronous fuel yielding and epoch deadlines.
- `Signal::HotReload` and pending-reload handling structures exist.
- Module compilation, signature-validation, registry, snapshot/restore, and resource-transfer helpers exist.
- Watch mode can detect a file change and enqueue a reload request.
- Unit/integration tests exercise individual reload components and manually composed memory replacement.

Component presence is not production-path completion.

## Current Blocking Gaps

1. **Running-instance ownership:** the guest future takes the Wasmtime instance out of `ProcessContext`. The reload handler later attempts to take an instance from that now-empty option and can return `No instance available for hot reload` (`crates/lunatic-process/src/wasm.rs`, `crates/lunatic-process/src/lib.rs`).
2. **Epoch coverage:** each `WasmtimeRuntime` constructs an engine, but the ticker-start guard is process-global. The single ticker advances only stores associated with the first-created engine; later independently constructed engines do not receive its epoch increments (`crates/lunatic-process/src/runtimes/wasmtime.rs`).
3. **No acknowledgement protocol:** enqueueing a signal is treated as reload success. There is no process-result acknowledgement that drives atomic commit, version lifecycle, or rollback (`crates/lunatic-process/src/hot_reload.rs`).
4. **Rollback is not production-verified:** rollback messages can fail and are logged without recovery. Existing tests do not prove a failed live reload restoring a running guest.
5. **Benchmark boundary:** `benches/hot_reload.rs` manually compiles/instantiates v1 and v2 and copies linear-memory bytes. It does not call the live signal path, interrupt a running guest, receive acknowledgement, commit a version, or roll back.
6. **Resource boundary:** a directly tested same-runtime helper can move supported live resource maps, including active TLS sessions, between replacement states. It is not proven reachable through live Wasm reload. Serialized snapshot, host-restart, and cross-node restoration of active TCP/TLS streams remain unsupported.

## Claims Not Established

The current evidence does not establish:

- hot reload for all process types or infinite loops;
- zero-downtime replacement of a live running Wasm process;
- a 50–200ms or `<100ms` production response time;
- state preservation through the public/watch-mode path;
- atomic multi-process reload and rollback;
- negligible CPU/memory overhead or million-process scalability;
- production readiness.

## Completion Evidence Required

A future completion claim must include executable tests that:

1. Keep a guest process alive and verify it is interrupted through `Signal::HotReload`.
2. Prove state before/after replacement through the live path.
3. Return explicit success/failure acknowledgement from every targeted process.
4. Commit a new version only after the configured atomic condition succeeds.
5. Restore prior code/state after compilation, validation, migration, or process failure.
6. Exercise multiple independently constructed runtime engines.
7. Benchmark the same live path with the tested commit, hardware, toolchain, workload, and unsupported cases recorded.

See [`HOT_RELOAD_ARCHITECTURE.md`](HOT_RELOAD_ARCHITECTURE.md) for the broader target design and [`../core_values/status.md`](../core_values/status.md) for current status.
