# Lunatic Performance Analysis — Historical and Scope-Limited

Original analysis: 2025-10-06

Evidence review: 2026-07-21

Canonical current status: [`docs/core_values/status.md`](../core_values/status.md)

> **Evidence scope**
>
> The numeric results discussed here are an October 2025 snapshot with no recorded commit identifier or reproducible hardware profile. They are not current-HEAD performance evidence.
>
> `benches/mailbox.rs` measures local mailbox construction, N direct pushes, and one pop—not process-to-process delivery. `benches/hot_reload.rs` manually compiles and instantiates two modules and calls memory snapshot/restore—not the live `Signal::HotReload` path. Memory and million-process figures are estimates or arithmetic projections—not RSS/heap or scale/soak measurements.
>
> Consequently this document does not certify `CORE_VALUES.md` targets or production readiness.

## Target Evidence Summary

| Target | Historical observation | Evidence boundary | Result |
| --- | ---: | --- | --- |
| Process spawn `<10µs` | 23.055µs | Minimal precompiled `hello.wat` spawn-and-join harness | Recorded run missed target; current HEAD not measured here |
| End-to-end message `<1µs` | 353.23ns | Local mailbox creation, 10 pushes, and one FIFO pop | End-to-end path not measured |
| Live reload `<100ms` | 758.94µs | Manual compile/instantiate/registry/snapshot/restore sequence | Live path not measured |
| Memory/process `<1KiB` | ~66KiB | Lower-bound projection including a 64KiB example linear-memory page | RSS/reachable heap not measured; projected value misses target |
| One million live processes | ~66GiB | Arithmetic extrapolation | No scale or soak run |

## Component Analysis

### Process spawning

`benches/spawn.rs` compiles the small module before measurement, then constructs process state, calls `spawn_wasm`, and waits for the short-lived guest to finish. The historical Criterion interval was 22.971–23.139µs with a 23.055µs mean.

Useful inference: precompilation and minimal guest work can make this narrow path inexpensive relative to earlier estimates. Unknowns include current dependency behavior, representative initialization, process lifetime, concurrent spawning, and hardware variation.

### Local mailbox operations

`benches/mailbox.rs` creates and fills one mailbox inside every iteration and removes one item. The historical results ranged from 353.23ns for the ten-item FIFO fixture to about 25.4µs for a 1,000-item selective fixture.

The measurements characterize local queue construction/push/search/pop cost for those fixtures. They exclude guest host-call overhead, serialization, scheduling, sender/receiver handoff, contention, bounded-queue behavior, and transport. They therefore cannot establish end-to-end message latency or production workload optimality.

### Live hot reload

Production live-reload latency was not measured by the October 2025 suite. The benchmark named `hot_reload_FULL_CYCLE` creates fresh v1/v2 instances itself and manually transfers linear-memory bytes. It does not interrupt a running guest, deliver `Signal::HotReload`, observe process success/failure acknowledgement, commit a coordinated version, or roll back.

The historical 758.94µs value remains useful as a regression point for that manual component composition only. It cannot be reported as live hot-reload latency or as proof that the `<100ms` target is met.

### Memory

`benches/memory_profile.rs` combines Rust `size_of` values, an exported one-page Wasm memory snapshot, and arithmetic estimates. `size_of` does not include reachable heap allocations, allocator overhead, Wasmtime engine/store allocations, thread stacks, shared pages, or operating-system RSS behavior.

The 64KiB observation is the example module's exported linear-memory size. The derived ~66KiB-per-process value is a rough lower-bound model, not an actual resident-memory measurement.

### Scalability

Stores belonging to the first-created Wasmtime engine share one epoch ticker instead of one ticker per process. That removes a per-process ticker-task growth factor within that engine, but it does not demonstrate one million processes and does not advance later independently constructed engines.

The memory-profile concurrency fixture constructs at most 100 state values and multiplies a Rust type size by the count. Values shown for 1,000 through one million processes were arithmetic projections. A scalability claim still requires live processes, representative mailboxes/resources, measured RSS/CPU, useful work, and a sustained soak window.

## Historical Architecture Changes

### Global epoch ticker

The first-created Wasmtime engine's stores share one epoch ticker instead of a ticker per process (`crates/lunatic-process/src/runtimes/wasmtime.rs`). The start guard is process-global even though later runtimes construct new engines, so those later engines do not receive the ticker's epoch increments. Neither behavior has been measured at million-process scale.

### Mailbox implementation investigation

Earlier experiments compared selective-receive data structures. The current queue remains a simple implementation with linear search characteristics. The historical microbenchmarks do not justify a universal “optimal for real-world workloads” conclusion; future decisions should use representative queue depths, tag distributions, concurrency, and bounded backpressure.

### Resource limits

Memory/table limiting and several networking quotas are implemented. Earlier documents quoted 10–50ns or `<1%` enforcement overhead without a corresponding result in this suite; those values are unverified and are withdrawn. Performance and completeness need dedicated production-path benchmarks, especially across process/message/signal limits and handle accounting.

## Current Benchmark Gaps

The following evidence is required before the corresponding core-value target can be marked complete:

1. Live Wasm `Signal::HotReload` success, guest interruption, state verification, failure acknowledgement, and rollback timings.
2. Local sender-to-live-receiver guest process round trips, including bounded-mailbox pressure and selective receive.
3. Remote guest process round trips through registry lookup, routing, QUIC transport, and destination mailbox.
4. Actual per-process RSS/reachable-heap measurements across idle and loaded workloads.
5. Sustained process-count scale and soak tests with CPU, memory, queue depth, tail latency, failures, and recovery recorded.
6. Contended resource-limit and capability-check overhead measurements.

## Benchmark Commands

```bash
cargo bench --bench spawn
cargo bench --bench mailbox
cargo bench --bench messaging
cargo bench --bench hot_reload
cargo bench --bench memory_profile
cargo bench --bench distributed_messaging
cargo bench --bench distributed_latency
cargo bench --bench instance_pool
```

See [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) for the preserved historical numbers and [`BENCHMARK_SUITE.md`](BENCHMARK_SUITE.md) for suite boundaries. There is deliberately no aggregate performance/compliance score: dissimilar microbenchmarks cannot establish production readiness.
