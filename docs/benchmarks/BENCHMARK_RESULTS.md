# Lunatic Benchmark Results — Historical Snapshot

Recorded: 2025-10-06

Evidence review: 2026-07-21

Recorded commit: unknown

Hardware: local development machine; CPU and memory not recorded

Criterion: 0.4

> **Evidence scope**
>
> These numbers are an October 2025 snapshot. Because the original record omitted a commit identifier and reproducible hardware profile, they are not current-HEAD performance evidence.
>
> `benches/mailbox.rs` creates an in-memory mailbox, performs N local `push` calls, and performs one `pop`; it does not measure sender-to-receiver process delivery. `benches/hot_reload.rs` directly compiles and instantiates v1/v2 modules and invokes linear-memory snapshot/restore; it does not execute the live `Signal::HotReload` path, interrupt a running guest, receive a process acknowledgement, commit a coordinated version, or exercise rollback. Memory totals and the one-million-process figures are estimates or arithmetic extrapolations, not RSS/heap or scale-test measurements.
>
> These results do not establish current `CORE_VALUES.md` compliance or production readiness. See the [current implementation status](../core_values/status.md).

## Historical Observations

| Claim area | Historical observation | Evidence boundary | Current target status |
| --- | ---: | --- | --- |
| Process spawn | 23.055µs | Oct 2025 precompiled minimal `hello.wat` spawn-and-join harness | Not current HEAD; the recorded run missed the 10µs target |
| Process messaging | 353.23ns | Local mailbox creation, 10 pushes, and one FIFO pop | End-to-end delivery was not measured |
| Selective mailbox receive | 390ns–25.4µs | Local mailbox setup and one tagged pop over 10–1,000 queued messages | Process delivery and sustained contention were not measured |
| Live hot reload | 758.94µs | Manual compile/instantiate/registry/snapshot/restore sequence | Live reload signal path was not measured |
| Memory/process | ~66KiB | Lower-bound estimate using one 64KiB linear-memory page and Rust `size_of` values | Process RSS/reachable heap was not measured |
| One million processes | ~66GiB | Arithmetic extrapolation from the estimate above | No million-process scale or soak run exists |

## Process Spawn Harness

Benchmark: `benches/spawn.rs`, Criterion label `spawn process`.

The harness uses a precompiled minimal `wat/hello.wat` module, constructs `DefaultProcessState`, calls `spawn_wasm`, waits for the guest to finish, and records one iteration.

```text
spawn process           time:   [22.971 µs 23.055 µs 23.139 µs]
                        Found 4 outliers among 100 measurements (4.00%)
```

The historical mean was 23.055µs. It was 2.3 times the stated `<10µs` goal, so the run did not meet that goal. The minimal precompiled/short-lived workload must not be generalized to arbitrary process startup.

## Local Mailbox Queue Harness

Benchmark: `benches/mailbox.rs`.

Each iteration creates a mailbox, pushes every test message locally, and pops one message. The time therefore includes queue setup/work for that specific workload, but excludes serialization, process scheduling, sender/receiver handoff, guest host calls, backpressure, and distributed transport.

### FIFO observation

| Queued messages | Historical iteration mean |
| ---: | ---: |
| 10 | 353.23ns |
| 100 | 1.85µs |
| 1,000 | 24.1µs |

### Selective-receive observation

| Queued messages | Tags | Historical iteration mean |
| ---: | ---: | ---: |
| 10 | 1 | 390.08ns |
| 10 | 5 | 401.18ns |
| 10 | 10 | 372.72ns |
| 100 | 1 | 1.8461µs |
| 100 | 5 | 1.9720µs |
| 100 | 10 | 1.9563µs |
| 1,000 | 1 | 24.102µs |
| 1,000 | 5 | 25.211µs |
| 1,000 | 10 | 25.413µs |

These observations show workload-dependent local queue cost and roughly linear scanning in the tested range. They do not establish a `<1µs` end-to-end message guarantee or prove optimality for production workloads.

## Manual Reload-Component Harness

Benchmark: `benches/hot_reload.rs`, Criterion label `hot_reload_FULL_CYCLE`.

```text
hot_reload_FULL_CYCLE   time:   [754.40 µs 758.94 µs 763.33 µs]
```

The iteration directly performs these operations in the benchmark:

1. Compile v1 and add it to a fresh `ModuleRegistry`.
2. Instantiate v1 and snapshot its exported linear memory.
3. Compile v2 and add it to the registry.
4. Instantiate v2 and restore the memory bytes.

The 758.94µs historical mean is a component-harness result, not end-to-end live hot reload. Separate Criterion functions also recorded compilation, registry, snapshot, and restore observations. Those independently measured values are not an additive percentage breakdown of the full-cycle iteration.

This harness does not call the live process reload handler and therefore cannot be compared directly with the `<100ms` live-reload goal.

## Memory Probes and Projections

Benchmark: `benches/memory_profile.rs`.

Historical component observations included:

| Component/probe | Historical value | Limitation |
| --- | ---: | --- |
| `size_of`-based process state | ~400 bytes | Rust value size, not reachable allocations or RSS |
| Empty mailbox wrapper | ~8 bytes | Wrapper size only |
| Example `Message` enum | ~40 bytes | Excludes payload allocations |
| Exported Wasm memory snapshot | 65,536 bytes | One-page example, not complete instance/runtime memory |

The harness constructs at most 100 process-state values and calculates `size_of::<DefaultProcessState>() * count`. The earlier ~66KiB-per-process and ~66GiB-for-one-million figures are rough lower-bound projections. They do not demonstrate actual resident memory, instance overhead, one million live processes, scheduling behavior, or sustained stability.

## Reproducing Current Results

Run individual suites from the repository root:

```bash
cargo bench --bench spawn
cargo bench --bench mailbox
cargo bench --bench hot_reload
cargo bench --bench memory_profile
```

A result intended as current evidence must record at least:

- exact Git commit and dirty-tree state;
- OS, CPU, memory, Rust, and dependency versions;
- benchmark parameters and workload fixture;
- whether the path is a microbenchmark, component harness, transport boundary, or production E2E path;
- raw Criterion output or artifact location.

Use [`BENCHMARK_SUITE.md`](BENCHMARK_SUITE.md) for suite instructions and [`../../CORE_VALUES.md`](../../CORE_VALUES.md) for target definitions. Target completion remains governed by [`../core_values/status.md`](../core_values/status.md).
