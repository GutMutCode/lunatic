# Lunatic Benchmark Suite

Evidence review: 2026-07-21

Canonical implementation status: [`docs/core_values/status.md`](../core_values/status.md)

## Purpose and Evidence Classes

The repository contains targeted micro-, component-, and transport-boundary benchmarks. A benchmark name such as “round trip” or “full cycle” is an internal Criterion label and must not be interpreted as production E2E evidence without inspecting the harness.

Use these evidence classes when reporting results:

- **Microbenchmark** — an in-memory operation or narrow function path.
- **Component harness** — multiple components composed directly by the benchmark.
- **Probe/projection** — type-size, snapshot-size, or arithmetic evidence that is not a resident-memory or scale measurement.
- **Transport-boundary benchmark** — real transport through decode/dispatch, stopping before a live destination process.
- **Production E2E benchmark** — public/runtime entry point through a live outcome, including acknowledgement and failure behavior.

The current suite contains microbenchmark, component-harness, probe/projection, transport-boundary, and production E2E evidence. The production E2E coverage is currently limited to distributed registry lookup and live native-process mailbox delivery; live hot reload and guest-Wasm messaging still lack production E2E benchmarks.

Historical numeric results in the companion documents are from 2025-10-06. They have no recorded commit or exact hardware/toolchain profile and therefore are not a reproducible current baseline.

## Suite Inventory

| Suite | Evidence class | What it exercises | Explicit boundary |
| --- | --- | --- | --- |
| `spawn.rs` | Component harness | Precompiled minimal Wasm process state, spawn, and join | One tiny short-lived guest; no concurrent/load scenario |
| `mailbox.rs` | Microbenchmark | New local mailbox, N direct pushes, one FIFO/selective pop | No sender/receiver processes, host calls, or backpressure |
| `messaging.rs` | Microbenchmark | Direct push/pop on one local mailbox | Its `round_trip` label is not a process round trip |
| `hot_reload.rs` | Component harness | Direct compile, registry, instantiate, memory snapshot/restore | No running guest, `Signal::HotReload`, acknowledgement, commit, or rollback |
| `memory_profile.rs` | Probe/projection | Rust type sizes, example memory snapshot, up to 100 state constructions | No RSS/reachable-heap measurement or large-scale live process run |
| `instance_pool.rs` | Component harness | Pool acquire/release and hit/miss behavior | Does not establish whole-process spawn behavior under production load |
| `distributed_messaging.rs` | Micro + transport boundary | Encode/decode and real loopback mTLS QUIC framing/reassembly/dispatch | Stops at decoded callback; no registry-to-live-mailbox delivery |
| `distributed_latency.rs` | Component + production E2E | Control-plane node lookup; global registry lookup followed by a confirmed two-node mTLS QUIC request/reply through live Lunatic native-process mailboxes | Cluster creation, quorum registration, and connection warm-up are outside measurement; no guest-Wasm host-call boundary |

`distributed_registry_live_mailbox_round_trip` is the production-path measurement. Its persistent two-node harness resolves the live echo process from the global registry on node 1, sends through the production distributed client and full node server to node 2, receives the request in a real Lunatic mailbox, sends a confirmed reply, and observes that reply in a live node-1 mailbox. By contrast, `distributed_quic_message_dispatch_2kb` in `distributed_messaging.rs` deliberately stops at the decoded callback boundary and is transport-boundary evidence only.

## Quick Start

Run all Criterion suites:

```bash
cargo bench
```

Run one suite:

```bash
cargo bench --bench spawn
cargo bench --bench mailbox
cargo bench --bench messaging
cargo bench --bench hot_reload
cargo bench --bench memory_profile
cargo bench --bench instance_pool
cargo bench --bench distributed_messaging
cargo bench --bench distributed_latency
```

Criterion HTML reports are written below `target/criterion/`.

## Historical Observation Summary

The following values are preserved only to explain the October 2025 reports. They are not current-HEAD evidence.

| Area | Historical observation | Correct interpretation |
| --- | ---: | --- |
| Spawn | 23.055µs | Minimal precompiled `hello.wat` spawn-and-join harness; recorded run missed the `<10µs` goal |
| Mailbox FIFO | 353.23ns | New mailbox + ten local pushes + one pop; not end-to-end message latency |
| Selective mailbox | 390ns–25.4µs | Local fixture across 10–1,000 queued items |
| Reload composition | 758.94µs | Manual v1/v2 compile/instantiate/snapshot/restore; not live reload |
| Memory/process | ~66KiB | Lower-bound estimate, not RSS |
| One million processes | ~66GiB | Arithmetic projection; never executed as a scale/soak test |

See [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) for the retained details.

## CI Coverage

On Linux, `.github/workflows/ci.yml` is configured to invoke all eight suites listed above and retain their combined textual output for 30 days.

The workflow also runs `scripts/check_bench_thresholds.py`, which separately executes and enforces ceilings for these four suites only:

- `spawn`;
- `messaging`;
- `distributed_messaging`;
- `distributed_latency`.

The script checks configured absolute upper bounds for specific Criterion labels. It does not compare a pull request against a stored historical baseline. The workflow explicitly reports that baseline regression comparison remains manual.

The `spawn process` guard is calibrated to the Wasmtime 46 hosted-Linux baseline recorded on 2026-07-21 at commit `100767b` ([Actions run 29803866397](https://github.com/GutMutCode/lunatic/actions/runs/29803866397)): a 53.827µs Criterion upper bound, with a warning above 60µs and a hard failure above 75µs. The margin provides headroom for shared-runner variance while still detecting a material regression. These CI limits do not replace the separate aspirational `<10µs` product target.

Passing a threshold protects only that named harness and workload. It does not promote a micro/component/transport benchmark into production E2E evidence and does not mark a `CORE_VALUES.md` target complete. The live-mailbox threshold likewise covers only its fixed two-node native-process fixture, not the guest-Wasm boundary or broader production readiness.

## Adding or Changing a Benchmark

1. State the public/runtime entry point and the end boundary.
2. Classify the evidence as micro, component, transport-boundary, or production E2E.
3. Name all excluded work, including setup performed outside measurement.
4. Use a representative fixture and record queue sizes, payloads, concurrency, and failure mode.
5. Record commit, dirty state, OS, CPU, memory, toolchain, and dependency lockfile.
6. Use Criterion warmup/sample controls and retain raw artifacts.
7. Add CI execution and, where appropriate, a justified threshold.
8. Do not translate one fixture into a product-wide latency, scale, or readiness claim.

Minimal Criterion example:

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn my_benchmark(c: &mut Criterion) {
    c.bench_function("component_operation", |b| {
        b.iter(|| black_box(component_operation()));
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
```

Register a new file in `Cargo.toml`:

```toml
[[bench]]
harness = false
name = "my_bench"
```

## Interpreting Criterion Output

```text
spawn process           time:   [22.971 µs 23.055 µs 23.139 µs]
```

- The bracketed values are Criterion's estimated interval for that harness on that run.
- Outliers and changes must be evaluated against the recorded environment and raw report.
- Generic labels such as “excellent” or “slow” are not evidence; compare against a workload-specific service objective.
- A historical result without commit and hardware metadata is a note, not a regression baseline.

## Required Production Evidence

The suite still needs:

1. A live running-Wasm `Signal::HotReload` benchmark that asserts interruption, state transition, acknowledgement, commit, failure, and rollback.
2. A local guest sender-to-live-guest receiver benchmark with serialization, scheduling, bounded-mailbox pressure, and tail latency.
3. A distributed guest-Wasm round trip through registry lookup, host calls, routing, mTLS QUIC, destination mailbox, and reply. The native-process production-path benchmark does not include the guest host-call boundary.
4. Actual process RSS/reachable-heap measurement under idle and loaded states.
5. Increasing-count scale tests and sustained soak tests with resource usage, queue depth, latency percentiles, failures, and recovery.
6. Supervisor/link failure and resource-limit overhead benchmarks on their production paths.

Until those exist, the live reload, end-to-end messaging, one-million-process, and production-readiness targets remain unverified regardless of component benchmark speed.

## References

- [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) — historical numbers with their boundaries.
- [`PERFORMANCE_ANALYSIS.md`](PERFORMANCE_ANALYSIS.md) — scoped analysis and evidence gaps.
- [`../../CORE_VALUES.md`](../../CORE_VALUES.md) — goals, not a completion report.
- [`../core_values/status.md`](../core_values/status.md) — canonical current implementation status.
