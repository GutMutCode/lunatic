# Lunatic Benchmark Suite

Evidence review: 2026-07-23

Canonical implementation status: [`docs/core_values/status.md`](../core_values/status.md)

## Purpose and Evidence Classes

The repository contains targeted micro-, component-, and transport-boundary benchmarks. A benchmark name such as “round trip” or “full cycle” is an internal Criterion label and must not be interpreted as production E2E evidence without inspecting the harness.

Use these evidence classes when reporting results:

- **Microbenchmark** — an in-memory operation or narrow function path.
- **Component harness** — multiple components composed directly by the benchmark.
- **Probe/projection** — type-size, snapshot-size, or arithmetic evidence that is not a resident-memory or scale measurement.
- **Transport-boundary benchmark** — real transport through decode/dispatch, stopping before a live destination process.
- **Production E2E benchmark** — public/runtime entry point through a live outcome, including acknowledgement and failure behavior.

The current suite contains microbenchmark, component-harness, probe/projection, transport-boundary, production E2E, and bounded production acceptance evidence. Production-path coverage now includes distributed registry lookup and live native-process mailbox delivery, sixteen sequential actual guest-Wasm request/reply samples over loopback mTLS, a local actual-Wasm scale/pressure/short-soak gate with measured throughput/rate and p50/p95/p99, and acknowledged local live reload/rollback under full mailboxes. It still does not establish multi-host or partitioned guest latency, large-cluster performance, or multi-day/longitudinal stability.

Historical numeric results in the companion documents are from 2025-10-06. They have no recorded commit or exact hardware/toolchain profile and therefore are not a reproducible current baseline.

## Suite Inventory

| Suite | Evidence class | What it exercises | Explicit boundary |
| --- | --- | --- | --- |
| `spawn.rs` | Component harness | Precompiled minimal Wasm process state, spawn, and join | One tiny short-lived guest; no concurrent/load scenario |
| `mailbox.rs` | Microbenchmark | New local mailbox, N direct pushes, one FIFO/selective pop | No sender/receiver processes, host calls, or backpressure |
| `messaging.rs` | Microbenchmark | Direct push/pop on one local mailbox | Its `round_trip` label is not a process round trip |
| `hot_reload.rs` | Component harness | Direct compile, registry, instantiate, memory snapshot/restore | No running guest, `Signal::HotReload`, acknowledgement, commit, or rollback |
| `wasm_scale_soak::actual_wasm_scale_soak_and_resource_pressure_are_bounded` | Bounded production acceptance gate | Actual live Wasm populations 1/8/32, one discarded warm-up plus five measured batches per population, 16 echo rounds per measured batch, tagged request/reply, spawn-batch and mailbox throughput/rate plus spawn/readiness/echo p50/p95/p99, one pre-guest RSS baseline followed by cumulative live populations 1/8/32, 64KiB committed Wasm bytes per guest, exact process admission, mailbox saturation/reuse, sibling progress, a two-page memory ceiling, and 80 spawn/load/kill/join lifecycles | Same-process local fixture; Linux RSS is process-wide and informational, and each RSS delta divided by the live guest count is explicitly non-allocator-attributable; 32 live guests and 80 lifecycles are not a large or multi-day production soak |
| `wasm_scale_soak::extended_actual_wasm_soak_is_bounded` via `production-soak.yml` | Scheduled and release-tag production endurance gate | Two-hour population-32 spawn/readiness/message/kill/join cycling, exact registration cleanup, and a 512MiB process-wide RSS-growth guard with retained environment/output; the reusable workflow is also a required dependency of version-tag publication. Verified two-hour soak: [GitHub Actions run 29984715567](https://github.com/GutMutCode/lunatic/actions/runs/29984715567) at commit `30b0e1b11bada8bdefca848dee77b5b6aa180cc6`. The retained result completed 7,200 seconds, 164,745,248 process lifecycles, 658,980,992 mailbox echo samples, zero registrations after shutdown, and 5,881,856 bytes of RSS growth under the guard | RSS remains process-wide, and the workload does not inject partitions or Supervisor crashes |
| `live_hot_reload_scale::production_hot_reload_scale_soak_preserves_full_mailboxes_and_rolls_back` | Production E2E adversarial gate | Sixteen running Wasm processes, ten alternating acknowledged commits/full-target rollbacks, 1,280 FIFO messages, 160 exact mailbox-full denials, stable identities, a workload summary, and per-run p50/p95/p99 | Local compatible-memory fixture; hard ceilings are CI runaway guards, not a portable `<100ms` product SLA or distributed reload proof |
| `memory_profile.rs` | Probe/projection | Rust type sizes, example memory snapshot, up to 100 state constructions | No RSS/reachable-heap measurement or large-scale live process run |
| `instance_pool.rs` | Component harness | Pool acquire/release and hit/miss behavior | Does not establish whole-process spawn behavior under production load |
| `distributed_messaging.rs` | Micro + transport boundary | Encode/decode and real loopback mTLS QUIC framing/reassembly/dispatch | Stops at decoded callback; no registry-to-live-mailbox delivery |
| `distributed_latency.rs` | Component + production E2E | Control-plane node lookup; global registry lookup followed by a confirmed two-node mTLS QUIC request/reply through live Lunatic native-process mailboxes | Cluster creation, quorum registration, and connection warm-up are outside measurement; no guest-Wasm host-call boundary |
| `distributed_registry_e2e::guest_registry_resolves_a_live_remote_mailbox_and_cleans_up_owner_exit` | Production guest-Wasm E2E gate | Sixteen sequential guest registry lookups and confirmed loopback-mTLS request/replies through two live guest-Wasm mailboxes, with average/p50/p95/p99/rate evidence and healthy owner cleanup | Fixed two-node localhost fixture; no multi-host network, partitioned traffic, or portable latency objective |
| `congestion::tests::adversarial_slow_destination_fairness_benchmark` | Adversarial transport gate | Holds one 128 KiB logical lane above the 64 KiB QUIC stream window while sampling an independent lane 16 times | Loopback mTLS and scheduler-to-peer receipt only; no destination mailbox or production-network latency claim |
| `registry_coordination::recovered_node_resynchronizes_a_commit_missed_during_partition` | Production-transport recovery gate | Three independent three-node loopback-mTLS partitions and registry resynchronizations with recovery timing | Fresh cluster per cycle; no partitioned guest mailbox or owner-exit recovery |
| `wasm_link_death::actual_wasm_trap_drives_supervisor_restart_policy` | Production local recovery gate | Three immediate actual-Wasm traps drive three Supervisor replacements before a stable fourth start, followed by zero-registration teardown | Host-side local Supervisor only; no distributed supervision or recovery-time objective |
| `distributed_registry_e2e::replayed_message_executes_server_side_effect_once_across_reconnect` | Production replay E2E | Reconnects an authenticated peer with the same transport ID and proves the real destination mailbox observes the side effect once | Deterministic correctness gate, not a throughput measurement |

`distributed_registry_live_mailbox_round_trip` is the Criterion production-path measurement for native processes. Its persistent two-node harness resolves the live echo process from the global registry on node 1, sends through the production distributed client and full node server to node 2, receives the request in a real Lunatic mailbox, sends a confirmed reply, and observes that reply in a live node-1 mailbox. The guest-Wasm E2E gate separately includes both guest host-call boundaries and reports a fixed sixteen-sample distribution. By contrast, `distributed_quic_message_dispatch_2kb` in `distributed_messaging.rs` deliberately stops at the decoded callback boundary and is transport-boundary evidence only.

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

Run the bounded production acceptance gates:

```bash
cargo test --release --test wasm_scale_soak \
  actual_wasm_scale_soak_and_resource_pressure_are_bounded \
  -- --exact --nocapture

cargo test --release --test live_hot_reload_scale \
  production_hot_reload_scale_soak_preserves_full_mailboxes_and_rolls_back \
  -- --ignored --exact --nocapture
```

The first gate emits `LUNATIC_SCALE_EVIDENCE` JSON lines. On Linux those lines include one process-wide RSS baseline before the incremental 1/8/32 live population and the cumulative after/delta values for each point; other platforms emit `null` rather than substituting a projection. The reload gate prints the actual fixed-round commit and rollback distributions. Both use generous deadlock/runaway watchdogs and exact semantic assertions.

Criterion HTML reports are written below `target/criterion/`.

Compare process spawn between two commits on the same idle machine:

```bash
python scripts/compare_spawn_bench.py <base> <head> \
  --output-dir spawn-comparison
```

This is the regression contract for `spawn process`; a single current-commit
run remains useful descriptive evidence but is not the CI regression decision.

Run the deterministic adversarial backpressure gate separately:

```bash
cargo test -p lunatic-distributed congestion::tests::adversarial_slow_destination_fairness_benchmark -- --nocapture
```

The gate first proves that the stalled lane retains its only admission slot, then requires all 16 independent-lane samples—and therefore the nearest-rank p99—to reach the peer within 100 ms. It also verifies that the stalled message keeps its outbound byte lease until cancellation and that every lease is released afterward. The threshold is a same-host regression guard, not a general production latency objective.

Run the production side-effect replay gate separately:

```bash
cargo test --test distributed_registry_e2e replayed_message_executes_server_side_effect_once_across_reconnect -- --nocapture
```

The outbound scheduler reserves lane 0 and a separate 64-message/1 MiB budget for responses and registry control traffic. Data routes use the remaining lanes, with exact retained-allocation accounting at each lane and node plus the shared 1,024-message/32 MiB outbound ceiling. A transport-complete data message remains owned until its authenticated application response arrives; a response from an earlier attempt cancels an unfinished replay.

Replay protection is deliberately finite and fail-closed. Application retries have a 45-second absolute window once the first attempt can reach the peer. Receivers retain at most 16,384 terminal fingerprints and bounded responses for 180 seconds, which is longer than the retry window plus the 60-second message-reassembly deadline and three 10-second stream-idle margins. At the hard entry ceiling, new side effects are rejected with delivery backpressure instead of evicting a live tombstone. Consequently, a sustained rate above roughly 91 new replay-protected requests per second per node can reach that ceiling before expiry; this is a correctness and memory bound, not a throughput target. A fenced acknowledgement protocol or configurable larger cache is required before claiming a higher sustained replay-protected rate.

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

The separate pinned-Rust `production_evidence` job runs scale, live-reload, timed guest-Wasm distribution, three-cycle partition recovery, slow-consumer fairness, and three-crash Supervisor recovery gates in release mode. It records commit/toolchain/kernel/CPU/memory metadata and retains the raw scale/reload/resilience output for 30 days. Release publication depends on this job and, for version tags, on the reusable two-hour `production-soak.yml` workflow. `scripts/check_core_value_docs.py` parses those records and keeps the corresponding product targets unchecked unless their evidence boundary actually qualifies; it also rejects stale README/CORE_VALUES/status claims. The soak's accepted-run state is recorded in the inventory row above and the canonical status.

The workflow also runs `scripts/check_bench_thresholds.py`, which separately executes and enforces ceilings for these three suites only:

- `messaging`;
- `distributed_messaging`;
- `distributed_latency`.

The script checks configured absolute upper bounds for specific Criterion labels.

`spawn process` instead has a dedicated Ubuntu 24.04/Rust 1.95.0 job. Base
and head are built in isolated worktrees and measured on the same runner in
alternating order. Ten paired Criterion slope estimates are collected first;
the tool may extend to 15 pairs when the interval is noisy. A median increase
above 5% warns, a 95% paired-bootstrap interval wholly above +10% fails, and a
95% lower bound above 75µs for head fails as an emergency absolute backstop.
An interval still wider than 10 percentage points at 15 pairs is inconclusive
and fails instead of silently passing uncertain evidence. Raw logs, JSON,
metadata, and a Markdown summary are retained for 30 days.

The former 60µs warning / 75µs single-run gate was calibrated from Wasmtime
46 hosted-Linux observations of 49–54µs. It was replaced because one shared-runner
upper endpoint cannot distinguish code change from runner variance. The 75µs
value remains only as the paired tool's emergency backstop. None of these CI
limits replaces the aspirational `<10µs` product target. See
[`SPAWN_BASELINE_ANALYSIS.md`](SPAWN_BASELINE_ANALYSIS.md) for the decision and
reproduction record.

Passing a threshold protects only that named harness and workload. It does not promote a micro/component/transport benchmark into production E2E evidence and does not mark a `CORE_VALUES.md` target complete. The Criterion live-mailbox threshold covers only its fixed two-node native-process fixture; the separate guest-Wasm evidence covers its fixed sixteen-sample localhost workload, not broader production readiness.

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

The bounded gates close the former absence of any live reload or live-Wasm scale measurement for their exact fixtures. The suite still needs:

1. A calibrated paired-base/head policy for the live-reload distribution and a broader guest entrypoint-reentry workload before treating `<100ms` as portable product evidence.
2. A sustained guest sender-to-live-guest workload combining guest-side serialization, bounded-mailbox pressure, scheduler contention, and tail latency. The new distributed guest fixture measures sixteen sequential request/replies, while the scale gate uses a host sender and native observer.
3. Multi-host and partitioned guest-Wasm latency/recovery through registry lookup, host calls, routing, mTLS QUIC, destination mailbox, and reply. The current guest measurement is a fixed healthy loopback fixture.
4. Allocator/reachable-heap attribution per live process under idle and loaded states. The Linux `[1, 8, 32]` curve uses one pre-guest process-wide RSS baseline and cumulative live populations, while each delta divided by live guest count remains only a non-allocator-attributable proxy for committed bytes.
5. Fresh retained runs of the two-hour population-32 soak, followed by larger increasing-count and sustained chaos runs with longitudinal CPU, queue depth, failure, and recovery telemetry. Ten rounds or 80 lifecycles remain bounded regression gates and do not establish multi-day stability.
6. Supervisor/link failure and resource-limit overhead benchmarks on their production paths.

Until those exist, the live reload, end-to-end messaging, one-million-process, and production-readiness targets remain unverified regardless of component benchmark speed.

## References

- [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) — historical numbers with their boundaries.
- [`PERFORMANCE_ANALYSIS.md`](PERFORMANCE_ANALYSIS.md) — scoped analysis and evidence gaps.
- [`SPAWN_BASELINE_ANALYSIS.md`](SPAWN_BASELINE_ANALYSIS.md) — reproducible spawn comparison and accepted gate.
- [`../../CORE_VALUES.md`](../../CORE_VALUES.md) — goals, not a completion report.
- [`../core_values/status.md`](../core_values/status.md) — canonical current implementation status.
