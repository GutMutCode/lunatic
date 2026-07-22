# CI Benchmark Integration

Reviewed against the repository on 2026-07-22.

Lunatic keeps Criterion benchmarks in the repository-root [`benches/`](../benches/)
directory. The Linux leg of [the main CI workflow](../.github/workflows/ci.yml)
runs them, uploads the textual output for the commit under test, and then runs
the executable threshold checker.

## Current benchmark targets

| Target | Measured boundary |
| --- | --- |
| `spawn` | Local runtime process-spawn path |
| `mailbox` | Local mailbox queue operations |
| `hot_reload` | In-process hot-reload component paths |
| `memory_profile` | Local runtime memory workloads |
| `instance_pool` | Wasm instance-pool operations |
| `messaging` | Local signal-ingress and mailbox operations |
| `distributed_messaging` | Distributed request encoding/decoding and QUIC dispatch fixtures |
| `distributed_latency` | Control lookup and confirmed QUIC-to-live-mailbox round trip |

These targets do not all measure the same boundary. In particular, `mailbox`
and `messaging` are local microbenchmarks and must not be described as
end-to-end process-delivery latency. The distributed targets use local test
nodes and are not substitutes for a multi-host production benchmark.

## CI behavior

On Linux, `test_or_release` performs three distinct actions:

1. runs every benchmark target listed above and writes `benchmark_output.txt`;
2. uploads that file as `benchmark-results-<commit SHA>`; and
3. executes [`scripts/check_bench_thresholds.py`](../scripts/check_bench_thresholds.py).

The threshold checker reruns its configured Criterion workloads, parses their
confidence intervals, and fails when an upper bound exceeds the checked-in
limit. It currently covers `spawn`, `messaging`, `distributed_messaging`, and
`distributed_latency`. The separate pull-request summary still describes
manual baseline comparison; CI does not yet maintain a historical trend store
or automatically compare a pull request with its base commit.

## Local commands

Run one target directly:

```bash
cargo bench --bench spawn
cargo bench --bench distributed_latency
```

Run the same hard-threshold gate used by CI:

```bash
./scripts/check_bench_thresholds.py
```

Criterion writes detailed local output below `target/criterion/`. Generated
results are not source-controlled and a number copied from one run is not a
portable performance guarantee.

## Claim requirements

A performance report should identify:

- the commit, benchmark target, and exact workload;
- whether the path is a local microbenchmark or production-path E2E;
- toolchain, operating system, CPU, and runner class;
- sample count and the reported distribution or confidence interval; and
- the comparison baseline and permitted variance.

Do not infer cross-process, cross-node, or application latency from a queue
operation. Do not publish a nanosecond or microsecond value as a current
contract unless the referenced executable benchmark measures that exact path
on the stated commit.

## Adding a benchmark

1. Add `benches/<name>.rs` and a matching `[[bench]]` entry in `Cargo.toml`.
2. Add the target to the Linux benchmark step in `.github/workflows/ci.yml`.
3. Add it to `scripts/check_bench_thresholds.py` only when a justified,
   runner-tolerant hard limit exists.
4. Document the measured boundary and what the result does not establish.
5. Verify both `cargo bench --bench <name>` and the threshold checker.
