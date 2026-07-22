# CI Benchmark Integration

Reviewed against the repository on 2026-07-23.

Lunatic keeps Criterion benchmarks in the repository-root [`benches/`](../benches/)
directory. [The main CI workflow](../.github/workflows/ci.yml) retains the
current commit's Linux results, applies absolute limits to workloads with a
stable service boundary, and evaluates process spawn with a same-runner paired
base/head comparison.

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
limit. It covers `messaging`, `distributed_messaging`, and
`distributed_latency`.

The separate `spawn_regression` job checks out the base and head commits into
detached worktrees on one Ubuntu 24.04 runner and builds both with Rust 1.95.0.
It alternates `base/head` and `head/base` execution order, starts with 10 paired
measurements, and extends to at most 15 pairs when the interval remains noisy.
Each invocation uses 50 Criterion samples, a 1-second warmup, and a 3-second
measurement window. The comparison uses Criterion's slope point estimate and
paired log ratios, not two unrelated confidence-interval endpoints.

The paired spawn policy is:

- report a warning when the median paired increase is greater than 5%;
- fail when the 95% bootstrap interval is wholly above a 10% increase;
- fail when the 95% bootstrap lower bound for head latency is above 75µs;
- report an inconclusive failure when the relative interval is still wider
  than 10 percentage points after 15 pairs.

Once the comparison script starts, it creates `report.json`, `summary.md`, raw
command output, and environment metadata. The following `if: always()` artifact
step uploads any available evidence as `spawn-comparison-<commit SHA>`; a failure
before the comparison starts can legitimately leave no artifact. The 75µs
backstop is an emergency bound for this minimal harness; paired change is the
normal regression signal.

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

Run the paired spawn comparison used by CI:

```bash
python scripts/compare_spawn_bench.py <base> <head> \
  --output-dir spawn-comparison
```

Use an otherwise idle machine. The script isolates the commits' Cargo targets
and Criterion homes, records the resolved revisions and environment, and
removes its temporary worktrees when finished.

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
5. Verify both `cargo bench --bench <name>` and the applicable absolute or
   paired checker.
