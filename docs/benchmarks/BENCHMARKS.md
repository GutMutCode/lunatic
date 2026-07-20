# Benchmark Documentation Index

Evidence review: 2026-07-21

This file previously described benchmark files and results that do not exist in the repository, including `process_creation.rs`, `message_passing.rs`, and `wasm_execution.rs`. It also presented untraceable throughput, memory, scale, runtime-comparison, and production-readiness claims. Those claims are withdrawn.

Use the maintained documents instead:

- [`BENCHMARK_SUITE.md`](BENCHMARK_SUITE.md) — current suite inventory, evidence classes, CI coverage, and commands.
- [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) — preserved October 2025 observations with exact harness boundaries.
- [`PERFORMANCE_ANALYSIS.md`](PERFORMANCE_ANALYSIS.md) — scope-limited interpretation and missing production evidence.
- [`BENCHMARK_COMPLETION_SUMMARY.md`](BENCHMARK_COMPLETION_SUMMARY.md) — historical sprint delivery record, not production certification.
- [`../core_values/status.md`](../core_values/status.md) — canonical current implementation status.

The existing executable suites are `spawn`, `mailbox`, `messaging`, `hot_reload`, `memory_profile`, `instance_pool`, `distributed_messaging`, and `distributed_latency`. Inspect each source file before interpreting its Criterion label: local mailbox and manual reload-component harnesses are not end-to-end process messaging or live hot reload.
