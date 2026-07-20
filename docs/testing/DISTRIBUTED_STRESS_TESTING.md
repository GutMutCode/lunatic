# Distributed Stress Testing Status

**Status**: Not implemented
**Last reviewed**: 2026-07-21

The repository currently has distributed correctness E2E tests and Criterion
microbenchmarks. It does not have a sustained multi-node scheduler stress suite,
cluster-wide quota test, soak test, or chaos harness.

## What Exists

- `registry_coordination.rs`: real mTLS QUIC correctness across 2, 3, and 5
  endpoints, including quorum and partition recovery.
- `quic_transport.rs`: multi-chunk production framing and dispatch correctness.
- `distributed_messaging.rs`: serialization and real QUIC dispatch latency
  measurements.
- `distributed_latency.rs`: control-plane lookup latency measurements.

These are valuable evidence, but none establishes a sustained messages-per-second
rate, failure-recovery SLA, or cluster-wide resource ceiling.

## Required Work Before Claiming Coverage

1. Drive live distributed processes rather than mock nodes or handler calls.
2. Define load shape, warm-up, duration, node counts, payload sizes, and supported
   hardware.
3. Measure success rate, throughput, p50/p95/p99 latency, resource use, and
   recovery time.
4. Inject bounded node loss and partitions with deterministic synchronization.
5. Verify process, memory, file, and network quotas at both node and cluster
   boundaries.
6. Store machine-readable baselines and enforce reviewed regression thresholds.
7. Run the suite in a dedicated CI or scheduled environment suitable for timing
   and soak tests.

## Why the Previous Claim Was Removed

The former documentation attributed specific throughput and success percentages
to `scheduler_stress_test.rs`, `node_failure.rs`, and
`cross_node_hot_reload.rs`. The first file is absent, while the latter two used
obsolete APIs and mock scenarios and no longer compiled. Those claims are not
current executable evidence.

Until the work above lands, `docs/core_values/status.md` must report distributed
stress and cluster-wide quotas as open gaps.
