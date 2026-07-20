# Benchmark Suite Implementation — Historical Delivery Record

Original sprint: 2025-10-06

Evidence review: 2026-07-21

Status: benchmark implementation sprint completed; production validation not claimed

This file records the scope delivered by the October 2025 benchmark sprint. That sprint added useful component harnesses, CI execution, and documentation. It did not validate all `CORE_VALUES.md` targets and is not a production-readiness certificate.

The original record omitted an exact Git commit and reproducible hardware/toolchain profile. Its numeric observations are therefore historical notes rather than a current baseline. The [canonical implementation status](../core_values/status.md) supersedes earlier completion or readiness language.

## Delivered in the Sprint

- Criterion harnesses for the then-current spawn and local mailbox paths.
- A manual reload-component harness covering direct compilation, instantiation, registry operations, and memory snapshot/restore.
- Memory probes using Rust type sizes, an example one-page Wasm memory snapshot, and small state-construction fixtures.
- Linux CI execution and benchmark artifact upload.
- Initial benchmark documentation and run instructions.

Later work expanded the suite with instance-pool, local messaging, distributed encode/decode and loopback QUIC dispatch, and control-plane lookup benchmarks. See [`BENCHMARK_SUITE.md`](BENCHMARK_SUITE.md) for the current inventory.

## Correct Interpretation of the Historical Results

| Claim area | Historical observation | What the harness actually established |
| --- | ---: | --- |
| Process spawn | 23.055µs | A minimal precompiled `hello.wat` spawn-and-join iteration; it missed the `<10µs` goal |
| Process messaging | 353.23ns | A new in-memory mailbox, ten direct pushes, and one FIFO pop; no sender/receiver processes |
| Live hot reload | 758.94µs | A direct v1/v2 compile/instantiate/registry/snapshot/restore sequence; no live signal path |
| Memory/process | ~66KiB | A lower-bound estimate using one linear-memory page and Rust `size_of` values; not RSS |
| One million processes | ~66GiB | Arithmetic extrapolation; no million-process run or soak test |

The independently measured reload component values were not an additive percentage breakdown of the manual full-sequence iteration.

## CI Boundary

The sprint configured automated execution and artifact retention. The current workflow is configured to invoke the suite on Linux and apply absolute thresholds to selected labels. It does not yet compare every change with a stored commit-bound baseline; the workflow states that historical regression comparison is manual.

Passing CI means the configured harnesses ran within their configured limits. It does not prove:

- end-to-end guest process messaging;
- a live running-Wasm reload via `Signal::HotReload`;
- process acknowledgement, atomic commit, or rollback;
- actual per-process resident memory;
- one-million-process capacity or sustained stability;
- production readiness or aggregate core-values compliance.

## Validation Record

| Sprint deliverable | Delivery status | Evidence status |
| --- | --- | --- |
| Benchmark source files | Delivered | Component/microbenchmark evidence only |
| CI execution and artifacts | Delivered | Selected harness execution; no universal baseline comparison |
| Historical result documentation | Delivered | Missing commit and exact environment metadata |
| `CORE_VALUES.md` performance validation | Not established | Production E2E and scale evidence absent |
| Production-readiness validation | Not established | Outside the delivered harness scope |

## Required Follow-Up Evidence

1. Commit-bound, reproducible reruns with full machine/toolchain metadata.
2. Local and distributed live process message round trips, including backpressure and tail latency.
3. Live reload success/failure/rollback benchmarks through the actual process signal path.
4. RSS/reachable-heap measurements and representative resource use.
5. Increasing-count scale tests and sustained soak tests.

## Conclusion

The October 2025 sprint established useful component-level benchmark harnesses and CI execution. It did not validate end-to-end process messaging, the live reload signal path, rollback, actual per-process RSS, million-process operation, or production readiness. Those claims require current, commit-bound production E2E and scale/soak evidence.

See [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md) for the preserved measurements, [`PERFORMANCE_ANALYSIS.md`](PERFORMANCE_ANALYSIS.md) for the scoped analysis, and [`../core_values/status.md`](../core_values/status.md) for current implementation status.
