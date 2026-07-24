# Embedded Runtime Comparison v3 — Final Results

Execution date: 2026-07-24 UTC

Lunatic runtime baseline: `main@f5ba0831ef757e2a134fbeafe622c0d0280a55dd`

Status: decision-bearing f3 execution complete.

## Outcome

The literal claim that this job **must use Lunatic** is rejected. The completed,
independently reviewed direct-Wasmtime implementation passed all nine retained
blocks.

The frozen verdict is nevertheless:

`LUNATIC_PACKAGING_VALUE_SUPPORTED` with `moderate` strength.

For this specific embedded, volatile-state workload, Lunatic packaged the same
mandatory contract in 1,587 candidate-specific production SLOC versus 1,887 for
direct Wasmtime. The 300-line reduction met the frozen moderate threshold, and
all 14 paired median Lunatic/direct performance-cost ratios were at most 2.00.
This is evidence of packaging leverage, not technical monopoly, market demand,
or universal superiority.

## Frozen Job

The decision-bearing profile ran 32 mutually untrusted, stateful Wasm tenants
inside one long-lived OS process, with no Kubernetes, sidecar, external
placement service, or external database. State could reset after a tenant crash,
but these properties were mandatory:

- tenant failure and CPU-loop isolation;
- least authority proven by six external-effect canaries;
- finite admission and retry-safe backpressure;
- automatic volatile recovery;
- tested-guest-state-preserving valid update and exact failed-update rollback;
- explicit command identity, ordering, and duplicate semantics;
- cleanup without stale endpoints or unbounded RSS growth.

There were ten Latin-ordered blocks. Block 0 was the designated warm-up and was
excluded. Blocks 1–9 were retained independently for each candidate. Every
block included actual guest Wasm, 10,240 measured normal commands, pressure and
duplicate cases, 30 traps, 30 CPU loops, 30 failed plus 30 valid rollouts, six
authority canaries, and 30 retained cleanup cycles.

The candidates were:

- Lunatic fork adapter on the baseline above, embedding Wasmtime 46.0.1;
- direct Wasmtime 46.0.1 plus Tokio 1.53.1;
- Extism 1.30.0, whose pinned dependency graph embeds Wasmtime 43.0.2.

All candidates used one OS process and no long-lived helper or external
database. Extism used 32 dedicated tenant threads; the other candidates used
bounded Tokio-task execution with four Tokio workers.

## Eligibility

| Candidate | Retained full passes | Mandatory gates | Absolute SLOs | Eligible |
| --- | ---: | ---: | ---: | --- |
| Lunatic | 9/9 | 9/9 | 9/9 | yes |
| Extism | 0/9 | 9/9 | 0/9 | no |
| Direct Wasmtime | 9/9 | 9/9 | 9/9 | yes |

Extism failed exactly one absolute metric in every retained block: normal
command p99 exceeded the frozen 5 ms limit. Its nine p99 values ranged from
15.7276 ms to 15.9000 ms, with a median of 15.7391 ms. Its authority,
isolation, recovery, update, state, cleanup, resource, and one-process gates all
passed. The result therefore does not say Extism lacks the required semantics;
it says this adapter did not meet this workload's latency SLO.

## Adopter Code and Effort

| Candidate | Production SLOC | Time to first full pass | Extra process/service |
| --- | ---: | ---: | --- |
| Lunatic | 1,587 | 106 min | no |
| Extism | 1,831 | 120 min | no |
| Direct Wasmtime | 1,887 | 123 min | no |

The time values are wall-clock intervals from candidate-directory creation to
the first complete core-plus-authority pass. They were derived from development
timestamps rather than a controlled human time log, so SLOC is the stronger
effort evidence.

Because Extism was ineligible, the eligible reference was direct Wasmtime:

- reference SLOC: 1,887;
- Lunatic delta: 300 lines, or 15.90% less;
- strong cap: floor(70% × 1,887) = 1,320 — not met;
- moderate cap: floor(85% × 1,887) = 1,603 — met.

## Paired Performance Cost

Each value below is the median of nine same-block
`Lunatic / direct-Wasmtime` ratios. Lower is better.

| Metric | Ratio |
| --- | ---: |
| Shared initialization | 0.984× |
| Warm create p99 | 1.102× |
| Normal command p99 | 1.022× |
| Normal command total | 1.003× |
| Pressure admission p99 | 0.690× |
| Fault sibling p99 | 1.070× |
| Trap recovery p99 | 1.244× |
| CPU-loop recovery p99 | 0.843× |
| Failed-update unavailability p99 | 1.386× |
| Valid-update unavailability p99 | 1.520× |
| Failed rollout p99 | 1.095× |
| Valid rollout p99 | 1.283× |
| Ready-32 total RSS | 1.743× |
| Peak total RSS | 1.686× |

Sensitivity counts were 9/14 metrics at or below 1.25×, 11/14 at or below
1.50×, and 14/14 at or below 2.00×. Direct Wasmtime retained a material memory
advantage, while Lunatic was faster on shared initialization, pressure
admission, and CPU-loop recovery in the paired median. These measurements do
not support a universal speed claim for either runtime.

## Decision Interpretation

Direct Wasmtime passing 9/9 means the underlying job is reproducible without
Lunatic. The direct implementation assembled the required boundaries from
maintained Wasmtime and Tokio primitives plus application-owned lifecycle,
admission, recovery, idempotency, authority, and rollout code.

Lunatic's measured value is narrower and still useful: it reduced the amount of
candidate-specific production code implementing the contract while keeping the
frozen performance cost within the accepted 2× envelope and requiring no extra
service. This supports an independent runtime as a packaged contract for this
job, but only at the moderate tier.

The conclusion is sensitive. If the unchanged 1,831-SLOC Extism implementation
met the 5 ms normal p99 SLO without adding disallowed infrastructure, it would
become the reference. Lunatic would then be 86.67% of the reference and miss
the 85% moderate cap, so the same frozen rule would report packaging value as
unsupported.

The defensible positioning is therefore:

> Lunatic is not the only way to run untrusted stateful Wasm actors. In the
> tested embedded profile, it packaged the required failure, authority,
> recovery, update, and evidence contracts with about 16% less adopter code
> than a reviewed direct-Wasmtime design, within a 2× paired performance-cost
> bound.

## Evidence Integrity and Exclusions

All 30 f3 full-run invocations exited zero, and every run recorded a zero
candidate-process exit. Execution ran from 2026-07-24T04:58:13.640Z through
2026-07-24T05:27:27.412Z (1,753.772 seconds). The frozen CLI read
`verdict-input.json`, which contains the 27 retained-run objects, and produced
the saved verdict deterministically.

The earlier freeze-2 samples were discarded after a causal-order bug was found
in the Oracle: a fast rollout terminal could be observed before two initial
worker controls were issued. The corrected Oracle routes initial, subsequent,
and retry workers through one causal send-or-receive gate. A successful
decision-external b05 preflight found 60/60 rollout terminals, zero same-attempt
worker sends after terminal, and an exact 2,682/2,682 trace-to-summary physical
worker count. The final decision-bearing b05 Lunatic trace independently
repeated the invariant with 2,545/2,545.

One first f3 preflight was also excluded because a descriptive scratch path
made SQLite's rollback-journal path 267 characters on Windows, causing the
Oracle's permissive positive control to fail with `SQLITE_CANTOPEN`. A fresh
short scratch root reduced the path to a safe budget; the unchanged Oracle and
candidate then passed. This was a harness-path configuration failure, not a
candidate authority failure.

Key identities:

- config bundle SHA-256:
  `23eac1fd06a241082b136bcc6f36f68e04a0aad4712ca46a12c047e157f8c88f`;
- Oracle source tar SHA-256:
  `3c815e20de41b6346712f388cdf3be6ea33988c9cb5f4eb990185b0d6fa74e3d`;
- Oracle runner SHA-256:
  `21c8195632466dee8f4680fc66c71291a144acebd3c8015201f3285f375e00f4`;
- verdict input SHA-256:
  `042906ce2e3525fad2e577d3eadc2185892d530561e80a79b61ad288952b3dc1`;
- verdict output SHA-256:
  `1ebb424a0adb2cec78311b5f603f4e35b2d8ab16b7c73980e6645c4c3addb794`.

The compact result bundle is under
`experiments/embedded-v3/results/f3/`. Raw decision evidence remains at
`C:/tmp/e3f3-e`; the result bundle records its paths, bytes, and hashes.

## Limits

This experiment does not establish:

- market demand, adoption, or maintainer capacity;
- that Lunatic is better than every runtime or platform;
- durable-state recovery, multi-host HA, or distributed supervision;
- Kubernetes/platform-profile superiority;
- production behavior on another OS, CPU, or workload;
- equal high-level SDK maturity across guest languages;
- a universal latency, throughput, or memory advantage.

The tested alternatives are strong controls for this embedded profile, not an
exhaustive market comparison. Runtime maintenance and ecosystem risk also remain
product-selection costs outside the frozen SLOC and performance rule.
