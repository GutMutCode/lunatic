# Embedded Runtime Comparison Preregistration v1

Experiment ID: `embedded-v1-2026-07-23`

Frozen date: 2026-07-23 Asia/Seoul

Protocol: `docs/comparisons/RUNTIME_VALUE_EXPERIMENT_V2.md`

This file contains no deferred decision threshold. Any semantic, candidate,
workload, SLO, or analysis change after the first candidate-specific code edit
creates a new experiment ID. Pre-freeze fixture and tooling runs are smoke tests
and do not count as evidence.

## Decision Scope

### Target user and job

A product developer embeds a runtime in a desktop or server application to run
third-party automation or plugin code supplied by 32 mutually untrusted tenants.
The product is deployed without Kubernetes, sidecars, an external placement
service, or an external state database in this first volatile-state variant.

Tenant code is long-running and stateful between commands. A tenant crash may
reset volatile state and identity. The host must contain buggy or hostile code,
bound admission, recover service, and update code without falsely reporting
success or losing already accepted commands.

### Decision this experiment may make

- Whether Lunatic has a technical and adopter-effort advantage for this exact
  embedded job over the strongest direct Wasmtime design.
- Whether that advantage is large enough to justify an independent-runtime
  hypothesis for further user validation.

This experiment cannot establish market demand, multi-host HA, durable actor
state, SDK quality across languages, or long-term maintainer viability.

## Frozen Candidates

### Lunatic

- Revision: `f5ba0831ef757e2a134fbeafe622c0d0280a55dd`
- Supported public/runtime paths may be used.
- Candidate-specific adapter, workload and deployment code is counted.
- Existing runtime implementation is not counted as adopter glue, but its total
  maintenance surface is reported separately before an independent-runtime
  conclusion.

### Strongest direct alternative

- Wasmtime: `46.0.1`
- Tokio: `1.53.1`
- Rust: `1.95.0`
- No `lunatic-*` process, messaging, OTP, reload, config, audit, or security
  dependency.
- Allowed: shared Engine and InstancePre, pooling allocator, tenant-specific
  Store/instance, StoreLimits or ResourceLimiter, fuel and epoch interruption,
  bounded Tokio channels, JoinSet/task supervision, tenant-specific Linkers,
  maintained general-purpose Rust crates, host-managed state, and side-by-side
  instance migration.

This candidate is the decision-bearing strongest alternative. It may implement
the user outcome rather than Lunatic's internal abstractions.

### wasmCloud contextual candidate

- `wash` and runtime release: `2.5.1`
- The documented standalone custom-host path may participate.
- Workload Service, components, host interfaces and supported plugins are
  allowed and counted.
- Non-participation is `not-applicable`, not failure. Results are contextual and
  do not replace the direct-Wasmtime comparison.

## Frozen Environment

- OS: Microsoft Windows 11 Home `10.0.26200`, build `26200`
- CPU: Intel Core i7-14700K, 20 physical cores, 28 logical processors
- Physical memory: `68,475,179,008` bytes
- Rust: `rustc 1.95.0 (59807616e 2026-04-14)`
- Cargo: `1.95.0 (f2d3ce0bd 2026-03-21)`
- Build profile: Cargo `--release --locked`
- Tokio worker threads: 4 for every decision-bearing candidate
- Candidate processes run separately and never concurrently.
- Network-facing product services are disabled; loopback is used only by a
  candidate whose supported embedded API requires it and is counted.

Ten paired repetitions alternate order: odd pairs run Lunatic then direct
Wasmtime; even pairs run direct Wasmtime then Lunatic. The first complete pair
is discarded as process and filesystem warm-up. Nine pairs remain for relative
comparisons.

## Trust and State Contract

- Isolation boundary: one independently interruptible Wasm Store/instance or a
  documented stronger boundary per tenant. Multiple tenants in one
  non-interruptible Wasm instance fail the isolation gate.
- Ambient filesystem and network authority: none.
- The guest may call only explicitly linked comparison imports.
- If guest-controlled delegation is exposed, delegated authority is a subset of
  parent authority. Not exposing delegation passes this variant.
- Accepted state: an increment mutates state exactly once when its externally
  visible result is `accepted` and completed. `retryable_rejection` does not
  mutate state. `unknown` may be retried with the same `command_id` and must not
  produce more than one mutation.
- Ordering: accepted commands for one tenant are applied by ascending sequence;
  no global cross-tenant order is required.
- Crash state: `volatile_reset`. A recovered tenant starts at counter 0 with a
  new generation. PID preservation is neither required nor rewarded.
- Update state: accepted counter state survives supported version change. An
  invalid rollout may leave tenants on different reported versions for at most
  the frozen mixed-version window, but may not lose accepted counter state.

## Frozen Workload

- Tenants: 32
- Guest artifacts: one shared v1 digest, one valid v2 digest, one v2 artifact
  that instantiates successfully but fails its readiness probe for tenant 7
- Counter representation: unsigned 64-bit logical value
- Command fields: tenant ID, 128-bit command ID, 64-bit sequence, operation,
  64-byte payload, expected version
- Per-tenant outstanding-command limit: 64
- Maximum accepted message size: 1,024 bytes
- Normal-load concurrency: at most one outstanding command per tenant
- Warm-up: 1,024 completed round-robin increment commands
- Measured normal load: 10,240 completed commands, 320 per tenant
- Admission pressure: pause tenant 0 after readiness, submit 65 commands, require
  at least one explicit retryable rejection, then resume and prove reuse
- Fault cycles: 30 guest-trap cycles and 30 CPU-budget cycles, rotating tenant
  IDs and restoring the full 32-tenant population after each cycle
- CPU budget: 5,000,000 Wasmtime fuel units for the infinite-loop command;
  normal commands receive a fresh budget large enough to complete
- Update cycles: 30 failed-readiness rollouts followed by 30 valid rollouts,
  targeting 16 tenants per rollout
- Cleanup cycles: 30 complete create, load, fault/update-smoke and shutdown
  cycles after five unmeasured warm-up cycles

## Frozen Black-box Gates

All seven gates are mandatory for Lunatic and direct Wasmtime.

| Gate | Frozen pass condition |
| --- | --- |
| Isolation | Trap and CPU-budget exhaustion do not terminate the host; every unrelated-tenant probe completes within the sibling SLO. |
| Authority | Filesystem and network canaries show no external side effect; absent delegation is accepted, exposed delegation cannot amplify authority. |
| Admission | Limit 64 is enforced; excess work returns `retryable_rejection`; rejected work does not change the counter; capacity is reusable. |
| Recovery | Failure is classified at least as `guest_trap` versus `cpu_budget_exhausted`; replacement reaches ready state with generation incremented and counter 0 within RTO. |
| Update safety | Failed readiness never returns rollout success; accepted state has RPO 0; per-tenant version is observable; mixed-version and downtime limits hold. |
| State semantics | Per-tenant accepted sequences and counters are monotonic; valid v2 preserves accepted state; duplicate command IDs mutate at most once. |
| Cleanup | No stale tenant endpoint remains; every accepted command has a terminal result; final working-set delta and growth slope stay within limits. |

## Frozen Absolute SLOs

Nearest-rank percentiles are computed from raw samples.

- 32-tenant warm start-to-ready p99: at most 10 ms per tenant
- Normal accepted-command p99: at most 5 ms
- Admission-result p99 during pressure: at most 20 ms
- Unrelated-tenant command p99 during one trap or CPU loop: at most 20 ms
- Fault trigger to replacement-ready p99: at most 500 ms
- Per-tenant request unavailability during update p99: at most 100 ms
- Whole 16-tenant rollout completion p99: at most 2 seconds
- Failed-rollout mixed-version window: at most 2 seconds
- Update RPO for completed accepted commands: 0
- Incremental whole candidate-process working set at 32 ready tenants: at most
  512 MiB above the pre-tenant baseline
- Cleanup: final working set at most 64 MiB above warm baseline and least-squares
  growth slope at most 1 MiB per measured cleanup cycle

An absolute SLO failure fails the associated gate even if the other candidate is
slower.

## Frozen Relative Margins

Relative comparisons use the median of the nine retained paired ratios.

Lunatic is performance-noninferior only when all applicable ratios versus direct
Wasmtime are at most `2.00` for:

- warm start-to-ready p99;
- normal command p99;
- pressured sibling p99;
- replacement-ready p99;
- update request-downtime p99; and
- 32-tenant incremental working set.

Lunatic demonstrates adopter-effort superiority only if it passes every gate
and its candidate-specific correctness-critical non-test source is both:

- at least 30% smaller than direct Wasmtime; and
- at least 300 nonblank, noncomment lines smaller.

Correctness-critical source includes guest adapter, admission, tenant lifecycle,
security, recovery, update, persistence and deployment logic. Shared business
logic, generated bindings, vendored code, lockfiles and the common oracle are
excluded. Configuration and manifests are included.

Independent-runtime continuation is supported by this technical experiment only
if Lunatic passes all mandatory gates and absolute SLOs, is performance-
noninferior, and meets the adopter-effort superiority margin. Otherwise:

- gate/SLO failure: reject the embedded positioning in its current form;
- passes but lacks adapter superiority: prefer a narrower library or direct
  Wasmtime design pending stronger user evidence;
- only contract-parity advantage: treat it as reusable-library evidence, not an
  independent-runtime result.

## Implementation and Tuning Budgets

- Outcome-equivalent adapter: maximum 8 elapsed working hours and 1,500
  candidate-specific non-test source lines per candidate
- Contract-parity adapter: maximum 12 elapsed working hours and 2,600
  candidate-specific non-test source lines per candidate
- At most two performance-only tuning revisions after correctness first passes
- Correctness fixes remain allowed but reset all affected performance samples
- Candidate start/end timestamps, failed attempts and dependency additions are
  logged

Reusing maintained crates and existing candidate APIs is allowed. Copying
Lunatic runtime code into the direct alternative is not.

## Frozen Observation and Statistics

Every candidate emits newline-delimited JSON with:

- experiment ID, candidate, revision/version and clean/dirty state;
- machine and toolchain identity;
- phase, pair index, iteration, tenant, command ID, sequence and generation;
- admission outcome, counter, version, failure class and rollout state;
- start, end and elapsed monotonic nanoseconds;
- process working set before tenants, at readiness, at peak and after cleanup;
- counts of active tenants, outstanding commands and terminal results.

The external runner validates the schema, unique command IDs, sequence/counter
invariants, sample counts and SLOs. Raw JSON is retained. Summary statistics do
not replace raw evidence.

- Percentiles: nearest-rank
- Paired comparison: median of nine retained head/base ratios
- No interpolation or outlier removal
- A timed-out or crashed candidate run is retained as failure, not discarded
- The whole candidate process and any child process are included in resource
  accounting

## Preregistered Expected Traces

- Normal: `accepted -> completed(counter=n, version=v1)`
- Pressure: `retryable_rejection -> no state mutation -> later accepted`
- Trap: `guest_trap -> generation+1 ready(counter=0)`
- CPU: `cpu_budget_exhausted -> generation+1 ready(counter=0)`
- Failed update: `started -> readiness_failed -> success=false`, with per-tenant
  version and accepted state preserved within the declared window
- Valid update: `started -> ready(v2) -> success=true`, with accepted state
  preserved
- Shutdown: all accepted command IDs terminal, tenant endpoints unreachable,
  process resource counters at zero or documented baseline

## Protocol Freeze

The SHA-256 of this file is recorded in
`docs/comparisons/EMBEDDED_PREREGISTRATION_V1.sha256`. Any content change requires
a new experiment ID and a new hash; existing raw results remain bound to the old
hash.
