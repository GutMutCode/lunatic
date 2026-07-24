# Runtime Value Comparison Experiment

Date: 2026-07-23

Lunatic baseline: `main@f5ba083`

## Decision Question

Does Lunatic provide a materially better way to run untrusted, stateful Wasm
actors than either a direct Wasmtime embedding or an existing Wasm platform?

The experiment is intended to decide among three outcomes:

1. continue Lunatic as an independent runtime;
2. narrow Lunatic to an embeddable Wasmtime actor library; or
3. integrate the useful contracts into an existing platform instead.

Feature presence alone is not a win. A candidate must produce the required
outcomes with acceptable performance and lower total implementation and
operational burden for at least one non-contrived deployment profile.

## Candidates

- Lunatic at the baseline commit above.
- Direct Wasmtime `46.0.1` plus Tokio, with no Lunatic process or OTP crates.
- The current supported wasmCloud v2 runtime and tooling pinned by the
  experiment evidence record.

Spin, Dapr, Orleans, and BEAM are contextual controls. They are not primary
implementations in the first experiment because their default execution units
do not match the long-running untrusted Wasm workload. A final conclusion must
still state when one of them is the better product choice.

## Deployment Profiles

### Embedded profile

- One host process and no Kubernetes, sidecar, external placement service, or
  external state database.
- Tenant-supplied Wasm remains resident and owns in-memory state.
- The host must remain healthy when a tenant traps, spins, exhausts a queue, or
  attempts an unauthorized operation.

### Platform profile

- Multi-node deployment and ordinary production control-plane dependencies are
  allowed.
- External durable state and orchestration are allowed but must be counted.
- Node loss, placement, rollout, observability, and operator experience count
  as first-class outcomes.

The profiles are scored separately. Success in the embedded profile must not be
presented as a general platform win, and platform maturity must not erase a
meaningful embedded-runtime advantage.

## Workload: Tenant Automation Actor

Each implementation may use its native model, but it must produce the same
observable outcomes.

1. Start 32 tenant actors from one precompiled Wasm program.
2. Give each actor a private counter and an ordered command inbox.
3. Complete 16 increment-and-echo commands per measured round.
4. Saturate one actor's inbox at a declared finite limit. The rejected command
   must remain owned by the sender, and unrelated actors must continue.
5. Make one tenant attempt a forbidden host operation and a stronger child
   authority. Neither attempt may produce an external side effect.
6. Make one tenant trap and one tenant run CPU-bound code. The host and siblings
   must remain responsive, and the declared recovery policy must run.
7. Prepare a version-two update for 16 live actors. A deliberately invalid
   candidate must not leave a mixed committed cohort or falsely report success.
8. Apply a valid candidate. Preserved state, message ordering, identity changes,
   and any restart or re-entry behavior must be reported exactly rather than
   normalized into a common claim.
9. Shut down and prove that actor, message, waiter, capability, and resource
   registrations return to zero or to a documented platform baseline.

## Correctness Gates

Results use `native`, `configured`, `custom`, `unsupported`, or `unverified`.
Only an executed artifact can receive `native`, `configured`, or `custom`.

| Gate | Required observation |
| --- | --- |
| Isolation | A failing or CPU-bound tenant does not corrupt or permanently block siblings or the host. |
| Authority | Forbidden operations fail before side effects; child authority cannot exceed its parent. |
| Backpressure | The inbox is finite, rejection is observable, payload ownership is defined, and capacity is reusable. |
| Recovery | The configured policy observes the actual termination reason and reaches a declared stable state. |
| Update safety | Failed rollout cannot report success or leave an undocumented mixed committed cohort. |
| State semantics | Counter, inbox, identity, and re-entry behavior across recovery and update are explicit and tested. |
| Cleanup | All workload-owned registrations and resource leases are released after shutdown. |

A candidate that cannot pass a mandatory outcome may still remain the better
choice for workloads that do not require that outcome. Missing behavior must
not be silently replaced with a Lunatic-specific primitive in the specification.

## Measurements

Record raw samples and environment metadata. Do not compare values gathered on
different machines as if they were paired results.

- actor start-to-ready p50/p95/p99 and throughput;
- steady echo p50/p95/p99 and rate;
- sibling latency during inbox saturation and CPU-bound execution;
- trap-to-stable-recovery latency;
- failed and successful update latency;
- process-wide RSS baseline, peak, and post-cleanup value;
- executable handwritten source lines, excluding generated bindings and tests;
- number of custom security- or lifecycle-sensitive mechanisms;
- required long-running processes and external services;
- build, start, failure-injection, diagnosis, and update commands;
- setup time and unresolved operational assumptions.

Source lines are descriptive, not a quality score. A short but opaque or
externally delegated implementation is not automatically cheaper.

## Neutrality Rules

1. Measure end-user outcomes, not identically named primitives.
2. Use supported releases and ordinary recommended configuration.
3. Allow each candidate to use its native architecture, while counting external
   services and weaker or different semantics.
4. Do not move required work outside a timed boundary only to improve a number.
5. Distinguish cold setup, warm steady state, and cleanup.
6. Record unsupported behavior instead of implementing a replacement platform
   inside the benchmark unless custom implementation cost is the subject.
7. Run paired repetitions on the same idle machine when comparing latency or
   memory.
8. Preserve failures and raw output alongside the summarized result.

## Decision Rule

Continue as an independent runtime only if all of the following survive the
experiment:

1. At least one deployment profile represents a credible user job rather than a
   benchmark invented from Lunatic APIs.
2. Lunatic passes that profile's mandatory correctness gates.
3. The strongest alternative needs materially more custom correctness-critical
   machinery, weaker guarantees, or additional operational infrastructure.
4. Lunatic meets the workload's stated latency, throughput, and memory budget.
5. The advantage remains after SDK, tooling, observability, ecosystem, and
   maintenance costs are counted.

If only the direct Wasmtime comparison is favorable, prefer an embeddable
library scope unless independent-runtime behavior is necessary. If wasmCloud or
another supported platform satisfies the same job with lower total burden,
prefer integration or contribution over runtime duplication.

## Evidence Record

The following must be filled from executed artifacts or linked primary sources.

| Item | Lunatic | Wasmtime + Tokio | wasmCloud |
| --- | --- | --- | --- |
| Pinned revision/version | `f5ba083` | Pending | Pending |
| Embedded artifact | Pending | Pending | Pending |
| Platform artifact | Pending | Pending | Pending |
| Correctness-gate result | Pending | Pending | Pending |
| Performance evidence | Pending | Pending | Pending |
| Custom mechanism inventory | Pending | Pending | Pending |
| External dependency inventory | Pending | Pending | Pending |
| Unsupported or different semantics | Pending | Pending | Pending |
