# Embedded Runtime Comparison Preregistration v2

Experiment ID: `embedded-v2-2026-07-23`

Frozen date: 2026-07-23 Asia/Seoul

Status: protocol freeze. No candidate-specific source existed when this version
was frozen. v1 is superseded because its per-command fuel, repeated rollout,
measurement-boundary, and SLOC rules were not implementable without
post-registration choices. Pre-v2 runs are smoke evidence only.

The files listed in `EMBEDDED_PREREGISTRATION_V2.sha256` are the complete
normative bundle. Earlier drafts and unlisted tooling are non-normative.

## Question and Scope

The target user embeds a runtime in a desktop or server product that executes
long-running automation or plugin Wasm from 32 mutually untrusted tenants. The
deployment has one host process, no Kubernetes, no sidecar, no external
placement service, and no external state database. Volatile state may reset
after a tenant crash, but isolation, least authority, bounded admission,
automatic recovery, and non-deceptive updates are mandatory.

The experiment asks two distinct questions:

1. Can Lunatic and the strongest direct Wasmtime design satisfy that job?
2. If both can, does Lunatic package enough correct machinery to justify an
   independent runtime instead of a smaller reusable library?

It cannot establish market demand, durable state, multi-host HA, SDK quality,
maintainer capacity, or BEAM/OTP parity.

## Decision-bearing Lane

Only the **outcome-equivalent embedded volatile-state lane** is decision-bearing.
Each candidate may choose its strongest architecture and candidate-specific
guest artifacts. A common external oracle checks observable behavior. A future
same-guest-byte contract-parity lane is exploratory library evidence and may not
replace this lane.

Every normal increment, probe, trap, and CPU-loop operation must actually enter
tenant Wasm. State may live in guest memory or a tenant-scoped host side table,
but the guest must mediate the operation and report its executing logical
version. A host-only actor simulation is invalid. Candidate source and execution
traces are reviewed for this condition before the execution freeze.

## Candidates

### Lunatic

- Runtime source: `f5ba0831ef757e2a134fbeafe622c0d0280a55dd`.
- Public runtime paths and maintained crates already present at that revision
  may be used.
- Runtime source is not adopter glue. Candidate adapter, guest, configuration,
  lifecycle, admission, recovery, security, and rollout code is adopter glue.
- The tracked `src`, `crates`, root `Cargo.toml`, and root `Cargo.lock` trees must
  match that revision at execution freeze. Experiment files may be untracked or
  added outside those paths.

### Strongest direct alternative

- Wasmtime `46.0.1`, Tokio `1.53.1`, Rust `1.95.0`.
- No `lunatic-*` package, copied Lunatic source, or dependency on the oracle.
- Allowed: a shared Engine, component or core-module APIs, pooling allocator,
  InstancePre, one Store per tenant, ResourceLimiter/StoreLimits, fuel or epoch
  interruption, bounded Tokio channels, JoinSet/task supervision,
  tenant-specific Linkers, host-managed volatile state, idempotency caches, and
  side-by-side/rolling update orchestration.
- Maintained general-purpose crates are allowed, pinned, and reported.

Both candidates run as one long-lived OS process. A required long-lived helper
process violates this embedded profile. Build-time compilers are allowed.

### Context only

wasmCloud `2.5.1` may be reported using its documented standalone host. Its
absence is `not_applicable`, not a primary-candidate failure. Platform/Kubernetes
strengths do not answer this embedded lane.

## Frozen Environment

- Windows 11 Home `10.0.26200`, build `26200`
- Intel Core i7-14700K, 20 physical cores, 28 logical processors
- Physical memory: `68,475,179,008` bytes
- `rustc 1.95.0 (59807616e 2026-04-14)`
- `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`
- Node.js `24.4.1` for the frozen SLOC tool only
- Release builds: `cargo build --release --locked`
- Exactly four Tokio worker threads per candidate
- Candidates run separately, never concurrently
- No network-facing product service; stdin/stdout NDJSON is the control plane

## State and Authority Contract

- One independently interruptible Store/instance or a documented stronger
  boundary per tenant. Multiple tenants in one non-interruptible instance fail.
- Ambient filesystem and network authority are absent.
- Guest-visible imports are explicitly linked per production tenant policy.
- Not exposing guest delegation passes. If exposed, delegated authority must be
  a subset of the parent authority.
- A completed accepted increment mutates its tenant exactly once. A
  `retryable_rejection` occurs before acceptance and mutates nothing.
- The same command ID may be retried, but all attempts produce at most one
  mutation and one distinct completed result.
- Accepted sequences and counters are monotonic within one generation.
- Crash recovery is `volatile_reset`: generation increments and counter resets
  to zero. PID preservation is not required or rewarded.
- Update and rollback preserve all completed accepted increments. A valid update
  changes logical guest version without requiring PID preservation.
- Every accepted command becomes terminal before teardown or external rollout
  completion. This experiment does not admit an `unknown` command outcome.

## Frozen Workload and Protocol

The exact schedule is `experiments/embedded-v2/protocol/scenario.json`. Control
and event envelopes are the adjacent JSON Schemas. Communication is stdin/stdout
NDJSON; stdout contains only schema-valid events and stderr contains diagnostics.
The oracle owns request IDs, command IDs, send/receive clocks, timeouts, raw
traces, and resource samples. Candidate timestamps are diagnostic only.

The fixed core is:

- 32 tenants, mailbox limit 64 per tenant, logical payload limit 1,024 bytes
- 1,024 sequential warm-up increments
- 10,240 measured increments: 320 closed-loop rounds, one outstanding command
  per tenant and 32 concurrent tenants
- 80 deterministic completed-command duplicate probes
- one closed-gate burst of 65 commands: exactly 64 accepted, one rejected, then
  drain and unchanged retry; separate 1,025-byte rejection and 1,024-byte pass
- 30 rotating guest traps and 30 rotating guest CPU loops
- 30 failed rollouts and 30 valid rollouts, always targeting tenants 0..15
- five cleanup warm-ups and 30 measured full lifecycle cycles

### CPU fault

Lunatic has no public command-scoped fuel recharge API, so this experiment does
not invent one. The candidate arms a 50 ms wall-clock execution deadline when a
`cpu_loop` guest invocation begins and interrupts that generation using its
strongest supported mechanism. The guest must execute an actual non-terminating
Wasm loop. Admission must be prompt; the external failed terminal must arrive
40-100 ms after the accepted admission event. Source review rejects host-side
special-casing that merely sleeps, rejects, or labels the command without guest
execution. The runner measures recovery from control-request write, so queueing
and interruption overhead remain visible.

### Update state machine

There are four logical candidate-specific artifacts: valid `A`, valid `B`,
`bad_A`, and `bad_B`. Valid A and B have compatible state semantics. The bad
artifact behaves as its named version for tenants other than 7 but fails
activation for tenant 7 before it can serve an externally completed command.
It may fail during start/instantiation or an explicit candidate readiness check.

For even cycles the state machine is `A -> bad_B` (must fail and roll back), then
`A -> B` (must succeed). For odd cycles it is `B -> bad_A`, then `B -> A`.
Targets are always tenants 0..15, so tenant 7 is always present. A candidate may
preflight all targets and avoid partial activation; this is a valid stronger
implementation.

External rollout terminal states are only:

- `succeeded`: every target serves the new version, state is preserved, all
  accepted rollout-window commands are terminal, and three oracle snapshots
  confirm the new version and expected counter.
- `failed_rolled_back`: every target serves the old version, the failed artifact
  is unreachable, state including completed rollout-window increments is
  preserved, all accepted commands are terminal, and three oracle snapshots
  confirm the old version and expected counter.
- `in_doubt`: the candidate cannot prove either condition. This is honest but
  fails the update-safety gate.

Internal pointer swaps, registry commits, or ACKs are not external success. The
oracle's rollout completion is the latest of candidate terminal receipt, all
pre-terminal accepted-command terminals, and the third confirmation snapshot.
During a rollout one closed-loop probe and one increment are issued per target.
Pre-acceptance rejection is allowed and retried unchanged. Rejection, timeout,
or absence of a completed probe all count toward update unavailability; fast
rejection cannot report zero downtime.

### Authority canary

For every retained run the oracle creates a nonce, writable temporary directory,
nonce-containing sentinel, absent create target, TCP listener, and UDP listener.
Before candidate use, positive controls prove create/delete and TCP/UDP effects
are observable and then clear the observations.

A canary tenant using exactly the production tenant Linker/runtime config must
use actual guest-reachable filesystem/network interfaces to attempt sentinel
read/overwrite/delete/rename, file create/write, TCP connect/write, and UDP send.
Unlinked production imports may cause `denied_at_link`; a comparison-only
`deny()` import is invalid. The observer remains active through canary teardown
plus 500 ms. It checks filesystem change events, recursive names/content,
sentinel content, returned secret bytes, TCP accepts, and UDP datagrams.

Pass requires no filesystem event or final change, no proof of secret read, no
TCP accept, no UDP datagram, and an explicit denied/link-denied/guest-trap result.
Positive-control failure or a canary-only weaker policy invalidates the run.

## Mandatory Gates

Every retained Lunatic and direct-Wasmtime run must pass every gate.

| Gate | Pass condition |
| --- | --- |
| Isolation | Trap and real guest CPU loop do not terminate the host; unrelated-tenant probes satisfy the sibling SLO. |
| Authority | The production-policy canary produces no externally observed filesystem or network effect and no secret-read proof. |
| Admission | Exactly 64 queued commands are accepted behind the closed gate; excess and 1,025-byte payload are retryably rejected without mutation; drained capacity and 1,024-byte boundary are reusable. |
| Recovery | Trap and CPU deadline are distinguished; replacement becomes ready at generation+1 and counter 0 within RTO. |
| Update safety | No false success; successful and failed rollouts meet terminal proofs, preserve completed accepted state, and satisfy version/downtime bounds. |
| State semantics | Per-generation order/counter invariants and duplicate idempotency hold for all oracle commands. |
| Cleanup | All accepted commands are terminal, stale endpoints are unreachable, active counts are zero, and memory bounds hold. |

## Absolute SLOs

Nearest-rank percentiles use all applicable samples within one candidate run.
Each of the nine retained runs must independently pass; samples are never pooled
across runs and a run median cannot hide a run failure.

- concurrent warm create-to-ready p99 after shared initialization: <= 10 ms
- normal completed increment p99: <= 5 ms
- pressure admission-result p99: <= 20 ms
- unrelated sibling completion p99 over all 60 fault probes: <= 20 ms
- trap replacement-ready p99 and CPU replacement-ready p99, separately: <= 500 ms
- every CPU failed terminal after admission: 40-100 ms
- per-target update unavailability p99, failed and valid rollouts separately:
  <= 100 ms
- whole 16-tenant rollout completion p99, failed and valid separately: <= 2 s
- failed- and valid-rollout mixed-version windows, separately: <= 2 s
- update RPO for completed accepted commands: 0
- total candidate-process working set at 32 ready tenants: <= 768 MiB
- peak 32-tenant working-set increase over minimal-host baseline: <= 512 MiB
- after cleanup: <= 64 MiB above minimal-host baseline and least-squares slope
  <= 1 MiB per measured cleanup cycle

Cold process start through shared initialization is recorded separately without
an absolute gate. Warm create excludes Engine/module compilation only after the
oracle has measured the minimum process baseline and initialization cost.

## Measurement Boundaries and RSS

For each request, the oracle records monotonic time immediately before a flushed
stdin write and immediately after parsing the relevant terminal event. Elapsed
nanoseconds must be positive. Candidate-emitted durations cannot replace them.

RSS includes the candidate process and every descendant and is sampled at:

1. minimal host after `hello`, before Engine, module, or pool initialization;
2. after shared initialization;
3. after all 32 tenants are ready;
4. phase peaks; and
5. every post-teardown cleanup cycle.

Three samples 100 ms apart are taken at stable points and their median is used;
phase peak is the maximum periodic 10 ms sample. A negative or zero
ready-minus-minimal delta invalidates resource instrumentation instead of being
used in a ratio. Preallocating before the minimum baseline is forbidden.

## Repetitions and Statistics

There are exactly ten paired runs. Pair 0 is the designated instrumentation
warm-up and is never replaced or repeated; a functional failure stops the
execution freeze rather than creating a more favorable replacement. Pairs 1-9
are retained. Odd retained pairs run Lunatic first; even retained pairs run
direct Wasmtime first.

- nearest-rank percentile, no interpolation
- no outlier removal
- timed-out/crashed retained run is a mandatory-gate failure
- relative result is the median of nine positive `Lunatic/direct-Wasmtime`
  per-pair metric ratios
- if an absolute gate fails, the relative metric cannot rescue it

The phrase `performance cost tolerated up to 2x` is used instead of statistical
non-inferiority. All listed performance ratios must be <= 2.00. Sensitivity at
1.25, 1.50, and 2.00 is reported; no threshold may be changed after results.

Ratios cover warm create p99, normal p99, sibling p99, trap RTO p99, CPU RTO
p99, valid/failed update downtime p99, valid/failed rollout p99, and peak
ready-minus-minimal RSS.

## Effort, Tuning, and Anti-strawman Rules

Adopter effort and technical performance are separate stages.

1. **Time-box checkpoint:** each candidate receives up to 16 logged elapsed work
   hours and 2,500 production SLOC for the first full correctness attempt.
   Incompleteness is adopter-effort evidence, not proof of technical inability.
2. **Completion and review:** both may then be completed. The direct design must
   receive an independent architecture review for unfair serialization,
   avoidable copies, allocator/Engine misuse, and missing maintained crates.
   Lunatic receives the same review standard.
3. **Performance tuning:** each candidate receives at most four
   correctness-preserving revisions and 16 additional logged hours. Development
   samples are smoke only.
4. **Execution freeze:** one final revision per candidate is selected before any
   paired decision run. All ten pairs run once from those revisions. No best-run
   or best-revision selection is allowed.

Correctness changes after an execution freeze create a new freeze and discard
all affected decision samples. The report shows checkpoint time/status, time to
first full pass, final SLOC, tuning revisions, and failed attempts.

## SLOC and Dependency Accounting

The only SLOC command is:

`node experiments/embedded-v2/tools/sloc.mjs <candidate-manifest.json>`

Each candidate manifest conforms to `sloc-manifest.schema.json`, lives outside
the candidate root, and explicitly attributes every supported file below that
root. The tool errors on unlisted or duplicate files and reports file hashes.
Decision SLOC is the nonblank, noncomment physical lines with role `production`.

Production includes handwritten guest source, protocol conversion, lifecycle,
admission, authority, recovery, update, state/idempotency, deployment,
configuration, and manifests regardless of directory name. Tests are reported
but not counted. Lockfiles and reproducibly generated artifacts/bindings are
reported as zero with an explicit reason. Generated source may not contain
handwritten policy. Candidate executables may not be Cargo test targets.

The common oracle, schemas, and validation-only DTOs are excluded, and neither
candidate may depend on or include them. A helper used by only one candidate is
candidate production SLOC. Direct dependencies, transitive packages, binary
size, required processes, and the existing Lunatic runtime maintenance SLOC are
reported separately.

Lunatic has a material packaging advantage only if final production SLOC is
both at least 30% and at least 300 lines smaller than direct Wasmtime.

## Decision Rule: Does It Have To Be Lunatic?

The experiment deliberately sets a high bar for uniqueness:

- **Reject this positioning:** Lunatic fails any mandatory gate or absolute SLO.
- **No unique technical necessity:** independently reviewed direct Wasmtime
  passes all mandatory gates and SLOs. Lunatic may still be the better packaged
  product, but the job does not require Lunatic.
- **Independent-runtime hypothesis supported:** both pass, Lunatic meets the
  SLOC advantage, every performance-cost ratio is <= 2.00, and it adds no
  disallowed deployment dependency. This means material packaging leverage,
  not monopoly or market validation.
- **Library-shaped advantage:** both pass but Lunatic misses the SLOC margin, or
  its only advantage appears in contract-parity primitives. Prefer extracting
  reusable lifecycle/security/update libraries unless user evidence supplies a
  stronger runtime-level reason.
- **Tested technical necessity:** Lunatic passes but the reviewed and completed
  direct design cannot satisfy a mandatory gate for a reason traced to a
  missing architectural primitive rather than time-box exhaustion or a known
  implementation defect. This is the only result supporting “this job needs
  Lunatic,” and it remains limited to the frozen job.

## Execution Freeze and Evidence

Before paired runs, record and hash:

- final candidate source revisions and clean candidate workspaces
- all candidate lockfiles and SLOC manifests/results
- release binaries and candidate-specific A/B/bad-A/bad-B guest artifacts
- dependency graphs and direct dependency lists
- proof that direct Wasmtime has no Lunatic dependency
- proof that Lunatic runtime paths match `f5ba0831...`
- machine fingerprint, build commands, tuning log, architecture reviews
- common oracle revision/binary and all protocol bundle hashes

Raw control, event, oracle-timestamp, stderr, process-exit, filesystem observer,
network observer, and RSS traces are retained. A summary never substitutes for
raw evidence.

Any semantic, workload, SLO, analysis, protocol, oracle, or SLOC-policy change
after this protocol freeze requires a new experiment ID. Implementation-only
oracle bug fixes before candidate execution require a new protocol hash and a
documented audit that they did not use candidate outcome data.
