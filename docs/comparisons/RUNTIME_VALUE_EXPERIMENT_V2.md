# Runtime Value Comparison Experiment v2

Date: 2026-07-23

Lunatic baseline: `main@f5ba083`

Status: exploratory protocol draft. Runs performed before the preregistration
block is complete are instrumentation smoke tests and are not decision-bearing
evidence.

## Decision Question

Does Lunatic provide a materially better way to run untrusted, stateful Wasm
tenant workloads than either a direct Wasmtime embedding or an existing Wasm
platform?

The experiment informs three separate decisions:

1. technical fit for a specified workload;
2. product selection advantage over the strongest alternative; and
3. viability as an independent runtime rather than a library or contribution to
   another platform.

A single technical workload cannot establish market demand or maintainer
capacity. Independent-runtime viability additionally requires external user or
design-partner evidence.

## Candidates

- Lunatic at the baseline commit above.
- Direct Wasmtime `46.0.1` plus the lockfile-pinned Tokio version, without any
  `lunatic-*` process, messaging, OTP, reload, or security dependency.
- wasmCloud `2.5.1`, using its documented standalone host for the embedded
  profile and its Kubernetes operator for the platform profile.

Spin, Dapr, Orleans, and BEAM are contextual controls. The final report must
state when one of them is the better product choice even though they are not
primary implementations in the first experiment.

## Comparison Lanes

The lanes answer different questions and may not substitute for each other.

### Contract-parity lane

Lunatic and direct Wasmtime execute the same guest bytes through the same minimal
guest ABI. This measures how much reusable correctness-critical machinery
Lunatic supplies. It does not determine the best architecture for a new product.

### Outcome-equivalent lane

Each candidate may use its strongest supported architecture, including
host-managed state, recreate plus migration, rolling or blue-green deployment,
Kubernetes policy, and external durable services. A shared black-box oracle
checks outcomes. Every additional process, service, persistence dependency, and
custom extension is counted.

## Deployment Profiles

### Embedded profile

- One host application and no required Kubernetes, sidecar, external placement
  service, or external state database in the volatile-state variant.
- Tenant-supplied Wasm remains resident and owns or is associated with state.
- The host remains healthy when a tenant traps, spins, exceeds admission, or
  attempts an unauthorized operation.

Primary candidates: Lunatic and direct Wasmtime. wasmCloud's supported
standalone host is optional and reported without treating non-participation as a
failure.

### Platform profile

- Multi-node deployment and ordinary production control-plane dependencies are
  allowed and counted.
- External durable state and orchestration are allowed.
- Node loss, placement, rollout, observability, and operator experience are
  first-class outcomes.

Primary candidates: Lunatic with its required operating stack and wasmCloud v2
on Kubernetes. A custom raw-Wasmtime platform is optional.

The profiles are scored separately. `not-applicable`, `unverified`, and `fail`
are distinct. Embedded success is not a platform win, and platform maturity does
not erase a meaningful embedded-runtime advantage.

## Shared Workload: Tenant Automation Execution Units

Each implementation may use its native model, but the external harness observes
the same behavior specification and test vectors.

1. Create 32 tenant trust boundaries from one guest program. Disclose whether
   the isolation unit is a process, Store, instance, service, pod, or another
   unit.
2. Submit commands carrying `tenant_id`, `command_id`, and `sequence`. Admission
   reports `accepted`, `retryable_rejection`, or `unknown`.
3. Accepted increment results are monotonic within the declared ordering scope.
   Duplicate and unknown outcomes follow a preregistered retry and idempotency
   rule.
4. Exceed a preregistered outstanding-command limit. Excess work receives an
   explicit result within the response SLO, rejected work does not mutate state,
   capacity becomes reusable, and unrelated tenants remain responsive.
5. Attempt filesystem and network canary side effects outside the tenant grant.
   An external observer verifies that no effect occurred. If guest-controlled
   delegation exists, child authority may not amplify its parent. Not exposing
   delegation is also a pass.
6. Inject one guest trap and one CPU-bound infinite loop. The host and unrelated
   tenants must meet the frozen sibling-latency SLO. The declared recovery policy
   must reach its stable state within the frozen RTO.
7. Start a version-two rollout whose artifact instantiates successfully but
   fails readiness after deployment begins. A candidate may use in-place,
   recreate plus migration, rolling, or blue-green update.
8. Verify the frozen downtime, mixed-version window, RPO, and success-reporting
   policy for the failed rollout, then apply a valid version.
9. Repeat startup and teardown. No stale endpoint or tenant authority remains,
   no accepted command is orphaned outside the declared delivery contract, and
   resource growth stays below the frozen slope.

In the first volatile-state variant, state persists between commands and across
a supported update, but crash replacement may start with a new identity and
initial state. Exact behavior is reported. A later durable-state variant gives
all candidates the same external persistence and counts adapter and idempotency
machinery.

## Result Model

Each result has two independent dimensions.

- Outcome: `pass`, `partial`, `fail`, `unverified`, or `not-applicable`.
- Mechanism: `built-in`, `supported-config`, `application-code`,
  `external-managed-service`, `custom-host-extension`, or `runtime-patch`.

Only an executed artifact can receive `pass` or `partial`. A behavior is called
unsupported only when primary documentation or a maintainer statement supports
that conclusion; otherwise it is unverified.

## Black-box Correctness Gates

| Gate | Required external observation |
| --- | --- |
| Isolation | A failing or CPU-bound tenant does not corrupt or permanently block unrelated tenants or the host. |
| Authority | Forbidden canary effects do not occur; exposed delegation cannot amplify authority. |
| Admission | Accepted, retryable rejection, and unknown outcomes have explicit state and retry semantics. |
| Recovery | A diagnostically useful failure class is observable and the declared policy reaches a stable state within RTO. |
| Update safety | Failed rollout meets the declared downtime, mixed-version, RPO, and success-reporting policy. |
| State semantics | Counter, ordering, identity, durability, migration, and re-entry behavior are explicit and tested. |
| Cleanup | No stale authority or endpoint remains, no accepted work is silently orphaned, and resource growth is bounded. |

A gate is mandatory only when the preregistered target job requires it. Missing
behavior must not be silently replaced with a Lunatic-specific primitive.

## Competitor-native Designs Explicitly Allowed

Direct Wasmtime may use a shared Engine, tenant-specific Store/instance,
`StoreLimits` or `ResourceLimiter`, fuel or epoch interruption, bounded Tokio
channels, `JoinSet` supervision, tenant-specific Linkers, maintained Rust crates,
and side-by-side instances with host-managed migration.

wasmCloud may use a tenant-specific `WorkloadDeployment`, a long-running
stateful Service, stateless ingress components, `poolSize`, `maxInvocations`, WIT
`hostInterfaces`, `allowedHosts`, Kubernetes RBAC/admission/NetworkPolicy,
service restart, RollingUpdate/Recreate, blue-green or GitOps tooling, external
durable state, Kubernetes Events, and OpenTelemetry. Their cost and any stronger
durability are recorded rather than treated as disqualifying.

## Measurements

Record raw samples and environment metadata. Never present measurements from
different machines as paired results.

- cold compile and warm start-to-ready p50/p95/p99 and rate;
- steady command p50/p95/p99 and rate;
- sibling latency during admission pressure and CPU-bound execution;
- fault-to-observation and fault-to-stable-recovery latency;
- failed and successful update latency, downtime, mixed-version window, and RPO;
- host-only and whole-system incremental RSS/CPU/storage, including sidecars and
  control-plane processes;
- post-cleanup resource slope over repeated cycles;
- handwritten nonblank, noncomment source lines classified as business logic,
  guest adapter, host lifecycle, security, reload, persistence, deployment, and
  shared test harness;
- direct dependencies, external processes and services, setup time, build/run/
  failure/update commands, and unresolved operational assumptions.

Correctness uses 16 commands only as a smoke phase. Decision-bearing latency
requires at least 1,000 command samples. Commit, rollback, and recovery require
at least 30 measured repetitions after warm-up. Exact repetition counts and
statistical tests are frozen in preregistration.

Source lines are descriptive rather than an automatic quality score. Generated
bindings, vendored code, lockfiles, and the shared harness are excluded; runtime
configuration and deployment manifests are included.

## Neutrality Rules

1. Measure end-user outcomes, not identically named primitives.
2. Use supported releases and ordinary recommended configuration.
3. Allow each candidate's native architecture while counting external services
   and weaker or different semantics.
4. Do not move required work outside a timed boundary to improve a number.
5. Separate cold setup, warm steady state, failure, update, and cleanup.
6. Do not build a replacement platform inside a candidate unless custom
   implementation cost is the subject of that lane.
7. Run candidates as separate processes and use paired repetitions on the same
   idle machine for latency and memory comparisons.
8. Preserve failures and raw output alongside summaries.
9. A failed correctness dimension has no comparable performance result.
10. Any protocol change after the first decision-bearing run creates a new
    experiment revision and invalidates cross-revision comparisons.

## Preregistration Blockers

No result is decision-bearing until all of the following are frozen with a
content hash and contain no `TBD` value:

- target user and deployment job;
- trust boundary and accepted-state semantics;
- candidate versions, architecture, supported extension points, and strongest
  alternative selection;
- mandatory-gate matrix per profile;
- absolute SLOs and relative non-inferiority or superiority margins;
- outstanding-command limit, message size, load shape, fault schedule, rollout
  policy, RPO, and RTO;
- implementation and tuning time boxes;
- machine/container topology and existing-cluster versus greenfield accounting;
- external observer, expected traces, repetition counts, and statistical
  protocol.

## Decision Rule

For a frozen target job, evaluate in this order:

1. mandatory black-box gates;
2. absolute SLOs;
3. frozen non-inferiority margin against the strongest alternative;
4. a frozen superiority margin in at least one of custom correctness machinery,
   implementation effort, or operational burden; and
5. architecture review by someone familiar with the competing candidate.

If only the contract-parity Wasmtime lane favors Lunatic, prefer an embeddable
library scope unless independent-runtime behavior is separately necessary. If
the outcome-equivalent alternative satisfies the same job with lower total
burden, prefer integration or contribution over runtime duplication.

## Current Exploratory Evidence

These entries establish fixture and tooling feasibility only.

| Item | Lunatic | Wasmtime + Tokio | wasmCloud |
| --- | --- | --- | --- |
| Pinned revision/version | `f5ba083` | `46.0.1`; Tokio lockfile pin | `2.5.1` |
| Tool/artifact smoke | Scale, attenuation, Supervisor and release reload tests executed | Design inventory complete; implementation pending | Attested Windows `wash 2.5.1`; official service template built |
| Decision-bearing correctness | Not yet preregistered | Not yet preregistered | Not yet preregistered |
| Decision-bearing performance | None | None | None |
| Important observed boundary | Crash restart uses fresh state; CPU-hog production fixture absent | Actor lifecycle, admission, supervision and update are embedder code | Stateful unit is a workload Service; platform rollout differs from in-place reload |
