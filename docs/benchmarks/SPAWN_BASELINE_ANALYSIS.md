# Spawn Baseline Analysis

Decision review: 2026-07-23

## Decision

Process spawn remains an aspirational `<10µs` product goal, not a claim about
the current runtime and not a CI limit derived from an undocumented historical
machine. The supported runtime is kept on Wasmtime 46.0.1, including its
per-process isolation, resource limiting, fuel, epoch interruption, and WASI
setup. Spawn regressions are evaluated by repeated paired base/head measurements
on the same runner. A 75µs lower-confidence-bound backstop catches an absolute
failure of the minimal harness.

The old `60µs` warning / `75µs` single-run upper-bound gate is retired. It was
useful as a temporary hosted-runner ceiling, but one confidence-interval
endpoint cannot separate a code change from machine variance.

## Measured boundary

[`benches/spawn.rs`](../../benches/spawn.rs) constructs the Tokio runtime,
Wasmtime engine, compiled module, and shared `LunaticEnvironment` before the
timer. Each timed iteration then:

1. allocates a fresh process registry;
2. constructs `DefaultProcessState` and its per-process resources;
3. creates and configures a Wasmtime store;
4. instantiates the already-compiled module;
5. schedules its exported `hello` function; and
6. waits for process cleanup and join completion.

[`wat/hello.wat`](../../wat/hello.wat) exports one no-op function. The result is
a component benchmark for a minimal short-lived process. It excludes engine
creation, compilation, concurrent load, realistic guest initialization, and
steady-state application work.

## Evidence audit

The October 2025 report retained this Criterion interval:

```text
spawn process           time:   [22.971 µs 23.055 µs 23.139 µs]
```

`23.139µs` is the reported interval's upper endpoint, not the slowest observed
sample. The retained output identifies 100 measurements and four outliers, but
does not retain a commit, dirty state, exact OS, CPU, memory, Rust version,
Wasmtime version, warmup/measurement settings, or raw Criterion directory.
Consequently it is historical context, not a reproducible base for calculating
a 2026 regression.

After the Wasmtime 46 upgrade, shared hosted-Linux runs produced upper endpoints
around 49–54µs. The temporary 60/75µs gate was calibrated to those observations,
including 53.827µs at commit `100767bd306709099338137a135dc40033aa4545`.
Those runs establish the scale of that runner class but do not isolate the
runtime upgrade from hardware, OS, toolchain, or scheduler differences.

## Current-HEAD descriptive run

The following five sequential runs used:

```text
Commit:      9afadbb6f7ee0a2782f9f0066b88e1fac37b5a1b (clean)
Command:     cargo bench --bench spawn -- --sample-size 30
Rust/Cargo:  1.95.0
Wasmtime:    46.0.1
Criterion:   0.4.0
OS:          Windows 11 Home 10.0.26200, build 26200
CPU:         Intel Core i7-14700K, 20 cores / 28 logical processors
Memory:      63.77 GiB
```

| Run | Criterion interval (µs) |
| ---: | ---: |
| 1 | `[33.867, 34.384, 34.920]` |
| 2 | `[34.217, 34.598, 35.032]` |
| 3 | `[34.487, 34.820, 35.142]` |
| 4 | `[34.754, 35.239, 35.764]` |
| 5 | `[34.355, 34.763, 35.212]` |

The median point estimate was 34.763µs and the upper endpoints ranged from
34.920µs to 35.764µs. This confirms that the current harness does not meet the
`<10µs` goal on this machine. It is not compared numerically with the 2025
observation because the environments are not equivalent.

## Controlled comparison protocol

[`scripts/compare_spawn_bench.py`](../../scripts/compare_spawn_bench.py) makes
the outer adjacent base/head pair the independent statistical unit:

- resolve and record both immutable commit SHAs;
- require byte-for-byte parity of `benches/spawn.rs` and `wat/hello.wat`;
- create clean detached worktrees with distinct Cargo target directories;
- use one pinned toolchain and locked dependencies for both revisions;
- alternate pair order as `base/head`, then `head/base`;
- give every invocation a distinct Criterion home;
- use 50 samples, 1 second of warmup, and 3 seconds of measurement;
- start with 10 pairs and extend to at most 15 when the relative interval is
  wider than 10 percentage points; and
- retain raw command output, Criterion estimates, hashes, environment metadata,
  `report.json`, and `summary.md`.

For every pair, the script reads Criterion's slope point estimate and computes
`log(head/base)`. A fixed-seed percentile bootstrap estimates the median paired
ratio and its 95% interval. CI applies these outcomes:

| Condition | Outcome |
| --- | --- |
| median paired increase `>5%` | warning |
| ratio 95% interval lower bound `>+10%` | failure |
| head median 95% lower bound `>75µs` | failure |
| ratio interval width `>10` percentage points after 15 pairs | inconclusive failure |

Run the default protocol on an otherwise idle machine:

```bash
python scripts/compare_spawn_bench.py <base> <head> \
  --output-dir spawn-comparison
```

The JSON report is the authoritative result. The Markdown summary is a compact
CI rendering of the same data.

## Upgrade boundary and accepted tradeoff

The controlled Wasmtime milestone pair is:

- base `8e7086778815ad32ca57cb8391ca0610e846d4a7` (Wasmtime 8.0.1); and
- head `e838d45bc6df30cffe2e1148c96ee1b7f197baf2` (Wasmtime 46.0.1).

The commits are adjacent, and the spawn harness and no-op fixture are unchanged.
The upgrade updates the Wasmtime/WASI dependency stack and adapts store fuel,
resource-limiter, feature, and instantiation APIs. It therefore provides a much
stronger causal comparison than the undocumented 2025 number, while still
representing the whole upgrade patch rather than a single internal Wasmtime
function.

The default paired protocol completed 10 pairs on the Windows machine recorded
above between `2026-07-22T20:33:54Z` and `20:39:15Z`. Both revisions used Rust
1.95.0, 50 Criterion samples per invocation, a 1-second warmup, and a 3-second
measurement window. The harness and fixture SHA-256 values matched exactly and
both detached worktrees remained clean.

| Metric | Point | 95% bootstrap interval |
| --- | ---: | ---: |
| Wasmtime 8 base median | 45.339µs | 44.986–45.514µs |
| Wasmtime 46 head median | 30.598µs | 30.335–30.814µs |
| head/base paired ratio | 0.6730 | 0.6695–0.6830 |

The upgrade boundary was about 32.7% faster on this controlled Windows run;
every paired log ratio favored Wasmtime 46. This rules out a Wasmtime-46
slowdown on the measured Windows configuration, but it does not isolate
Linux-specific behavior. The historical 23.139µs versus hosted 49–54µs contrast
still cannot be attributed to the runtime upgrade from the available evidence:
the old record's missing machine and toolchain metadata prevents causal
separation, and a pinned Linux milestone pair would be required for a
Linux-specific attribution.

## Profile evidence

The same milestone commits were profiled sequentially with Rust 1.95.0 and
their locked dependencies on Windows 11 build 26200, an Intel Core i7-14700K
with 28 logical processors, and 63.77 GiB of memory. Each commit ran:

```text
lunatic-stack-sampler.exe OUT.tsv 2 1200 BENCH_EXE \
  --bench --profile-time 10 --noplot "spawn process"
```

The available machine lacked permission for ETW/WPR collection and had no
WSL/perf, Valgrind, or flamegraph installation. The fallback profiler therefore
used DbgHelp/PDB user-mode stack sampling at a 2ms polling interval, discarded
the first 1.2 seconds of setup/warmup, and excluded samples whose top frame was
an OS wait. It retained 251 active base stacks and 199 active head stacks.

The mutually exclusive, unweighted stack-family attribution was:

| Category | Wasmtime 8 base | Wasmtime 46 head |
| --- | ---: | ---: |
| async Fiber lifecycle | 51.4% | 43.2% |
| WASI construction/probes | 14.7% | 3.5% |
| Wasmtime VM/instance excluding Fiber | 4.8% | 12.6% |
| Lunatic state/process excluding the categories above | 4.0% | 6.5% |
| Tokio/mio | 25.1% | 33.7% |
| unassigned OS/CRT | 0.0% | 0.5% |

Inclusive stacks make the two largest reductions more concrete:

| Inclusive symbol/path | Wasmtime 8 base | Wasmtime 46 head |
| --- | ---: | ---: |
| `DeleteFiber` | 31.9% | 23.1% |
| `NtFreeVirtualMemory` | 31.5% | 23.1% |
| `CreateFiberEx` | 12.4% | 14.1% |
| `DefaultProcessState::new` | 15.1% | 3.5% |
| `lunatic_wasi_api::build_wasi` | 14.7% | 3.5% |
| Wasmtime-8 console detection | 13.9% | 0.0% observed |
| `WasiCtxBuilder::inherit_stdio` | 9.6% | 0.5% |

Fiber stack creation stayed comparable while teardown samples decreased. The
other major reduction was the old WASI stdio/console-probe path. Non-Fiber
Wasmtime VM bookkeeping occupied a larger *share* in the head profile, but that
share is within a total wall time that was 32.7% lower; it is a remaining cost,
not evidence of an overall VM regression. Tokio's high inclusive share is also
not diagnostic because both async paths share scheduler frames.

Multiplying sample shares by paired medians suggests, only as an explanatory
approximation, about -10.08µs from Fiber lifecycle and -5.61µs from WASI,
partly offset by +1.68µs in non-Fiber VM work. These are not measured phase
durations. Cooperative suspend/unwind can perturb execution, async Fiber stacks
can be missed or repeated, the sample counts are modest, and this single
Windows profile does not prove a Linux cost breakdown. Repeated Linux `perf`
profiles are the appropriate follow-up if Linux-specific attribution is needed.

The retained local raw profiles are:

- `C:\tmp\lunatic-spawn-profile-1671-base.tsv` (430,175 bytes, SHA-256
  `79F371DB635F73FEA65F175AA23111C32B518F78C64EC3083B6575B7CB87AD31`); and
- `C:\tmp\lunatic-spawn-profile-1671-head.tsv` (362,737 bytes, SHA-256
  `B035DD8469DE3A0D8B4EB503B49F22772EA39534BDF212D68BA3CB1032DC84BC`).

The repository-tracked
[`spawn-wasmtime-8-to-46-profile.json`](evidence/spawn-wasmtime-8-to-46-profile.json)
preserves the input hashes, row and cycle totals, exact collector commands and
target hashes, stack-family precedence and regexes, inclusive numerators, tool
versions, and limitations. The collector source was not retained, so the JSON
supports audit of this result but does not claim exact collector replay.

No setup is moved out of the timed iteration merely to improve the number.
Caching `DefaultProcessState`, the store, quota registration, WASI context, or
process lifecycle objects would change the isolation and resource-accounting
boundary represented by a process spawn. Reverting to an unsupported Wasmtime
release would also trade away security and maintenance support. Those are not
safe benchmark optimizations. The accepted tradeoff is to keep the supported
runtime and security boundary, disclose the current cost, and reject future
regressions with paired evidence. WASI construction explains part of the
base-to-head improvement and is only 3.5% of head samples; it is not the first
current-head target. Fiber lifecycle is the leading current candidate, followed
by phase measurement of VM and scheduler work before any semantics-preserving
optimization, without removing work from the measured process boundary.

## Interpretation limits

Passing this gate means only that the minimal precompiled-Wasm spawn-and-join
path did not regress under the stated paired contract. It does not prove the
`<10µs` product goal, production tail latency, concurrent spawn throughput,
memory efficiency, or million-process scale. Those require separate workloads
and service objectives.
