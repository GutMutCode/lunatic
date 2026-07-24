<div align="center">
    <a href="https://lunatic.solutions/" target="_blank">
        <img width="60" 
             src="https://raw.githubusercontent.com/lunatic-solutions/lunatic/main/assets/logo.svg"
             alt="lunatic logo"
        >
    </a>
    <p>&nbsp;</p>
</div>

Lunatic is a universal runtime designed for **fast**, **robust** and **scalable** server-side applications.
It's inspired by Erlang. Languages can target it when their toolchains emit compatible [WebAssembly][1]
and provide bindings for the Lunatic host APIs they need.
You can read more about the motivation behind Lunatic [here][2].

## Positioning and evidence

Lunatic packages actor lifecycle, recovery, authority, backpressure, and transactional-update
contracts for stateful Wasm applications. These contracts are not unique to Lunatic: in a frozen
32-tenant embedded comparison, a reviewed direct-Wasmtime implementation also passed every retained
block. Lunatic implemented the mandatory contract in 1,587 candidate-specific production SLOC versus
1,887 for direct Wasmtime (15.9% less), while all paired performance-cost ratios stayed within the
experiment's accepted 2x envelope. This supports modest packaging leverage for the tested workload,
not technical monopoly, market demand, or universal performance superiority. See the
[reproducible comparison](docs/comparisons/EMBEDDED_V3_RESULTS.md) for the method, results, and limits.

Language bindings and ecosystem libraries are available for:

- [Rust][3]
- [AssemblyScript][11]

If you would like to see other languages supported or just follow the discussions around Lunatic,
[join our discord server][4].

## Supported features

- [x] Creating, cancelling & waiting on processes
- [ ] Capability and resource isolation (least-privilege config defaults and attenuation are implemented; complete FD/network/process/queue accounting remains open)
- [ ] Process supervision (the native Supervisor automatically consumes acknowledged child monitor events and escalates restart exhaustion; guest-side supervisor trees and distributed supervision remain pending)
- [x] Channel based message passing
- [x] TCP networking
- [x] Filesystem access
- [ ] Distributed nodes (two actual-Wasm guests complete 16 measured registry lookup → loopback mTLS QUIC → remote mailbox → reply rounds; multi-host operation, partitioned owner exit, and cross-node failure recovery remain unverified)
- [ ] Hot reload (the watch-equivalent path interrupts running Wasm, preserves compatible state, waits for acknowledgements, and commits or rolls back locally; the current 16-process evidence measures bounded live-path latency, but no portable latency target is established and the entrypoint-reentry contract and distributed reload remain unverified)
- [ ] OTP patterns (native GenServer/Supervisor/GenStatem/GenEvent adapters and an actual-Wasm OTP call/reply/timeout/stop contract are verified; high-level cross-language SDKs and distributed OTP remain pending)

Unchecked entries are active implementation areas, not unavailable concepts. The canonical evidence and known gaps are maintained in [the core-values status](docs/core_values/status.md); historical phase and benchmark reports do not override it.

## Documentation

- [**CORE_VALUES.md**](CORE_VALUES.md) - Design principles and Erlang inspiration
- [**docs/core_values/status.md**](docs/core_values/status.md) - Up-to-date implementation compliance review
- [**docs/comparisons/EMBEDDED_V3_RESULTS.md**](docs/comparisons/EMBEDDED_V3_RESULTS.md) - Reproducible embedded-runtime comparison and positioning limits
- [**docs/security/AUDIT_LOGGING.md**](docs/security/AUDIT_LOGGING.md) - How to capture and route audit log events
- [**docs/security/NODE_CONTROL_BEARER.md**](docs/security/NODE_CONTROL_BEARER.md) - Node-control bearer transport, rotation, and revocation contract
- [**docs/hot_reload/HOT_RELOAD_ARCHITECTURE.md**](docs/hot_reload/HOT_RELOAD_ARCHITECTURE.md) - Hot reload system design
- [**docs/hot_reload/HOT_RELOAD_PREEMPTIVE.md**](docs/hot_reload/HOT_RELOAD_PREEMPTIVE.md) - Historical preemptive hot reload design notes
- [**docs/benchmarks/PERFORMANCE_ANALYSIS.md**](docs/benchmarks/PERFORMANCE_ANALYSIS.md) - Historical component metrics and analysis
- [**docs/benchmarks/BENCHMARK_RESULTS.md**](docs/benchmarks/BENCHMARK_RESULTS.md) - Historical benchmark measurements and their scope
- [**docs/benchmarks/BENCHMARK_SUITE.md**](docs/benchmarks/BENCHMARK_SUITE.md) - Benchmark suite guide and coverage boundaries

## Installation

If you have rust (cargo) installed, you can build and install the lunatic runtime with:

```bash
cargo install lunatic-runtime
```

---

On **macOS** you can use [Homebrew][6] too:

```bash
brew tap lunatic-solutions/lunatic
brew install lunatic
```

---

We also provide pre-built binaries for **Windows**, **Linux** and **macOS** on the
[releases page][5], that you can include in your `PATH`.

---

And as always, you can also clone this repository and build it locally. The only dependency is
[a rust compiler][7]:

```bash
# Clone the repository
git clone https://github.com/lunatic-solutions/lunatic.git
# Jump into the cloned folder
cd lunatic
# Build and install lunatic
cargo install --path .
```

## Usage

After installation, you can use the `lunatic` binary to run WASM modules.

To learn how to build modules, check out language-specific bindings:

- [Rust](https://github.com/lunatic-solutions/rust-lib)
- [AssemblyScript](https://github.com/lunatic-solutions/as-lunatic)

## Architecture

Lunatic's design centers on lightweight isolated processes, comparable in role to green threads or
[go-routines][8] in other runtimes. Low spawn cost, small memory overhead, and massive concurrency are
design goals; current bounded live-Wasm scale/pressure evidence includes one warm-up plus five measured batches per population with measured spawn and mailbox throughput/rate,
p50/p95/p99, 64KiB committed Wasm bytes per guest, and a single-baseline cumulative live-population RSS delta divided by guest count
that is explicitly not allocator-attributable, while the remaining large-scale, multi-day/longitudinal, and
multi-host gaps are recorded in the
[implementation status](docs/core_values/status.md).

Some common use cases for processes are:

- HTTP request handling
- Long running requests, like WebSocket connections
- Long running background tasks, like email sending
- Calling untrusted libraries in an sandboxed environment

### Isolation

What makes the last use case possible are the sandboxing capabilities of [WebAssembly][1]. WebAssembly was
originally developed to run in the browser and provides extremely strong sandboxing on multiple levels.
Lunatic's processes inherit these properties.

Each process has its own stack, heap, and syscall capability context. A guest trap is contained to that
process's Wasm instance; higher-level link and supervision behavior is tracked separately in the
[implementation status](docs/core_values/status.md).

Code such as C can participate when compiled to compatible WebAssembly. Wasm memory isolation contains many
guest memory faults, while safety still depends on the runtime, configured capabilities, host imports, and
resource limits; it is not a blanket guarantee for arbitrary native vulnerabilities.

Per-process configuration provides configured filesystem preopens, compile/create/spawn capability flags,
memory/table/fuel limits, and network-connection quotas. Untrusted configs deny privileged capabilities by
default, and child configs cannot exceed their parent's represented authority. Enforcement coverage still
varies by resource type; see the [capability contract](docs/security/CAPABILITY_ATTENUATION.md) and
[implementation status](docs/core_values/status.md).

### Scheduling

Wasm execution uses Wasmtime's async support with fuel yielding and configured epoch deadlines on a
[work stealing async executor][9]. The current process-global ticker advances only stores associated with the
first-created engine; independently constructed later engines do not receive its epoch increments. Async host APIs avoid
blocking an executor thread while they wait, but not every host operation is currently proven non-blocking and
bounded; see the [implementation status](docs/core_values/status.md) for the current boundary.

### Compatibility

We intend to eventually make Lunatic completely compatible with [WASI][10]. Ideally, you could take existing code,
compile it to WebAssembly and run on top of Lunatic; creating the best developer experience possible. We're not
quite there yet.

The Rust host/runtime crates follow a coordinated release train. Version 0.14 intentionally changes
process delivery, spawning, mailbox backpressure, resource snapshots, and several embedding types;
host integrations upgrading from 0.13 must follow the
[0.13 to 0.14 migration guide](docs/MIGRATING_0.13_TO_0.14.md). Guest ABI compatibility and any
temporary legacy imports are called out separately in that guide. The new `lunatic-otp-patterns`
crate is independently versioned at 0.1.0 and requires the 0.14 runtime crates.

## License

Licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

[1]: https://webassembly.org/
[2]: https://kolobara.com/lunatic/index.html#motivation
[3]: https://crates.io/crates/lunatic
[4]: https://discord.gg/b7zDqpXpB4
[5]: https://github.com/lunatic-solutions/lunatic/releases
[6]: https://brew.sh/
[7]: https://rustup.rs/
[8]: https://golangbot.com/goroutines
[9]: https://tokio.rs
[10]: https://wasi.dev/
[11]: https://github.com/lunatic-solutions/as-lunatic
