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

Language bindings and ecosystem libraries are available for:

- [Rust][3]
- [AssemblyScript][11]

If you would like to see other languages supported or just follow the discussions around Lunatic,
[join our discord server][4].

## Supported features

- [x] Creating, cancelling & waiting on processes
- [ ] Capability and resource isolation (core checks and quotas exist; least-privilege defaults and attenuation are still being completed)
- [ ] Process supervision (host-side GenServer/Supervisor process paths exist; automatic monitor intake and guest-WASM adapters are pending)
- [x] Channel based message passing
- [x] TCP networking
- [x] Filesystem access
- [ ] Distributed nodes (mTLS QUIC and registry coordination exist; live cross-node guest mailbox delivery is not yet proven end to end)
- [ ] Hot reload (snapshot, validation, and resource-transfer components exist; the live running-Wasm reload path is not yet production-verified)
- [ ] OTP patterns (host-side GenServer and Supervisor are integrated; GenStatem/GenEvent runtime adapters and guest bindings are pending)

Unchecked entries are active implementation areas, not unavailable concepts. The canonical evidence and known gaps are maintained in [the core-values status](docs/core_values/status.md); historical phase and benchmark reports do not override it.

## Documentation

- [**CORE_VALUES.md**](CORE_VALUES.md) - Design principles and Erlang inspiration
- [**docs/core_values/status.md**](docs/core_values/status.md) - Up-to-date implementation compliance review
- [**docs/security/AUDIT_LOGGING.md**](docs/security/AUDIT_LOGGING.md) - How to capture and route audit log events
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
design goals; current measured boundaries and missing scale/soak evidence are recorded in the
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
memory/table/fuel limits, and network-connection quotas. Least-privilege defaults, attenuation, and coverage
vary by resource type, as detailed in the [implementation status](docs/core_values/status.md).

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
