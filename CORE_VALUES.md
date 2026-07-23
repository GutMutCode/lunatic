# Lunatic Core Values & Design Principles

**Last Updated**: July 23, 2026

**Historical Performance Analysis (October 2025)**: See [docs/benchmarks/PERFORMANCE_ANALYSIS.md](docs/benchmarks/PERFORMANCE_ANALYSIS.md)

**Current Compliance Status**: See [docs/core_values/status.md](docs/core_values/status.md)

This document defines design goals. It is not an implementation-completion report; the linked compliance status is authoritative for overall feature completion. In the reference sections below, ✅ means only that the named component has direct executable evidence, not that the broader production feature is complete. Historical benchmark values are component evidence, not production guarantees.

## Executive Summary

Lunatic is a universal runtime inspired by Erlang/BEAM, designed to bring proven distributed systems principles to WebAssembly. This document captures our core values and design philosophy to guide feature development and architectural decisions.

---

## Core Values

### 1. **Fast, Robust, and Scalable**

**Fast**
- Sub-100ms hot reload response time
- Efficient work-stealing executor
- Minimal runtime overhead
- Zero-copy message passing where possible

**Robust**
- Process isolation (separate stack, heap, syscalls)
- "Let it crash" philosophy
- Supervised process trees
- Automatic failure recovery

**Scalable**
- Millions of lightweight processes
- Efficient resource utilization
- Horizontal scalability
- Small engineering teams can manage large systems

**Guiding Questions:**
- Does this feature maintain sub-100ms latency?
- Does it improve fault isolation?
- Can it scale to millions of processes?
- Does it reduce operational complexity?

### 2. **Language Independence via WebAssembly**

**Principles**
- The execution ABI should remain usable from languages that can target compatible WASM
- No single language lock-in
- Polyglot systems are a first-class design goal
- Interoperability across language boundaries must be demonstrated by guest SDKs and tests

**Guidelines**
- Host APIs must be language-agnostic
- Documentation should include examples in multiple languages
- Performance should not degrade for non-Rust WASM modules
- Security model must work across all WASM languages

**Guiding Questions:**
- Can this feature be used from any WASM language?
- Does it introduce language-specific assumptions?
- Is the API ergonomic for non-Rust developers?

### 3. **Security Through Isolation**

**Sandboxing Guarantees**
- Each process has isolated memory
- Fine-grained permission control (filesystem, network, etc.)
- Capability-based security model
- Unsafe C code runs safely in isolated processes

**Resource Control**
- Per-process resource limits (memory, CPU, network)
- Syscall-level permission enforcement
- No shared mutable state between processes
- Audit trail for security-sensitive operations

**Guiding Questions:**
- Does this feature maintain process isolation?
- Can permissions be controlled at syscall level?
- Does it introduce shared mutable state?
- Can untrusted code be safely executed?

### 4. **Fault Tolerance & High Availability**

**Design Patterns**
- Process failure doesn't affect other processes
- Supervisors restart failed processes
- Links and monitors for failure detection
- State recovery mechanisms (hot reload, snapshots)

**Operational Excellence**
- Zero-downtime deployments
- Hot code reloading for eligible processes, with explicit unsupported-resource behavior
- Graceful degradation
- Self-healing systems

**Guiding Questions:**
- What happens when this component fails?
- Can the system continue operating?
- Is state preserved during updates?
- Can failures be detected and recovered automatically?

### 5. **Asynchronous by Default**

**Execution Model**
- All code runs async on work-stealing executor
- Blocking operations automatically yield
- Preemptive scheduling (via fuel/epoch)
- Fair resource distribution

**Developer Experience**
- Write synchronous-looking code
- Runtime handles async complexity
- No callback hell or explicit async/await (in guest code)
- Bounded queues and explicit backpressure behavior

**Guiding Questions:**
- Does this operation yield when blocking?
- Is the API synchronous from guest perspective?
- Does it prevent process starvation?
- Is backpressure handled correctly?

### 6. **Erlang-Inspired, WebAssembly-Native**

**What We Keep from Erlang**
- Actor model (processes communicate via messages)
- Hot code reloading
- Supervision trees
- Let it crash philosophy
- Process links and monitors

**WebAssembly Adaptations**
- Module-level hot reload (not function-level)
- Capability-based security (vs. Erlang's trust model)
- Linear memory snapshots (vs. heap garbage collection)
- Epoch interruption (vs. reduction counting)

**Guiding Questions:**
- How would Erlang solve this problem?
- Does our WASM-based solution maintain the same guarantees?
- Are we introducing unnecessary complexity?
- Can we leverage WASM's unique strengths?

---

## Erlang/BEAM Key Features (Reference)

### Process Model

**Lightweight Processes**
- Millions of processes on single machine
- ~300 bytes per process overhead
- Isolated memory (no shared state)
- Fast process spawn (~1-2μs)

**Message Passing**
- Asynchronous message sending
- Mailbox per process
- Pattern matching on messages
- Selective receive

**Lunatic Implementation Status:**
- ⚠️ Lightweight Wasm process abstraction exists; Erlang-comparable spawn, memory, and scale goals remain unmet or unverified
- ✅ Isolated memory
- ✅ Message passing via mailboxes
- ⚠️  Fast spawn (slower due to WASM instantiation)
- ✅ Actual-Wasm lifecycle tests preserve normal-versus-failure reasons on links and verify one monitor notification for normal exit, guest trap, host panic, kill, and missing-process failure

### Hot Code Loading

**Two-Version Policy**
- Maximum 2 versions of a module in memory
- Graceful transition between versions
- Third version load triggers old version purge

**Code Change Mechanism**
- External calls use latest version
- Local calls stay on current version
- `code_change/3` callback for state migration
- Tail-recursive loops enable version switching

**Lunatic Implementation Status:**
- ✅ `ModuleRegistry` assigns unique versions and tracks live process-version transitions through the production reload path
- ✅ The watch-equivalent update path interrupts running Wasm, preserves compatible linear memory and FIFO mailbox state, waits for every process acknowledgement, and either commits atomically or rolls the full target set back; explicit in-doubt state prevents false success. The current bounded workload summary is 16 processes, 10 rounds, 5 commit + 5 rollback, 1,280 FIFO messages, and 160 full denials.
- ⚠️ Reload re-enters the configured guest export rather than preserving the interrupted instruction pointer, native stack, private globals, or tables; modules still need a compatible entrypoint-reentry contract
- ⚠️ Candidate-instantiation failure is exercised through the live rollback path, while signature compatibility remains component-tested rather than a separately asserted live-path E2E case
- ⚠️ A directly tested same-runtime helper transfers supported live resource maps, including active TLS sessions, between replacement states. Guest-driven proof for every resource type, serialized snapshots, host restart, and cross-node restoration of active TCP/TLS streams remain unsupported

### Fault Tolerance

**Supervision Trees**
- Supervisors monitor worker processes
- Restart strategies (one-for-one, one-for-all, rest-for-one)
- Escalation to parent supervisor
- Configurable restart intensity

**Links and Monitors**
- Links: Bidirectional failure propagation
- Monitors: Unidirectional failure notification
- Exit signals: normal, killed, error
- Trapping exits for custom handling

**Lunatic Implementation Status:**
- ✅ Process links preserve actual-Wasm normal-versus-failure reasons, while monitors notify once; default links terminate peers and trapping exits preserve the tagged notification
- ✅ A native Supervisor process automatically consumes acknowledged child monitor events with preserved normal/failure reasons; actual-process tests cover immediate exit/panic, kill, OneForOne, OneForAll, RestForOne, restart intensity, reverse shutdown, and failure escalation. The actual-Wasm Supervisor test survives three immediate traps, performs three replacements, reaches a stable fourth start, and tears down without retained registrations.
- ✅ Registry partition recovery is repeated three times against fresh production-transport clusters, and slow-consumer fairness emits 16-sample bounded-lane evidence
- ⚠️ Guest-Wasm supervisor trees and distributed supervision remain pending

### Distribution

**Transparent Distribution**
- Send messages to remote processes
- Location transparency (process IDs work across nodes)
- Automatic node discovery
- Network partition handling

**Global State**
- Distributed process registry
- Global name registration
- Replicated state (ETS, Mnesia)

**Lunatic Implementation Status:**
- ⚠️  Distributed messaging (lunatic-distributed crate)
- ✅ Registry coordination uses the runtime mTLS QUIC control path and is covered by localhost multi-endpoint quorum/partition tests
- ✅ Actual Wasm guests resolve a globally registered process and exchange a confirmed request/reply through live cross-node mailboxes over the production localhost mTLS path; healthy owner exit removes the registration from both replicas. Two guest-Wasm processes now perform 16 sequential production registry lookup → loopback mTLS QUIC → remote live mailbox → reply round trips with average/p50/p95/p99/rate evidence.
- ⚠️ Environment-aware guest handles, partitioned owner exit, node-failure recovery, multi-host guest traffic, cross-host deployment, and transparent distributed process reconstruction remain unverified
- ❌ Distributed hot reload (Phase 10)

### OTP Patterns

**GenServer (Generic Server)**
- Synchronous and asynchronous calls
- State management
- Initialization and termination hooks
- Timeout handling

**GenStatem (State Machine)**
- Finite state machine behavior
- Event-driven state transitions
- Timeouts and state data

**Supervisor**
- Child specification
- Restart strategies
- Dynamic children management

**Lunatic Implementation Status:**
- ✅ GenServer native process spawn, mailbox call/cast, correlated replies, timeout, stop, kill, and handler-error propagation are connected and covered by process-level integration tests
- ✅ GenServer runs on a native Lunatic process; optional names use the bounded, owner-indexed production local registry with collision rejection and exit cleanup
- ✅ Supervisor OneForOne, OneForAll, RestForOne, restart policies, intensity limits, ordered shutdown, and escalation are driven by actual process monitor events, including an actual guest-Wasm trap and children that terminate before ordinary monitor intake
- ✅ GenStatem and GenEvent have native Lunatic-process mailbox/lifecycle adapters
- ✅ An actual-Wasm OTP1 fixture verifies guest cast, correlated call/reply, timeout, and acknowledged stop over the existing language-neutral message ABI
- ✅ Pinned Rust, TinyGo, and AssemblyScript compiler outputs exercise guest process spawn, tagged message round trip, timeout, and attenuated-child permission denial in CI
- ⚠️ High-level Rust/TinyGo/AssemblyScript guest SDKs, language-specific OTP1 adapters, guest-side Supervisor/GenStatem/GenEvent libraries, and distributed OTP integration remain pending

---

## Decision Framework

When designing or reviewing features, ask:

### 1. **Does it align with core values?**
- Fast, robust, scalable?
- Language-independent?
- Maintains security isolation?
- Improves fault tolerance?
- Async by default?
- Erlang-inspired?

### 2. **Does it match Erlang's guarantees?**
- Process isolation maintained?
- Hot reload supported?
- Failure recovery possible?
- Distribution-ready?

### 3. **Does it leverage WebAssembly strengths?**
- Strong sandboxing?
- Fast compilation?
- Portable across platforms?
- Language interoperability?

### 4. **What are the tradeoffs?**
- Performance impact?
- Memory overhead?
- API complexity?
- Breaking changes?

### 5. **Can it scale to WhatsApp-level workloads?**
- 1 billion users
- Handful of engineers
- High availability (99.999%)
- Zero-downtime deployments

---

## Anti-Patterns to Avoid

### ❌ Shared Mutable State
- Violates process isolation
- Introduces race conditions
- Defeats fault tolerance

### ❌ Blocking Operations Without Yielding
- Starves other processes
- Defeats fair scheduling
- Use fuel/epoch for preemption

### ❌ Global Singletons
- Single point of failure
- Bottleneck for scalability
- Use process registries instead

### ❌ Synchronous External Calls
- Blocks calling process
- Cascading failures
- Use async messages or timeouts

### ❌ Language-Specific APIs
- Violates language independence
- Creates ecosystem fragmentation
- Design for polyglot from day one

### ❌ Trust-Based Security
- WASM runs untrusted code
- Always validate at syscall boundary
- Capability-based, never trust-based

---

## Success Metrics

Checkboxes in this section represent current production-path verification, not whether a supporting component or historical microbenchmark exists. Measurement scope and reviewed commit are recorded in [the canonical status](docs/core_values/status.md).

### Performance
- [ ] Process spawn < 10μs (aspirational product target; a 2026-07-23 Windows component run at `9afadbb` had a 34.763μs median point estimate, and the [spawn analysis](docs/benchmarks/SPAWN_BASELINE_ANALYSIS.md) records the boundary and same-runner paired regression contract)
- [ ] Live hot reload < 100ms (a bounded 16-process full-mailbox gate records production-path commit/rollback p50/p95/p99 on every Linux CI run, but no portable calibrated product threshold has been established)
- [ ] End-to-end process message passing < 1μs (historical local 10-message FIFO mailbox creation/push/pop harness: 353ns; live guest delivery is now measured at the registry-to-live-mailbox boundary, but sub-µs end-to-end delivery remains unproven)
- [ ] Memory overhead < 1KB per process (historical lower-bound estimate: ~66KiB including one 64KiB Wasm page; current scale evidence reports 64KiB committed Wasm bytes per guest and a single-baseline process-wide RSS curve at cumulative live populations 1/8/32, but not allocator-attributable per-process heap overhead)

### Reliability
- [ ] Production isolation contract (Wasm memory isolation, least-privilege defaults, and linked failure propagation are verified; bounded queues, process counts, and complete resource accounting remain open)
- [x] Local live hot reload state preservation with acknowledgement and rollback proof (compatible linear memory, FIFO mailbox state, and supported same-runtime resources; instruction-pointer and distributed continuation are excluded)
- [ ] 99.999% uptime (application-dependent)
- [x] Automatic local failure recovery via OTP supervisors (actual child monitor events drive restart strategies and intensity escalation; distributed recovery is excluded)

### Security
- [x] Node-control bearer boundary (redacted runtime secrets, same-origin transport, redirect/proxy fail-closed behavior, acknowledgement-safe rotation, expiry/revocation, and production-path E2E; remote HTTPS plus enrollment admission remain operator responsibilities and the legacy submillisecond server is quarantined)

### Developer Experience
- [ ] Multi-language guest API support (a pinned Rust, TinyGo, and AssemblyScript primitive ABI matrix now runs in CI; stable equivalent SDK packages and broad API parity remain open)
- [ ] Rich ecosystem (libraries in Rust, JS, Go, etc.)
- [ ] Comprehensive documentation
- [ ] Production-ready tooling

### Erlang Parity
- [ ] Erlang-comparable lightweight processes (the Wasm process abstraction exists; spawn, memory, and scale targets remain unmet or unverified)
- [ ] Bounded message passing with production backpressure guarantees
- [ ] Live hot code loading with acknowledged version transition and rollback
- [ ] Full OTP patterns
- [ ] Transparent distribution

---

## Inspiration: The WhatsApp Story

**Challenge**: Scale to 1 billion users with minimal engineering team

**Erlang's Solution**:
- 50 engineers supported 900M users (2015)
- Hot code upgrades with zero downtime
- Fault-tolerant across distributed datacenters
- Let it crash + supervision trees

**Lunatic's Goal**: Bring these capabilities to WebAssembly ecosystem

**Key Takeaways**:
1. Simplicity scales better than complexity
2. Process isolation prevents cascading failures
3. Hot reload enables fearless deployments
4. Message passing decouples components
5. Small teams can manage large systems with the right runtime

---

## References

- [Erlang Hot Code Loading](http://erlang.org/doc/reference_manual/code_loading.html)
- [WhatsApp Engineering Blog](https://www.wired.com/2015/09/whatsapp-serves-900-million-users-50-engineers/)
- [Lunatic FAQ](https://lunatic.solutions/faq)
- [Joe Armstrong - Making reliable distributed systems](https://www.erlang.org/download/armstrong_thesis_2003.pdf)
- [Learn You Some Erlang for Great Good](https://learnyousomeerlang.com/)

---

## Document Maintenance

**Update this document when:**
- Adding major features
- Making architectural decisions
- Changing core APIs
- Implementing new Erlang-inspired patterns

**Review frequency:** Quarterly or before major releases

**Owners:** Core maintainers

---

**Remember**: Lunatic's mission is to make building distributed, fault-tolerant systems as simple as Erlang made it for telecoms, but with the security, portability, and language flexibility of WebAssembly.
