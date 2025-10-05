# Lunatic Core Values & Design Principles

**Last Updated**: October 5, 2025

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
- Any language that compiles to WASM is supported
- No single language lock-in
- Polyglot systems are first-class citizens
- Interoperability across language boundaries

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
- Hot code reloading for all processes
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
- Automatic backpressure management

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
- ✅ Lightweight processes (WASM instances)
- ✅ Isolated memory
- ✅ Message passing via mailboxes
- ⚠️  Fast spawn (slower due to WASM instantiation)

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
- ✅ Module versioning (ModuleRegistry)
- ✅ State preservation (memory snapshots)
- ✅ Preemptive hot reload (epoch interruption)
- ✅ Signature validation
- ❌ Resource migration (Phase 7)

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
- ✅ Process links
- ✅ Process monitors
- ✅ Death notifications
- ⚠️  Supervisor patterns (application-level, not runtime)

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
- ❌ Transparent process IDs across nodes
- ❌ Global registry
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
- ⚠️  Guest library responsibility (not runtime)
- 📝 Reference implementations needed in multiple languages

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

### Performance
- [ ] Process spawn < 10μs (currently slower)
- [x] Hot reload < 100ms
- [ ] Message passing < 1μs
- [ ] Memory overhead < 1KB per process

### Reliability
- [x] Process isolation (100% guaranteed)
- [x] Hot reload state preservation
- [ ] 99.999% uptime (application-dependent)
- [x] Automatic failure recovery (via supervisors)

### Developer Experience
- [x] Multi-language support
- [ ] Rich ecosystem (libraries in Rust, JS, Go, etc.)
- [ ] Comprehensive documentation
- [ ] Production-ready tooling

### Erlang Parity
- [x] Lightweight processes
- [x] Message passing
- [x] Hot code loading (preemptive)
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
