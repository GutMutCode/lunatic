# Lunatic OTP Patterns

This crate contains Erlang/OTP-inspired callback traits and runtime adapters for Lunatic processes.

## Current implementation status

All four behavior patterns have native Lunatic-process adapters:

- `GenServer::spawn` registers a native Lunatic process and consumes requests through its `MessageMailbox`.
- `call` uses per-request correlation IDs, configurable timeouts, and propagates handler or process-exit errors.
- `cast`, graceful `stop`, forced `kill`, `is_alive`, and termination waiting are implemented.
- Named `GenServer::spawn_in` uses an injected bounded `DistributedRegistry` local namespace, rejects collisions before `init`, and removes owner names before exit waiters return.
- `crates/lunatic-otp-patterns/tests/gen_server_runtime.rs` exercises the public API against real native Lunatic processes.
- `Supervisor::spawn` creates a native supervisor process. Child starters receive its managed `Environment`; acknowledged monitor registration and retained terminal reasons close the race with immediately exiting children.
- OneForOne, OneForAll, and RestForOne consume reason-preserving child monitor events automatically, restart in specification order, enforce restart intensity, and escalate after reverse-order shutdown.
- `crates/lunatic-otp-patterns/tests/supervisor_runtime.rs` exercises strategies, immediate normal/error/panic exits, restart policies, and orphan cleanup against real native Lunatic processes; `tests/wasm_link_death.rs` adds an immediate guest-trap replacement case.
- GenEvent snapshots `Arc` handlers before invoking user code, dispatches the snapshot concurrently
  outside its registry lock, and reports per-handler success, error, panic, or runtime failure.
- Targeted GenEvent delivery calls exactly one handler. Add/remove operations affect future
  snapshots and never cancel an already snapshotted delivery.
- GenEvent and GenStatem can be moved into native processes with mailbox-backed handles and lifecycle waiting.
- Synchronous handle waits use Tokio's blocking region on multi-thread runtimes and are regression-tested with one worker.
- The `guest` module defines the language-neutral OTP1 envelope; `tests/otp_guest_wasm.rs` verifies actual-Wasm cast, call/reply, timeout, and acknowledged stop.
- The Rust examples use the process-backed GenServer and Supervisor APIs.

The native adapters require a multi-thread Tokio runtime. OTP1 is a low-level wire contract rather than a complete language SDK; packaged Rust, TinyGo, and AssemblyScript guest libraries, guest-side supervision/state-machine helpers, global naming, and distributed supervision remain pending.

## Overview

Lunatic implements actor-model primitives inspired by Erlang/BEAM. This crate builds higher-level process, mailbox, registry, monitor, and lifecycle adapters on those primitives.

## Patterns

### GenServer

Process-backed stateful server pattern with synchronous calls and asynchronous casts.

```rust
use lunatic_otp_patterns::{GenServer, GenServerConfig};
use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Counter {
    count: i64,
}

#[derive(Debug, Serialize, Deserialize)]
enum CounterRequest {
    Increment,
    Decrement,
    Get,
    Set(i64),
}

#[derive(Debug, Serialize, Deserialize)]
enum CounterResponse {
    Ok,
    Value(i64),
}

impl GenServer for Counter {
    type State = Self;
    type Call = CounterRequest;
    type CallReply = CounterResponse;
    type Cast = CounterRequest;

    fn init() -> Self::State {
        Counter { count: 0 }
    }

    fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
        match request {
            CounterRequest::Increment => {
                self.count += 1;
                Ok(CounterResponse::Ok)
            }
            CounterRequest::Decrement => {
                self.count -= 1;
                Ok(CounterResponse::Ok)
            }
            CounterRequest::Get => Ok(CounterResponse::Value(self.count)),
            CounterRequest::Set(value) => {
                self.count = value;
                Ok(CounterResponse::Ok)
            }
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) -> Result<()> {
        match request {
            CounterRequest::Increment => self.count += 1,
            CounterRequest::Decrement => self.count -= 1,
            CounterRequest::Get => {}
            CounterRequest::Set(value) => self.count = value,
        }
        Ok(())
    }
}
```

### Supervisor

Process-backed supervision with restart strategies and bounded restart intensity.

```rust
use lunatic_otp_patterns::{
    ChildSpec, ChildType, RestartPolicy, RestartStrategy, ShutdownPolicy,
    Supervisor, SupervisorSpec,
};
use lunatic_process::{env::Environment, spawn_native, Process};
use std::{future, sync::Arc};

fn start_worker(environment: Arc<dyn Environment>) -> Result<Arc<dyn Process>, String> {
    let (_join, process) = spawn_native(environment, |_process, _mailbox| async move {
        future::pending::<anyhow::Result<()>>().await
    })
    .map_err(|error| error.to_string())?;
    Ok(Arc::new(process))
}

let spec = SupervisorSpec {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 3,
    max_seconds: 5,
    children: vec![
        ChildSpec {
            id: "worker1".to_string(),
            start: start_worker,
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Timeout(5000),
            child_type: ChildType::Worker,
        },
    ],
};

let supervisor = Supervisor::spawn(spec)?;
// Child failures are consumed automatically from runtime monitor events.
supervisor.shutdown()?;
```

`shutdown` terminates active children in reverse specification order. Restart-intensity exhaustion and restart failures stop the supervisor with an error that can propagate to its own links or monitors. The direct `Supervisor::new` bookkeeping API remains available, but `Supervisor::spawn` is the automatic production path.

### GenEvent

In-memory event fan-out with lock-free user-code invocation and per-handler outcomes.

```rust
use lunatic_otp_patterns::{GenEvent, HandlerOutcome, LogEvent};

let events = GenEvent::<LogEvent>::new();
events
    .add_handler("console".to_string(), |event| {
        println!("{event:?}");
    })
    .await;
events
    .add_fallible_handler("checked".to_string(), |_event| -> Result<(), &'static str> {
        Ok(())
    })
    .await;

let report = events
    .notify(LogEvent::Info("ready".to_string()))
    .await;
assert_eq!(report.outcome("console"), Some(&HandlerOutcome::Succeeded));

let targeted = events
    .notify_handler("checked", LogEvent::Info("only once".to_string()))
    .await;
assert_eq!(targeted, Some(HandlerOutcome::Succeeded));
```

`notify` snapshots the current handler IDs and `Arc` callbacks, releases the `RwLock`, then runs all
snapshotted callbacks concurrently on Tokio's blocking pool. A handler added after the snapshot
does not receive that event. Removing or replacing a handler does not cancel a delivery that was
already snapshotted. Panic and returned errors are isolated in `NotifyReport`; `notify` waits for
all snapshotted outcomes before returning.

After configuring handlers, `events.spawn(GenEventConfig::default())` moves the manager into a
native Lunatic process. `GenEventHandle::notify`, `stop`, `kill`, and `wait_for_exit` then use its
mailbox and lifecycle path.

### GenStatem

Generic state machine pattern for implementing finite state machines with event-driven transitions.

```rust
use lunatic_otp_patterns::{GenStatem, StateData};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum DoorState {
    Locked,
    Unlocked,
}

impl GenStatem for DoorState {
    type State = Self;
    type Data = DoorData;
    type Event = DoorEvent;

    fn init() -> StateData<Self::State, Self::Data> {
        StateData::new(DoorState::Locked, DoorData { code: "1234".to_string() })
    }

    fn handle_event(&mut self, state: Self::State, event: Self::Event, data: Self::Data)
        -> StateData<Self::State, Self::Data> {
        match (state, event) {
            (DoorState::Locked, DoorEvent::EnterCode(code)) => {
                if code == data.code {
                    StateData::new(DoorState::Unlocked, data)
                } else {
                    StateData::new(DoorState::Locked, data)
                }
            }
            // ... other transitions
        }
    }
}
```

`machine.spawn(GenStatemConfig::default())` runs transitions, timeout callbacks, entry/exit
callbacks, and termination inside a native Lunatic process. The handle exposes `send_event`,
`timeout`, `stop`, `kill`, and lifecycle waiting.

## Runtime boundary

The native adapter flow is:

1. Implement the trait for your server/state machine
2. Enter a multi-thread Tokio runtime and call the pattern's `spawn` entry point
3. Use mailbox-backed call/cast messages and correlated replies
4. For Supervisor children, pass the supplied environment to the child spawn function; the supervisor registers and consumes monitor events automatically
5. Stop or kill the process through its handle or Supervisor

Guest SDKs can encode `GuestOtpHeader` and use existing `lunatic::message` imports. Calls wait on a
non-zero correlation tag, casts do not carry a reply target, and replies use both the `Reply`
envelope and the same mailbox tag. Stop is acknowledged with that reply envelope before exit.
The WAT fixture in `tests/otp_guest_wasm.rs` is executable ABI evidence; it is not yet a packaged
high-level API for every advertised guest language.

## Core Values Compliance

The API is intended to align with Lunatic's core values while keeping its evidence boundary explicit:

- **Fast, Robust, and Scalable**: GenServer uses lightweight native Lunatic processes; end-to-end performance thresholds are not yet established
- **Language Independence**: Native adapters are Rust; the OTP1 guest envelope is explicit little-endian data over language-neutral host imports
- **Security Through Isolation**: The native process is registered in a Lunatic environment; this is not a Wasm sandbox boundary
- **Fault Tolerance**: GenServer exit/error handling and automatic Supervisor monitor-driven replacement/escalation are implemented locally
- **Asynchronous by Default**: Casts and events use mailbox delivery; synchronous calls add correlated replies and timeouts; GenEvent runs synchronous callbacks concurrently outside its registry lock
- **Erlang-Inspired**: Callback and strategy APIs are modeled after OTP

## License

MIT OR Apache-2.0
