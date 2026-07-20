# Lunatic OTP Patterns

This crate contains Erlang/OTP-inspired callback traits and runtime adapters for Lunatic processes.

## Current implementation status

GenServer and Supervisor are connected to Lunatic's host-side native process runtime; the other patterns remain partial:

- `GenServer::spawn` registers a native Lunatic process and consumes requests through its `MessageMailbox`.
- `call` uses per-request correlation IDs, configurable timeouts, and propagates handler or process-exit errors.
- `cast`, graceful `stop`, forced `kill`, `is_alive`, and termination waiting are implemented.
- `crates/lunatic-otp-patterns/tests/gen_server_runtime.rs` exercises the public API against real native Lunatic processes.
- Supervisor child starters receive the managed `Environment` and return an actual `Process` handle.
- OneForOne, OneForAll, and RestForOne terminate old processes, restart in specification order, preserve restart counts, enforce restart intensity, and reject duplicate starts.
- `crates/lunatic-otp-patterns/tests/supervisor_runtime.rs` exercises strategies and restart policies against real native Lunatic processes.
- GenStatem tests drive transitions directly in memory.
- The Rust examples use the process-backed GenServer and Supervisor APIs.

The current GenServer and Supervisor adapters require a multi-thread Tokio runtime and manage host-side native Lunatic processes. Guest-WASM SDK adapters, named-process registration, and automatic Supervisor monitor-event intake are still pending.

## Overview

Lunatic implements actor-model primitives inspired by Erlang/BEAM. This crate builds higher-level patterns on those primitives; GenServer has mailbox and lifecycle integration, while Supervisor has real process shutdown and ordered restart integration.

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
    });
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

let mut supervisor = Supervisor::new(spec);
supervisor.start_children()?;
```

Call `handle_child_exit` after receiving a child exit notification. `shutdown` terminates active children in reverse specification order. The public example is `examples/rust/src/supervisor_example.rs`.

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

## Runtime boundary

The current host-side flow is:

1. Implement the trait for your server/state machine
2. Enter a multi-thread Tokio runtime and call `GenServer::spawn` or `Supervisor::start_children`
3. Use mailbox-backed call/cast messages and correlated replies
4. For Supervisor children, pass the supplied environment to the child spawn function and report exit reasons through `handle_child_exit`
5. Stop or kill the process through its handle or Supervisor

`examples/rust/src/gen_server_example.rs` and `examples/rust/src/supervisor_example.rs` demonstrate these paths. They are not yet guest-WASM bindings: guest SDK host imports and cross-language message compatibility remain separate follow-up work.

## Core Values Compliance

The API is intended to align with Lunatic's core values. Claims below distinguish the connected GenServer and Supervisor paths from the still-partial patterns:

- **Fast, Robust, and Scalable**: GenServer uses lightweight native Lunatic processes; end-to-end performance thresholds are not yet established
- **Language Independence**: Pure Rust with serde serialization
- **Security Through Isolation**: The native process is registered in a Lunatic environment; this is not a Wasm sandbox boundary
- **Fault Tolerance**: GenServer exit/error handling and Supervisor process replacement are implemented; automatic monitor-event intake is pending
- **Asynchronous by Default**: Casts use mailbox delivery; synchronous calls add correlated replies and timeouts
- **Erlang-Inspired**: Callback and strategy APIs are modeled after OTP

## License

MIT OR Apache-2.0
