# Lunatic OTP Patterns

This crate contains Erlang/OTP-inspired callback traits and runtime adapters for Lunatic processes.

## Current implementation status

GenServer is connected to Lunatic's host-side native process runtime; the other patterns remain partial:

- `GenServer::spawn` registers a native Lunatic process and consumes requests through its `MessageMailbox`.
- `call` uses per-request correlation IDs, configurable timeouts, and propagates handler or process-exit errors.
- `cast`, graceful `stop`, forced `kill`, `is_alive`, and termination waiting are implemented.
- `crates/lunatic-otp-patterns/tests/gen_server_runtime.rs` exercises the public API against real native Lunatic processes.
- Supervisor strategy bookkeeping uses caller-provided start closures and stored numeric IDs; shutdown does not terminate a Lunatic process.
- GenStatem tests drive transitions directly in memory.
- The Rust example now uses the process-backed GenServer API.

The current GenServer adapter requires a multi-thread Tokio runtime and runs as a host-side native Lunatic process. A guest-WASM SDK adapter and named-process registration are still pending.

## Overview

Lunatic implements actor-model primitives inspired by Erlang/BEAM. This crate builds higher-level patterns on those primitives; GenServer now has process creation, mailbox, reply correlation, timeout, exit, and termination integration.

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

In-memory supervision strategy bookkeeping. The current implementation can invoke a supplied start closure, but it does not stop or monitor real Lunatic processes.

```rust
use lunatic_otp_patterns::{Supervisor, SupervisorSpec, RestartStrategy, ChildSpec};

let spec = SupervisorSpec {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 3,
    max_seconds: 5,
    children: vec![
        ChildSpec {
            id: "worker1".to_string(),
            start: || Ok(12345), // Placeholder numeric ID, not a spawned process
            restart: RestartPolicy::Permanent,
            shutdown: ShutdownPolicy::Timeout(5000),
            child_type: ChildType::Worker,
        },
    ],
};

let mut supervisor = Supervisor::new(spec);
supervisor.start_children()?;
```

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
2. Enter a multi-thread Tokio runtime and call `GenServer::spawn`
3. Use mailbox-backed call/cast messages and correlated replies
4. Stop or kill the process through its handle

`examples/rust/src/gen_server_example.rs` demonstrates this path. It is not yet a guest-WASM binding: guest SDK host imports and cross-language message compatibility remain separate follow-up work.

## Core Values Compliance

The API is intended to align with Lunatic's core values. Claims below distinguish the connected GenServer path from the still-partial patterns:

- **Fast, Robust, and Scalable**: GenServer uses lightweight native Lunatic processes; end-to-end performance thresholds are not yet established
- **Language Independence**: Pure Rust with serde serialization
- **Security Through Isolation**: The native process is registered in a Lunatic environment; this is not a Wasm sandbox boundary
- **Fault Tolerance**: GenServer exit/error handling exists; Supervisor recovery of real processes is pending
- **Asynchronous by Default**: Casts use mailbox delivery; synchronous calls add correlated replies and timeouts
- **Erlang-Inspired**: Callback and strategy APIs are modeled after OTP

## License

MIT OR Apache-2.0
