# Lunatic OTP Patterns

This crate contains Erlang/OTP-inspired callback traits, message envelopes, and in-memory strategy logic intended for future use with Lunatic WASM processes.

## Current implementation status

The crate is an API and algorithm scaffold, not a connected OTP runtime:

- `GenServer` callback traits and message serialization are implemented.
- `GenServer::spawn`, `GenServerHandle::call`, and `cast` return explicit “not yet implemented” errors.
- Supervisor strategy bookkeeping uses caller-provided start closures and stored numeric IDs; shutdown does not terminate a Lunatic process.
- GenStatem tests drive transitions directly in memory.
- The Rust example invokes callbacks directly; its process-based usage block is pseudo-code.

Use the crate to evaluate the proposed APIs, not as evidence of process-level fault tolerance.

## Overview

Lunatic implements actor-model primitives inspired by Erlang/BEAM. This crate sketches higher-level abstractions that still need process creation, mailbox, reply, timeout, exit, and termination integration.

## Patterns

### GenServer

Callback and message types for a stateful server pattern. Synchronous/asynchronous process communication is not wired yet.

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

## Usage in WASM

The intended future WASM flow is:

1. Implement the trait for your server/state machine
2. Spawn a Lunatic process through the future runtime adapter
3. Use mailbox-backed call/cast messages and correlated replies

Steps 2 and 3 are not implemented by this crate. `examples/rust/src/gen_server_example.rs` is a callback-only example and labels process usage as pseudo-code.

## Core Values Compliance

The proposed API is intended to align with Lunatic's core values, but the following are design goals rather than current runtime guarantees:

- **Fast, Robust, and Scalable**: Intended to use lightweight processes with supervision
- **Language Independence**: Pure Rust with serde serialization
- **Security Through Isolation**: Intended to inherit Lunatic process isolation once connected
- **Fault Tolerance**: Restart strategy logic exists; real process recovery is pending
- **Asynchronous by Default**: Message envelopes exist; mailbox transport is pending
- **Erlang-Inspired**: Callback and strategy APIs are modeled after OTP

## License

MIT OR Apache-2.0
