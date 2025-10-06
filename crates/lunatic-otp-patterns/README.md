# Lunatic OTP Patterns

This crate provides Erlang/OTP-inspired behavior patterns for Lunatic WASM processes, enabling fault-tolerant and scalable application development.

## Overview

Lunatic implements the actor model inspired by Erlang/BEAM, and this crate provides higher-level abstractions that make it easier to build robust distributed systems.

## Patterns

### GenServer

Generic server pattern for implementing stateful server processes with synchronous and asynchronous message handling.

```rust
use lunatic_otp_patterns::{GenServer, GenServerConfig};
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

    fn handle_call(&mut self, request: Self::Call) -> Self::CallReply {
        match request {
            CounterRequest::Increment => {
                self.count += 1;
                CounterResponse::Ok
            }
            CounterRequest::Get => CounterResponse::Value(self.count),
            // ... other handlers
        }
    }

    fn handle_cast(&mut self, request: Self::Cast) {
        match request {
            CounterRequest::Increment => self.count += 1,
            // ... other handlers
        }
    }
}
```

### Supervisor

Process supervision with configurable restart strategies for building fault-tolerant systems.

```rust
use lunatic_otp_patterns::{Supervisor, SupervisorSpec, RestartStrategy, ChildSpec};

let spec = SupervisorSpec {
    strategy: RestartStrategy::OneForOne,
    max_restarts: 3,
    max_seconds: 5,
    children: vec![
        ChildSpec {
            id: "worker1".to_string(),
            start: || Ok(12345), // Process ID
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

These patterns are designed to be used from within WASM guest code. In a real Lunatic application, you would:

1. Implement the trait for your server/state machine
2. Spawn processes using the Lunatic runtime APIs
3. Use message passing for communication

See `examples/rust/src/gen_server_example.rs` for a complete example.

## Core Values Compliance

This crate adheres to Lunatic's core values:

- **Fast, Robust, and Scalable**: Lightweight processes with supervision
- **Language Independence**: Pure Rust with serde serialization
- **Security Through Isolation**: Process-based isolation
- **Fault Tolerance**: Supervisor patterns and restart strategies
- **Asynchronous by Default**: Message-passing concurrency
- **Erlang-Inspired**: Direct implementation of OTP patterns

## License

MIT OR Apache-2.0