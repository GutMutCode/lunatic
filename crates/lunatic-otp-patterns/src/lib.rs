//! OTP-Inspired Patterns for Lunatic
//!
//! This crate provides Erlang/OTP behavior patterns for Lunatic WASM processes:
//! - **GenServer**: Generic server with synchronous and asynchronous calls
//! - **Supervisor**: Process supervision with restart strategies
//! - **GenStatem**: Generic state machine with event-driven transitions
//!
//! These patterns are designed to be used from WASM guest code and provide
//! the same fault-tolerance and concurrency primitives that make Erlang/OTP successful.

pub mod gen_server;
pub mod supervisor;
pub mod gen_statem;

pub use gen_server::{GenServer, GenServerHandle, GenServerConfig, TerminateReason, ServerMessage, ServerReply};
pub use supervisor::{Supervisor, SupervisorSpec, RestartStrategy, ChildSpec, ChildType, RestartPolicy, ShutdownPolicy, ExitReason, ChildInfo, ChildrenCount};
pub use gen_statem::{GenStatem, StateData, TransitionResult, StopReason, GenStatemHandle, StatemMessage};
