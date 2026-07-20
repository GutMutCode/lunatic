//! OTP-Inspired Patterns for Lunatic
//!
//! This crate provides Erlang/OTP behavior patterns for Lunatic WASM processes:
//! - **GenServer**: Generic server with synchronous and asynchronous calls
//! - **Supervisor**: Process supervision with restart strategies
//! - **GenStatem**: Generic state machine with event-driven transitions
//!
//! These patterns are designed to be used from WASM guest code and provide
//! the same fault-tolerance and concurrency primitives that make Erlang/OTP successful.

pub mod error;
pub mod gen_event;
pub mod gen_server;
pub mod gen_statem;
pub mod supervisor;

pub use error::{OtpError, OtpResult};
pub use gen_event::{
    Event, GenEvent, LogEvent, MetricEvent, TerminateReason as EventTerminateReason,
};
pub use gen_server::{
    GenServer, GenServerConfig, GenServerHandle, ServerMessage, ServerReply, TerminateReason,
};
pub use gen_statem::{
    GenStatem, GenStatemHandle, StateData, StatemMessage, StopReason, TransitionResult,
};
pub use supervisor::{
    ChildInfo, ChildSpec, ChildStart, ChildType, ChildrenCount, ExitReason, RestartPolicy,
    RestartStrategy, ShutdownPolicy, Supervisor, SupervisorSpec,
};
