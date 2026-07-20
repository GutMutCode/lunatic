//! OTP-Inspired Patterns for Lunatic
//!
//! This crate provides Erlang/OTP-inspired host-side runtime adapters and
//! in-memory behavior patterns:
//! - **GenServer**: Generic server with synchronous and asynchronous calls
//! - **Supervisor**: Process supervision with restart strategies
//! - **GenEvent**: Isolated concurrent event fan-out with targeted delivery
//! - **GenStatem**: Generic state machine with event-driven transitions
//!
//! GenServer and Supervisor connect to host-side native Lunatic processes.
//! GenEvent and GenStatem remain in-memory patterns. Guest-WASM adapters are
//! separate future work.

pub mod error;
pub mod gen_event;
pub mod gen_server;
pub mod gen_statem;
pub mod supervisor;

pub use error::{OtpError, OtpResult};
pub use gen_event::{
    Event, GenEvent, HandlerOutcome, LogEvent, MetricEvent, NotifyReport,
    TerminateReason as EventTerminateReason,
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
