//! OTP-Inspired Patterns for Lunatic
//!
//! This crate provides Erlang/OTP-inspired process runtime adapters:
//! - **GenServer**: Generic server with synchronous and asynchronous calls
//! - **Supervisor**: Process supervision with restart strategies
//! - **GenEvent**: Isolated concurrent event fan-out with targeted delivery
//! - **GenStatem**: Generic state machine with event-driven transitions
//!
//! All four patterns have native Lunatic-process paths. Supervisor consumes
//! acknowledged runtime monitor events automatically, named GenServers use the
//! bounded production local registry, and [`guest`] defines the language-neutral
//! OTP1 envelope used by guest-Wasm SDKs over existing message imports.

pub mod error;
pub mod gen_event;
pub mod gen_server;
pub mod gen_statem;
pub mod guest;
pub mod supervisor;

pub use error::{OtpError, OtpResult};
pub use gen_event::{
    Event, GenEvent, GenEventConfig, GenEventHandle, HandlerOutcome, LogEvent, MetricEvent,
    NotifyReport, TerminateReason as EventTerminateReason,
};
pub use gen_server::{
    GenServer, GenServerConfig, GenServerHandle, GenServerRuntime, ServerMessage, ServerReply,
    TerminateReason,
};
pub use gen_statem::{
    GenStatem, GenStatemConfig, GenStatemHandle, StateData, StatemMessage, StopReason,
    TransitionResult,
};
pub use guest::{
    decode_guest_otp_message, encode_guest_otp_message, GuestOtpHeader, GuestOtpKind,
    GUEST_OTP_BACKPRESSURE, GUEST_OTP_HEADER_LEN, GUEST_OTP_MAGIC, GUEST_OTP_OK,
    GUEST_OTP_TARGET_MISSING, GUEST_OTP_TIMEOUT,
};
pub use supervisor::{
    ChildInfo, ChildSpec, ChildStart, ChildType, ChildrenCount, ExitReason, RestartPolicy,
    RestartStrategy, ShutdownPolicy, Supervisor, SupervisorHandle, SupervisorSpec,
};
