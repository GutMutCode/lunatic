//! GenServer: Generic Server Pattern
//!
//! Inspired by Erlang's `gen_server`, this module provides a behavior pattern
//! for implementing stateful server processes with synchronous and asynchronous
//! message handling.
//!
//! ## Example
//!
//! ```ignore
//! use lunatic_otp_patterns::GenServer;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Counter {
//!     count: i64,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! enum Request {
//!     Increment,
//!     Decrement,
//!     Get,
//! }
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! enum Response {
//!     Ok,
//!     Value(i64),
//! }
//!
//! impl GenServer for Counter {
//!     type State = Self;
//!     type Call = Request;
//!     type CallReply = Response;
//!     type Cast = Request;
//!
//!     fn init() -> Self::State {
//!         Counter { count: 0 }
//!     }
//!
//!     fn handle_call(&mut self, request: Self::Call) -> Self::CallReply {
//!         match request {
//!             Request::Increment => {
//!                 self.count += 1;
//!                 Response::Ok
//!             }
//!             Request::Decrement => {
//!                 self.count -= 1;
//!                 Response::Ok
//!             }
//!             Request::Get => Response::Value(self.count),
//!         }
//!     }
//!
//!     fn handle_cast(&mut self, request: Self::Cast) {
//!         match request {
//!             Request::Increment => self.count += 1,
//!             Request::Decrement => self.count -= 1,
//!             Request::Get => {} // Cast ignores return value
//!         }
//!     }
//! }
//! ```

use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use anyhow::Result;

/// GenServer behavior trait
///
/// Implement this trait to create a generic server process following
/// Erlang's gen_server pattern.
pub trait GenServer: Sized {
    /// Server state type
    type State;

    /// Synchronous call request type
    type Call: Serialize + for<'de> Deserialize<'de> + Debug;

    /// Synchronous call reply type
    type CallReply: Serialize + for<'de> Deserialize<'de> + Debug;

    /// Asynchronous cast request type
    type Cast: Serialize + for<'de> Deserialize<'de> + Debug;

    /// Initialize server state
    ///
    /// Called when the server process starts
    fn init() -> Self::State;

    /// Handle synchronous call
    ///
    /// Receives request, modifies state, returns reply to caller
    fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply>;

    /// Handle asynchronous cast
    ///
    /// Receives request, modifies state, no reply sent
    fn handle_cast(&mut self, request: Self::Cast) -> Result<()>;

    /// Handle generic info message (optional)
    ///
    /// Override to handle messages not sent via call/cast
    fn handle_info(&mut self, _message: Vec<u8>) {
        // Default: ignore info messages
    }

    /// Termination callback (optional)
    ///
    /// Called when server is about to shut down
    fn terminate(&mut self, _reason: TerminateReason) {
        // Default: no-op
    }

    /// Spawn a new GenServer process
    ///
    /// This creates a new process running the GenServer message loop
    fn spawn(config: GenServerConfig) -> Result<GenServerHandle<Self::Call, Self::Cast, Self::CallReply>>
    where
        Self: Sized + 'static,
    {
        // Implementation will use lunatic-process-api
        // For now, return a placeholder error
        Err(anyhow::anyhow!("GenServer spawn not yet implemented"))
    }
}

/// Reason for server termination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminateReason {
    /// Normal shutdown
    Normal,
    /// Server crashed
    Shutdown,
    /// Supervisor killed server
    Killed,
    /// Custom reason
    Other(String),
}

/// GenServer message envelope
#[derive(Debug, Serialize, Deserialize)]
pub enum ServerMessage<Call, Cast> {
    /// Synchronous call (expects reply)
    Call { request: Call, reply_to: u64 },
    /// Asynchronous cast (no reply)
    Cast { request: Cast },
    /// Generic info message
    Info { data: Vec<u8> },
    /// Shutdown signal
    Stop { reason: TerminateReason },
}

/// GenServer reply message
#[derive(Debug, Serialize, Deserialize)]
pub struct ServerReply<Reply> {
    pub reply: Reply,
}

/// GenServer handle for client interactions
///
/// Provides call() and cast() methods to interact with the server
#[derive(Debug, Clone)]
pub struct GenServerHandle<Call, Cast, Reply> {
    pub process_id: u64,
    _phantom: std::marker::PhantomData<(Call, Cast, Reply)>,
}

impl<Call, Cast, Reply> GenServerHandle<Call, Cast, Reply>
where
    Call: Serialize + for<'de> Deserialize<'de> + Debug,
    Cast: Serialize + for<'de> Deserialize<'de> + Debug,
    Reply: Serialize + for<'de> Deserialize<'de> + Debug,
{
    /// Create a new handle for a server process
    pub fn new(process_id: u64) -> Self {
        Self {
            process_id,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Get the process ID
    pub fn id(&self) -> u64 {
        self.process_id
    }

    /// Send a synchronous call to the server
    ///
    /// This will block until the server responds
    pub fn call(&self, request: Call) -> Result<Reply> {
        // Implementation will use lunatic-messaging-api
        // For now, return a placeholder error
        Err(anyhow::anyhow!("GenServer call not yet implemented"))
    }

    /// Send an asynchronous cast to the server
    ///
    /// This will not wait for a response
    pub fn cast(&self, request: Cast) -> Result<()> {
        // Implementation will use lunatic-messaging-api
        // For now, return a placeholder error
        Err(anyhow::anyhow!("GenServer cast not yet implemented"))
    }
}

/// GenServer spawn configuration
#[derive(Debug, Clone)]
pub struct GenServerConfig {
    pub name: Option<String>,
    pub timeout_ms: Option<u64>,
}

impl Default for GenServerConfig {
    fn default() -> Self {
        Self {
            name: None,
            timeout_ms: Some(5000), // 5 second default timeout
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestServer {
        value: i32,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum TestCall {
        Get,
        Set(i32),
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum TestReply {
        Value(i32),
        Ok,
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum TestCast {
        Increment,
        Decrement,
    }

    impl GenServer for TestServer {
        type State = Self;
        type Call = TestCall;
        type CallReply = TestReply;
        type Cast = TestCast;

        fn init() -> Self::State {
            TestServer { value: 0 }
        }

        fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
            match request {
                TestCall::Get => Ok(TestReply::Value(self.value)),
                TestCall::Set(v) => {
                    self.value = v;
                    Ok(TestReply::Ok)
                }
            }
        }

        fn handle_cast(&mut self, request: Self::Cast) -> Result<()> {
            match request {
                TestCast::Increment => {
                    self.value += 1;
                    Ok(())
                }
                TestCast::Decrement => {
                    self.value -= 1;
                    Ok(())
                }
            }
        }
    }

    #[test]
    fn test_gen_server_trait_implementation() {
        let mut server = TestServer::init();
        assert_eq!(server.value, 0);

        // Test call
        let reply = server.handle_call(TestCall::Set(42)).unwrap();
        assert!(matches!(reply, TestReply::Ok));
        assert_eq!(server.value, 42);

        let reply = server.handle_call(TestCall::Get).unwrap();
        match reply {
            TestReply::Value(v) => assert_eq!(v, 42),
            _ => panic!("Expected Value"),
        }

        // Test cast
        server.handle_cast(TestCast::Increment).unwrap();
        assert_eq!(server.value, 43);

        server.handle_cast(TestCast::Decrement).unwrap();
        assert_eq!(server.value, 42);
    }

    #[test]
    fn test_server_message_serialization() {
        let call_msg: ServerMessage<TestCall, TestCast> = ServerMessage::Call {
            request: TestCall::Get,
            reply_to: 123,
        };

        // Test serialization round-trip
        let serialized = serde_json::to_string(&call_msg).unwrap();
        let deserialized: ServerMessage<TestCall, TestCast> =
            serde_json::from_str(&serialized).unwrap();

        match deserialized {
            ServerMessage::Call { request, reply_to } => {
                assert_eq!(reply_to, 123);
                assert!(matches!(request, TestCall::Get));
            }
            _ => panic!("Expected Call message"),
        }
    }

    #[test]
    fn test_terminate_reason() {
        let mut server = TestServer::init();
        server.terminate(TerminateReason::Normal);
        // Should not panic - default implementation is no-op
    }
}
