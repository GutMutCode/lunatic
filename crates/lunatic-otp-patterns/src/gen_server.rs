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
//!     fn handle_call(&mut self, request: Self::Call) -> anyhow::Result<Self::CallReply> {
//!         match request {
//!             Request::Increment => {
//!                 self.count += 1;
//!                 Ok(Response::Ok)
//!             }
//!             Request::Decrement => {
//!                 self.count -= 1;
//!                 Ok(Response::Ok)
//!             }
//!             Request::Get => Ok(Response::Value(self.count)),
//!         }
//!     }
//!
//!     fn handle_cast(&mut self, request: Self::Cast) -> anyhow::Result<()> {
//!         match request {
//!             Request::Increment => self.count += 1,
//!             Request::Decrement => self.count -= 1,
//!             Request::Get => {} // Cast ignores return value
//!         }
//!         Ok(())
//!     }
//! }
//! ```

use anyhow::{anyhow, Context, Result};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::{DataMessage, Message},
    spawn_native, NativeProcess, Process, Signal,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::Debug,
    marker::PhantomData,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Condvar, Mutex, OnceLock,
    },
    time::Duration,
};

/// GenServer behavior trait
///
/// Implement this trait to create a generic server process following
/// Erlang's gen_server pattern.
pub trait GenServer: Sized {
    /// Server state type
    type State;

    /// Synchronous call request type
    type Call: Serialize + DeserializeOwned + Debug + Send + 'static;

    /// Synchronous call reply type
    type CallReply: Serialize + DeserializeOwned + Debug + Send + 'static;

    /// Asynchronous cast request type
    type Cast: Serialize + DeserializeOwned + Debug + Send + 'static;

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
    fn spawn(
        config: GenServerConfig,
    ) -> Result<GenServerHandle<Self::Call, Self::Cast, Self::CallReply>>
    where
        Self: GenServer<State = Self> + Send + 'static,
    {
        let runtime = tokio::runtime::Handle::try_current()
            .context("GenServer::spawn requires a multi-thread Tokio runtime")?;
        if matches!(
            runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        ) {
            return Err(anyhow!(
                "GenServer::spawn requires a multi-thread Tokio runtime"
            ));
        }

        let environment = gen_server_environment();
        let shared = Arc::new(GenServerShared::new(environment.clone()));
        let process_shared = shared.clone();

        let (join, process) = spawn_native(environment, move |_process, mailbox| async move {
            let mut exit_guard = ExitGuard::new(process_shared.clone());
            let mut server = Self::init();

            loop {
                let message = mailbox.pop(None).await;
                let data = match message {
                    Message::Data(data) => data,
                    Message::LinkDied(_) => continue,
                    Message::ProcessDied(process_id) => {
                        server.handle_info(process_id.to_le_bytes().to_vec());
                        continue;
                    }
                };

                let message: ServerMessage<Self::Call, Self::Cast> =
                    bincode::deserialize(&data.buffer).context("invalid GenServer message")?;

                match message {
                    ServerMessage::Call { request, reply_to } => {
                        let response = server
                            .handle_call(request)
                            .and_then(|reply| {
                                bincode::serialize(&ServerReply { reply })
                                    .context("failed to serialize GenServer reply")
                            })
                            .map_err(|error| error.to_string());
                        process_shared.reply(reply_to, response);
                    }
                    ServerMessage::Cast { request } => {
                        if let Err(error) = server.handle_cast(request) {
                            server.terminate(TerminateReason::Shutdown);
                            exit_guard.fail(error.to_string());
                            return Err(error).context("GenServer cast handler failed");
                        }
                    }
                    ServerMessage::Info { data } => server.handle_info(data),
                    ServerMessage::Stop { reason } => {
                        server.terminate(reason);
                        exit_guard.complete();
                        return Ok(());
                    }
                }
            }
        });

        let join_shared = shared.clone();
        tokio::spawn(async move {
            let status = match join.await {
                Ok(Ok(())) => ExitStatus::Normal,
                Ok(Err(error)) => ExitStatus::Failed(error.to_string()),
                Err(error) => ExitStatus::Failed(format!("process task failed: {error}")),
            };
            join_shared.finish(status);
        });

        Ok(GenServerHandle::from_process(process, shared, config))
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

type PendingReply = std::result::Result<Vec<u8>, String>;

#[derive(Debug, Clone)]
enum ExitStatus {
    Normal,
    Failed(String),
}

struct GenServerShared {
    environment: Arc<LunaticEnvironment>,
    pending: Mutex<HashMap<u64, mpsc::Sender<PendingReply>>>,
    next_request_id: AtomicU64,
    exit: (Mutex<Option<ExitStatus>>, Condvar),
}

impl GenServerShared {
    fn new(environment: Arc<LunaticEnvironment>) -> Self {
        Self {
            environment,
            pending: Mutex::new(HashMap::new()),
            next_request_id: AtomicU64::new(1),
            exit: (Mutex::new(None), Condvar::new()),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    fn reply(&self, request_id: u64, reply: PendingReply) {
        if let Some(sender) = self
            .pending
            .lock()
            .expect("GenServer pending replies mutex poisoned")
            .remove(&request_id)
        {
            let _ = sender.send(reply);
        }
    }

    fn finish(&self, status: ExitStatus) {
        let (exit, condvar) = &self.exit;
        let mut exit = exit.lock().expect("GenServer exit mutex poisoned");
        if exit.is_some() {
            return;
        }

        let pending_error = match &status {
            ExitStatus::Normal => "GenServer stopped".to_string(),
            ExitStatus::Failed(error) => format!("GenServer terminated: {error}"),
        };
        *exit = Some(status);
        drop(exit);

        let mut pending = self
            .pending
            .lock()
            .expect("GenServer pending replies mutex poisoned");
        for (_, sender) in pending.drain() {
            let _ = sender.send(Err(pending_error.clone()));
        }
        condvar.notify_all();
    }

    fn exit_status(&self) -> Option<ExitStatus> {
        self.exit
            .0
            .lock()
            .expect("GenServer exit mutex poisoned")
            .clone()
    }

    fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<ExitStatus> {
        let (exit, condvar) = &self.exit;
        let exit = exit.lock().expect("GenServer exit mutex poisoned");
        let exit = match timeout {
            Some(timeout) => {
                let (exit, wait) = condvar
                    .wait_timeout_while(exit, timeout, |status| status.is_none())
                    .expect("GenServer exit mutex poisoned");
                if wait.timed_out() && exit.is_none() {
                    return Err(anyhow!("timed out waiting for GenServer to exit"));
                }
                exit
            }
            None => condvar
                .wait_while(exit, |status| status.is_none())
                .expect("GenServer exit mutex poisoned"),
        };

        exit.clone()
            .ok_or_else(|| anyhow!("GenServer exit status unavailable"))
    }
}

struct ExitGuard {
    shared: Arc<GenServerShared>,
    status: Option<ExitStatus>,
}

impl ExitGuard {
    fn new(shared: Arc<GenServerShared>) -> Self {
        Self {
            shared,
            status: None,
        }
    }

    fn complete(&mut self) {
        self.status = Some(ExitStatus::Normal);
    }

    fn fail(&mut self, error: String) {
        self.status = Some(ExitStatus::Failed(error));
    }
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        self.shared.finish(
            self.status
                .take()
                .unwrap_or_else(|| ExitStatus::Failed("process exited unexpectedly".to_string())),
        );
    }
}

fn gen_server_environment() -> Arc<LunaticEnvironment> {
    static ENVIRONMENT: OnceLock<Arc<LunaticEnvironment>> = OnceLock::new();
    ENVIRONMENT
        .get_or_init(|| Arc::new(LunaticEnvironment::new(0)))
        .clone()
}

/// GenServer handle for client interactions
///
/// Provides call() and cast() methods to interact with the server
pub struct GenServerHandle<Call, Cast, Reply> {
    pub process_id: u64,
    process: NativeProcess,
    shared: Arc<GenServerShared>,
    config: GenServerConfig,
    _phantom: PhantomData<(Call, Cast, Reply)>,
}

impl<Call, Cast, Reply> Clone for GenServerHandle<Call, Cast, Reply> {
    fn clone(&self) -> Self {
        Self {
            process_id: self.process_id,
            process: self.process.clone(),
            shared: self.shared.clone(),
            config: self.config.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<Call, Cast, Reply> std::fmt::Debug for GenServerHandle<Call, Cast, Reply>
where
    Call: Serialize + DeserializeOwned + Debug,
    Cast: Serialize + DeserializeOwned + Debug,
    Reply: Serialize + DeserializeOwned + Debug,
{
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GenServerHandle")
            .field("process_id", &self.process_id)
            .field("name", &self.config.name)
            .field("alive", &self.is_alive())
            .finish()
    }
}

impl<Call, Cast, Reply> GenServerHandle<Call, Cast, Reply>
where
    Call: Serialize + DeserializeOwned + Debug,
    Cast: Serialize + DeserializeOwned + Debug,
    Reply: Serialize + DeserializeOwned + Debug,
{
    fn from_process(
        process: NativeProcess,
        shared: Arc<GenServerShared>,
        config: GenServerConfig,
    ) -> Self {
        let process_id = process.id();
        Self {
            process_id,
            process,
            shared,
            config,
            _phantom: PhantomData,
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
        self.ensure_running()?;

        let request_id = self.shared.next_request_id();
        let (sender, receiver) = mpsc::channel();
        self.shared
            .pending
            .lock()
            .expect("GenServer pending replies mutex poisoned")
            .insert(request_id, sender);

        if let Err(error) = self.send_message(ServerMessage::Call {
            request,
            reply_to: request_id,
        }) {
            self.shared
                .pending
                .lock()
                .expect("GenServer pending replies mutex poisoned")
                .remove(&request_id);
            return Err(error);
        }

        let response = match self.config.timeout_ms {
            Some(timeout_ms) => receiver
                .recv_timeout(Duration::from_millis(timeout_ms))
                .map_err(|error| match error {
                    mpsc::RecvTimeoutError::Timeout => anyhow!(
                        "GenServer call {} to process {} timed out after {} ms",
                        request_id,
                        self.process_id,
                        timeout_ms
                    ),
                    mpsc::RecvTimeoutError::Disconnected => anyhow!(
                        "GenServer process {} terminated before replying to call {}",
                        self.process_id,
                        request_id
                    ),
                }),
            None => receiver.recv().map_err(|_| {
                anyhow!(
                    "GenServer process {} terminated before replying to call {}",
                    self.process_id,
                    request_id
                )
            }),
        };

        let response = match response {
            Ok(response) => response,
            Err(error) => {
                self.shared
                    .pending
                    .lock()
                    .expect("GenServer pending replies mutex poisoned")
                    .remove(&request_id);
                return Err(error);
            }
        }
        .map_err(anyhow::Error::msg)?;
        let reply: ServerReply<Reply> =
            bincode::deserialize(&response).context("invalid GenServer reply")?;
        Ok(reply.reply)
    }

    /// Send an asynchronous cast to the server
    ///
    /// This will not wait for a response
    pub fn cast(&self, request: Cast) -> Result<()> {
        self.ensure_running()?;
        self.send_message(ServerMessage::Cast { request })
    }

    /// Sends an untyped information message to the server.
    pub fn info(&self, data: Vec<u8>) -> Result<()> {
        self.ensure_running()?;
        self.send_message(ServerMessage::Info { data })
    }

    /// Stops the server gracefully and waits for its termination callback.
    pub fn stop(&self, reason: TerminateReason) -> Result<()> {
        self.ensure_running()?;
        self.send_message(ServerMessage::Stop { reason })?;
        match self.shared.wait_for_exit(self.config.timeout())? {
            ExitStatus::Normal => Ok(()),
            ExitStatus::Failed(error) => Err(anyhow!("GenServer terminated: {error}")),
        }
    }

    /// Kills the underlying Lunatic process and waits until it exits.
    pub fn kill(&self) -> Result<()> {
        self.ensure_running()?;
        self.process.send(Signal::Kill);
        self.shared.wait_for_exit(self.config.timeout()).map(|_| ())
    }

    /// Waits for the process to exit, returning an error for abnormal exits.
    pub fn wait_for_exit(&self, timeout: Option<Duration>) -> Result<()> {
        match self.shared.wait_for_exit(timeout)? {
            ExitStatus::Normal => Ok(()),
            ExitStatus::Failed(error) => Err(anyhow!("GenServer terminated: {error}")),
        }
    }

    /// Returns true while the process is registered in its Lunatic environment.
    pub fn is_alive(&self) -> bool {
        self.shared.exit_status().is_none()
            && self
                .shared
                .environment
                .get_process(self.process_id)
                .is_some()
    }

    fn ensure_running(&self) -> Result<()> {
        match self.shared.exit_status() {
            None if self.is_alive() => Ok(()),
            Some(ExitStatus::Normal) => Err(anyhow!("GenServer process {} has stopped", self.id())),
            Some(ExitStatus::Failed(error)) => Err(anyhow!(
                "GenServer process {} terminated: {}",
                self.id(),
                error
            )),
            None => Err(anyhow!(
                "GenServer process {} is not registered in its environment",
                self.id()
            )),
        }
    }

    fn send_message(&self, message: ServerMessage<Call, Cast>) -> Result<()> {
        let payload =
            bincode::serialize(&message).context("failed to serialize GenServer message")?;
        let message = DataMessage::new_from_vec(None, payload);
        self.process.send(Signal::Message(Message::Data(message)));
        Ok(())
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

impl GenServerConfig {
    fn timeout(&self) -> Option<Duration> {
        self.timeout_ms.map(Duration::from_millis)
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
        EchoAfter { value: i32, delay_ms: u64 },
        Fail,
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
        Fail,
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
                TestCall::EchoAfter { value, delay_ms } => {
                    std::thread::sleep(Duration::from_millis(delay_ms));
                    Ok(TestReply::Value(value))
                }
                TestCall::Fail => Err(anyhow!("call handler failed")),
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
                TestCast::Fail => Err(anyhow!("cast handler failed")),
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

    #[tokio::test(flavor = "current_thread")]
    async fn spawn_rejects_current_thread_runtime() {
        let error = match TestServer::spawn(GenServerConfig::default()) {
            Ok(_) => panic!("current-thread runtime should be rejected"),
            Err(error) => error,
        };

        assert!(error
            .to_string()
            .contains("requires a multi-thread Tokio runtime"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn process_call_cast_and_graceful_stop() {
        let handle = TestServer::spawn(GenServerConfig::default()).unwrap();
        assert!(handle.is_alive());

        handle.cast(TestCast::Increment).unwrap();
        let reply = handle.call(TestCall::Get).unwrap();
        assert!(matches!(reply, TestReply::Value(1)));

        handle.call(TestCall::Set(41)).unwrap();
        handle.cast(TestCast::Increment).unwrap();
        let reply = handle.call(TestCall::Get).unwrap();
        assert!(matches!(reply, TestReply::Value(42)));

        handle.stop(TerminateReason::Normal).unwrap();
        assert!(!handle.is_alive());
        assert!(handle.call(TestCall::Get).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_calls_keep_their_correlated_replies() {
        let handle = TestServer::spawn(GenServerConfig::default()).unwrap();
        let first = handle.clone();
        let second = handle.clone();

        let first = std::thread::spawn(move || {
            first.call(TestCall::EchoAfter {
                value: 11,
                delay_ms: 30,
            })
        });
        let second = std::thread::spawn(move || {
            second.call(TestCall::EchoAfter {
                value: 22,
                delay_ms: 0,
            })
        });

        assert!(matches!(
            first.join().unwrap().unwrap(),
            TestReply::Value(11)
        ));
        assert!(matches!(
            second.join().unwrap().unwrap(),
            TestReply::Value(22)
        ));
        handle.stop(TerminateReason::Normal).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn call_timeout_removes_pending_reply_and_server_recovers() {
        let handle = TestServer::spawn(GenServerConfig {
            name: None,
            timeout_ms: Some(20),
        })
        .unwrap();

        let error = handle
            .call(TestCall::EchoAfter {
                value: 7,
                delay_ms: 80,
            })
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(matches!(
            handle.call(TestCall::Get).unwrap(),
            TestReply::Value(0)
        ));
        handle.stop(TerminateReason::Normal).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn handler_errors_are_propagated_and_fatal_cast_exits() {
        let handle = TestServer::spawn(GenServerConfig::default()).unwrap();

        let error = handle.call(TestCall::Fail).unwrap_err();
        assert!(error.to_string().contains("call handler failed"));
        assert!(handle.is_alive());

        handle.cast(TestCast::Fail).unwrap();
        let error = handle
            .wait_for_exit(Some(Duration::from_secs(1)))
            .unwrap_err();
        assert!(error.to_string().contains("cast handler failed"));
        assert!(!handle.is_alive());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn kill_terminates_the_lunatic_process() {
        let handle = TestServer::spawn(GenServerConfig::default()).unwrap();
        assert!(handle.is_alive());

        handle.kill().unwrap();
        assert!(!handle.is_alive());
        assert!(handle.cast(TestCast::Increment).is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graceful_stop_runs_terminate_callback() {
        use std::sync::atomic::{AtomicBool, Ordering};

        static TERMINATED: AtomicBool = AtomicBool::new(false);

        #[derive(Debug)]
        struct TerminatingServer;

        #[derive(Debug, Serialize, Deserialize)]
        struct Ping;

        impl GenServer for TerminatingServer {
            type State = Self;
            type Call = Ping;
            type CallReply = Ping;
            type Cast = Ping;

            fn init() -> Self::State {
                TERMINATED.store(false, Ordering::SeqCst);
                Self
            }

            fn handle_call(&mut self, request: Self::Call) -> Result<Self::CallReply> {
                Ok(request)
            }

            fn handle_cast(&mut self, _request: Self::Cast) -> Result<()> {
                Ok(())
            }

            fn terminate(&mut self, _reason: TerminateReason) {
                TERMINATED.store(true, Ordering::SeqCst);
            }
        }

        let handle = TerminatingServer::spawn(GenServerConfig::default()).unwrap();
        handle.stop(TerminateReason::Normal).unwrap();
        assert!(TERMINATED.load(Ordering::SeqCst));
    }
}
