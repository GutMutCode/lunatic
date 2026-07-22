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
use lunatic_distributed::distributed::{DistributedRegistry, GlobalProcessId};
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
    time::{Duration, Instant},
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
        Self::spawn_in(gen_server_runtime(), config)
    }

    /// Spawn a new GenServer in an explicitly supplied runtime context.
    ///
    /// `GenServerConfig::name` is registered in the context's node-local
    /// production registry before [`GenServer::init`] runs. Cluster-wide
    /// quorum registration is deliberately outside this synchronous API.
    fn spawn_in(
        runtime: GenServerRuntime,
        config: GenServerConfig,
    ) -> Result<GenServerHandle<Self::Call, Self::Cast, Self::CallReply>>
    where
        Self: GenServer<State = Self> + Send + 'static,
    {
        let tokio_runtime = tokio::runtime::Handle::try_current()
            .context("GenServer::spawn requires a multi-thread Tokio runtime")?;
        if matches!(
            tokio_runtime.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread
        ) {
            return Err(anyhow!(
                "GenServer::spawn requires a multi-thread Tokio runtime"
            ));
        }

        let environment = runtime.environment.clone();
        let shared = Arc::new(GenServerShared::new(environment.clone()));
        let process_shared = shared.clone();
        let (start_sender, start_receiver) = tokio::sync::oneshot::channel();

        let (join, process) = spawn_native(environment, move |_process, mailbox| async move {
            let mut exit_guard = ExitGuard::new(process_shared.clone());
            // Keep the owner lease declared after ExitGuard. Rust drops locals
            // in reverse order, so registry cleanup completes before finish()
            // wakes stop/kill/wait_for_exit callers.
            let _registration: Option<LocalNameRegistration> = start_receiver
                .await
                .context("GenServer start cancelled before registry registration")?;
            let mut server = Self::init();

            loop {
                let message = mailbox.pop(None).await;
                let data = match message {
                    Message::Data(data) => data,
                    Message::LinkDied(_) => continue,
                    Message::ProcessDied { process_id, .. } => {
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
        })?;

        let join_shared = shared.clone();
        tokio::spawn(async move {
            let status = match join.await {
                Ok(Ok(())) => ExitStatus::Normal,
                Ok(Err(error)) => ExitStatus::Failed(error.to_string()),
                Err(error) => ExitStatus::Failed(format!("process task failed: {error}")),
            };
            join_shared.finish(status);
        });

        let registration = match config.name.as_deref() {
            Some(name) => match runtime.register_local(name, process.id()) {
                Ok(registration) => Some(registration),
                Err(error) => {
                    drop(start_sender);
                    let _ = process.send(Signal::Kill);
                    return Err(error);
                }
            },
            None => None,
        };

        if let Err(registration) = start_sender.send(registration) {
            drop(registration);
            let _ = process.send(Signal::Kill);
            return Err(anyhow!("GenServer process exited before startup completed"));
        }

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

struct PendingReply {
    result: std::result::Result<Vec<u8>, String>,
    completed_at: Instant,
}

/// Runtime resources used by process-backed GenServers.
///
/// Named servers use the supplied [`DistributedRegistry`]'s local namespace.
/// That is the same bounded, owner-indexed registry used by the production
/// distributed runtime, but it does not perform cluster quorum registration.
#[derive(Clone)]
pub struct GenServerRuntime {
    environment: Arc<dyn Environment>,
    node_id: u64,
    registry: Arc<DistributedRegistry>,
}

impl GenServerRuntime {
    pub fn new(
        environment: Arc<dyn Environment>,
        node_id: u64,
        registry: Arc<DistributedRegistry>,
    ) -> Self {
        Self {
            environment,
            node_id,
            registry,
        }
    }

    pub fn environment(&self) -> Arc<dyn Environment> {
        self.environment.clone()
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn registry(&self) -> Arc<DistributedRegistry> {
        self.registry.clone()
    }

    /// Resolve a node-local name to its full process identity.
    pub fn whereis(&self, name: &str) -> Option<GlobalProcessId> {
        self.registry
            .lookup_local(name)
            .map(|entry| entry.global_pid)
    }

    fn register_local(&self, name: &str, process_id: u64) -> Result<LocalNameRegistration> {
        if self.environment.get_process(process_id).is_none() {
            return Err(anyhow!(
                "GenServer process {process_id} is not registered in environment {}",
                self.environment.id()
            ));
        }
        let global_pid = GlobalProcessId::new(self.node_id, self.environment.id(), process_id);
        self.registry.register_local(name, global_pid)?;
        Ok(LocalNameRegistration {
            registry: self.registry.clone(),
            global_pid,
        })
    }
}

struct LocalNameRegistration {
    registry: Arc<DistributedRegistry>,
    global_pid: GlobalProcessId,
}

impl Drop for LocalNameRegistration {
    fn drop(&mut self) {
        self.registry.remove_local_registrations(self.global_pid);
    }
}

impl PendingReply {
    fn new(result: std::result::Result<Vec<u8>, String>) -> Self {
        Self {
            result,
            completed_at: Instant::now(),
        }
    }

    fn before_deadline(
        self,
        started_at: Instant,
        timeout: Duration,
    ) -> std::result::Result<Self, mpsc::RecvTimeoutError> {
        if self.completed_at.saturating_duration_since(started_at) >= timeout {
            Err(mpsc::RecvTimeoutError::Timeout)
        } else {
            Ok(self)
        }
    }
}

#[derive(Debug, Clone)]
enum ExitStatus {
    Normal,
    Failed(String),
}

struct GenServerShared {
    environment: Arc<dyn Environment>,
    pending: Mutex<HashMap<u64, mpsc::Sender<PendingReply>>>,
    next_request_id: AtomicU64,
    exit: (Mutex<Option<ExitStatus>>, Condvar),
}

impl GenServerShared {
    fn new(environment: Arc<dyn Environment>) -> Self {
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

    fn reply(&self, request_id: u64, reply: std::result::Result<Vec<u8>, String>) {
        let reply = PendingReply::new(reply);
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
            let _ = sender.send(PendingReply::new(Err(pending_error.clone())));
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
        blocking_wait(|| {
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
        })
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

fn gen_server_runtime() -> GenServerRuntime {
    static RUNTIME: OnceLock<GenServerRuntime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(0));
            GenServerRuntime::new(environment, 0, Arc::new(DistributedRegistry::new(0)))
        })
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

        // Measure the timeout from the beginning of the call. Starting a fresh
        // `recv_timeout` only after sending lets a heavily descheduled caller
        // accept a reply that arrived after the configured deadline.
        let started_at = Instant::now();
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

        let response = blocking_wait(|| match self.config.timeout_ms {
            Some(timeout_ms) => {
                let timeout = Duration::from_millis(timeout_ms);
                timeout
                    .checked_sub(started_at.elapsed())
                    .ok_or(mpsc::RecvTimeoutError::Timeout)
                    .and_then(|remaining| receiver.recv_timeout(remaining))
                    // `recv_timeout` checks for a queued reply before checking
                    // its deadline. Use the reply completion time so a caller
                    // descheduled past the deadline cannot accept a late reply.
                    .and_then(|response| response.before_deadline(started_at, timeout))
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
                    })
            }
            None => receiver.recv().map_err(|_| {
                anyhow!(
                    "GenServer process {} terminated before replying to call {}",
                    self.process_id,
                    request_id
                )
            }),
        });

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
        };
        let response = response.result.map_err(anyhow::Error::msg)?;
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
        self.process
            .send(Signal::Kill)
            .map_err(|error| anyhow!("Failed to kill GenServer: {error}"))?;
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
        self.process
            .send(Signal::Message(Message::Data(message)))
            .map_err(|error| anyhow!("Failed to send GenServer message: {error}"))
    }
}

/// GenServer spawn configuration
#[derive(Debug, Clone)]
pub struct GenServerConfig {
    /// Optional node-local registry name. Cluster-wide quorum registration is
    /// not performed by the synchronous GenServer API.
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

fn blocking_wait<T>(wait: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(runtime)
            if matches!(
                runtime.runtime_flavor(),
                tokio::runtime::RuntimeFlavor::MultiThread
            ) =>
        {
            tokio::task::block_in_place(wait)
        }
        _ => wait(),
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
    fn pending_reply_deadline_uses_completion_time() {
        let started_at = Instant::now();
        let timeout = Duration::from_millis(20);
        let on_time = PendingReply {
            result: Ok(Vec::new()),
            completed_at: started_at + Duration::from_millis(19),
        };
        let at_deadline = PendingReply {
            result: Ok(Vec::new()),
            completed_at: started_at + timeout,
        };

        assert!(on_time.before_deadline(started_at, timeout).is_ok());
        assert!(matches!(
            at_deadline.before_deadline(started_at, timeout),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
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
            // Keep the follow-up call reliable on loaded hosted runners while
            // the deliberately slow call still exceeds the deadline by 3x.
            timeout_ms: Some(250),
        })
        .unwrap();

        let error = handle
            .call(TestCall::EchoAfter {
                value: 7,
                delay_ms: 750,
            })
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert!(handle
            .shared
            .pending
            .lock()
            .expect("GenServer pending replies mutex poisoned")
            .is_empty());

        tokio::time::sleep(Duration::from_millis(1_000)).await;
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
