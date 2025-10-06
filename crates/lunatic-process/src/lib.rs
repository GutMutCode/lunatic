pub mod config;
pub mod env;
pub mod hot_reload;
pub mod instance_pool;
pub mod mailbox;
pub mod message;
pub mod module_registry;
pub mod reloadable_state;
pub mod resource_migration;
pub mod runtimes;
pub mod signature_validation;
pub mod state;
pub mod wasm;

use std::{collections::HashMap, fmt::Debug, future::Future, sync::Arc};

use anyhow::{anyhow, Result};
use env::Environment;
use log::{debug, log_enabled, trace, warn, Level};

use smallvec::SmallVec;
use state::ProcessState;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        Mutex,
    },
    task::JoinHandle,
};

use crate::{mailbox::MessageMailbox, message::Message};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;

/// Perform a pending hot reload with instance swap
async fn perform_pending_reload<S>(
    context: &ProcessContext<S>,
    env: Arc<dyn Environment>,
    module_id: u64,
    old_version: u32,
    new_version: u32,
) -> Result<()>
where
    S: ProcessState + Send + wasmtime::ResourceLimiter + 'static,
{
    log::info!(
        "Starting hot reload: module_id={}, {} -> {}",
        module_id,
        old_version,
        new_version
    );

    let module_registry = env
        .get_module_registry()
        .ok_or_else(|| anyhow!("ModuleRegistry not available in environment"))?;

    let module_registry = module_registry
        .downcast::<module_registry::ModuleRegistry<S>>()
        .map_err(|_| anyhow!("Failed to downcast ModuleRegistry"))?;

    let new_module = module_registry
        .get_version(module_id, new_version)
        .ok_or_else(|| anyhow!("Module version {} not found", new_version))?;

    let old_module = module_registry
        .get_version(module_id, old_version)
        .ok_or_else(|| anyhow!("Old module version {} not found", old_version))?;

    // Validate signature compatibility
    log::info!("Validating module compatibility...");
    let validation_errors =
        signature_validation::SignatureValidator::validate_compatibility(&old_module, &new_module)?;

    if !validation_errors.is_empty() {
        log::error!("Module incompatibility detected:");
        for error in &validation_errors {
            log::error!("  - {}", error);
        }
        return Err(anyhow!(
            "Module signature validation failed: {} incompatibilities found",
            validation_errors.len()
        ));
    }

    log::info!("Module signatures are compatible");

    let mut instance_guard = context.instance.write().await;
    let mut old_instance = instance_guard
        .take()
        .ok_or_else(|| anyhow!("No instance available for hot reload"))?;

    let mut resource_snapshot = None;
    let memory_snapshot = old_instance.snapshot_memory()?;
    if let Some(resources) = old_instance.state().capture_resource_snapshot()? {
        resource_snapshot = Some(resources);
    }
    let mailbox_snapshot = old_instance.state().message_mailbox().snapshot();

    log::info!(
        "Captured {} bytes of memory and {} messages",
        memory_snapshot.memory.len(),
        mailbox_snapshot.len()
    );

    let runtime = old_instance.state().runtime().clone();
    let config = old_instance.state().config().clone();
    let new_state = old_instance.state().new_state(new_module.clone(), config)?;

    let mut new_instance = runtime.instantiate(&new_module, new_state).await?;

    new_instance.restore_memory(&memory_snapshot)?;
    new_instance
        .state_mut()
        .message_mailbox()
        .restore(mailbox_snapshot);

    if let Some(resources) = resource_snapshot {
        if let Err(err) = new_instance
            .state_mut()
            .restore_resource_snapshot(resources)
        {
            log::warn!("Failed to restore resources during hot reload: {}", err);
        }
    }

    log::info!("Hot reload completed successfully");

    *instance_guard = Some(new_instance);

    Ok(())
}

/// Context for managing process execution and hot reload state
pub struct ProcessContext<S: Send> {
    pub instance: Arc<RwLock<Option<crate::runtimes::wasmtime::WasmtimeInstance<S>>>>,
    pub reload_in_progress: Arc<AtomicBool>,
    pub pending_reload: Arc<std::sync::Mutex<Option<(u64, u32)>>>,
}

impl<S: Send> ProcessContext<S> {
    pub fn new(instance: crate::runtimes::wasmtime::WasmtimeInstance<S>) -> Self {
        Self {
            instance: Arc::new(RwLock::new(Some(instance))),
            reload_in_progress: Arc::new(AtomicBool::new(false)),
            pending_reload: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Swap the instance atomically for hot reload
    pub async fn swap_instance(
        &self,
        new_instance: crate::runtimes::wasmtime::WasmtimeInstance<S>,
    ) -> Option<crate::runtimes::wasmtime::WasmtimeInstance<S>> {
        let mut instance = self.instance.write().await;
        instance.replace(new_instance)
    }

    /// Check if reload is pending
    pub fn check_pending_reload(&self) -> Option<(u64, u32)> {
        *self.pending_reload.lock().unwrap()
    }

    /// Set pending reload
    pub fn set_pending_reload(&self, module_id: u64, new_version: u32) {
        *self.pending_reload.lock().unwrap() = Some((module_id, new_version));
    }

    /// Clear pending reload
    pub fn clear_pending_reload(&self) {
        *self.pending_reload.lock().unwrap() = None;
    }
}

/// Signals that can be sent to processes
pub enum Signal {
    /// Send a message to the process
    Message(Message),
    /// Change the `die_when_link_dies` flag
    DieWhenLinkDies(bool),
    /// Link this process to another
    Link(Option<i64>, Arc<dyn Process>),
    /// Unlink this process from another
    UnLink { process_id: u64 },
    /// A linked process died
    LinkDied(u64, Option<i64>, DeathReason),
    /// Monitor this process
    Monitor(Arc<dyn Process>),
    /// Stop monitoring this process
    StopMonitoring { process_id: u64 },
    /// A monitored process died
    ProcessDied(u64),
    /// Kill the process
    Kill,
    /// Hot reload signal
    HotReload { module_id: u64, new_version: u32 },
}

/// Reason why a process died
#[derive(Debug, Clone, Copy)]
pub enum DeathReason {
    /// Process finished normally
    Normal,
    /// Process failed with an error
    Failure,
    /// Process no longer exists
    NoProcess,
}

/// Process trait for sending signals
pub trait Process: Send + Sync {
    /// Get the process ID
    fn id(&self) -> u64;
    /// Send a signal to the process
    fn send(&self, signal: Signal);
}

/// Handle to a WASM process
#[derive(Clone, Debug)]
pub struct WasmProcess {
    id: u64,
    signal_sender: tokio::sync::mpsc::UnboundedSender<Signal>,
}

impl WasmProcess {
    pub fn new(id: u64, signal_sender: tokio::sync::mpsc::UnboundedSender<Signal>) -> Self {
        Self { id, signal_sender }
    }
}

impl Process for WasmProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        // If the receiver doesn't exist or is closed, just ignore it
        let _ = self.signal_sender.send(signal);
    }
}

#[cfg(feature = "metrics")]
pub fn describe_metrics() {
    use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};

    describe_counter!(
        "lunatic.process.signals.send",
        Unit::Count,
        "Number of signals sent to processes since startup"
    );

    describe_counter!(
        "lunatic.process.signals.received",
        Unit::Count,
        "Number of signals received by processes since startup"
    );

    describe_counter!(
        "lunatic.process.messages.send",
        Unit::Count,
        "Number of messages sent to processes since startup"
    );

    describe_gauge!(
        "lunatic.process.messages.outstanding",
        Unit::Count,
        "Current number of messages that are ready to be consumed by the process"
    );

    describe_gauge!(
        "lunatic.process.links.alive",
        Unit::Count,
        "Number of links currently alive"
    );

    describe_counter!(
        "lunatic.process.messages.data.count",
        Unit::Count,
        "Number of data messages send since startup"
    );

    describe_histogram!(
        "lunatic.process.messages.data.resources.count",
        Unit::Count,
        "Number of resources used by each individual data message"
    );

    describe_histogram!(
        "lunatic.process.messages.data.size",
        Unit::Bytes,
        "Number of bytes used by each individual data message"
    );

    describe_counter!(
        "lunatic.process.messages.link_died.count",
        Unit::Count,
        "Number of LinkDied messages send since startup"
    );

    describe_gauge!(
        "lunatic.process.environment.process.count",
        Unit::Count,
        "Number of currently registered processes"
    );

    describe_gauge!(
        "lunatic.process.environment.count",
        Unit::Count,
        "Number of currently active environments"
    );
}

#[derive(Clone, Debug)]
pub struct NativeProcess {
    id: u64,
    signal_mailbox: UnboundedSender<Signal>,
}

/// Spawns a process from a closure.
pub fn spawn<T, F, K, R>(
    env: Arc<dyn Environment>,
    func: F,
) -> (JoinHandle<Result<T>>, NativeProcess)
where
    T: ProcessState + Send + Sync + wasmtime::ResourceLimiter + 'static,
    R: Into<ExecutionResult<T>> + Send + 'static,
    K: Future<Output = R> + Send + 'static,
    F: FnOnce(NativeProcess, MessageMailbox) -> K,
{
    let id = env.get_next_process_id();
    let (signal_sender, signal_mailbox) = unbounded_channel::<Signal>();
    let message_mailbox = MessageMailbox::default();
    let process = NativeProcess {
        id,
        signal_mailbox: signal_sender,
    };
    let fut = func(process.clone(), message_mailbox.clone());
    let signal_mailbox = Arc::new(Mutex::new(signal_mailbox));
    let join = tokio::task::spawn(new(
        fut,
        id,
        env.clone(),
        signal_mailbox,
        message_mailbox,
        None,
    ));
    (join, process)
}

impl Process for NativeProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels = [("process_kind", "native")];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [
            ("process_kind", "native"),
            ("process_id", self.id().to_string()),
        ];
        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.process.signals.send", &labels);

        // If the receiver doesn't exist or is closed, just ignore it and drop the `signal`.
        // lunatic can't guarantee that a message was successfully seen by the receiving side even
        // if this call succeeds. We deliberately don't expose this API, as it would not make sense
        // to relay on it and could signal wrong guarantees to users.
        let _ = self.signal_mailbox.send(signal);
    }
}

// Contains the result of a process execution.
//
// Can be also used to extract the state of a process after the execution is done.
pub struct ExecutionResult<T> {
    state: T,
    result: ResultValue,
}

impl<T> ExecutionResult<T> {
    // Returns the failure as `String` if the process failed.
    pub fn failure(&self) -> Option<&str> {
        match self.result {
            ResultValue::Failed(ref failure) => Some(failure),
            ResultValue::SpawnError(ref failure) => Some(failure),
            _ => None,
        }
    }

    // Returns the process state reference
    pub fn state(&self) -> &T {
        &self.state
    }

    // Returns the process state
    pub fn into_state(self) -> T {
        self.state
    }
}

// It's more convinient to return a `Result<T,E>` in a `NativeProcess`.
impl<T> From<Result<T>> for ExecutionResult<T>
where
    T: Default,
{
    fn from(result: Result<T>) -> Self {
        match result {
            Ok(t) => ExecutionResult {
                state: t,
                result: ResultValue::Ok,
            },
            Err(e) => ExecutionResult {
                state: T::default(),
                result: ResultValue::Failed(e.to_string()),
            },
        }
    }
}

pub enum ResultValue {
    Ok,
    Failed(String),
    SpawnError(String),
}

pub enum Finished<R> {
    Normal(R),
    KillSignal,
}

/// Enum containing a process name if available, otherwise its ID.
enum NameOrID<'a> {
    Names(SmallVec<[&'a str; 2]>),
    ID(u64),
}

impl<'a> NameOrID<'a> {
    /// Returns names, otherwise id if names is empty.
    fn or_id(self, id: u64) -> Self {
        match self {
            NameOrID::Names(ref names) if !names.is_empty() => self,
            _ => NameOrID::ID(id),
        }
    }
}

impl<'a> std::fmt::Display for NameOrID<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NameOrID::Names(names) => {
                for (i, name) in names.iter().enumerate() {
                    if i > 0 {
                        write!(f, " / ")?;
                    }
                    write!(f, "'{name}'")?;
                }
                Ok(())
            }
            NameOrID::ID(id) => write!(f, "{id}"),
        }
    }
}

impl<'a> FromIterator<&'a str> for NameOrID<'a> {
    fn from_iter<T: IntoIterator<Item = &'a str>>(iter: T) -> Self {
        let names = SmallVec::from_iter(iter);
        NameOrID::Names(names)
    }
}

/// Turns a `Future` into a process, enabling signals (e.g. kill).
///
/// This function represents the core execution loop of lunatic processes:
///
/// 1. The process will first check if there are any new signals and handle them.
/// 2. If no signals are available, it will poll the `Future` and advance the execution.
///
/// This steps are repeated until the `Future` returns `Poll::Ready`, indicating the end of the
/// computation.
///
/// The `Future` is in charge to periodically yield back the execution with `Poll::Pending` to give
/// the signal handler a chance to run and process pending signals.
///
/// In case of success, the process state `S` is returned. It's not possible to return the process
/// state in case of failure because of limitations in the Wasmtime API:
/// https://github.com/bytecodealliance/wasmtime/issues/2986
pub(crate) async fn new<F, S, R>(
    fut: F,
    id: u64,
    env: Arc<dyn Environment>,
    signal_mailbox: Arc<Mutex<UnboundedReceiver<Signal>>>,
    message_mailbox: MessageMailbox,
    context: Option<ProcessContext<S>>,
) -> Result<S>
where
    S: ProcessState + Send + wasmtime::ResourceLimiter + 'static,
    R: Into<ExecutionResult<S>>,
    F: Future<Output = R> + Send + 'static,
{
    trace!("Process {} spawned", id);
    tokio::pin!(fut);

    // If the value is set to false, instead of dying too the process will receive a message about
    // the linked process' death.
    let mut die_when_link_dies = true;
    // Process linked to this one
    let mut links = HashMap::new();
    // Processes monitoring this one
    let mut monitors = HashMap::new();
    // TODO: Maybe wrapping this in some kind of `std::panic::catch_unwind` wold be a good idea,
    //       to protect against panics in host function calls that unwind through Wasm code.
    //       Currently a panic would just kill the task, but not notify linked processes.
    let mut signal_mailbox = signal_mailbox.lock().await;
    let mut has_sender = true;
    #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
    let labels: [(String, String); 0] = [];
    #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
    let labels = [("process_id", id.to_string())];
    let result = loop {
        tokio::select! {
            biased;
            // Handle signals first
            signal = signal_mailbox.recv(), if has_sender => {
                #[cfg(feature = "metrics")]
                metrics::increment_counter!("lunatic.process.signals.received", &labels);

                match signal.ok_or(()) {
                    Ok(Signal::Message(message)) => {

                        #[cfg(feature = "metrics")]
                        message.write_metrics();

                        message_mailbox.push(message);

                        // process metrics
                        #[cfg(feature = "metrics")]
                        metrics::increment_counter!("lunatic.process.messages.send", &labels);

                        #[cfg(feature = "metrics")]
                        metrics::gauge!("lunatic.process.messages.outstanding", message_mailbox.len() as f64, &labels);
                    },
                    Ok(Signal::DieWhenLinkDies(value)) => die_when_link_dies = value,
                    // Put process into list of linked processes
                    Ok(Signal::Link(tag, proc)) => {
                        links.insert(proc.id(), (proc, tag));

                        #[cfg(feature = "metrics")]
                        metrics::gauge!("lunatic.process.links.alive", links.len() as f64, &labels);
                    },
                    // Remove process from list
                    Ok(Signal::UnLink { process_id }) => {
                        links.remove(&process_id);

                        #[cfg(feature = "metrics")]
                        metrics::gauge!("lunatic.process.links.alive", links.len() as f64, &labels);
                    }
                    // Depending if `die_when_link_dies` is set, process will die or turn the
                    // signal into a message
                    Ok(Signal::LinkDied(id, tag, reason)) => {
                        links.remove(&id);

                        #[cfg(feature = "metrics")]
                        metrics::gauge!("lunatic.process.links.alive", links.len() as f64, &labels);
                        match reason {
                            DeathReason::Failure | DeathReason::NoProcess => {
                                if die_when_link_dies {
                                    // Even this was not a **kill** signal it has the same effect on
                                    // this process and should be propagated as such.
                                    break Finished::KillSignal
                                } else {
                                    let message = Message::LinkDied(tag);

                                    #[cfg(feature = "metrics")]
                                    metrics::increment_counter!("lunatic.process.messages.send", &labels);

                                    #[cfg(feature = "metrics")]
                                    metrics::gauge!("lunatic.process.messages.outstanding", message_mailbox.len() as f64, &labels);
                                    message_mailbox.push(message);
                                }
                            },
                            // In case a linked process finishes normally, don't do anything.
                            DeathReason::Normal => {},
                        }
                    },
                    // Put process into list of monitor processes
                    Ok(Signal::Monitor(proc)) => {
                        monitors.insert(proc.id(), proc);
                    }
                    // Remove process from monitor list
                    Ok(Signal::StopMonitoring { process_id }) => {
                        monitors.remove(&process_id);
                    }
                    // Notify process that a monitored process died
                    Ok(Signal::ProcessDied(id)) => {
                        message_mailbox.push(Message::ProcessDied(id));
                    }
                    // Kill the process
                    Ok(Signal::Kill) => break Finished::KillSignal,
                    // Hot reload signal - perform true hot reload
                    Ok(Signal::HotReload { module_id, new_version }) => {
                        if let Some(context) = &context {
                            log::info!("Processing HotReload signal for module {} version {}", module_id, new_version);

                            // Check if reload already in progress
                            if context.reload_in_progress.load(Ordering::SeqCst) {
                                log::warn!("Hot reload already in progress, ignoring signal");
                                continue;
                            }

                            // Set reload flag
                            context.reload_in_progress.store(true, Ordering::SeqCst);

                            // Get the old version from pending reload or default to 0
                            let old_version = {
                                let pending = context.pending_reload.lock().unwrap();
                                pending.map(|(_, v)| v).unwrap_or(0)
                            };

                            // Perform the hot reload immediately
                            match perform_pending_reload(
                                context,
                                env.clone(),
                                module_id,
                                old_version,
                                new_version,
                            ).await {
                                Ok(_) => {
                                    log::info!("Hot reload completed successfully for module {} -> version {}", module_id, new_version);
                                    context.clear_pending_reload();
                                }
                                Err(e) => {
                                    log::error!("Hot reload failed for module {}: {}", module_id, e);
                                    // Keep pending reload for retry
                                    context.set_pending_reload(module_id, new_version);
                                }
                            }

                            // Clear reload flag
                            context.reload_in_progress.store(false, Ordering::SeqCst);
                        } else {
                            log::warn!("Hot reload signal received but no context available (native process?)");
                        }
                    }
                    Err(_) => {
                        debug_assert!(has_sender);
                        has_sender = false;
                    }
                }
            }
            // Run process
            output = &mut fut => { break Finished::Normal(output); }
        }
    };

    env.remove_process(id);

    let final_result = match result {
        Finished::Normal(result) => {
            let result: ExecutionResult<_> = result.into();

            if let Some(failure) = result.failure() {
                let registry = result.state().registry().read().await;
                let name = registry
                    .iter()
                    .filter(|(_, (_, process_id))| process_id == &id)
                    .map(|(name, _)| name.splitn(4, '/').last().unwrap_or(name.as_str()))
                    .collect::<NameOrID>()
                    .or_id(id);
                warn!(
                    "Process {} failed, notifying: {} links {}",
                    name,
                    links.len(),
                    // If the log level is WARN instruct user how to display the stacktrace
                    if !log_enabled!(Level::Debug) {
                        "\n\t\t\t    (Set ENV variable `RUST_LOG=lunatic=debug` to show stacktrace)"
                    } else {
                        ""
                    }
                );
                debug!("{}", failure);

                Err(anyhow!(failure.to_string()))
            } else {
                Ok(result.into_state())
            }
        }
        Finished::KillSignal => {
            // TODO: We should return the state here too, but it's not possible with the current
            //       Wasmtime API. See: https://github.com/bytecodealliance/wasmtime/issues/2986
            Err(anyhow!("Process killed"))
        }
    };

    // Notify all monitors that this process died
    for monitor in monitors.values() {
        monitor.send(Signal::ProcessDied(id));
    }

    // Notify all links that this process died
    for (linked_process, tag) in links.values() {
        linked_process.send(Signal::LinkDied(id, *tag, DeathReason::Normal));
    }

    final_result
}
