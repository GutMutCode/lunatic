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

use std::{
    any::Any, collections::HashMap, fmt::Debug, future::Future, panic::AssertUnwindSafe, sync::Arc,
};

use anyhow::{anyhow, ensure, Result};
use env::Environment;
use futures_util::FutureExt;
use log::{debug, log_enabled, trace, warn, Level};

use smallvec::SmallVec;
use state::ProcessState;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot, Mutex,
    },
    task::JoinHandle,
};

use crate::{
    mailbox::MessageMailbox,
    message::Message,
    module_registry::{ModuleRegistry, ProcessKey},
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::RwLock;

fn validate_reload_target<S>(
    env: &Arc<dyn Environment>,
    module_id: u64,
    old_version: u32,
    new_version: u32,
) -> Result<Arc<runtimes::wasmtime::WasmtimeCompiledModule<S>>>
where
    S: ProcessState + Send + 'static,
{
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

    let validation_errors =
        signature_validation::SignatureValidator::validate_compatibility(&old_module, &new_module)?;

    if !validation_errors.is_empty() {
        for error in &validation_errors {
            log::error!("Hot reload incompatibility: {}", error);
        }
        return Err(anyhow!(
            "Module signature validation failed: {} incompatibilities found",
            validation_errors.len()
        ));
    }

    Ok(new_module)
}

/// Perform a pending hot reload with an atomic instance swap.
pub(crate) async fn perform_pending_reload<S>(
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

    log::info!("Validating module compatibility...");
    let new_module = validate_reload_target::<S>(&env, module_id, old_version, new_version)?;
    log::info!("Module signatures are compatible");

    let mut instance_guard = context.instance.write().await;
    let mut old_instance = instance_guard
        .take()
        .ok_or_else(|| anyhow!("No instance available for hot reload"))?;

    let reload_result = async {
        let memory_snapshot = old_instance.snapshot_memory()?;
        let remaining_fuel = old_instance.store().get_fuel()?;
        let queued_messages = old_instance.state().message_mailbox().len();

        log::info!(
            "Captured {} bytes of memory and {} messages",
            memory_snapshot.memory.len(),
            queued_messages
        );

        let runtime = old_instance.state().runtime().clone();
        let config = old_instance.state().config().clone();
        let new_state = old_instance
            .state()
            .new_state_for_reload(new_module.clone(), config)?;
        let mut new_instance = runtime.instantiate(&new_module, new_state).await?;

        new_instance.restore_memory(&memory_snapshot)?;
        // A reload must not replenish the process instruction budget.
        new_instance.store_mut().set_fuel(remaining_fuel)?;

        let transfer_report = old_instance
            .state_mut()
            .transfer_runtime_resources_to(new_instance.state_mut())?;

        Ok::<_, anyhow::Error>((new_instance, transfer_report))
    }
    .await;

    match reload_result {
        Ok((new_instance, transfer_report)) => {
            log::info!(
                "Transferred {} live runtime resource(s), including {} TLS stream(s) and {} TLS listener(s)",
                transfer_report.total(),
                transfer_report.tls_streams,
                transfer_report.tls_listeners
            );
            *instance_guard = Some(new_instance);
            log::info!("Hot reload completed successfully");
            Ok(())
        }
        Err(error) => {
            *instance_guard = Some(old_instance);
            Err(error)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessReloadStatus {
    Applied,
    AlreadyAtTarget,
    Failed(String),
}

impl ProcessReloadStatus {
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Applied | Self::AlreadyAtTarget)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReloadAcknowledgement {
    pub process_id: u64,
    pub module_id: u64,
    pub previous_version: u32,
    pub current_version: u32,
    pub status: ProcessReloadStatus,
}

pub type ReloadAckSender = oneshot::Sender<ReloadAcknowledgement>;

pub(crate) fn acknowledge_reload(
    acknowledgement: Option<ReloadAckSender>,
    process_id: u64,
    module_id: u64,
    previous_version: u32,
    current_version: u32,
    status: ProcessReloadStatus,
) {
    if let Some(acknowledgement) = acknowledgement {
        let _ = acknowledgement.send(ReloadAcknowledgement {
            process_id,
            module_id,
            previous_version,
            current_version,
            status,
        });
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReloadAction {
    HotReload,
    Rollback,
}

#[derive(Debug)]
pub(crate) enum ReloadCommand {
    HotReload {
        module_id: u64,
        expected_version: Option<u32>,
        new_version: u32,
        acknowledgement: Option<ReloadAckSender>,
    },
    Rollback {
        module_id: u64,
        expected_version: Option<u32>,
        target_version: u32,
        acknowledgement: Option<ReloadAckSender>,
    },
}

impl ReloadCommand {
    pub(crate) fn into_parts(
        self,
    ) -> (ReloadAction, u64, Option<u32>, u32, Option<ReloadAckSender>) {
        match self {
            Self::HotReload {
                module_id,
                expected_version,
                new_version,
                acknowledgement,
            } => (
                ReloadAction::HotReload,
                module_id,
                expected_version,
                new_version,
                acknowledgement,
            ),
            Self::Rollback {
                module_id,
                expected_version,
                target_version,
                acknowledgement,
            } => (
                ReloadAction::Rollback,
                module_id,
                expected_version,
                target_version,
                acknowledgement,
            ),
        }
    }
}

struct ProcessVersionState<S: ProcessState> {
    registry: Option<Arc<ModuleRegistry<S>>>,
    process: Option<ProcessKey>,
    current_versions: HashMap<u64, u32>,
}

struct ProcessVersionTracking<S: ProcessState> {
    state: std::sync::Mutex<ProcessVersionState<S>>,
}

struct ProcessVersionRegistrations<S: ProcessState> {
    registry: Option<Arc<ModuleRegistry<S>>>,
    process: Option<ProcessKey>,
    versions: HashMap<u64, u32>,
}

impl<S: ProcessState> ProcessVersionTracking<S> {
    fn take_registrations(&self) -> ProcessVersionRegistrations<S> {
        let mut state = self.state.lock().expect("version tracking mutex poisoned");
        ProcessVersionRegistrations {
            registry: state.registry.take(),
            process: state.process.take(),
            versions: std::mem::take(&mut state.current_versions),
        }
    }

    fn unregister_all(&self) {
        let ProcessVersionRegistrations {
            registry,
            process,
            versions,
        } = self.take_registrations();
        if let (Some(registry), Some(process)) = (registry, process) {
            for (module_id, version) in versions {
                if let Err(error) = registry.unregister_process(module_id, version, process) {
                    log::error!(
                        "Failed to unregister process {} from module {} version {}: {}",
                        process.process_id,
                        module_id,
                        version,
                        error
                    );
                }
            }
        }
    }
}

impl<S: ProcessState> Default for ProcessVersionTracking<S> {
    fn default() -> Self {
        Self {
            state: std::sync::Mutex::new(ProcessVersionState {
                registry: None,
                process: None,
                current_versions: HashMap::new(),
            }),
        }
    }
}

impl<S: ProcessState> Drop for ProcessVersionTracking<S> {
    fn drop(&mut self) {
        self.unregister_all();
    }
}

/// Context for managing process execution and hot reload state.
pub struct ProcessContext<S: ProcessState + Send + 'static> {
    pub instance: Arc<RwLock<Option<crate::runtimes::wasmtime::WasmtimeInstance<S>>>>,
    pub reload_in_progress: Arc<AtomicBool>,
    reload_sender: UnboundedSender<ReloadCommand>,
    reload_receiver: Arc<std::sync::Mutex<Option<UnboundedReceiver<ReloadCommand>>>>,
    version_tracking: Arc<ProcessVersionTracking<S>>,
}

impl<S: ProcessState + Send + 'static> Clone for ProcessContext<S> {
    fn clone(&self) -> Self {
        Self {
            instance: self.instance.clone(),
            reload_in_progress: self.reload_in_progress.clone(),
            reload_sender: self.reload_sender.clone(),
            reload_receiver: self.reload_receiver.clone(),
            version_tracking: self.version_tracking.clone(),
        }
    }
}

impl<S: ProcessState + Send + 'static> ProcessContext<S> {
    pub fn new(instance: crate::runtimes::wasmtime::WasmtimeInstance<S>) -> Self {
        let (reload_sender, reload_receiver) = unbounded_channel();
        Self {
            instance: Arc::new(RwLock::new(Some(instance))),
            reload_in_progress: Arc::new(AtomicBool::new(false)),
            reload_sender,
            reload_receiver: Arc::new(std::sync::Mutex::new(Some(reload_receiver))),
            version_tracking: Arc::new(ProcessVersionTracking::default()),
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

    pub(crate) fn take_reload_receiver(&self) -> UnboundedReceiver<ReloadCommand> {
        self.reload_receiver
            .lock()
            .unwrap()
            .take()
            .expect("reload receiver can only be owned by the Wasm execution driver")
    }

    pub(crate) fn request_reload(
        &self,
        command: ReloadCommand,
    ) -> std::result::Result<(), tokio::sync::mpsc::error::SendError<ReloadCommand>> {
        self.reload_sender.send(command)
    }

    pub(crate) fn track_initial_version(
        &self,
        registry: Arc<ModuleRegistry<S>>,
        module_id: u64,
        version: u32,
        process: ProcessKey,
    ) -> Result<()> {
        let mut tracking = self
            .version_tracking
            .state
            .lock()
            .expect("version tracking mutex poisoned");
        ensure!(
            !tracking.current_versions.contains_key(&module_id),
            "Process {} already tracks module {}",
            process.process_id,
            module_id
        );
        if let Some(existing_registry) = &tracking.registry {
            ensure!(
                Arc::ptr_eq(existing_registry, &registry),
                "A process cannot track versions from multiple module registries"
            );
        }
        if let Some(existing_process) = tracking.process {
            ensure!(
                existing_process == process,
                "Version tracking process identity cannot change"
            );
        }

        registry.register_process(module_id, version, process)?;
        tracking.registry = Some(registry);
        tracking.process = Some(process);
        tracking.current_versions.insert(module_id, version);
        Ok(())
    }

    pub(crate) fn current_version(&self, module_id: u64) -> Option<u32> {
        self.version_tracking
            .state
            .lock()
            .expect("version tracking mutex poisoned")
            .current_versions
            .get(&module_id)
            .copied()
    }

    pub(crate) fn transition_current_version(
        &self,
        module_id: u64,
        old_version: u32,
        new_version: u32,
    ) -> Result<()> {
        let mut tracking = self
            .version_tracking
            .state
            .lock()
            .expect("version tracking mutex poisoned");
        ensure!(
            tracking.current_versions.get(&module_id).copied() == Some(old_version),
            "Process version changed while reload was in progress"
        );
        let registry = tracking
            .registry
            .as_ref()
            .ok_or_else(|| anyhow!("Process has no module registry version tracking"))?;
        let process = tracking
            .process
            .ok_or_else(|| anyhow!("Process has no version tracking identity"))?;
        registry.transition_process(module_id, old_version, new_version, process)?;
        tracking.current_versions.insert(module_id, new_version);
        Ok(())
    }

    fn unregister_all_versions(&self) {
        self.version_tracking.unregister_all();
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
    /// Hot reload request. Coordinated callers provide an expected version and
    /// acknowledgement channel; fire-and-forget callers may omit both.
    HotReload {
        module_id: u64,
        expected_version: Option<u32>,
        new_version: u32,
        acknowledgement: Option<ReloadAckSender>,
    },
    /// Rollback request. The same acknowledgement contract applies so a
    /// coordinator can distinguish confirmed rollback from an in-doubt state.
    Rollback {
        module_id: u64,
        expected_version: Option<u32>,
        target_version: u32,
        acknowledgement: Option<ReloadAckSender>,
    },
}

/// Reason why a process died
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    T: ProcessState
        + Send
        + Sync
        + wasmtime::ResourceLimiter
        + crate::reloadable_state::ReloadableState
        + 'static,
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

/// Spawns a native Lunatic process whose future returns no Wasm process state.
///
/// Unlike [`spawn`], this entry point is intended for host-side process
/// abstractions that still need Lunatic mailboxes, signals, links, monitors and
/// lifecycle handling, but do not own a Wasmtime [`ProcessState`]. The process
/// is registered in the supplied environment for the duration of its task.
pub fn spawn_native<F, K>(
    env: Arc<dyn Environment>,
    func: F,
) -> (JoinHandle<Result<()>>, NativeProcess)
where
    K: Future<Output = Result<()>> + Send + 'static,
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

    env.add_process(id, Arc::new(process.clone()));
    let join = tokio::task::spawn(run_native_process(
        fut,
        id,
        env,
        signal_mailbox,
        message_mailbox,
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
    Panicked(String),
}

async fn run_native_process<F>(
    fut: F,
    id: u64,
    env: Arc<dyn Environment>,
    signal_mailbox: Arc<Mutex<UnboundedReceiver<Signal>>>,
    message_mailbox: MessageMailbox,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    trace!("Native process {} spawned", id);
    let mut die_when_link_dies = true;
    let mut links = HashMap::new();
    let mut monitors = HashMap::new();
    let mut signal_mailbox = signal_mailbox.lock().await;
    let mut has_sender = true;

    let result = {
        let fut = AssertUnwindSafe(fut).catch_unwind();
        tokio::pin!(fut);

        loop {
            tokio::select! {
                biased;
                signal = signal_mailbox.recv(), if has_sender => {
                    match signal {
                        Some(Signal::Message(message)) => message_mailbox.push(message),
                        Some(Signal::DieWhenLinkDies(value)) => die_when_link_dies = value,
                        Some(Signal::Link(tag, process)) => {
                            links.insert(process.id(), (process, tag));
                        }
                        Some(Signal::UnLink { process_id }) => {
                            links.remove(&process_id);
                        }
                        Some(Signal::LinkDied(process_id, tag, reason)) => {
                            links.remove(&process_id);
                            match reason {
                                DeathReason::Failure | DeathReason::NoProcess if die_when_link_dies => {
                                    break Finished::KillSignal;
                                }
                                DeathReason::Failure | DeathReason::NoProcess => {
                                    message_mailbox.push(Message::LinkDied(tag));
                                }
                                DeathReason::Normal => {}
                            }
                        }
                        Some(Signal::Monitor(process)) => {
                            monitors.insert(process.id(), process);
                        }
                        Some(Signal::StopMonitoring { process_id }) => {
                            monitors.remove(&process_id);
                        }
                        Some(Signal::ProcessDied(process_id)) => {
                            message_mailbox.push(Message::ProcessDied(process_id));
                        }
                        Some(Signal::Kill) => break Finished::KillSignal,
                        Some(Signal::HotReload {
                            module_id,
                            acknowledgement,
                            ..
                        }) => {
                            warn!("Hot reload is not supported for native process {}", id);
                            acknowledge_reload(
                                acknowledgement,
                                id,
                                module_id,
                                0,
                                0,
                                ProcessReloadStatus::Failed(
                                    "Hot reload is not supported for native processes".to_string(),
                                ),
                            );
                        }
                        Some(Signal::Rollback {
                            module_id,
                            acknowledgement,
                            ..
                        }) => {
                            warn!("Rollback is not supported for native process {}", id);
                            acknowledge_reload(
                                acknowledgement,
                                id,
                                module_id,
                                0,
                                0,
                                ProcessReloadStatus::Failed(
                                    "Rollback is not supported for native processes".to_string(),
                                ),
                            );
                        }
                        None => has_sender = false,
                    }
                }
                output = &mut fut => {
                    match output {
                        Ok(result) => break Finished::Normal(result),
                        Err(payload) => break Finished::Panicked(format_panic_payload(payload)),
                    }
                }
            }
        }
    };

    env.remove_process(id);

    let (result, death_reason) = match result {
        Finished::Normal(Ok(())) => (Ok(()), DeathReason::Normal),
        Finished::Normal(Err(error)) => (Err(error), DeathReason::Failure),
        Finished::KillSignal => (Err(anyhow!("Process killed")), DeathReason::Failure),
        Finished::Panicked(message) => (
            Err(anyhow!("Process panicked: {message}")),
            DeathReason::Failure,
        ),
    };

    for monitor in monitors.values() {
        monitor.send(Signal::ProcessDied(id));
    }
    for (linked_process, tag) in links.values() {
        linked_process.send(Signal::LinkDied(id, *tag, death_reason));
    }

    result
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
    S: ProcessState
        + Send
        + wasmtime::ResourceLimiter
        + crate::reloadable_state::ReloadableState
        + 'static,
    R: Into<ExecutionResult<S>>,
    F: Future<Output = R> + Send + 'static,
{
    trace!("Process {} spawned", id);

    // If the value is set to false, instead of dying too the process will receive a message about
    // the linked process' death.
    let mut die_when_link_dies = true;
    // Process linked to this one
    let mut links = HashMap::new();
    // Processes monitoring this one
    let mut monitors = HashMap::new();
    // Panics inside host calls are captured by the `AssertUnwindSafe(...).catch_unwind()` wrapper
    // above so we can still notify linked processes before the task unwinds.
    let mut signal_mailbox = signal_mailbox.lock().await;
    let mut has_sender = true;
    #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
    let labels: [(String, String); 0] = [];
    #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
    let labels = [("process_id", id.to_string())];
    let result = {
        let fut = AssertUnwindSafe(fut).catch_unwind();
        tokio::pin!(fut);

        loop {
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
                    Ok(Signal::HotReload {
                        module_id,
                        expected_version,
                        new_version,
                        acknowledgement,
                    }) => {
                        if let Some(context) = &context {
                            log::info!("Processing HotReload signal for module {} version {}", module_id, new_version);
                            let Some(old_version) = context.current_version(module_id) else {
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    0,
                                    0,
                                    ProcessReloadStatus::Failed(format!(
                                        "Process does not run registered module {}",
                                        module_id
                                    )),
                                );
                                continue;
                            };

                            if context.reload_in_progress.load(Ordering::SeqCst) {
                                log::info!("Queueing hot reload behind the active transaction");
                            }
                            // Version equality, expected-version checks, and compatibility are
                            // authoritative only in the execution driver. The tracked version can
                            // still describe the preceding queued transaction at this point.
                            let command = ReloadCommand::HotReload {
                                module_id,
                                expected_version,
                                new_version,
                                acknowledgement,
                            };
                            if let Err(error) = context.request_reload(command) {
                                log::error!("Failed to queue hot reload: {}", error);
                                let (_, _, _, _, acknowledgement) = error.0.into_parts();
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    old_version,
                                    old_version,
                                    ProcessReloadStatus::Failed(
                                        "Wasm reload execution driver is no longer running"
                                            .to_string(),
                                    ),
                                );
                            }
                        } else {
                            log::warn!("Hot reload signal received but no context available (native process?)");
                            acknowledge_reload(
                                acknowledgement,
                                id,
                                module_id,
                                0,
                                0,
                                ProcessReloadStatus::Failed(
                                    "Process has no Wasm reload context".to_string(),
                                ),
                            );
                        }
                    }
                    // Rollback signal - restore previous version
                    Ok(Signal::Rollback {
                        module_id,
                        expected_version,
                        target_version,
                        acknowledgement,
                    }) => {
                        if let Some(context) = &context {
                            log::info!("Processing Rollback signal for module {} to version {}", module_id, target_version);
                            let Some(current_version) = context.current_version(module_id) else {
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    0,
                                    0,
                                    ProcessReloadStatus::Failed(format!(
                                        "Process does not run registered module {}",
                                        module_id
                                    )),
                                );
                                continue;
                            };

                            // Always enqueue rollback, even when the process still reports the
                            // target version here. A timed-out apply may already be queued or
                            // executing without having updated version accounting yet. FIFO at
                            // the execution driver is what makes the late apply run before this
                            // idempotent rollback; only the driver may acknowledge final state.
                            let command = ReloadCommand::Rollback {
                                module_id,
                                expected_version,
                                target_version,
                                acknowledgement,
                            };
                            if let Err(error) = context.request_reload(command) {
                                log::error!("Failed to queue rollback: {}", error);
                                let (_, _, _, _, acknowledgement) = error.0.into_parts();
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    current_version,
                                    current_version,
                                    ProcessReloadStatus::Failed(
                                        "Wasm reload execution driver is no longer running"
                                            .to_string(),
                                    ),
                                );
                            }
                        } else {
                            log::warn!("Rollback signal received but no context available (native process?)");
                            acknowledge_reload(
                                acknowledgement,
                                id,
                                module_id,
                                0,
                                0,
                                ProcessReloadStatus::Failed(
                                    "Process has no Wasm reload context".to_string(),
                                ),
                            );
                        }
                    }
                    Err(_) => {
                        debug_assert!(has_sender);
                        has_sender = false;
                    }
                }
            }
            // Run process (guarding against unwinding panics inside host calls)
            output = &mut fut => {
                match output {
                    Ok(result) => break Finished::Normal(result),
                    Err(payload) => break Finished::Panicked(format_panic_payload(payload)),
                }
            }
            }
        }
    };

    // The guest future, including an active Wasmtime fiber, is fully cancelled and dropped before
    // the process disappears from lifecycle tracking or notifications are emitted. Membership is
    // cleared synchronously with environment removal so a new reload cannot snapshot a dead PID.
    if let Some(context) = &context {
        context.unregister_all_versions();
    }
    env.remove_process(id);

    let (final_result, death_reason) = match result {
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

                (Err(anyhow!(failure.to_string())), DeathReason::Failure)
            } else {
                (Ok(result.into_state()), DeathReason::Normal)
            }
        }
        Finished::Panicked(message) => {
            warn!("Process {} panicked, notifying: {} links", id, links.len());

            (
                Err(anyhow!(format!("Process panicked: {message}"))),
                DeathReason::Failure,
            )
        }
        Finished::KillSignal => {
            // TODO: We should return the state here too, but it's not possible with the current
            //       Wasmtime API. See: https://github.com/bytecodealliance/wasmtime/issues/2986
            (Err(anyhow!("Process killed")), DeathReason::Failure)
        }
    };

    // Notify all monitors that this process died
    for monitor in monitors.values() {
        monitor.send(Signal::ProcessDied(id));
    }

    // Notify all links that this process died
    for (linked_process, tag) in links.values() {
        linked_process.send(Signal::LinkDied(id, *tag, death_reason));
    }

    final_result
}

fn format_panic_payload(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "process panicked with non-string payload".to_string(),
        },
    }
}
