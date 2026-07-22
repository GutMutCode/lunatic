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
use env::{register_process, Environment, ProcessRegistration};
use futures_util::FutureExt;
use log::{debug, log_enabled, trace, warn, Level};

use smallvec::SmallVec;
pub use state::ProcessLifecycleHandle;
use state::{
    default_mailboxes, MonitorNotification, MonitorNotificationError, ProcessState, SignalReceiver,
    SignalReceiverGuard, SignalSendError, SignalSender,
};
use tokio::{
    sync::{
        mpsc::{channel, error::TrySendError, Receiver, Sender},
        oneshot, OwnedSemaphorePermit,
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
/// Signals that a monitor relation has been installed in the target process.
///
/// Senders use `try_send`, so callers requesting acknowledgement must create a
/// sync channel with capacity for at least one value.
pub type MonitorAckSender = std::sync::mpsc::SyncSender<()>;

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
    reload_sender: Sender<ReloadCommand>,
    reload_receiver: Arc<std::sync::Mutex<Option<Receiver<ReloadCommand>>>>,
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
        const RELOAD_QUEUE_CAPACITY: usize = 16;
        let (reload_sender, reload_receiver) = channel(RELOAD_QUEUE_CAPACITY);
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

    pub(crate) fn take_reload_receiver(&self) -> Receiver<ReloadCommand> {
        self.reload_receiver
            .lock()
            .unwrap()
            .take()
            .expect("reload receiver can only be owned by the Wasm execution driver")
    }

    pub(crate) fn request_reload(
        &self,
        command: ReloadCommand,
    ) -> std::result::Result<(), TrySendError<ReloadCommand>> {
        self.reload_sender.try_send(command)
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
    /// Legacy one-sided link command.
    ///
    /// Built-in process senders reject this form; use [`link_processes`] so both sides are
    /// admitted atomically.
    Link(Option<i64>, Arc<dyn Process>),
    /// Legacy one-sided unlink command.
    ///
    /// Built-in process senders reject this form; use [`unlink_process`] instead.
    UnLink { process_id: u64 },
    /// A linked process died
    LinkDied(u64, Option<i64>, DeathReason),
    /// Monitor this process
    Monitor {
        process: Arc<dyn Process>,
        /// Optional processing barrier emitted after the relation is installed.
        acknowledgement: Option<MonitorAckSender>,
    },
    /// Stop monitoring this process
    StopMonitoring { process_id: u64 },
    /// A monitored process died
    ProcessDied {
        process_id: u64,
        reason: DeathReason,
    },
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

/// A bilateral link operation could not be completed without violating the bounded lifecycle
/// contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LinkError {
    /// The process does not expose runtime-owned bounded lifecycle state.
    UnsupportedProcess,
    /// The supplied process identity does not match its runtime-owned state.
    IdentityMismatch { process_id: u64 },
    /// The process has already entered its terminal state.
    Terminated { process_id: u64 },
    /// The process has reached its configured maximum number of live links.
    CapacityExhausted { process_id: u64 },
    /// A pre-existing one-sided relation was detected and was not overwritten.
    InconsistentState {
        left_process_id: u64,
        right_process_id: u64,
    },
}

impl std::fmt::Display for LinkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedProcess => formatter
                .write_str("process does not provide runtime-owned bounded lifecycle state"),
            Self::IdentityMismatch { process_id } => write!(
                formatter,
                "process identity {process_id} does not match its lifecycle state"
            ),
            Self::Terminated { process_id } => {
                write!(formatter, "process {process_id} has already terminated")
            }
            Self::CapacityExhausted { process_id } => {
                write!(formatter, "process {process_id} link capacity is exhausted")
            }
            Self::InconsistentState {
                left_process_id,
                right_process_id,
            } => write!(
                formatter,
                "processes {left_process_id} and {right_process_id} have inconsistent link state"
            ),
        }
    }
}

impl std::error::Error for LinkError {}

/// Process trait for sending signals
pub trait Process: Send + Sync {
    /// Get the stable process ID.
    ///
    /// Implementations must return promptly and must return the identity represented by any
    /// delegated [`ProcessLifecycleHandle`].
    fn id(&self) -> u64;
    /// Send a signal to the process
    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError>;

    /// Reserves durable mailbox admission for a future monitor notification.
    ///
    /// Built-in processes return a direct-delivery reservation. The default
    /// keeps custom process implementations source-compatible and uses the
    /// signal-based notification path instead.
    #[doc(hidden)]
    fn reserve_monitor_notification(
        &self,
    ) -> std::result::Result<Option<MonitorNotification>, MonitorNotificationError> {
        Ok(None)
    }

    /// Returns the terminal reason retained by built-in process handles.
    ///
    /// Custom process implementations may leave the default when they do not
    /// retain lifecycle state after their signal receiver closes.
    #[doc(hidden)]
    fn terminal_reason(&self) -> Option<DeathReason> {
        None
    }

    /// Returns the opaque bounded lifecycle state used for atomic link operations.
    ///
    /// The default deliberately opts custom process implementations out. A custom process may
    /// continue to receive ordinary signals, but it cannot participate in links unless it safely
    /// delegates to a runtime-owned process handle for the same identity. Overrides must return
    /// promptly, must not allocate unbounded state, and may only clone a handle originally issued
    /// by a built-in process. Panics are contained and treated as an unsupported process.
    #[doc(hidden)]
    fn lifecycle_handle(&self) -> Option<ProcessLifecycleHandle> {
        None
    }
}

fn checked_lifecycle_handle(
    process: &dyn Process,
    expected_process_id: Option<u64>,
) -> std::result::Result<ProcessLifecycleHandle, LinkError> {
    let handle = std::panic::catch_unwind(AssertUnwindSafe(|| process.lifecycle_handle()))
        .map_err(|_| LinkError::UnsupportedProcess)?
        .ok_or(LinkError::UnsupportedProcess)?;
    let claimed_process_id = match expected_process_id {
        Some(process_id) => process_id,
        None => std::panic::catch_unwind(AssertUnwindSafe(|| process.id()))
            .map_err(|_| LinkError::UnsupportedProcess)?,
    };
    if claimed_process_id != handle.process_id() {
        return Err(LinkError::IdentityMismatch {
            process_id: claimed_process_id,
        });
    }
    Ok(handle)
}

/// Atomically installs the reciprocal sides of one link.
///
/// `left_exit_tag` is delivered to `right` when `left` exits, and vice versa. No state is retained
/// when either process is unsupported, terminal, or out of link capacity.
#[doc(hidden)]
pub fn link_processes(
    left: &dyn Process,
    expected_left_id: Option<u64>,
    left_exit_tag: Option<i64>,
    right: &dyn Process,
    expected_right_id: Option<u64>,
    right_exit_tag: Option<i64>,
) -> std::result::Result<(), LinkError> {
    let left = checked_lifecycle_handle(left, expected_left_id)?;
    let right = checked_lifecycle_handle(right, expected_right_id)?;
    ProcessLifecycleHandle::link_pair(&left, left_exit_tag, &right, right_exit_tag)
}

/// Atomically removes the caller's relation and its reciprocal peer relation when still live.
#[doc(hidden)]
pub fn unlink_process(
    process: &dyn Process,
    expected_process_id: Option<u64>,
    peer_id: u64,
) -> std::result::Result<bool, LinkError> {
    let process = checked_lifecycle_handle(process, expected_process_id)?;
    Ok(process.unlink_peer(peer_id))
}

type MonitorRelations = HashMap<
    u64,
    (
        Arc<dyn Process>,
        OwnedSemaphorePermit,
        Option<MonitorNotification>,
    ),
>;

fn admit_monitor(
    monitors: &mut MonitorRelations,
    process: Arc<dyn Process>,
    acknowledgement: Option<MonitorAckSender>,
    monitor_permit: Option<OwnedSemaphorePermit>,
    monitor_notification: Option<MonitorNotification>,
) {
    monitors.insert(
        process.id(),
        (
            process,
            monitor_permit.expect("monitor signal must reserve monitor capacity"),
            monitor_notification,
        ),
    );
    if let Some(acknowledgement) = acknowledgement {
        let _ = acknowledgement.try_send(());
    }
}

/// Publishes terminal state, closes ingress, and applies queued monitor
/// relation changes in FIFO order. A target-aware sender that races with the
/// close either leaves its envelope in this drain or completes it directly.
fn close_signal_ingress(
    signal_mailbox: &mut SignalReceiverGuard<'_>,
    monitors: &mut MonitorRelations,
    death_reason: DeathReason,
) {
    signal_mailbox.close_with_reason(death_reason);
    while let Some(envelope) = signal_mailbox.try_recv() {
        let (signal, _, monitor_permit, monitor_notification) = envelope.into_process_parts();
        match signal {
            Signal::Monitor {
                process,
                acknowledgement,
            } => admit_monitor(
                monitors,
                process,
                acknowledgement,
                monitor_permit,
                monitor_notification,
            ),
            Signal::StopMonitoring { process_id } => {
                monitors.remove(&process_id);
            }
            _ => {}
        }
    }
}

pub(crate) struct ProcessTaskGuard {
    registration: Option<ProcessRegistration>,
    lifecycle: ProcessLifecycleHandle,
    finished: bool,
}

impl ProcessTaskGuard {
    pub(crate) fn new(
        registration: ProcessRegistration,
        lifecycle: ProcessLifecycleHandle,
    ) -> Self {
        Self {
            registration: Some(registration),
            lifecycle,
            finished: false,
        }
    }

    fn unregister(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.unregister();
        }
    }

    fn relation_count(&self) -> usize {
        self.lifecycle.relation_count()
    }

    fn unlink_peer(&self, peer_id: u64) -> bool {
        self.lifecycle.unlink_peer(peer_id)
    }

    fn finish(mut self, reason: DeathReason) {
        self.unregister();
        self.lifecycle.terminate(reason);
        self.finished = true;
    }
}

impl Drop for ProcessTaskGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        self.unregister();
        // A dropped execution future has no classified guest result. NoProcess preserves the
        // fail-closed link semantics while distinguishing cancellation from a reported failure.
        self.lifecycle.terminate(DeathReason::NoProcess);
        self.finished = true;
    }
}

/// Handle to a WASM process
#[derive(Clone, Debug)]
pub struct WasmProcess {
    id: u64,
    signal_sender: SignalSender,
}

impl WasmProcess {
    pub fn new(id: u64, signal_sender: SignalSender) -> Self {
        Self { id, signal_sender }
    }
}

impl Process for WasmProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
        self.signal_sender.send_to_process(self.id, signal)
    }

    fn reserve_monitor_notification(
        &self,
    ) -> std::result::Result<Option<MonitorNotification>, MonitorNotificationError> {
        self.signal_sender.reserve_monitor_notification().map(Some)
    }

    fn terminal_reason(&self) -> Option<DeathReason> {
        self.signal_sender.terminal_reason()
    }

    fn lifecycle_handle(&self) -> Option<ProcessLifecycleHandle> {
        self.signal_sender.lifecycle_handle(self.id)
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

    describe_gauge!(
        "lunatic.process.node.process.count",
        Unit::Count,
        "Number of processes currently admitted across all node environments"
    );
}

#[derive(Clone, Debug)]
pub struct NativeProcess {
    id: u64,
    signal_mailbox: SignalSender,
}

/// Spawns a process from a closure.
pub fn spawn<T, F, K, R>(
    env: Arc<dyn Environment>,
    func: F,
) -> Result<(JoinHandle<Result<T>>, NativeProcess)>
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
    let ((signal_sender, signal_mailbox), message_mailbox) = default_mailboxes();
    let process = NativeProcess {
        id,
        signal_mailbox: signal_sender,
    };
    let registration = register_process(env.clone(), id, Arc::new(process.clone()))?;
    let lifecycle = process
        .lifecycle_handle()
        .ok_or_else(|| anyhow!("native process {id} has no lifecycle state"))?;
    let lifecycle_guard = ProcessTaskGuard::new(registration, lifecycle);
    let fut = func(process.clone(), message_mailbox.clone());
    let join = tokio::task::spawn(new(
        fut,
        id,
        signal_mailbox,
        message_mailbox,
        None,
        lifecycle_guard,
    ));
    Ok((join, process))
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
) -> Result<(JoinHandle<Result<()>>, NativeProcess)>
where
    K: Future<Output = Result<()>> + Send + 'static,
    F: FnOnce(NativeProcess, MessageMailbox) -> K,
{
    let id = env.get_next_process_id();
    let ((signal_sender, signal_mailbox), message_mailbox) = default_mailboxes();
    let process = NativeProcess {
        id,
        signal_mailbox: signal_sender,
    };
    let registration = register_process(env.clone(), id, Arc::new(process.clone()))?;
    let lifecycle = process
        .lifecycle_handle()
        .ok_or_else(|| anyhow!("native process {id} has no lifecycle state"))?;
    let lifecycle_guard = ProcessTaskGuard::new(registration, lifecycle);
    let fut = func(process.clone(), message_mailbox.clone());

    let join = tokio::task::spawn(run_native_process(
        fut,
        id,
        signal_mailbox,
        message_mailbox,
        lifecycle_guard,
    ));
    Ok((join, process))
}

impl Process for NativeProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
        #[cfg(all(feature = "metrics", not(feature = "detailed_metrics")))]
        let labels = [("process_kind", "native")];
        #[cfg(all(feature = "metrics", feature = "detailed_metrics"))]
        let labels = [
            ("process_kind", "native".to_owned()),
            ("process_id", self.id().to_string()),
        ];
        #[cfg(feature = "metrics")]
        metrics::increment_counter!("lunatic.process.signals.send", &labels);

        self.signal_mailbox.send_to_process(self.id, signal)
    }

    fn reserve_monitor_notification(
        &self,
    ) -> std::result::Result<Option<MonitorNotification>, MonitorNotificationError> {
        self.signal_mailbox.reserve_monitor_notification().map(Some)
    }

    fn terminal_reason(&self) -> Option<DeathReason> {
        self.signal_mailbox.terminal_reason()
    }

    fn lifecycle_handle(&self) -> Option<ProcessLifecycleHandle> {
        self.signal_mailbox.lifecycle_handle(self.id)
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
    signal_mailbox: SignalReceiver,
    message_mailbox: MessageMailbox,
    mut lifecycle_guard: ProcessTaskGuard,
) -> Result<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    trace!("Native process {} spawned", id);
    let mut die_when_link_dies = true;
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
                    let Some(envelope) = signal else {
                        has_sender = false;
                        continue;
                    };
                    let (signal, mailbox_permit, monitor_permit, monitor_notification) =
                        envelope.into_process_parts();
                    match signal {
                        Signal::Message(message) => message_mailbox
                            .push_with_permit(
                                message,
                                mailbox_permit.expect("message signal must reserve mailbox capacity"),
                            )
                            .expect("signal sender must reserve from the destination mailbox"),
                        Signal::DieWhenLinkDies(value) => die_when_link_dies = value,
                        Signal::Link(_, _) | Signal::UnLink { .. } => {
                            warn!("Ignored a legacy one-sided link signal for process {id}");
                        }
                        Signal::LinkDied(process_id, tag, reason) => {
                            lifecycle_guard.unlink_peer(process_id);
                            match reason {
                                DeathReason::Failure | DeathReason::NoProcess if die_when_link_dies => {
                                    break Finished::KillSignal;
                                }
                                DeathReason::Failure | DeathReason::NoProcess => {
                                    message_mailbox
                                        .push_with_permit(
                                            Message::LinkDied(tag),
                                            mailbox_permit.expect(
                                                "link-death signal must reserve mailbox capacity",
                                            ),
                                        )
                                        .expect(
                                            "signal sender must reserve from the destination mailbox",
                                        );
                                }
                                DeathReason::Normal => {}
                            }
                        }
                        Signal::Monitor {
                            process,
                            acknowledgement,
                        } => admit_monitor(
                            &mut monitors,
                            process,
                            acknowledgement,
                            monitor_permit,
                            monitor_notification,
                        ),
                        Signal::StopMonitoring { process_id } => {
                            monitors.remove(&process_id);
                        }
                        Signal::ProcessDied { process_id, reason } => {
                            message_mailbox
                                .push_with_permit(
                                    Message::ProcessDied { process_id, reason },
                                    mailbox_permit.expect(
                                        "process-death signal must reserve mailbox capacity",
                                    ),
                                )
                                .expect(
                                    "signal sender must reserve from the destination mailbox",
                                );
                        }
                        Signal::Kill => break Finished::KillSignal,
                        Signal::HotReload {
                            module_id,
                            acknowledgement,
                            ..
                        } => {
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
                        Signal::Rollback {
                            module_id,
                            acknowledgement,
                            ..
                        } => {
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

    let (result, death_reason) = match result {
        Finished::Normal(Ok(())) => (Ok(()), DeathReason::Normal),
        Finished::Normal(Err(error)) => (Err(error), DeathReason::Failure),
        Finished::KillSignal => (Err(anyhow!("Process killed")), DeathReason::Failure),
        Finished::Panicked(message) => (
            Err(anyhow!("Process panicked: {message}")),
            DeathReason::Failure,
        ),
    };

    lifecycle_guard.unregister();
    close_signal_ingress(&mut signal_mailbox, &mut monitors, death_reason);
    lifecycle_guard.finish(death_reason);

    for (_, (monitor, _, notification)) in monitors {
        if let Some(notification) = notification {
            if let Err(error) = notification.deliver(id, death_reason) {
                warn!("Failed to deliver reserved monitor notification for process {id}: {error}");
            }
        } else if let Err(error) = monitor.send(Signal::ProcessDied {
            process_id: id,
            reason: death_reason,
        }) {
            warn!("Failed to notify custom monitor that process {id} died: {error}");
        }
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
    signal_mailbox: SignalReceiver,
    message_mailbox: MessageMailbox,
    context: Option<ProcessContext<S>>,
    mut lifecycle_guard: ProcessTaskGuard,
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

                let Some(envelope) = signal else {
                    debug_assert!(has_sender);
                    has_sender = false;
                    continue;
                };
                let (signal, mailbox_permit, monitor_permit, monitor_notification) =
                    envelope.into_process_parts();
                match signal {
                    Signal::Message(message) => {

                        #[cfg(feature = "metrics")]
                        message.write_metrics();

                        message_mailbox
                            .push_with_permit(
                                message,
                                mailbox_permit.expect(
                                    "message signal must reserve mailbox capacity",
                                ),
                            )
                            .expect("signal sender must reserve from the destination mailbox");

                        // process metrics
                        #[cfg(feature = "metrics")]
                        metrics::increment_counter!("lunatic.process.messages.send", &labels);

                        #[cfg(feature = "metrics")]
                        metrics::gauge!("lunatic.process.messages.outstanding", message_mailbox.len() as f64, &labels);
                    },
                    Signal::DieWhenLinkDies(value) => die_when_link_dies = value,
                    Signal::Link(_, _) | Signal::UnLink { .. } => {
                        warn!("Ignored a legacy one-sided link signal for process {id}");
                    },
                    // Depending if `die_when_link_dies` is set, process will die or turn the
                    // signal into a message
                    Signal::LinkDied(process_id, tag, reason) => {
                        lifecycle_guard.unlink_peer(process_id);
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
                                    message_mailbox
                                        .push_with_permit(
                                            message,
                                            mailbox_permit.expect(
                                                "link-death signal must reserve mailbox capacity",
                                            ),
                                        )
                                        .expect(
                                            "signal sender must reserve from the destination mailbox",
                                        );
                                }
                            },
                            // In case a linked process finishes normally, don't do anything.
                            DeathReason::Normal => {},
                        }
                    },
                    // Put process into list of monitor processes
                    Signal::Monitor {
                        process: proc,
                        acknowledgement,
                    } => admit_monitor(
                        &mut monitors,
                        proc,
                        acknowledgement,
                        monitor_permit,
                        monitor_notification,
                    ),
                    // Remove process from monitor list
                    Signal::StopMonitoring { process_id } => {
                        monitors.remove(&process_id);
                    }
                    // Notify process that a monitored process died
                    Signal::ProcessDied {
                        process_id: id,
                        reason,
                    } => {
                        message_mailbox
                            .push_with_permit(
                                Message::ProcessDied {
                                    process_id: id,
                                    reason,
                                },
                                mailbox_permit.expect(
                                    "process-death signal must reserve mailbox capacity",
                                ),
                            )
                            .expect("signal sender must reserve from the destination mailbox");
                    }
                    // Kill the process
                    Signal::Kill => break Finished::KillSignal,
                    // Hot reload signal - perform true hot reload
                    Signal::HotReload {
                        module_id,
                        expected_version,
                        new_version,
                        acknowledgement,
                    } => {
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
                                let failure = error.to_string();
                                log::error!("Failed to queue hot reload: {failure}");
                                let (_, _, _, _, acknowledgement) = error.into_inner().into_parts();
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    old_version,
                                    old_version,
                                    ProcessReloadStatus::Failed(
                                        format!("Failed to queue Wasm reload command: {failure}"),
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
                    Signal::Rollback {
                        module_id,
                        expected_version,
                        target_version,
                        acknowledgement,
                    } => {
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
                                let failure = error.to_string();
                                log::error!("Failed to queue rollback: {failure}");
                                let (_, _, _, _, acknowledgement) = error.into_inner().into_parts();
                                acknowledge_reload(
                                    acknowledgement,
                                    id,
                                    module_id,
                                    current_version,
                                    current_version,
                                    ProcessReloadStatus::Failed(
                                        format!("Failed to queue Wasm rollback command: {failure}"),
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
                    lifecycle_guard.relation_count(),
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
            warn!(
                "Process {} panicked, notifying: {} links",
                id,
                lifecycle_guard.relation_count()
            );

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

    lifecycle_guard.unregister();
    close_signal_ingress(&mut signal_mailbox, &mut monitors, death_reason);
    lifecycle_guard.finish(death_reason);

    // Notify all monitors that this process died
    for (_, (monitor, _, notification)) in monitors {
        if let Some(notification) = notification {
            if let Err(error) = notification.deliver(id, death_reason) {
                warn!("Failed to deliver reserved monitor notification for process {id}: {error}");
            }
        } else if let Err(error) = monitor.send(Signal::ProcessDied {
            process_id: id,
            reason: death_reason,
        }) {
            warn!("Failed to notify custom monitor that process {id} died: {error}");
        }
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

#[cfg(test)]
mod process_backpressure_tests {
    use std::{collections::HashMap, future::pending, sync::Arc, time::Duration};

    use crate::{
        close_signal_ingress,
        env::{register_process, Environment, LunaticEnvironment},
        link_processes,
        mailbox::DEFAULT_MESSAGE_MAILBOX_CAPACITY,
        message::Message,
        run_native_process, spawn_native,
        state::{mailboxes_with_capacity, SignalSendErrorKind, DEFAULT_SIGNAL_QUEUE_CAPACITY},
        DeathReason, NativeProcess, Process, ProcessTaskGuard, Signal, WasmProcess,
    };

    struct OrderingObserver {
        id: u64,
        environment: Arc<dyn Environment>,
        target_id: u64,
        delivery: std::sync::mpsc::SyncSender<(bool, DeathReason)>,
    }

    impl Process for OrderingObserver {
        fn id(&self) -> u64 {
            self.id
        }

        fn send(&self, signal: Signal) -> std::result::Result<(), crate::state::SignalSendError> {
            let Signal::ProcessDied { process_id, reason } = signal else {
                panic!("ordering observer received an unexpected signal");
            };
            assert_eq!(process_id, self.target_id);
            self.delivery
                .try_send((self.environment.get_process(process_id).is_none(), reason))
                .unwrap();
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_spawn_rejects_at_quota_and_abort_restores_capacity() {
        let environment: Arc<dyn Environment> =
            Arc::new(LunaticEnvironment::with_max_processes(91, 1));

        let (join, _) = spawn_native(environment.clone(), |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        assert_eq!(environment.process_count(), 1);

        let rejected = spawn_native(environment.clone(), |_, _| async move { Ok(()) });
        assert!(rejected.is_err());
        assert_eq!(environment.process_count(), 1);

        join.abort();
        assert!(join.await.is_err());
        assert_eq!(environment.process_count(), 0);

        let (reused_join, reused_process) = spawn_native(environment.clone(), |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        assert_eq!(environment.process_count(), 1);
        reused_process.send(Signal::Kill).unwrap();
        assert!(reused_join.await.unwrap().is_err());
        assert_eq!(environment.process_count(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aborting_a_linked_task_reclaims_both_relations_and_permits() {
        let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(910));
        let (join_a, process_a) = spawn_native(environment.clone(), |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        let (join_b, process_b) = spawn_native(environment, |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        let handle_a = process_a.lifecycle_handle().unwrap();
        let handle_b = process_b.lifecycle_handle().unwrap();

        link_processes(
            &process_a,
            Some(process_a.id()),
            None,
            &process_b,
            Some(process_b.id()),
            None,
        )
        .unwrap();
        assert_eq!(
            (handle_a.relation_count(), handle_b.relation_count()),
            (1, 1)
        );

        join_a.abort();
        assert!(join_a.await.unwrap_err().is_cancelled());
        let peer_result = tokio::time::timeout(Duration::from_secs(1), join_b)
            .await
            .expect("linked peer must receive the cancellation death")
            .unwrap();
        assert!(peer_result.is_err());

        assert_eq!(
            (handle_a.relation_count(), handle_b.relation_count()),
            (0, 0)
        );
        assert_eq!(
            handle_a.available_link_capacity(),
            DEFAULT_SIGNAL_QUEUE_CAPACITY
        );
        assert_eq!(
            handle_b.available_link_capacity(),
            DEFAULT_SIGNAL_QUEUE_CAPACITY
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_link_died_signal_removes_the_bilateral_relation() {
        let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(911));
        let (join_a, process_a) = spawn_native(environment.clone(), |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        let (join_b, process_b) = spawn_native(environment, |_, _| async move {
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        let handle_a = process_a.lifecycle_handle().unwrap();
        let handle_b = process_b.lifecycle_handle().unwrap();
        link_processes(
            &process_a,
            Some(process_a.id()),
            None,
            &process_b,
            Some(process_b.id()),
            None,
        )
        .unwrap();

        process_a
            .send(Signal::LinkDied(process_b.id(), None, DeathReason::Normal))
            .unwrap();
        let (acknowledgement, acknowledged) = tokio::sync::oneshot::channel();
        process_a
            .send(Signal::HotReload {
                module_id: 0,
                expected_version: None,
                new_version: 1,
                acknowledgement: Some(acknowledgement),
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), acknowledged)
            .await
            .expect("native signal barrier timed out")
            .expect("native signal barrier sender dropped");

        assert_eq!(
            (handle_a.relation_count(), handle_b.relation_count()),
            (0, 0)
        );
        assert_eq!(
            handle_a.available_link_capacity(),
            DEFAULT_SIGNAL_QUEUE_CAPACITY
        );
        assert_eq!(
            handle_b.available_link_capacity(),
            DEFAULT_SIGNAL_QUEUE_CAPACITY
        );

        process_a.send(Signal::Kill).unwrap();
        process_b.send(Signal::Kill).unwrap();
        assert!(join_a.await.unwrap().is_err());
        assert!(join_b.await.unwrap().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_receiver_is_bounded_and_recovered_payload_can_be_retried() {
        let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(92));
        let (mailbox_sender, mailbox_receiver) = tokio::sync::oneshot::channel();
        let (join, process) = spawn_native(environment, move |_, mailbox| async move {
            let _ = mailbox_sender.send(mailbox);
            pending::<anyhow::Result<()>>().await
        })
        .unwrap();
        let mailbox = mailbox_receiver.await.unwrap();

        for process_id in 0..DEFAULT_MESSAGE_MAILBOX_CAPACITY as u64 {
            process
                .send(Signal::Message(Message::ProcessDied {
                    process_id,
                    reason: DeathReason::Failure,
                }))
                .unwrap();
        }

        let error = process
            .send(Signal::Message(Message::ProcessDied {
                process_id: u64::MAX,
                reason: DeathReason::NoProcess,
            }))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::MailboxFull);
        assert_eq!(mailbox.available_capacity(), 0);

        let recovered = error.into_signal();
        assert!(matches!(
            mailbox.pop(None).await,
            Message::ProcessDied {
                process_id: 0,
                reason: DeathReason::Failure
            }
        ));
        process.send(recovered).unwrap();
        assert_eq!(mailbox.available_capacity(), 0);

        join.abort();
        let _ = join.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn monitor_death_delivery_survives_saturated_observer_ingress() {
        let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(93));

        let ((observer_sender, _observer_receiver), observer_mailbox) =
            mailboxes_with_capacity(1, 1);
        let observer = Arc::new(NativeProcess {
            id: environment.get_next_process_id(),
            signal_mailbox: observer_sender.clone(),
        });

        let ((target_sender, target_receiver), target_mailbox) = mailboxes_with_capacity(2, 1);
        let target_id = environment.get_next_process_id();
        let target = Arc::new(NativeProcess {
            id: target_id,
            signal_mailbox: target_sender.clone(),
        });
        let registration =
            register_process(environment, target_id, target.clone()).expect("target admission");
        let lifecycle_guard = ProcessTaskGuard::new(
            registration,
            target.lifecycle_handle().expect("target lifecycle"),
        );
        let (release_sender, release_receiver) = tokio::sync::oneshot::channel::<()>();
        let join = tokio::spawn(run_native_process(
            async move {
                release_receiver
                    .await
                    .map_err(|_| anyhow::anyhow!("test release channel closed"))?;
                Ok(())
            },
            target_id,
            target_receiver,
            target_mailbox,
            lifecycle_guard,
        ));

        let (monitor_acknowledgement, monitor_registered) = std::sync::mpsc::sync_channel(1);
        target
            .send(Signal::Monitor {
                process: observer,
                acknowledgement: Some(monitor_acknowledgement),
            })
            .unwrap();
        monitor_registered
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        assert_eq!(observer_mailbox.available_capacity(), 0);
        observer_sender
            .send(Signal::DieWhenLinkDies(false))
            .unwrap();
        assert_eq!(observer_sender.available_capacity(), 0);

        release_sender.send(()).unwrap();
        assert!(join.await.unwrap().is_ok());
        assert!(matches!(
            observer_mailbox.pop(None).await,
            Message::ProcessDied {
                process_id: id,
                reason: DeathReason::Normal
            } if id == target_id
        ));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_monitor_after_exit_preserves_reason_and_acknowledges_once() {
        for (node_id, expected_reason, fail) in [
            (94, DeathReason::Normal, false),
            (95, DeathReason::Failure, true),
        ] {
            let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(node_id));
            let (join, target) = spawn_native(environment, move |_, _| async move {
                if fail {
                    Err(anyhow::anyhow!("expected test failure"))
                } else {
                    Ok(())
                }
            })
            .unwrap();
            let target_id = target.id();
            assert_eq!(join.await.unwrap().is_err(), fail);

            let ((observer_sender, _observer_receiver), observer_mailbox) =
                mailboxes_with_capacity(1, 1);
            let observer = Arc::new(NativeProcess {
                id: 10_000 + node_id,
                signal_mailbox: observer_sender,
            });
            let (acknowledgement, registered) = std::sync::mpsc::sync_channel(1);

            target
                .send(Signal::Monitor {
                    process: observer,
                    acknowledgement: Some(acknowledgement),
                })
                .unwrap();

            registered.recv_timeout(Duration::from_secs(1)).unwrap();
            assert_eq!(target.terminal_reason(), Some(expected_reason));
            assert_eq!(observer_mailbox.len(), 1);
            assert!(matches!(
                observer_mailbox.pop(None).await,
                Message::ProcessDied { process_id, reason }
                    if process_id == target_id && reason == expected_reason
            ));
            assert!(observer_mailbox.is_empty());
        }
    }

    #[tokio::test]
    async fn wasm_handle_monitor_after_exit_preserves_failure_and_acknowledges_once() {
        let ((target_sender, target_receiver), _target_mailbox) = mailboxes_with_capacity(1, 1);
        let target = WasmProcess::new(123, target_sender);
        {
            let mut receiver = target_receiver.lock().await;
            receiver.close_with_reason(DeathReason::Failure);
        }

        let ((observer_sender, _observer_receiver), observer_mailbox) =
            mailboxes_with_capacity(1, 1);
        let observer = Arc::new(NativeProcess {
            id: 124,
            signal_mailbox: observer_sender,
        });
        let (acknowledgement, registered) = std::sync::mpsc::sync_channel(1);

        target
            .send(Signal::Monitor {
                process: observer,
                acknowledgement: Some(acknowledgement),
            })
            .unwrap();

        registered.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(target.terminal_reason(), Some(DeathReason::Failure));
        assert_eq!(observer_mailbox.len(), 1);
        assert!(matches!(
            observer_mailbox.pop(None).await,
            Message::ProcessDied {
                process_id: 123,
                reason: DeathReason::Failure
            }
        ));
        assert!(observer_mailbox.is_empty());
    }

    #[tokio::test]
    async fn exit_drain_applies_pending_monitor_changes_in_fifo_order() {
        let ((target_sender, target_receiver), _target_mailbox) = mailboxes_with_capacity(3, 1);
        let ((observer_sender, _observer_receiver), observer_mailbox) =
            mailboxes_with_capacity(1, 2);
        let observer = Arc::new(NativeProcess {
            id: 201,
            signal_mailbox: observer_sender,
        });
        let (first_acknowledgement, first_registered) = std::sync::mpsc::sync_channel(1);
        let (second_acknowledgement, second_registered) = std::sync::mpsc::sync_channel(1);

        target_sender
            .send(Signal::Monitor {
                process: observer.clone(),
                acknowledgement: Some(first_acknowledgement),
            })
            .unwrap();
        target_sender
            .send(Signal::StopMonitoring {
                process_id: observer.id(),
            })
            .unwrap();
        target_sender
            .send(Signal::Monitor {
                process: observer,
                acknowledgement: Some(second_acknowledgement),
            })
            .unwrap();

        let mut receiver = target_receiver.lock().await;
        let mut monitors = HashMap::new();
        close_signal_ingress(&mut receiver, &mut monitors, DeathReason::Failure);
        first_registered
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        second_registered
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(monitors.len(), 1);

        for (_, (_, _, notification)) in monitors {
            notification
                .expect("built-in observer reservation")
                .deliver(200, DeathReason::Failure)
                .unwrap();
        }
        assert_eq!(observer_mailbox.len(), 1);
        assert!(matches!(
            observer_mailbox.pop(None).await,
            Message::ProcessDied {
                process_id: 200,
                reason: DeathReason::Failure
            }
        ));
        assert!(observer_mailbox.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn monitor_ack_race_still_notifies_after_environment_removal() {
        let environment: Arc<dyn Environment> = Arc::new(LunaticEnvironment::new(96));
        let (join, target) = spawn_native(environment.clone(), |_, _| async move {
            tokio::task::yield_now().await;
            Ok(())
        })
        .unwrap();
        let target_id = target.id();
        let (delivery, delivered) = std::sync::mpsc::sync_channel(1);
        let observer = Arc::new(OrderingObserver {
            id: 20_000,
            environment: environment.clone(),
            target_id,
            delivery,
        });
        let (acknowledgement, registered) = std::sync::mpsc::sync_channel(1);

        target
            .send(Signal::Monitor {
                process: observer,
                acknowledgement: Some(acknowledgement),
            })
            .unwrap();
        registered.recv_timeout(Duration::from_secs(1)).unwrap();

        assert!(join.await.unwrap().is_ok());
        let (removed_before_notification, reason) =
            delivered.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(removed_before_notification);
        assert_eq!(reason, DeathReason::Normal);
        assert_eq!(target.terminal_reason(), Some(DeathReason::Normal));
    }
}
