use std::{
    collections::HashMap,
    error::Error,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use anyhow::Result;
use hash_map_id::HashMapId;
use tokio::sync::{
    mpsc::{self, error::TrySendError},
    Mutex, MutexGuard, Notify, OwnedSemaphorePermit, RwLock, Semaphore,
};
use wasmtime::Linker;

use crate::{
    config::{
        ProcessConfig, DEFAULT_MAX_MESSAGE_RESOURCES, DEFAULT_MAX_MESSAGE_SIZE,
        DEFAULT_MAX_SIGNAL_QUEUE,
    },
    mailbox::{MailboxPermit, MailboxPushError, MessageMailbox, DEFAULT_MESSAGE_MAILBOX_CAPACITY},
    resource_migration::{ResourceMigrationSnapshot, ResourceTransferReport},
    runtimes::wasmtime::{WasmtimeCompiledModule, WasmtimeRuntime},
    Signal,
};

pub type ConfigResources<T> = HashMapId<T>;

/// Maximum number of names retained by one shared process registry.
///
/// A registry may outlive the processes that inserted entries, so it needs an
/// independent finite ceiling instead of relying only on process admission.
pub const MAX_REGISTRY_ENTRIES: usize = crate::env::DEFAULT_MAX_PROCESSES;

/// Maximum UTF-8 byte length of one process registry name.
pub const MAX_REGISTRY_NAME_BYTES: usize = 1024;

/// Validates a registry insertion while the caller holds the registry's write
/// lock. Existing entries may always be replaced at capacity.
pub fn ensure_registry_insert_capacity(
    registry: &HashMap<String, (u64, u64)>,
    name: &str,
) -> Result<()> {
    ensure_registry_insert_capacity_with_limit(registry, name, MAX_REGISTRY_ENTRIES)
}

fn ensure_registry_insert_capacity_with_limit(
    registry: &HashMap<String, (u64, u64)>,
    name: &str,
    max_entries: usize,
) -> Result<()> {
    anyhow::ensure!(
        !name.chars().any(char::is_control),
        "Registry names cannot contain control characters"
    );
    anyhow::ensure!(
        name.len() <= MAX_REGISTRY_NAME_BYTES,
        "Registry name is {} bytes; maximum is {} bytes",
        name.len(),
        MAX_REGISTRY_NAME_BYTES
    );
    anyhow::ensure!(
        registry.contains_key(name) || registry.len() < max_entries,
        "Registry capacity ({max_entries}) reached"
    );
    Ok(())
}

/// Default maximum number of signals waiting to be handled by a process.
pub const DEFAULT_SIGNAL_QUEUE_CAPACITY: usize = DEFAULT_MAX_SIGNAL_QUEUE as usize;

/// A signal staged in the bounded ingress queue.
///
/// Signals that can become mailbox messages carry an admission permit for the
/// destination mailbox. If the signal is handled without being converted, the
/// permit is released when this envelope is dropped.
pub struct SignalEnvelope {
    signal: Signal,
    mailbox_permit: Option<MailboxPermit>,
    // Message-producing signals use a second admission pool that is one slot
    // smaller than the physical signal queue. This keeps lifecycle control
    // enqueueable during a data flood at normal configured capacities.
    _data_queue_permit: Option<OwnedSemaphorePermit>,
    link_permit: Option<OwnedSemaphorePermit>,
    monitor_permit: Option<OwnedSemaphorePermit>,
    monitor_notification: Option<MonitorNotification>,
}

impl SignalEnvelope {
    fn control(signal: Signal) -> Self {
        Self {
            signal,
            mailbox_permit: None,
            _data_queue_permit: None,
            link_permit: None,
            monitor_permit: None,
            monitor_notification: None,
        }
    }
}

impl SignalEnvelope {
    /// Borrows the queued signal without changing its admission state.
    pub fn signal(&self) -> &Signal {
        &self.signal
    }

    /// Returns whether this signal has reserved a future mailbox slot.
    pub fn has_mailbox_permit(&self) -> bool {
        self.mailbox_permit.is_some()
    }

    /// Splits the envelope so a message-producing signal can transfer its
    /// reservation into [`MessageMailbox::push_with_permit`].
    pub fn into_parts(self) -> (Signal, Option<MailboxPermit>) {
        (self.signal, self.mailbox_permit)
    }

    pub(crate) fn into_process_parts(
        self,
    ) -> (
        Signal,
        Option<MailboxPermit>,
        Option<OwnedSemaphorePermit>,
        Option<OwnedSemaphorePermit>,
        Option<MonitorNotification>,
    ) {
        (
            self.signal,
            self.mailbox_permit,
            self.link_permit,
            self.monitor_permit,
            self.monitor_notification,
        )
    }

    /// Returns the original signal and releases any mailbox reservation.
    pub fn into_signal(self) -> Signal {
        self.signal
    }
}

/// A mailbox slot reserved for one future monitor-death notification.
///
/// The reservation is acquired when the monitor relation is admitted and is
/// owned by that relation until it is removed or the monitored process exits.
/// This makes delivery independent of signal-queue and mailbox saturation at
/// exit time while keeping the retained capacity finite.
pub struct MonitorNotification {
    mailbox: MessageMailbox,
    permit: MailboxPermit,
}

impl MonitorNotification {
    fn reserve(mailbox: MessageMailbox) -> Result<Self, MonitorNotificationError> {
        let permit = mailbox
            .try_reserve()
            .map_err(|_| MonitorNotificationError::MailboxFull)?;
        Ok(Self { mailbox, permit })
    }

    pub(crate) fn deliver(self, process_id: u64) -> Result<(), MailboxPushError> {
        self.mailbox.push_with_permit(
            crate::message::Message::ProcessDied(process_id),
            self.permit,
        )
    }
}

impl fmt::Debug for MonitorNotification {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MonitorNotification { reserved: true }")
    }
}

/// The observer could not reserve durable delivery for a monitor relation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MonitorNotificationError {
    /// The observer's message mailbox has no unreserved slot.
    MailboxFull,
    /// The observer has already stopped receiving signals.
    Closed,
}

impl fmt::Display for MonitorNotificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MailboxFull => formatter.write_str("observer message mailbox is full"),
            Self::Closed => formatter.write_str("observer signal receiver is closed"),
        }
    }
}

impl Error for MonitorNotificationError {}

impl fmt::Debug for SignalEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignalEnvelope")
            .field("has_mailbox_permit", &self.has_mailbox_permit())
            .finish_non_exhaustive()
    }
}

/// Synchronous, bounded signal ingress for a process.
#[derive(Clone)]
pub struct SignalSender {
    sender: mpsc::Sender<SignalEnvelope>,
    kill: Arc<KillSignal>,
    data_admission: Arc<Semaphore>,
    link_admission: Arc<Semaphore>,
    monitor_admission: Arc<Semaphore>,
    message_mailbox: MessageMailbox,
    capacity: usize,
    max_message_bytes: u64,
    max_message_resources: u32,
}

impl SignalSender {
    /// Attempts to enqueue a signal without waiting for capacity.
    ///
    /// Message-producing signals reserve mailbox capacity before entering the
    /// signal queue. Every failure returns ownership of the original signal.
    pub fn send(&self, signal: Signal) -> Result<(), SignalSendError> {
        // Prefer reporting a terminal receiver closure over a transient
        // mailbox-capacity failure when closure is already observable.
        if self.sender.is_closed() {
            return Err(SignalSendError::Closed(signal));
        }

        // Kill is idempotent lifecycle state, not queue data. Keeping it in a
        // dedicated latch makes abort reliable even when a capacity-one queue
        // or a flood of non-message control signals occupies every slot.
        if matches!(signal, Signal::Kill) {
            self.kill.requested.store(true, Ordering::Release);
            self.kill.notify.notify_one();
            return Ok(());
        }

        if let Signal::Message(crate::message::Message::Data(message)) = &signal {
            // `Vec` capacity, not just its logical length, is retained while a
            // message waits outside the Wasm Store. Counting the allocation
            // prevents a native producer from queueing a tiny payload backed
            // by an arbitrarily large buffer.
            let message_bytes = u64::try_from(message.buffer.capacity()).unwrap_or(u64::MAX);
            if message_bytes > self.max_message_bytes {
                return Err(SignalSendError::MessageTooLarge {
                    signal,
                    actual: message_bytes,
                    max: self.max_message_bytes,
                });
            }

            // Count allocated slots, including spare capacity and `None`.
            // Every slot consumes host memory even when it currently contains
            // no resource.
            let message_resources = u64::try_from(message.resources.capacity()).unwrap_or(u64::MAX);
            if message_resources > u64::from(self.max_message_resources) {
                return Err(SignalSendError::TooManyMessageResources {
                    signal,
                    actual: message_resources,
                    max: self.max_message_resources,
                });
            }
        }

        let mailbox_permit = if reserves_mailbox_capacity(&signal) {
            match self.message_mailbox.try_reserve() {
                Ok(permit) => Some(permit),
                Err(_) => return Err(SignalSendError::MailboxFull(signal)),
            }
        } else {
            None
        };

        let data_queue_permit = if mailbox_permit.is_some() {
            match Arc::clone(&self.data_admission).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => return Err(SignalSendError::QueueFull(signal)),
            }
        } else {
            None
        };

        let link_permit = if matches!(signal, Signal::Link(_, _)) {
            match Arc::clone(&self.link_admission).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => return Err(SignalSendError::QueueFull(signal)),
            }
        } else {
            None
        };

        let monitor_permit = if matches!(signal, Signal::Monitor(_)) {
            match Arc::clone(&self.monitor_admission).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => return Err(SignalSendError::QueueFull(signal)),
            }
        } else {
            None
        };

        // A monitor is useful only if its eventual ProcessDied message has
        // durable admission. Reserve that observer-owned slot before the
        // relation enters the target's queue. Built-in processes support
        // direct delivery; custom Process implementations may opt out and
        // retain the legacy signal-based notification path.
        let monitor_notification = if let Signal::Monitor(observer) = &signal {
            match observer.reserve_monitor_notification() {
                Ok(notification) => notification,
                Err(MonitorNotificationError::MailboxFull) => {
                    return Err(SignalSendError::MailboxFull(signal));
                }
                Err(MonitorNotificationError::Closed) => {
                    return Err(SignalSendError::Closed(signal));
                }
            }
        } else {
            None
        };

        let envelope = SignalEnvelope {
            signal,
            mailbox_permit,
            _data_queue_permit: data_queue_permit,
            link_permit,
            monitor_permit,
            monitor_notification,
        };
        match self.sender.try_send(envelope) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(envelope)) => {
                Err(SignalSendError::QueueFull(envelope.into_signal()))
            }
            Err(TrySendError::Closed(envelope)) => {
                Err(SignalSendError::Closed(envelope.into_signal()))
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }

    /// Returns the configured signal queue capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns signal queue slots currently available to this sender.
    pub fn available_capacity(&self) -> usize {
        self.sender.capacity()
    }

    /// Returns the largest data-message payload accepted by this destination.
    pub fn max_message_bytes(&self) -> u64 {
        self.max_message_bytes
    }

    /// Returns the largest data-message resource table accepted by this destination.
    pub fn max_message_resources(&self) -> u32 {
        self.max_message_resources
    }

    pub(crate) fn reserve_monitor_notification(
        &self,
    ) -> Result<MonitorNotification, MonitorNotificationError> {
        if self.sender.is_closed() {
            return Err(MonitorNotificationError::Closed);
        }
        MonitorNotification::reserve(self.message_mailbox.clone())
    }
}

impl fmt::Debug for SignalSender {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SignalSender")
            .field("capacity", &self.capacity)
            .field("available_capacity", &self.available_capacity())
            .field("max_message_bytes", &self.max_message_bytes)
            .field("max_message_resources", &self.max_message_resources)
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// Cloneable access to a process's single bounded signal receiver.
#[derive(Clone)]
pub struct SignalReceiver {
    receiver: Arc<Mutex<mpsc::Receiver<SignalEnvelope>>>,
    kill: Arc<KillSignal>,
}

#[derive(Debug, Default)]
struct KillSignal {
    requested: AtomicBool,
    notify: Notify,
}

pub struct SignalReceiverGuard<'a> {
    receiver: MutexGuard<'a, mpsc::Receiver<SignalEnvelope>>,
    kill: Arc<KillSignal>,
}

impl SignalReceiverGuard<'_> {
    pub async fn recv(&mut self) -> Option<SignalEnvelope> {
        loop {
            if self.kill.requested.swap(false, Ordering::AcqRel) {
                return Some(SignalEnvelope::control(Signal::Kill));
            }

            tokio::select! {
                biased;
                _ = self.kill.notify.notified() => continue,
                signal = self.receiver.recv() => return signal,
            }
        }
    }
}

impl SignalReceiver {
    /// Locks the underlying single-consumer queue for a process execution loop.
    pub async fn lock(&self) -> SignalReceiverGuard<'_> {
        SignalReceiverGuard {
            receiver: self.receiver.lock().await,
            kill: self.kill.clone(),
        }
    }

    /// Receives one signal envelope.
    pub async fn recv(&self) -> Option<SignalEnvelope> {
        self.lock().await.recv().await
    }
}

impl fmt::Debug for SignalReceiver {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SignalReceiver { .. }")
    }
}

/// Stable classification for a bounded signal send failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalSendErrorKind {
    /// A data-message buffer allocation exceeds the destination's byte limit.
    MessageTooLarge,
    /// A data-message resource table exceeds the destination's slot limit.
    TooManyMessageResources,
    /// A signal that could become a message could not reserve mailbox space.
    MailboxFull,
    /// The bounded signal ingress itself has no free queue slots.
    QueueFull,
    /// The receiving side has been closed or dropped.
    Closed,
}

/// An ownership-preserving signal send failure.
pub enum SignalSendError {
    MessageTooLarge {
        signal: Signal,
        actual: u64,
        max: u64,
    },
    TooManyMessageResources {
        signal: Signal,
        actual: u64,
        max: u32,
    },
    MailboxFull(Signal),
    QueueFull(Signal),
    Closed(Signal),
}

impl SignalSendError {
    pub fn kind(&self) -> SignalSendErrorKind {
        match self {
            Self::MessageTooLarge { .. } => SignalSendErrorKind::MessageTooLarge,
            Self::TooManyMessageResources { .. } => SignalSendErrorKind::TooManyMessageResources,
            Self::MailboxFull(_) => SignalSendErrorKind::MailboxFull,
            Self::QueueFull(_) => SignalSendErrorKind::QueueFull,
            Self::Closed(_) => SignalSendErrorKind::Closed,
        }
    }

    pub fn signal(&self) -> &Signal {
        match self {
            Self::MessageTooLarge { signal, .. }
            | Self::TooManyMessageResources { signal, .. }
            | Self::MailboxFull(signal)
            | Self::QueueFull(signal)
            | Self::Closed(signal) => signal,
        }
    }

    pub fn into_signal(self) -> Signal {
        match self {
            Self::MessageTooLarge { signal, .. }
            | Self::TooManyMessageResources { signal, .. }
            | Self::MailboxFull(signal)
            | Self::QueueFull(signal)
            | Self::Closed(signal) => signal,
        }
    }
}

impl fmt::Debug for SignalSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("SignalSendError");
        debug.field("kind", &self.kind());
        match self {
            Self::MessageTooLarge { actual, max, .. } => {
                debug.field("actual", actual).field("max", max);
            }
            Self::TooManyMessageResources { actual, max, .. } => {
                debug.field("actual", actual).field("max", max);
            }
            Self::MailboxFull(_) | Self::QueueFull(_) | Self::Closed(_) => {}
        }
        debug.finish_non_exhaustive()
    }
}

impl fmt::Display for SignalSendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind() {
            SignalSendErrorKind::MessageTooLarge => {
                let Self::MessageTooLarge { actual, max, .. } = self else {
                    unreachable!("error kind must match variant")
                };
                write!(
                    formatter,
                    "message buffer retains {actual} bytes, exceeding destination limit {max}"
                )
            }
            SignalSendErrorKind::TooManyMessageResources => {
                let Self::TooManyMessageResources { actual, max, .. } = self else {
                    unreachable!("error kind must match variant")
                };
                write!(
                    formatter,
                    "message has {actual} resource slots, exceeding destination limit {max}"
                )
            }
            SignalSendErrorKind::MailboxFull => formatter.write_str("message mailbox is full"),
            SignalSendErrorKind::QueueFull => formatter.write_str("signal queue is full"),
            SignalSendErrorKind::Closed => formatter.write_str("signal receiver is closed"),
        }
    }
}

impl Error for SignalSendError {}

fn reserves_mailbox_capacity(signal: &Signal) -> bool {
    matches!(
        signal,
        Signal::Message(_)
            | Signal::ProcessDied(_)
            | Signal::LinkDied(
                _,
                _,
                crate::DeathReason::Failure | crate::DeathReason::NoProcess
            )
    )
}

/// Creates a bounded signal ingress tied to an existing message mailbox.
///
/// `signal_capacity` must be greater than zero, matching Tokio's bounded MPSC
/// channel contract.
pub fn signal_mailbox(
    signal_capacity: usize,
    message_mailbox: &MessageMailbox,
) -> (SignalSender, SignalReceiver) {
    signal_mailbox_with_limits(
        signal_capacity,
        message_mailbox,
        DEFAULT_MAX_MESSAGE_SIZE,
        DEFAULT_MAX_MESSAGE_RESOURCES,
    )
}

/// Creates bounded signal ingress with destination-owned data-message limits.
pub fn signal_mailbox_with_limits(
    signal_capacity: usize,
    message_mailbox: &MessageMailbox,
    max_message_bytes: u64,
    max_message_resources: u32,
) -> (SignalSender, SignalReceiver) {
    assert!(
        signal_capacity > 0,
        "signal queue capacity must be non-zero"
    );
    let (sender, receiver) = mpsc::channel(signal_capacity);
    let kill = Arc::new(KillSignal::default());
    // Capacity-one queues retain their legacy ability to carry a message;
    // larger queues reserve one physical slot from data admission.
    let data_capacity = signal_capacity.saturating_sub(1).max(1);
    (
        SignalSender {
            sender,
            kill: kill.clone(),
            data_admission: Arc::new(Semaphore::new(data_capacity)),
            link_admission: Arc::new(Semaphore::new(signal_capacity)),
            monitor_admission: Arc::new(Semaphore::new(signal_capacity)),
            message_mailbox: message_mailbox.clone(),
            capacity: signal_capacity,
            max_message_bytes,
            max_message_resources,
        },
        SignalReceiver {
            receiver: Arc::new(Mutex::new(receiver)),
            kill,
        },
    )
}

/// Creates bounded signal and message mailboxes with explicit capacities.
pub fn mailboxes_with_capacity(
    signal_capacity: usize,
    message_capacity: usize,
) -> ((SignalSender, SignalReceiver), MessageMailbox) {
    mailboxes_with_limits(
        signal_capacity,
        message_capacity,
        DEFAULT_MAX_MESSAGE_SIZE,
        DEFAULT_MAX_MESSAGE_RESOURCES,
    )
}

/// Creates bounded mailboxes with explicit queue and data-message limits.
pub fn mailboxes_with_limits(
    signal_capacity: usize,
    message_capacity: usize,
    max_message_bytes: u64,
    max_message_resources: u32,
) -> ((SignalSender, SignalReceiver), MessageMailbox) {
    let message_mailbox =
        MessageMailbox::with_limits(message_capacity, max_message_bytes, max_message_resources);
    let signal_mailbox = signal_mailbox_with_limits(
        signal_capacity,
        &message_mailbox,
        max_message_bytes,
        max_message_resources,
    );
    (signal_mailbox, message_mailbox)
}

/// Creates signal and message mailboxes with finite native-process defaults.
pub fn default_mailboxes() -> ((SignalSender, SignalReceiver), MessageMailbox) {
    mailboxes_with_limits(
        DEFAULT_SIGNAL_QUEUE_CAPACITY,
        DEFAULT_MESSAGE_MAILBOX_CAPACITY,
        DEFAULT_MAX_MESSAGE_SIZE,
        DEFAULT_MAX_MESSAGE_RESOURCES,
    )
}

/// The internal state of a process.
///
/// The `ProcessState` has two main roles:
/// - It holds onto all vm resources (file descriptors, tcp streams, channels, ...)
/// - Registers all host functions working on those resources to the `Linker`
pub trait ProcessState: Sized + crate::reloadable_state::ReloadableState {
    type Config: ProcessConfig + Default + Send + Sync;

    // Create a new `ProcessState` using the parent's state (self) to inherit environment and
    // other parts of the state.
    // This is used in the guest function `spawn` which uses this trait and not the concrete state.
    fn new_state(
        &self,
        module: Arc<WasmtimeCompiledModule<Self>>,
        config: Arc<Self::Config>,
    ) -> Result<Self>;

    /// Create the replacement state used by an in-process hot reload.
    ///
    /// Unlike [`ProcessState::new_state`], which creates a child process, a
    /// replacement must retain the current process identity and share its
    /// signal and message mailboxes. Implementations that support hot reload
    /// must override this method. The default fails closed so a reload cannot
    /// silently turn into a different process or lose queued messages.
    fn new_state_for_reload(
        &self,
        _module: Arc<WasmtimeCompiledModule<Self>>,
        _config: Arc<Self::Config>,
    ) -> Result<Self> {
        anyhow::bail!("ProcessState does not implement in-process hot reload replacement")
    }

    /// Register all host functions to the linker.
    fn register(linker: &mut Linker<Self>) -> Result<()>;
    /// Marks a wasm instance as initialized
    fn initialize(&mut self);
    /// Returns true if the instance was initialized
    fn is_initialized(&self) -> bool;

    /// Returns the WebAssembly runtime
    fn runtime(&self) -> &WasmtimeRuntime;
    // Returns the WebAssembly module
    fn module(&self) -> &Arc<WasmtimeCompiledModule<Self>>;
    /// Returns the process configuration
    fn config(&self) -> &Arc<Self::Config>;

    // Returns process ID
    fn id(&self) -> u64;
    /// Returns the node identity attached to audit events, when known.
    fn audit_node_id(&self) -> Option<u64> {
        None
    }
    /// Returns the environment identity attached to audit events, when known.
    fn audit_environment_id(&self) -> Option<u64> {
        None
    }
    // Returns signal mailbox
    fn signal_mailbox(&self) -> &(SignalSender, SignalReceiver);
    // Returns message mailbox
    fn message_mailbox(&self) -> &MessageMailbox;

    // Config resources
    fn config_resources(&self) -> &ConfigResources<Self::Config>;
    fn config_resources_mut(&mut self) -> &mut ConfigResources<Self::Config>;

    // Registry
    fn registry(&self) -> &Arc<RwLock<HashMap<String, (u64, u64)>>>;

    fn capture_resource_snapshot(&self) -> Result<Option<ResourceMigrationSnapshot>> {
        Ok(None)
    }

    fn restore_resource_snapshot(&mut self, _snapshot: ResourceMigrationSnapshot) -> Result<()> {
        Ok(())
    }

    /// Transfer runtime-owned resources into a replacement state during an
    /// in-process hot reload.
    ///
    /// The default implementation uses the serialized snapshot contract.
    /// States that own live network resources should override this method so
    /// opaque handles and protocol sessions can be moved without serialization.
    /// Implementations must leave `self` unchanged when returning an error.
    fn transfer_runtime_resources_to(
        &mut self,
        target: &mut Self,
    ) -> Result<ResourceTransferReport> {
        let Some(snapshot) = self.capture_resource_snapshot()? else {
            return Ok(ResourceTransferReport::default());
        };
        let report = ResourceTransferReport::from_snapshot(&snapshot);
        target.restore_resource_snapshot(snapshot)?;
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use crate::{
        mailbox::MessageMailbox,
        message::{DataMessage, Message},
        DeathReason, NativeProcess, Signal,
    };

    use super::{
        ensure_registry_insert_capacity_with_limit, signal_mailbox, signal_mailbox_with_limits,
        SignalSendErrorKind, MAX_REGISTRY_NAME_BYTES,
    };

    #[test]
    fn registry_admission_bounds_names_and_entries_but_allows_replacement() {
        let mut registry = HashMap::new();
        registry.insert("existing".to_owned(), (0, 1));

        ensure_registry_insert_capacity_with_limit(&registry, "existing", 1).unwrap();
        assert!(
            ensure_registry_insert_capacity_with_limit(&registry, "new", 1)
                .unwrap_err()
                .to_string()
                .contains("capacity")
        );

        let oversized = "x".repeat(MAX_REGISTRY_NAME_BYTES + 1);
        assert!(
            ensure_registry_insert_capacity_with_limit(&HashMap::new(), &oversized, 1)
                .unwrap_err()
                .to_string()
                .contains("Registry name")
        );
    }

    #[test]
    fn registry_admission_rejects_control_characters_without_echoing_the_name() {
        for name in [
            "line\nbreak",
            "nul\0byte",
            "escape\u{1b}[31m",
            "unicode\u{85}next",
        ] {
            let error = ensure_registry_insert_capacity_with_limit(&HashMap::new(), name, 1)
                .expect_err("control characters must be rejected")
                .to_string();
            assert_eq!(error, "Registry names cannot contain control characters");
            assert!(!error.contains(name));
        }
    }

    #[tokio::test]
    async fn staged_message_keeps_mailbox_slot_through_transfer_and_pop() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox(4, &mailbox);

        sender
            .send(Signal::Message(Message::ProcessDied(7)))
            .unwrap();
        assert_eq!(mailbox.available_capacity(), 0);
        assert!(mailbox.is_empty());

        let envelope = receiver.recv().await.unwrap();
        assert!(envelope.has_mailbox_permit());
        assert_eq!(mailbox.available_capacity(), 0);
        let (signal, permit) = envelope.into_parts();
        let message = match signal {
            Signal::Message(message) => message,
            _ => panic!("received the wrong signal"),
        };
        mailbox
            .push_with_permit(message, permit.expect("message permit"))
            .unwrap();

        assert_eq!(mailbox.len(), 1);
        assert_eq!(mailbox.available_capacity(), 0);
        assert!(matches!(mailbox.pop(None).await, Message::ProcessDied(7)));
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn every_message_producing_signal_reserves_capacity() {
        let mailbox = MessageMailbox::new(3);
        let (sender, receiver) = signal_mailbox(4, &mailbox);

        sender
            .send(Signal::Message(Message::ProcessDied(1)))
            .unwrap();
        sender.send(Signal::ProcessDied(2)).unwrap();
        sender
            .send(Signal::LinkDied(3, Some(4), DeathReason::Failure))
            .unwrap();
        assert_eq!(mailbox.available_capacity(), 0);

        // Control-only signals do not need mailbox admission, even when the
        // message mailbox is full.
        sender.send(Signal::Kill).unwrap();

        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::Kill
        ));

        for _ in 0..3 {
            let envelope = receiver.recv().await.unwrap();
            assert!(envelope.has_mailbox_permit());
            drop(envelope);
        }
        assert_eq!(mailbox.available_capacity(), 3);
    }

    #[tokio::test]
    async fn mailbox_full_error_returns_original_signal() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox(4, &mailbox);

        sender.send(Signal::ProcessDied(10)).unwrap();
        let error = sender.send(Signal::ProcessDied(99)).unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::MailboxFull);
        assert!(matches!(error.signal(), Signal::ProcessDied(99)));
        assert!(matches!(error.into_signal(), Signal::ProcessDied(99)));

        drop(receiver.recv().await.unwrap());
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn admitted_monitor_delivers_directly_through_saturated_observer_ingress() {
        let observer_mailbox = MessageMailbox::new(1);
        let (observer_sender, observer_receiver) = signal_mailbox(1, &observer_mailbox);
        let observer = Arc::new(NativeProcess {
            id: 9,
            signal_mailbox: observer_sender.clone(),
        });
        let target_mailbox = MessageMailbox::new(1);
        let (target_sender, target_receiver) = signal_mailbox(2, &target_mailbox);

        target_sender.send(Signal::Monitor(observer)).unwrap();
        assert_eq!(observer_mailbox.available_capacity(), 0);

        // Saturate the observer's independent signal ingress after monitor
        // admission. The reserved ProcessDied delivery must not use it.
        observer_sender
            .send(Signal::DieWhenLinkDies(false))
            .unwrap();
        assert_eq!(observer_sender.available_capacity(), 0);

        let envelope = target_receiver.recv().await.unwrap();
        let (signal, _, _, monitor_permit, notification) = envelope.into_process_parts();
        assert!(matches!(signal, Signal::Monitor(_)));
        assert!(monitor_permit.is_some());
        notification.unwrap().deliver(42).unwrap();

        assert!(matches!(
            observer_mailbox.pop(None).await,
            Message::ProcessDied(42)
        ));
        assert_eq!(observer_mailbox.available_capacity(), 1);
        drop(observer_receiver);
    }

    #[test]
    fn rejected_monitor_releases_observer_notification_reservation() {
        let observer_mailbox = MessageMailbox::new(1);
        let (observer_sender, _observer_receiver) = signal_mailbox(1, &observer_mailbox);
        let observer = Arc::new(NativeProcess {
            id: 10,
            signal_mailbox: observer_sender,
        });
        let target_mailbox = MessageMailbox::new(1);
        let (target_sender, _target_receiver) = signal_mailbox(1, &target_mailbox);

        target_sender.send(Signal::DieWhenLinkDies(false)).unwrap();
        let error = target_sender.send(Signal::Monitor(observer)).unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::QueueFull);
        assert!(matches!(error.into_signal(), Signal::Monitor(_)));
        assert_eq!(observer_mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn signal_queue_full_error_returns_payload_and_releases_reservation() {
        let mailbox = MessageMailbox::new(2);
        let (sender, receiver) = signal_mailbox(1, &mailbox);

        sender
            .send(Signal::Message(Message::ProcessDied(1)))
            .unwrap();
        assert_eq!(mailbox.available_capacity(), 1);

        let error = sender
            .send(Signal::Message(Message::ProcessDied(2)))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::QueueFull);
        match error.into_signal() {
            Signal::Message(Message::ProcessDied(process_id)) => assert_eq!(process_id, 2),
            _ => panic!("queue-full send returned the wrong signal"),
        }
        assert_eq!(mailbox.available_capacity(), 1);

        drop(receiver.recv().await.unwrap());
        assert_eq!(mailbox.available_capacity(), 2);
    }

    #[tokio::test]
    async fn data_flood_preserves_signal_headroom_for_kill() {
        let mailbox = MessageMailbox::new(4);
        let (sender, receiver) = signal_mailbox(4, &mailbox);

        for process_id in 0..3 {
            sender.send(Signal::ProcessDied(process_id)).unwrap();
        }
        let error = sender.send(Signal::ProcessDied(99)).unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::QueueFull);
        assert!(matches!(error.into_signal(), Signal::ProcessDied(99)));
        assert_eq!(mailbox.available_capacity(), 1);

        sender.send(Signal::Kill).unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::Kill
        ));
        for _ in 0..3 {
            assert!(receiver.recv().await.unwrap().has_mailbox_permit());
        }
        assert_eq!(mailbox.available_capacity(), 4);
    }

    #[tokio::test]
    async fn kill_latch_bypasses_a_full_control_signal_queue() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox(1, &mailbox);

        sender.send(Signal::DieWhenLinkDies(false)).unwrap();
        let error = sender.send(Signal::DieWhenLinkDies(true)).unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::QueueFull);
        assert!(matches!(error.into_signal(), Signal::DieWhenLinkDies(true)));
        sender.send(Signal::Kill).unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::Kill
        ));
        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::DieWhenLinkDies(false)
        ));
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[test]
    fn closed_error_returns_original_signal_without_leaking_capacity() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox(1, &mailbox);
        drop(receiver);

        let error = sender.send(Signal::ProcessDied(88)).unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::Closed);
        assert!(matches!(error.into_signal(), Signal::ProcessDied(88)));
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn normal_link_death_uses_control_admission_without_a_mailbox_slot() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox(1, &mailbox);

        sender
            .send(Signal::LinkDied(5, None, DeathReason::Normal))
            .unwrap();
        assert_eq!(mailbox.available_capacity(), 1);
        let envelope = receiver.recv().await.unwrap();
        assert!(!envelope.has_mailbox_permit());
        assert!(matches!(
            envelope.into_signal(),
            Signal::LinkDied(5, None, DeathReason::Normal)
        ));
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn oversized_data_message_is_rejected_before_queue_or_mailbox_admission() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox_with_limits(1, &mailbox, 3, 2);
        let message = DataMessage::new_from_vec(Some(7), vec![1, 2, 3, 4]);

        let error = sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::MessageTooLarge);
        match &error {
            super::SignalSendError::MessageTooLarge { actual, max, .. } => {
                assert_eq!((*actual, *max), (4, 3));
            }
            _ => panic!("oversized message returned the wrong error"),
        }
        assert_eq!(sender.available_capacity(), 1);
        assert_eq!(mailbox.available_capacity(), 1);

        match error.into_signal() {
            Signal::Message(Message::Data(message)) => {
                assert_eq!(message.tag, Some(7));
                assert_eq!(message.buffer, vec![1, 2, 3, 4]);
            }
            _ => panic!("oversized send did not return its original message"),
        }

        // The failed send did not occupy the only signal slot.
        sender.send(Signal::Kill).unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::Kill
        ));
    }

    #[tokio::test]
    async fn empty_resource_slots_count_toward_destination_limit() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox_with_limits(1, &mailbox, 16, 2);
        let mut message = DataMessage::new_from_vec(None, vec![1]);
        message.resources = vec![None, None, None];

        let error = sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::TooManyMessageResources);
        match &error {
            super::SignalSendError::TooManyMessageResources { actual, max, .. } => {
                assert_eq!((*actual, *max), (3, 2));
            }
            _ => panic!("resource-heavy message returned the wrong error"),
        }
        assert_eq!(sender.available_capacity(), 1);
        assert_eq!(mailbox.available_capacity(), 1);

        match error.into_signal() {
            Signal::Message(Message::Data(message)) => {
                assert_eq!(message.buffer, vec![1]);
                assert_eq!(message.resources.len(), 3);
                assert!(message.resources.iter().all(Option::is_none));
            }
            _ => panic!("resource-limit send did not return its original message"),
        }

        sender.send(Signal::Kill).unwrap();
        assert!(matches!(
            receiver.recv().await.unwrap().into_signal(),
            Signal::Kill
        ));
    }

    #[test]
    fn oversized_native_preallocation_is_rejected_even_when_logically_empty() {
        let mailbox = MessageMailbox::new(1);
        let (sender, _receiver) = signal_mailbox_with_limits(1, &mailbox, 3, 2);
        let mut message = DataMessage::new(None, 4);
        message.resources = Vec::with_capacity(3);

        let error = sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::MessageTooLarge);
        assert_eq!(mailbox.available_capacity(), 1);

        let Signal::Message(Message::Data(mut message)) = error.into_signal() else {
            panic!("preallocation rejection did not preserve the original message")
        };
        message.buffer = Vec::new();
        let error = sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap_err();
        assert_eq!(error.kind(), SignalSendErrorKind::TooManyMessageResources);
        assert_eq!(mailbox.available_capacity(), 1);
    }

    #[tokio::test]
    async fn data_message_at_both_destination_limits_is_admitted() {
        let mailbox = MessageMailbox::new(1);
        let (sender, receiver) = signal_mailbox_with_limits(1, &mailbox, 3, 2);
        let mut message = DataMessage::new_from_vec(None, vec![1, 2, 3]);
        message.resources = vec![None, None];

        sender
            .send(Signal::Message(Message::Data(message)))
            .unwrap();
        assert_eq!(sender.available_capacity(), 0);
        assert_eq!(mailbox.available_capacity(), 0);

        let envelope = receiver.recv().await.unwrap();
        assert!(envelope.has_mailbox_permit());
        let (signal, permit) = envelope.into_parts();
        match signal {
            Signal::Message(Message::Data(message)) => {
                assert_eq!(message.buffer.len(), 3);
                assert_eq!(message.resources.len(), 2);
            }
            _ => panic!("received the wrong signal"),
        }
        drop(permit);
        assert_eq!(mailbox.available_capacity(), 1);
    }
}
