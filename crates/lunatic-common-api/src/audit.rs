//! Typed, bounded audit event delivery.
//!
//! Events contain only stable enum values and numeric resource identifiers. In
//! particular, [`AuditTarget`] has no string field that could accidentally
//! capture a path, hostname, token, or error message. Callers that encounter
//! sensitive target data record [`SensitiveData::Redacted`] instead.

use serde::Serialize;
use std::{
    fmt, io,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender, TrySendError},
        Arc, Condvar, Mutex, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

/// The current audit event schema version.
pub const AUDIT_SCHEMA_VERSION: u16 = 1;

/// The stable kind of security-relevant event that occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditEvent {
    ModuleCompile,
    ConfigCreate,
    ConfigUpdate,
    FilesystemPreopen,
    FilesystemAccess,
    ProcessSpawn,
    NetworkBind,
    NetworkAccept,
    NetworkConnect,
    NetworkSend,
    ResourceLimitDenied,
    HotReloadUpdate,
    DistributedRegistryChange,
    DistributedSnapshotApply,
    DistributedRequestAuthorization,
}

/// The operation represented by an audit event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditAction {
    Compile,
    Create,
    Mutate,
    Delegate,
    Validate,
    Spawn,
    LookupOrSpawn,
    Preopen,
    Bind,
    Accept,
    Connect,
    Send,
    SendTo,
    Resolve,
    Open,
    Read,
    Write,
    Remove,
    Rename,
    Link,
    SetTimes,
    Commit,
    Rollback,
    InDoubt,
    Register,
    Unregister,
    Resync,
    Grow,
    Exit,
}

/// The observable result of the operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Allowed,
    Denied,
    Succeeded,
    Failed,
    Cancelled,
}

/// A bounded, non-sensitive explanation for an audit result.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditReason {
    PolicyAllowed,
    PolicyDenied,
    CapabilityDenied,
    DelegationDenied,
    DelegationExceedsParent,
    Completed,
    InvalidInput,
    ResourceLimit,
    QuotaExceeded,
    NotFound,
    Conflict,
    IoError,
    RuntimeError,
    RuntimeFailure,
    Timeout,
    TimedOut,
    ProtocolDenied,
    ReloadNack,
    RollbackFailed,
    Unsupported,
    Cancelled,
    InternalError,
}

/// The runtime identity responsible for an operation.
///
/// Optional values deliberately serialize as `null` so all V1 schema keys are
/// present in every record.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub struct AuditSubject {
    node_id: Option<u64>,
    environment_id: Option<u64>,
    process_id: Option<u64>,
}

impl AuditSubject {
    pub const fn new() -> Self {
        Self {
            node_id: None,
            environment_id: None,
            process_id: None,
        }
    }

    pub const fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub const fn with_environment_id(mut self, environment_id: u64) -> Self {
        self.environment_id = Some(environment_id);
        self
    }

    pub const fn with_process_id(mut self, process_id: u64) -> Self {
        self.process_id = Some(process_id);
        self
    }

    pub const fn node_id(&self) -> Option<u64> {
        self.node_id
    }

    pub const fn environment_id(&self) -> Option<u64> {
        self.environment_id
    }

    pub const fn process_id(&self) -> Option<u64> {
        self.process_id
    }
}

/// The type of resource affected by an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditTargetKind {
    Module,
    Configuration,
    Process,
    Node,
    Environment,
    NetworkEndpoint,
    TcpListener,
    TcpStream,
    TlsListener,
    TlsStream,
    UdpSocket,
    DnsIterator,
    Memory,
    Table,
    Filesystem,
    HotReload,
    DistributedRegistry,
    DistributedRequest,
}

/// Whether sensitive target material existed but was excluded from the event.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SensitiveData {
    #[default]
    NotPresent,
    Redacted,
}

/// A typed audit target that cannot hold arbitrary strings.
///
/// Numeric IDs and ports are the only target values accepted. Paths,
/// hostnames, IP addresses, guest-provided names, errors, and details must not
/// be placed in an audit record; use [`SensitiveData::Redacted`] to record that
/// such material was intentionally omitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct AuditTarget {
    kind: AuditTargetKind,
    resource_id: Option<u64>,
    node_id: Option<u64>,
    environment_id: Option<u64>,
    process_id: Option<u64>,
    port: Option<u16>,
    sensitive_data: SensitiveData,
}

impl AuditTarget {
    pub const fn new(kind: AuditTargetKind) -> Self {
        Self {
            kind,
            resource_id: None,
            node_id: None,
            environment_id: None,
            process_id: None,
            port: None,
            sensitive_data: SensitiveData::NotPresent,
        }
    }

    pub const fn with_resource_id(mut self, resource_id: u64) -> Self {
        self.resource_id = Some(resource_id);
        self
    }

    pub const fn with_node_id(mut self, node_id: u64) -> Self {
        self.node_id = Some(node_id);
        self
    }

    pub const fn with_environment_id(mut self, environment_id: u64) -> Self {
        self.environment_id = Some(environment_id);
        self
    }

    pub const fn with_process_id(mut self, process_id: u64) -> Self {
        self.process_id = Some(process_id);
        self
    }

    pub const fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub const fn with_sensitive_data(mut self, sensitive_data: SensitiveData) -> Self {
        self.sensitive_data = sensitive_data;
        self
    }

    pub const fn kind(&self) -> AuditTargetKind {
        self.kind
    }

    pub const fn resource_id(&self) -> Option<u64> {
        self.resource_id
    }

    pub const fn node_id(&self) -> Option<u64> {
        self.node_id
    }

    pub const fn environment_id(&self) -> Option<u64> {
        self.environment_id
    }

    pub const fn process_id(&self) -> Option<u64> {
        self.process_id
    }

    pub const fn port(&self) -> Option<u16> {
        self.port
    }

    pub const fn sensitive_data(&self) -> SensitiveData {
        self.sensitive_data
    }
}

/// Version 1 of Lunatic's stable JSON audit event schema.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AuditEventV1 {
    schema_version: u16,
    sequence: u64,
    event: AuditEvent,
    action: AuditAction,
    result: AuditResult,
    reason: AuditReason,
    subject: AuditSubject,
    target: AuditTarget,
}

impl AuditEventV1 {
    pub const fn new(
        event: AuditEvent,
        action: AuditAction,
        result: AuditResult,
        reason: AuditReason,
        subject: AuditSubject,
        target: AuditTarget,
    ) -> Self {
        Self {
            schema_version: AUDIT_SCHEMA_VERSION,
            sequence: 0,
            event,
            action,
            result,
            reason,
            subject,
            target,
        }
    }

    pub const fn schema_version(&self) -> u16 {
        self.schema_version
    }

    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    pub const fn event(&self) -> AuditEvent {
        self.event
    }

    pub const fn action(&self) -> AuditAction {
        self.action
    }

    pub const fn result(&self) -> AuditResult {
        self.result
    }

    pub const fn reason(&self) -> AuditReason {
        self.reason
    }

    pub const fn subject(&self) -> &AuditSubject {
        &self.subject
    }

    pub const fn target(&self) -> &AuditTarget {
        &self.target
    }

    fn set_sequence(&mut self, sequence: u64) {
        self.sequence = sequence;
    }
}

/// Destination for audit records. A dispatcher invokes the sink only from its
/// dedicated writer thread.
pub trait AuditSink: Send + 'static {
    fn write(&mut self, event: &AuditEventV1) -> io::Result<()>;

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<T: AuditSink + ?Sized> AuditSink for Box<T> {
    fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
        (**self).write(event)
    }

    fn flush(&mut self) -> io::Result<()> {
        (**self).flush()
    }
}

/// Default sink that emits one compact JSON object to `target = "audit"`.
#[derive(Clone, Copy, Debug, Default)]
pub struct LogAuditSink;

impl AuditSink for LogAuditSink {
    fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
        if !log::log_enabled!(target: "audit", log::Level::Info) {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "the audit log target is disabled",
            ));
        }
        let json = serde_json::to_string(event).map_err(io::Error::other)?;
        log::info!(target: "audit", "{json}");
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        log::logger().flush();
        Ok(())
    }
}

/// Bounded audit dispatcher settings.
#[derive(Clone, Copy, Debug)]
pub struct AuditConfig {
    pub enabled: bool,
    pub queue_capacity: usize,
    pub flush_timeout: Duration,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_capacity: 1_024,
            flush_timeout: Duration::from_millis(250),
        }
    }
}

/// The result of a nonblocking emit attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditEmitOutcome {
    Enqueued,
    DroppedDisabled,
    DroppedFull,
    DroppedClosed,
    DroppedSinkUnavailable,
}

/// The result of a bounded flush attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditFlushOutcome {
    Flushed,
    TimedOut,
    Closed,
    SinkFailed,
}

/// Point-in-time observable audit delivery counters and health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuditStats {
    pub enabled: bool,
    pub attempted: u64,
    pub enqueued: u64,
    pub written: u64,
    pub dropped_disabled: u64,
    pub dropped_full: u64,
    pub dropped_closed: u64,
    pub dropped_sink_unavailable: u64,
    pub sink_failures: u64,
    pub flush_failures: u64,
    pub flush_timeouts: u64,
    pub queue_depth: usize,
    pub flush_pending: bool,
    pub sink_healthy: bool,
    pub closed: bool,
}

#[derive(Default)]
struct Counters {
    attempted: AtomicU64,
    enqueued: AtomicU64,
    written: AtomicU64,
    dropped_disabled: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
    dropped_sink_unavailable: AtomicU64,
    sink_failures: AtomicU64,
    flush_failures: AtomicU64,
    flush_timeouts: AtomicU64,
    event_epoch: AtomicU64,
    admission: AtomicUsize,
    queue_depth: AtomicUsize,
    flush_pending: AtomicBool,
    sink_healthy: AtomicBool,
    closed: AtomicBool,
}

const ADMISSION_CLOSED: usize = 1 << (usize::BITS - 1);
const ACTIVE_EMITTERS_MASK: usize = ADMISSION_CLOSED - 1;

enum Command {
    Event(AuditEventV1),
    Flush(Arc<FlushBarrier>),
}

struct FlushBarrier {
    event_epoch: u64,
    outcome: Mutex<Option<AuditFlushOutcome>>,
    ready: Condvar,
}

impl FlushBarrier {
    fn new(event_epoch: u64) -> Self {
        Self {
            event_epoch,
            outcome: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn covers(&self, required_event_epoch: u64) -> bool {
        self.event_epoch >= required_event_epoch
    }

    fn is_pending(&self) -> bool {
        self.outcome.lock().unwrap().is_none()
    }

    fn complete(&self, outcome: AuditFlushOutcome) {
        let mut current = self.outcome.lock().unwrap();
        if current.is_none() {
            *current = Some(outcome);
            self.ready.notify_all();
        }
    }

    fn wait_until(&self, deadline: Option<Instant>) -> AuditFlushOutcome {
        let mut outcome = self.outcome.lock().unwrap();
        loop {
            if let Some(outcome) = *outcome {
                return outcome;
            }

            if let Some(deadline) = deadline {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return AuditFlushOutcome::TimedOut;
                };
                let (next, timeout) = self.ready.wait_timeout(outcome, remaining).unwrap();
                outcome = next;
                if timeout.timed_out() && outcome.is_none() {
                    return AuditFlushOutcome::TimedOut;
                }
            } else {
                outcome = self.ready.wait(outcome).unwrap();
            }
        }
    }
}

struct DispatcherState {
    enabled: bool,
    sender: Option<SyncSender<Command>>,
    counters: Arc<Counters>,
    event_capacity: usize,
    flush_timeout: Duration,
    flush_barrier: Mutex<Option<Arc<FlushBarrier>>>,
}

/// A cheap-to-clone handle to a bounded, nonblocking audit writer.
#[derive(Clone)]
pub struct AuditDispatcher {
    state: Arc<DispatcherState>,
}

impl fmt::Debug for AuditDispatcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuditDispatcher")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl AuditDispatcher {
    /// Starts a dedicated writer when enabled. If the writer thread cannot be
    /// started, construction still succeeds in a closed, fail-open state.
    pub fn new<S: AuditSink>(config: AuditConfig, sink: S) -> Self {
        let counters = Arc::new(Counters::default());
        counters.sink_healthy.store(true, Ordering::Relaxed);
        if !config.enabled {
            counters
                .admission
                .store(ADMISSION_CLOSED, Ordering::Relaxed);
        }

        let event_capacity = config.queue_capacity.max(1);
        let sender = if config.enabled {
            // Reserve one physical slot for the single in-flight flush barrier;
            // event admission is enforced separately by `event_capacity`.
            let (sender, receiver) = mpsc::sync_channel(event_capacity.saturating_add(1));
            let worker_counters = Arc::clone(&counters);
            match thread::Builder::new()
                .name("lunatic-audit-writer".to_owned())
                .spawn(move || writer_loop(receiver, sink, worker_counters))
            {
                Ok(_worker) => Some(sender),
                Err(_) => {
                    counters
                        .admission
                        .fetch_or(ADMISSION_CLOSED, Ordering::Release);
                    counters.sink_healthy.store(false, Ordering::Relaxed);
                    counters.closed.store(true, Ordering::Relaxed);
                    None
                }
            }
        } else {
            None
        };

        Self {
            state: Arc::new(DispatcherState {
                enabled: config.enabled,
                sender,
                counters,
                event_capacity,
                flush_timeout: config.flush_timeout,
                flush_barrier: Mutex::new(None),
            }),
        }
    }

    /// Enqueues an event without waiting. New events are dropped if delivery is
    /// disabled, the bounded queue is full, or the writer has closed.
    pub fn emit(&self, event: AuditEventV1) -> AuditEmitOutcome {
        let counters = &self.state.counters;
        counters.attempted.fetch_add(1, Ordering::Relaxed);

        if !self.state.enabled {
            counters.dropped_disabled.fetch_add(1, Ordering::Relaxed);
            return AuditEmitOutcome::DroppedDisabled;
        }

        let Some(_admission) = ActiveEmitter::try_enter(&counters.admission) else {
            counters.dropped_closed.fetch_add(1, Ordering::Relaxed);
            return AuditEmitOutcome::DroppedClosed;
        };

        let Some(sender) = &self.state.sender else {
            counters.dropped_closed.fetch_add(1, Ordering::Relaxed);
            return AuditEmitOutcome::DroppedClosed;
        };

        if !counters.sink_healthy.load(Ordering::Relaxed) {
            counters
                .dropped_sink_unavailable
                .fetch_add(1, Ordering::Relaxed);
            return AuditEmitOutcome::DroppedSinkUnavailable;
        }

        if counters
            .queue_depth
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |depth| {
                (depth < self.state.event_capacity).then_some(depth + 1)
            })
            .is_err()
        {
            counters.dropped_full.fetch_add(1, Ordering::Relaxed);
            return AuditEmitOutcome::DroppedFull;
        }
        counters.enqueued.fetch_add(1, Ordering::Relaxed);
        match sender.try_send(Command::Event(event)) {
            Ok(()) => {
                counters.event_epoch.fetch_add(1, Ordering::Release);
                AuditEmitOutcome::Enqueued
            }
            Err(TrySendError::Full(_)) => {
                counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                counters.enqueued.fetch_sub(1, Ordering::Relaxed);
                counters.dropped_full.fetch_add(1, Ordering::Relaxed);
                AuditEmitOutcome::DroppedFull
            }
            Err(TrySendError::Disconnected(_)) => {
                counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                counters.enqueued.fetch_sub(1, Ordering::Relaxed);
                counters.dropped_closed.fetch_add(1, Ordering::Relaxed);
                counters.closed.store(true, Ordering::Relaxed);
                counters
                    .admission
                    .fetch_or(ADMISSION_CLOSED, Ordering::Release);
                counters.sink_healthy.store(false, Ordering::Relaxed);
                AuditEmitOutcome::DroppedClosed
            }
        }
    }

    /// Flushes using the timeout configured when the dispatcher was created.
    pub fn flush(&self) -> AuditFlushOutcome {
        self.flush_with_timeout(self.state.flush_timeout)
    }

    /// Atomically stops event admission, waits for already-admitted emitters,
    /// and flushes every event accepted before the close boundary.
    pub fn close_and_flush(&self, timeout: Duration) -> AuditFlushOutcome {
        if !self.state.enabled {
            return AuditFlushOutcome::Flushed;
        }

        let deadline = Instant::now().checked_add(timeout);
        self.state
            .counters
            .admission
            .fetch_or(ADMISSION_CLOSED, Ordering::AcqRel);
        self.state.counters.closed.store(true, Ordering::Relaxed);

        while self.state.counters.admission.load(Ordering::Acquire) & ACTIVE_EMITTERS_MASK != 0 {
            let Some(remaining) = remaining_until(deadline) else {
                self.state
                    .counters
                    .flush_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                return AuditFlushOutcome::TimedOut;
            };
            thread::park_timeout(remaining.min(Duration::from_millis(1)));
        }

        let Some(remaining) = remaining_until(deadline) else {
            self.state
                .counters
                .flush_timeouts
                .fetch_add(1, Ordering::Relaxed);
            return AuditFlushOutcome::TimedOut;
        };
        self.flush_with_timeout(remaining)
    }

    /// Waits at most `timeout` for all prior events and the sink to flush.
    pub fn flush_with_timeout(&self, timeout: Duration) -> AuditFlushOutcome {
        if !self.state.enabled {
            return AuditFlushOutcome::Flushed;
        }

        let Some(sender) = &self.state.sender else {
            return AuditFlushOutcome::Closed;
        };
        let deadline = Instant::now().checked_add(timeout);
        let required_event_epoch = self.state.counters.event_epoch.load(Ordering::Acquire);

        loop {
            if remaining_until(deadline).is_none() {
                self.state
                    .counters
                    .flush_timeouts
                    .fetch_add(1, Ordering::Relaxed);
                return AuditFlushOutcome::TimedOut;
            }

            let (barrier, leader) = {
                let mut current = self.state.flush_barrier.lock().unwrap();
                match current.as_ref().filter(|barrier| barrier.is_pending()) {
                    Some(barrier) => (Arc::clone(barrier), false),
                    None => {
                        let barrier = Arc::new(FlushBarrier::new(
                            self.state.counters.event_epoch.load(Ordering::Acquire),
                        ));
                        *current = Some(Arc::clone(&barrier));
                        self.state
                            .counters
                            .flush_pending
                            .store(true, Ordering::Release);
                        (barrier, true)
                    }
                }
            };

            if !barrier.covers(required_event_epoch) {
                let outcome = barrier.wait_until(deadline);
                if outcome == AuditFlushOutcome::TimedOut {
                    self.state
                        .counters
                        .flush_timeouts
                        .fetch_add(1, Ordering::Relaxed);
                    return outcome;
                }
                if outcome == AuditFlushOutcome::Closed {
                    return outcome;
                }
                continue;
            }

            if leader {
                let mut command = Command::Flush(Arc::clone(&barrier));
                loop {
                    match sender.try_send(command) {
                        Ok(()) => break,
                        Err(TrySendError::Disconnected(_)) => {
                            self.state
                                .counters
                                .flush_pending
                                .store(false, Ordering::Release);
                            self.state.counters.closed.store(true, Ordering::Relaxed);
                            self.state
                                .counters
                                .admission
                                .fetch_or(ADMISSION_CLOSED, Ordering::Release);
                            self.state
                                .counters
                                .sink_healthy
                                .store(false, Ordering::Relaxed);
                            barrier.complete(AuditFlushOutcome::Closed);
                            return AuditFlushOutcome::Closed;
                        }
                        Err(TrySendError::Full(returned)) => {
                            command = returned;
                            let Some(remaining) = remaining_until(deadline) else {
                                self.state
                                    .counters
                                    .flush_pending
                                    .store(false, Ordering::Release);
                                self.state
                                    .counters
                                    .flush_timeouts
                                    .fetch_add(1, Ordering::Relaxed);
                                barrier.complete(AuditFlushOutcome::TimedOut);
                                return AuditFlushOutcome::TimedOut;
                            };
                            thread::park_timeout(remaining.min(Duration::from_millis(1)));
                        }
                    }
                }
            }

            let outcome = barrier.wait_until(deadline);
            if outcome == AuditFlushOutcome::TimedOut {
                self.state
                    .counters
                    .flush_timeouts
                    .fetch_add(1, Ordering::Relaxed);
            }
            return outcome;
        }
    }

    pub fn stats(&self) -> AuditStats {
        let counters = &self.state.counters;
        AuditStats {
            enabled: self.state.enabled,
            attempted: counters.attempted.load(Ordering::Relaxed),
            enqueued: counters.enqueued.load(Ordering::Relaxed),
            written: counters.written.load(Ordering::Relaxed),
            dropped_disabled: counters.dropped_disabled.load(Ordering::Relaxed),
            dropped_full: counters.dropped_full.load(Ordering::Relaxed),
            dropped_closed: counters.dropped_closed.load(Ordering::Relaxed),
            dropped_sink_unavailable: counters.dropped_sink_unavailable.load(Ordering::Relaxed),
            sink_failures: counters.sink_failures.load(Ordering::Relaxed),
            flush_failures: counters.flush_failures.load(Ordering::Relaxed),
            flush_timeouts: counters.flush_timeouts.load(Ordering::Relaxed),
            queue_depth: counters.queue_depth.load(Ordering::Relaxed),
            flush_pending: counters.flush_pending.load(Ordering::Acquire),
            sink_healthy: counters.sink_healthy.load(Ordering::Relaxed),
            closed: counters.closed.load(Ordering::Relaxed),
        }
    }
}

struct ActiveEmitter<'a>(&'a AtomicUsize);

impl<'a> ActiveEmitter<'a> {
    fn try_enter(admission: &'a AtomicUsize) -> Option<Self> {
        let mut state = admission.load(Ordering::Acquire);
        loop {
            if state & ADMISSION_CLOSED != 0 || state & ACTIVE_EMITTERS_MASK == ACTIVE_EMITTERS_MASK
            {
                return None;
            }
            match admission.compare_exchange_weak(
                state,
                state + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(Self(admission)),
                Err(actual) => state = actual,
            }
        }
    }
}

impl Drop for ActiveEmitter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

fn remaining_until(deadline: Option<Instant>) -> Option<Duration> {
    match deadline {
        Some(deadline) => deadline.checked_duration_since(Instant::now()),
        None => Some(Duration::MAX),
    }
}

fn writer_loop<S: AuditSink>(
    receiver: mpsc::Receiver<Command>,
    mut sink: S,
    counters: Arc<Counters>,
) {
    let mut batch_failed = false;
    let mut next_sequence = 1_u64;
    while let Ok(command) = receiver.recv() {
        match command {
            Command::Event(mut event) => {
                counters.queue_depth.fetch_sub(1, Ordering::Relaxed);
                if !counters.sink_healthy.load(Ordering::Relaxed) {
                    batch_failed = true;
                    counters
                        .dropped_sink_unavailable
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                event.set_sequence(next_sequence);
                next_sequence = next_sequence.wrapping_add(1);
                match catch_unwind(AssertUnwindSafe(|| sink.write(&event))) {
                    Ok(Ok(())) => {
                        counters.written.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(Err(_)) | Err(_) => {
                        batch_failed = true;
                        counters.sink_failures.fetch_add(1, Ordering::Relaxed);
                        counters.sink_healthy.store(false, Ordering::Relaxed);
                    }
                }
            }
            Command::Flush(barrier) => {
                let sink_was_healthy = counters.sink_healthy.load(Ordering::Relaxed);
                let flush_succeeded = match catch_unwind(AssertUnwindSafe(|| sink.flush())) {
                    Ok(Ok(())) => true,
                    Ok(Err(_)) | Err(_) => {
                        counters.sink_failures.fetch_add(1, Ordering::Relaxed);
                        counters.flush_failures.fetch_add(1, Ordering::Relaxed);
                        counters.sink_healthy.store(false, Ordering::Relaxed);
                        false
                    }
                };
                let succeeded = sink_was_healthy && flush_succeeded && !batch_failed;
                batch_failed = false;
                counters.flush_pending.store(false, Ordering::Release);
                barrier.complete(if succeeded {
                    AuditFlushOutcome::Flushed
                } else {
                    AuditFlushOutcome::SinkFailed
                });
            }
        }
    }
    counters
        .admission
        .fetch_or(ADMISSION_CLOSED, Ordering::Release);
    counters.closed.store(true, Ordering::Relaxed);
}

static GLOBAL_AUDIT_DISPATCHER: OnceLock<AuditDispatcher> = OnceLock::new();

/// Returns the process-global dispatcher, lazily installing the default log
/// sink on first use.
pub fn global_audit_dispatcher() -> &'static AuditDispatcher {
    GLOBAL_AUDIT_DISPATCHER
        .get_or_init(|| AuditDispatcher::new(AuditConfig::default(), LogAuditSink))
}

/// Installs a custom global dispatcher before its first use.
pub fn install_global_audit_dispatcher(dispatcher: AuditDispatcher) -> Result<(), AuditDispatcher> {
    GLOBAL_AUDIT_DISPATCHER.set(dispatcher)
}

/// Nonblocking process-global audit emission.
pub fn emit_audit_event(event: AuditEventV1) -> AuditEmitOutcome {
    global_audit_dispatcher().emit(event)
}

/// Bounded process-global flush.
pub fn flush_audit(timeout: Duration) -> AuditFlushOutcome {
    global_audit_dispatcher().flush_with_timeout(timeout)
}

/// Closes process-global audit admission and flushes every previously accepted
/// event within `timeout`.
pub fn close_audit(timeout: Duration) -> AuditFlushOutcome {
    global_audit_dispatcher().close_and_flush(timeout)
}

/// Process-global audit delivery counters and health.
pub fn audit_stats() -> AuditStats {
    global_audit_dispatcher().stats()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Barrier, Mutex};

    fn event(action: AuditAction) -> AuditEventV1 {
        AuditEventV1::new(
            AuditEvent::ProcessSpawn,
            action,
            AuditResult::Succeeded,
            AuditReason::Completed,
            AuditSubject::new()
                .with_environment_id(7)
                .with_process_id(42),
            AuditTarget::new(AuditTargetKind::Process).with_process_id(99),
        )
    }

    #[test]
    fn exact_v1_schema_redacts_sensitive_target_data() {
        let event = AuditEventV1::new(
            AuditEvent::FilesystemAccess,
            AuditAction::Preopen,
            AuditResult::Allowed,
            AuditReason::PolicyAllowed,
            AuditSubject::new()
                .with_environment_id(7)
                .with_process_id(42),
            AuditTarget::new(AuditTargetKind::Filesystem)
                .with_resource_id(9)
                .with_sensitive_data(SensitiveData::Redacted),
        );

        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"schema_version":1,"sequence":0,"event":"filesystem_access","action":"preopen","result":"allowed","reason":"policy_allowed","subject":{"node_id":null,"environment_id":7,"process_id":42},"target":{"kind":"filesystem","resource_id":9,"node_id":null,"environment_id":null,"process_id":null,"port":null,"sensitive_data":"redacted"}}"#
        );
    }

    struct RecordingSink {
        events: Arc<Mutex<Vec<AuditEventV1>>>,
    }

    impl AuditSink for RecordingSink {
        fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    #[test]
    fn disabled_dispatcher_drops_without_touching_sink() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                enabled: false,
                ..AuditConfig::default()
            },
            RecordingSink {
                events: Arc::clone(&events),
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::DroppedDisabled
        );
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::Flushed);
        assert!(events.lock().unwrap().is_empty());
        assert_eq!(dispatcher.stats().attempted, 1);
        assert_eq!(dispatcher.stats().dropped_disabled, 1);
    }

    struct BlockingSink {
        entered: Option<mpsc::SyncSender<()>>,
        release: mpsc::Receiver<()>,
        events: Arc<Mutex<Vec<AuditEventV1>>>,
    }

    impl AuditSink for BlockingSink {
        fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.recv().unwrap();
            }
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    #[test]
    fn full_queue_drops_newest_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 1,
                flush_timeout: Duration::from_secs(1),
                ..AuditConfig::default()
            },
            BlockingSink {
                entered: Some(entered_tx),
                release: release_rx,
                events: Arc::clone(&events),
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            dispatcher.emit(event(AuditAction::Bind)),
            AuditEmitOutcome::Enqueued
        );
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::DroppedFull
        );

        release_tx.send(()).unwrap();
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::Flushed);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].sequence(), 1);
        assert_eq!(events[1].sequence(), 2);
        assert_eq!(dispatcher.stats().dropped_full, 1);
    }

    #[test]
    fn timed_out_flush_keeps_the_reserved_event_capacity_available() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 1,
                flush_timeout: Duration::from_millis(5),
                ..AuditConfig::default()
            },
            BlockingSink {
                entered: Some(entered_tx),
                release: release_rx,
                events: Arc::clone(&events),
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::TimedOut);
        assert!(dispatcher.stats().flush_pending);
        assert_eq!(dispatcher.stats().flush_timeouts, 1);
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::Enqueued,
            "a stale flush must not consume the event slot"
        );

        release_tx.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while dispatcher.stats().flush_pending && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(!dispatcher.stats().flush_pending);
        assert_eq!(
            dispatcher.flush_with_timeout(Duration::from_secs(1)),
            AuditFlushOutcome::Flushed
        );
        assert_eq!(events.lock().unwrap().len(), 2);
    }

    #[test]
    fn concurrent_flush_callers_join_the_pending_barrier() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 1,
                flush_timeout: Duration::from_secs(1),
                ..AuditConfig::default()
            },
            BlockingSink {
                entered: Some(entered_tx),
                release: release_rx,
                events,
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let start = Arc::new(Barrier::new(9));
        let mut callers = Vec::new();
        for _ in 0..8 {
            let dispatcher = dispatcher.clone();
            let start = Arc::clone(&start);
            callers.push(thread::spawn(move || {
                start.wait();
                dispatcher.flush()
            }));
        }
        start.wait();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !dispatcher.stats().flush_pending && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(dispatcher.stats().flush_pending);
        release_tx.send(()).unwrap();

        for caller in callers {
            assert_eq!(caller.join().unwrap(), AuditFlushOutcome::Flushed);
        }
        assert_eq!(dispatcher.stats().flush_timeouts, 0);
    }

    struct OrderedFlushSink {
        first_flush_entered: Option<mpsc::SyncSender<()>>,
        release_first_flush: mpsc::Receiver<()>,
        second_write_entered: mpsc::SyncSender<()>,
        release_second_write: mpsc::Receiver<()>,
        writes: usize,
    }

    impl AuditSink for OrderedFlushSink {
        fn write(&mut self, _event: &AuditEventV1) -> io::Result<()> {
            self.writes += 1;
            if self.writes == 2 {
                self.second_write_entered.send(()).unwrap();
                self.release_second_write.recv().unwrap();
            }
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            if let Some(entered) = self.first_flush_entered.take() {
                entered.send(()).unwrap();
                self.release_first_flush.recv().unwrap();
            }
            Ok(())
        }
    }

    #[test]
    fn later_flush_does_not_join_a_barrier_before_its_event() {
        let (first_flush_entered_tx, first_flush_entered_rx) = mpsc::sync_channel(1);
        let (release_first_flush_tx, release_first_flush_rx) = mpsc::sync_channel(1);
        let (second_write_entered_tx, second_write_entered_rx) = mpsc::sync_channel(1);
        let (release_second_write_tx, release_second_write_rx) = mpsc::sync_channel(1);
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 1,
                flush_timeout: Duration::from_secs(1),
                ..AuditConfig::default()
            },
            OrderedFlushSink {
                first_flush_entered: Some(first_flush_entered_tx),
                release_first_flush: release_first_flush_rx,
                second_write_entered: second_write_entered_tx,
                release_second_write: release_second_write_rx,
                writes: 0,
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );

        let first_dispatcher = dispatcher.clone();
        let first_flush = thread::spawn(move || first_dispatcher.flush());
        first_flush_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::Enqueued
        );

        let (second_done_tx, second_done_rx) = mpsc::sync_channel(1);
        let second_dispatcher = dispatcher.clone();
        let second_flush = thread::spawn(move || {
            let outcome = second_dispatcher.flush();
            second_done_tx.send(outcome).unwrap();
        });

        release_first_flush_tx.send(()).unwrap();
        assert_eq!(first_flush.join().unwrap(), AuditFlushOutcome::Flushed);
        second_write_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        assert!(matches!(
            second_done_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        release_second_write_tx.send(()).unwrap();
        assert_eq!(
            second_done_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            AuditFlushOutcome::Flushed
        );
        second_flush.join().unwrap();
    }

    struct FailingSink;

    impl AuditSink for FailingSink {
        fn write(&mut self, _event: &AuditEventV1) -> io::Result<()> {
            Err(io::Error::other("test sink failure"))
        }
    }

    #[test]
    fn sink_failure_is_observable_and_does_not_block_caller() {
        let dispatcher = AuditDispatcher::new(AuditConfig::default(), FailingSink);
        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::SinkFailed);
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::SinkFailed);

        let stats = dispatcher.stats();
        assert_eq!(stats.written, 0);
        assert_eq!(stats.sink_failures, 1);
        assert!(!stats.sink_healthy);
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::DroppedSinkUnavailable
        );
        assert_eq!(dispatcher.stats().dropped_sink_unavailable, 1);
    }

    struct BufferedThenFailingSink {
        writes: usize,
        pending: Vec<u64>,
        durable: Arc<Mutex<Vec<u64>>>,
        flushes: Arc<AtomicUsize>,
    }

    impl AuditSink for BufferedThenFailingSink {
        fn write(&mut self, event: &AuditEventV1) -> io::Result<()> {
            self.writes += 1;
            if self.writes == 2 {
                return Err(io::Error::other("second write fails"));
            }
            self.pending.push(event.sequence());
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.flushes.fetch_add(1, Ordering::Relaxed);
            self.durable.lock().unwrap().append(&mut self.pending);
            Ok(())
        }
    }

    #[test]
    fn unhealthy_sink_is_still_flushed_for_prior_buffered_records() {
        let durable = Arc::new(Mutex::new(Vec::new()));
        let flushes = Arc::new(AtomicUsize::new(0));
        let dispatcher = AuditDispatcher::new(
            AuditConfig::default(),
            BufferedThenFailingSink {
                writes: 0,
                pending: Vec::new(),
                durable: Arc::clone(&durable),
                flushes: Arc::clone(&flushes),
            },
        );
        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::Enqueued
        );

        assert_eq!(dispatcher.flush(), AuditFlushOutcome::SinkFailed);
        assert_eq!(flushes.load(Ordering::Relaxed), 1);
        assert_eq!(*durable.lock().unwrap(), vec![1]);
        assert_eq!(dispatcher.stats().written, 1);
        assert_eq!(dispatcher.stats().sink_failures, 1);
    }

    #[test]
    fn flush_preserves_fifo_order() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AuditDispatcher::new(
            AuditConfig::default(),
            RecordingSink {
                events: Arc::clone(&events),
            },
        );

        for action in [
            AuditAction::Compile,
            AuditAction::Create,
            AuditAction::Spawn,
        ] {
            assert!(matches!(
                dispatcher.emit(event(action)),
                AuditEmitOutcome::Enqueued
            ));
        }

        assert_eq!(dispatcher.flush(), AuditFlushOutcome::Flushed);
        let events = events.lock().unwrap();
        assert_eq!(
            events.iter().map(AuditEventV1::action).collect::<Vec<_>>(),
            vec![
                AuditAction::Compile,
                AuditAction::Create,
                AuditAction::Spawn
            ]
        );
        assert_eq!(
            events
                .iter()
                .map(AuditEventV1::sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(dispatcher.stats().written, 3);
        assert_eq!(dispatcher.stats().queue_depth, 0);
    }

    #[test]
    fn close_flushes_prior_events_and_rejects_later_producers() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AuditDispatcher::new(
            AuditConfig::default(),
            RecordingSink {
                events: Arc::clone(&events),
            },
        );

        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        assert_eq!(
            dispatcher.close_and_flush(Duration::from_secs(1)),
            AuditFlushOutcome::Flushed
        );
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::DroppedClosed
        );
        assert_eq!(events.lock().unwrap().len(), 1);
        let stats = dispatcher.stats();
        assert!(stats.closed);
        assert_eq!(stats.dropped_closed, 1);
    }

    #[test]
    fn close_boundary_flushes_every_concurrently_accepted_event() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 65_536,
                flush_timeout: Duration::from_secs(5),
                ..AuditConfig::default()
            },
            RecordingSink {
                events: Arc::clone(&events),
            },
        );
        let start = Arc::new(Barrier::new(5));
        let accepted = Arc::new(AtomicUsize::new(0));
        let mut producers = Vec::new();

        for _ in 0..4 {
            let dispatcher = dispatcher.clone();
            let start = Arc::clone(&start);
            let accepted = Arc::clone(&accepted);
            producers.push(thread::spawn(move || {
                start.wait();
                loop {
                    match dispatcher.emit(event(AuditAction::Spawn)) {
                        AuditEmitOutcome::Enqueued => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        AuditEmitOutcome::DroppedFull => thread::yield_now(),
                        AuditEmitOutcome::DroppedClosed => break,
                        outcome => panic!("unexpected emit outcome: {outcome:?}"),
                    }
                }
            }));
        }

        start.wait();
        while accepted.load(Ordering::Relaxed) == 0 {
            thread::yield_now();
        }
        assert_eq!(
            dispatcher.close_and_flush(Duration::from_secs(5)),
            AuditFlushOutcome::Flushed
        );
        for producer in producers {
            producer.join().unwrap();
        }

        assert_eq!(
            events.lock().unwrap().len(),
            accepted.load(Ordering::Relaxed)
        );
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::DroppedClosed
        );
    }

    #[test]
    fn concurrent_producers_receive_writer_ordered_sequences() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let dispatcher = AuditDispatcher::new(
            AuditConfig {
                queue_capacity: 1_024,
                flush_timeout: Duration::from_secs(2),
                ..AuditConfig::default()
            },
            RecordingSink {
                events: Arc::clone(&events),
            },
        );
        let barrier = Arc::new(Barrier::new(9));
        let mut producers = Vec::new();
        for _ in 0..8 {
            let dispatcher = dispatcher.clone();
            let barrier = Arc::clone(&barrier);
            producers.push(thread::spawn(move || {
                barrier.wait();
                for _ in 0..50 {
                    assert_eq!(
                        dispatcher.emit(event(AuditAction::Spawn)),
                        AuditEmitOutcome::Enqueued
                    );
                }
            }));
        }
        barrier.wait();
        for producer in producers {
            producer.join().unwrap();
        }

        assert_eq!(dispatcher.flush(), AuditFlushOutcome::Flushed);
        let sequences = events
            .lock()
            .unwrap()
            .iter()
            .map(AuditEventV1::sequence)
            .collect::<Vec<_>>();
        assert_eq!(sequences, (1..=400).collect::<Vec<_>>());
    }

    struct PanickingSink;

    impl AuditSink for PanickingSink {
        fn write(&mut self, _event: &AuditEventV1) -> io::Result<()> {
            panic!("test sink panic")
        }
    }

    #[test]
    fn sink_panic_opens_the_health_circuit() {
        let dispatcher = AuditDispatcher::new(AuditConfig::default(), PanickingSink);
        assert_eq!(
            dispatcher.emit(event(AuditAction::Spawn)),
            AuditEmitOutcome::Enqueued
        );
        assert_eq!(dispatcher.flush(), AuditFlushOutcome::SinkFailed);
        let stats = dispatcher.stats();
        assert_eq!(stats.sink_failures, 1);
        assert!(!stats.sink_healthy);
        assert!(!stats.closed);
        assert_eq!(
            dispatcher.emit(event(AuditAction::Connect)),
            AuditEmitOutcome::DroppedSinkUnavailable
        );
    }
}
