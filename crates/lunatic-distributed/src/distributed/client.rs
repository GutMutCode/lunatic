use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    sync::{
        atomic::{self, AtomicBool, AtomicU64, AtomicUsize},
        Arc, Mutex, Weak,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use async_cell::sync::AsyncCell;
use bytes::Bytes;
use sha2::{Digest, Sha256};

use crate::distributed::message;
use dashmap::mapref::entry::Entry as DashEntry;
use dashmap::DashMap;
use tokio::sync::{
    mpsc::{error::TrySendError, Receiver, Sender},
    watch, Mutex as AsyncMutex, Notify, OwnedMutexGuard, RwLock,
};

use crate::{
    congestion::{
        self, fair_node_connection_manager, node_queue_channels, AdmissionResult, AdmittedMessage,
        FairNodeConnectionManager, NodeQueueSender,
    },
    control,
    distributed::audit_verified_peer_protocol_denial,
    distributed::message::{Request, ResponseContent, Spawn},
    distributed::registry::{DistributedRegistry, ProcessName, RegistryLimits},
    distributed::registry_coordination::{RegistryCoordinationMessage, RegistryCoordinator},
    quic::{self, VerifiedNodeId},
};

use super::message::Response;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct EnvironmentId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct ProcessId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct NodeId(pub u64);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct MessageId(pub u64);

pub struct SendParams {
    /// Environment that owns the sending process and its outbound queue.
    pub source_env: EnvironmentId,
    /// Environment that owns the destination process on the remote node.
    pub target_env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub dest: ProcessId,
    pub tag: Option<i64>,
    pub data: Vec<u8>,
}

pub struct SpawnParams {
    pub env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub spawn: Spawn,
}

pub struct ResponseParams {
    pub node_id: NodeId,
    pub response: Response,
}

pub struct MessageCtx {
    pub message_id: MessageId,
    pub env: EnvironmentId,
    pub src: ProcessId,
    pub node: NodeId,
    pub dest: ProcessId,
    pub(crate) queue_generation: u64,
    pub chunk_id: AtomicU64,
    pub offset: AtomicUsize,
    pub data: Bytes,
    pub(crate) retained_bytes: usize,
    pub(crate) class: OutboundClass,
    pub(crate) application_ack: Option<ApplicationAckHandle>,
    pub(crate) outbound_lease: OutboundMessageLease,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OutboundClass {
    Data,
    Control,
}

struct NewMessageParams {
    message_id: MessageId,
    env: EnvironmentId,
    src: ProcessId,
    node: NodeId,
    dest: ProcessId,
    class: OutboundClass,
    data: Vec<u8>,
}

/// Stable production queue identity for one source-to-destination process pair.
///
/// Keeping the destination in the key prevents a saturated route from hiding another logical
/// stream behind the source process's FIFO. FIFO is still preserved within each exact pair.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ProcessQueueRoute {
    pub(crate) src: ProcessId,
    pub(crate) node: NodeId,
    pub(crate) dest: ProcessId,
}

impl ProcessQueueRoute {
    fn new(src: ProcessId, node: NodeId, dest: ProcessId) -> Self {
        Self { src, node, dest }
    }
}

pub(crate) struct ProcessQueueSender {
    pub(crate) generation: u64,
    pub(crate) sender: Sender<AdmittedMessage>,
    pub(crate) admission_gate: Arc<AsyncMutex<()>>,
}

pub(crate) struct ProcessQueueReceiver {
    pub(crate) generation: u64,
    pub(crate) receiver: RwLock<Receiver<AdmittedMessage>>,
    pub(crate) admission_gate: Arc<AsyncMutex<()>>,
}

pub(crate) struct NodeQueue {
    sender: NodeQueueSender,
    manager: tokio::task::AbortHandle,
}

impl NodeQueue {
    pub(crate) fn sender(&self) -> NodeQueueSender {
        self.sender.clone()
    }

    pub(crate) fn abort(self) {
        self.manager.abort();
    }
}

impl Drop for NodeQueue {
    fn drop(&mut self) {
        self.manager.abort();
    }
}

pub const MAX_OUTBOUND_IN_FLIGHT_MESSAGES: usize = 1_024;
pub const MAX_OUTBOUND_IN_FLIGHT_BYTES: usize = 32 * 1024 * 1024;
pub const OUTBOUND_PROCESS_QUEUE_CAPACITY: usize = 64;
pub const OUTBOUND_NODE_QUEUE_CAPACITY: usize = 256;
const MAX_CONTROL_OUTBOUND_MESSAGES: usize = 64;
const MAX_CONTROL_OUTBOUND_BYTES: usize = 1024 * 1024;
const MAX_APPLICATION_ACKS: usize = MAX_OUTBOUND_IN_FLIGHT_MESSAGES;
const MAX_INBOUND_REPLAY_ENTRIES: usize = 16_384;
// Keep terminal results beyond the complete replay window plus receiver reassembly and transport
// idle margins. A replay that was already in flight when its deadline elapsed must still encounter
// the tombstone instead of executing the side effect again after a slow or partitioned stream
// resumes.
const INBOUND_REPLAY_TTL: Duration = Duration::from_secs(180);
// Once the first send attempt can emit bytes, retries remain finite so the bounded receiver
// tombstone can outlive every attempt. Messages waiting for a connection or local admission do not
// start this deadline because they cannot yet have caused a remote side effect.
const APPLICATION_ACK_REPLAY_DEADLINE: Duration = Duration::from_secs(45);
const MAX_REPLAY_ERROR_BYTES: usize = 1024;
const MAX_REGISTRY_CLEANUP_IN_FLIGHT: usize = 8;
const REGISTRY_CLEANUP_RETRY_TICK: Duration = Duration::from_millis(100);
const MAX_REGISTRY_CLEANUP_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutboundLimits {
    pub max_messages: usize,
    pub max_bytes: usize,
}

impl Default for OutboundLimits {
    fn default() -> Self {
        Self {
            max_messages: MAX_OUTBOUND_IN_FLIGHT_MESSAGES,
            max_bytes: MAX_OUTBOUND_IN_FLIGHT_BYTES,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DistributedLimits {
    pub outbound: OutboundLimits,
    pub registry: RegistryLimits,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundLimitKind {
    Messages,
    Bytes,
    AccountingOverflow,
}

#[derive(Debug)]
pub struct OutboundSaturationError {
    kind: OutboundLimitKind,
    max_messages: usize,
    max_bytes: usize,
    requested_bytes: usize,
}

impl OutboundSaturationError {
    fn new(kind: OutboundLimitKind, limits: OutboundLimits, requested_bytes: usize) -> Self {
        Self {
            kind,
            max_messages: limits.max_messages,
            max_bytes: limits.max_bytes,
            requested_bytes,
        }
    }

    pub fn kind(&self) -> OutboundLimitKind {
        self.kind
    }
}

impl fmt::Display for OutboundSaturationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Distributed outbound {:?} limit reached (max_messages={}, max_bytes={}, requested_bytes={})",
            self.kind, self.max_messages, self.max_bytes, self.requested_bytes
        )
    }
}

impl std::error::Error for OutboundSaturationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendErrorKind {
    NodeNotFound,
    Backpressure,
    MessageTooLarge,
    QueueClosed,
    Serialization,
    EnvironmentNotFound,
    ProcessNotFound,
    RemoteBackpressure,
    RemoteMessageTooLarge,
    RemoteRejected,
    Connection,
    ResponseTimeout,
    UnexpectedResponse,
}

pub struct SendError {
    kind: SendErrorKind,
    message: String,
    data: Vec<u8>,
}

impl SendError {
    fn new(kind: SendErrorKind, message: impl Into<String>, data: Vec<u8>) -> Self {
        Self {
            kind,
            message: message.into(),
            data,
        }
    }

    pub fn kind(&self) -> SendErrorKind {
        self.kind
    }

    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

impl fmt::Debug for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SendError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("data_len", &self.data.len())
            .finish()
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SendError {}

fn remote_send_error(error: message::ClientError, data: Vec<u8>) -> SendError {
    use message::ClientError;

    match error {
        ClientError::Unexpected(message) => {
            SendError::new(SendErrorKind::RemoteRejected, message, data)
        }
        ClientError::Connection(message) => {
            SendError::new(SendErrorKind::Connection, message, data)
        }
        ClientError::NodeNotFound => SendError::new(
            SendErrorKind::NodeNotFound,
            "Remote node does not exist",
            data,
        ),
        ClientError::ModuleNotFound => SendError::new(
            SendErrorKind::UnexpectedResponse,
            "Remote delivery returned a module-not-found response",
            data,
        ),
        ClientError::ProcessNotFound => SendError::new(
            SendErrorKind::ProcessNotFound,
            "Remote process does not exist",
            data,
        ),
        ClientError::EnvironmentNotFound => SendError::new(
            SendErrorKind::EnvironmentNotFound,
            "Remote environment does not exist",
            data,
        ),
        ClientError::DeliveryBackpressure(message) => {
            SendError::new(SendErrorKind::RemoteBackpressure, message, data)
        }
        ClientError::DeliveryTooLarge(message) => {
            SendError::new(SendErrorKind::RemoteMessageTooLarge, message, data)
        }
        ClientError::DeliveryRejected(message) => {
            SendError::new(SendErrorKind::RemoteRejected, message, data)
        }
        ClientError::ResponseTimeout => SendError::new(
            SendErrorKind::ResponseTimeout,
            "Timed out waiting for remote mailbox admission",
            data,
        ),
    }
}

pub(crate) struct OutboundEnqueueError {
    kind: SendErrorKind,
    message: String,
}

impl OutboundEnqueueError {
    fn new(kind: SendErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn kind(&self) -> SendErrorKind {
        self.kind
    }
}

impl fmt::Display for OutboundEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl fmt::Debug for OutboundEnqueueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboundEnqueueError")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .finish()
    }
}

impl std::error::Error for OutboundEnqueueError {}

#[derive(Debug, Default)]
struct OutboundBudgetUsage {
    messages: usize,
    bytes: usize,
}

struct OutboundBudget {
    limits: OutboundLimits,
    usage: Mutex<OutboundBudgetUsage>,
}

impl OutboundBudget {
    fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            limits: OutboundLimits {
                max_messages,
                max_bytes,
            },
            usage: Mutex::new(OutboundBudgetUsage::default()),
        }
    }

    fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> std::result::Result<OutboundMessageLease, OutboundSaturationError> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_bytes = usage.bytes.checked_add(bytes).ok_or_else(|| {
            OutboundSaturationError::new(OutboundLimitKind::AccountingOverflow, self.limits, bytes)
        })?;
        if usage.messages >= self.limits.max_messages {
            return Err(OutboundSaturationError::new(
                OutboundLimitKind::Messages,
                self.limits,
                bytes,
            ));
        }
        if next_bytes > self.limits.max_bytes {
            return Err(OutboundSaturationError::new(
                OutboundLimitKind::Bytes,
                self.limits,
                bytes,
            ));
        }
        usage.messages += 1;
        usage.bytes = next_bytes;
        drop(usage);
        Ok(OutboundMessageLease {
            _inner: Arc::new(OutboundLeaseInner {
                budget: self.clone(),
                bytes,
            }),
        })
    }

    fn release(&self, bytes: usize) {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        usage.messages = usage
            .messages
            .checked_sub(1)
            .expect("distributed outbound message accounting cannot underflow");
        usage.bytes = usage
            .bytes
            .checked_sub(bytes)
            .expect("distributed outbound byte accounting cannot underflow");
    }

    #[cfg(test)]
    fn current_usage(&self) -> (usize, usize) {
        let usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (usage.messages, usage.bytes)
    }
}

impl Default for OutboundBudget {
    fn default() -> Self {
        let limits = OutboundLimits::default();
        Self::new(limits.max_messages, limits.max_bytes)
    }
}

struct OutboundLeaseInner {
    budget: Arc<OutboundBudget>,
    bytes: usize,
}

impl Drop for OutboundLeaseInner {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

#[derive(Clone)]
pub(crate) struct OutboundMessageLease {
    _inner: Arc<OutboundLeaseInner>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct OutboundBudgetProbe {
    budget: Arc<OutboundBudget>,
}

#[cfg(test)]
impl OutboundBudgetProbe {
    pub(crate) fn current_usage(&self) -> (usize, usize) {
        self.budget.current_usage()
    }
}

#[cfg(test)]
pub(crate) fn test_outbound_lease(bytes: usize) -> (OutboundMessageLease, OutboundBudgetProbe) {
    let budget = Arc::new(OutboundBudget::new(1, bytes));
    let lease = budget
        .try_reserve(bytes)
        .expect("test outbound lease must fit its dedicated budget");
    (lease, OutboundBudgetProbe { budget })
}

struct ApplicationAckEntry {
    expected_node: NodeId,
    acknowledged: watch::Sender<bool>,
}

struct ApplicationAckRegistry {
    entries: Mutex<HashMap<MessageId, ApplicationAckEntry>>,
    max_entries: usize,
}

impl ApplicationAckRegistry {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            max_entries,
        }
    }

    fn try_register(
        self: &Arc<Self>,
        message_id: MessageId,
        expected_node: NodeId,
    ) -> std::result::Result<ApplicationAckHandle, OutboundEnqueueError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() >= self.max_entries {
            return Err(OutboundEnqueueError::new(
                SendErrorKind::Backpressure,
                format!(
                    "Distributed application acknowledgement limit reached ({})",
                    self.max_entries
                ),
            ));
        }
        let (acknowledged, receiver) = watch::channel(false);
        let previous = entries.insert(
            message_id,
            ApplicationAckEntry {
                expected_node,
                acknowledged,
            },
        );
        debug_assert!(previous.is_none(), "distributed message IDs must be unique");
        drop(entries);
        Ok(ApplicationAckHandle {
            message_id,
            acknowledged: receiver,
            registry: self.clone(),
            deadline: None,
        })
    }

    fn expected_node(&self, message_id: MessageId) -> Option<NodeId> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&message_id)
            .map(|entry| entry.expected_node)
    }

    fn acknowledge(&self, message_id: MessageId, source_node: NodeId) -> bool {
        let entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(entry) = entries.get(&message_id) else {
            return false;
        };
        if entry.expected_node != source_node {
            return false;
        }
        entry.acknowledged.send_replace(true);
        true
    }
}

pub(crate) struct ApplicationAckHandle {
    message_id: MessageId,
    acknowledged: watch::Receiver<bool>,
    registry: Arc<ApplicationAckRegistry>,
    deadline: Option<Instant>,
}

impl ApplicationAckHandle {
    pub(crate) async fn wait(&mut self) -> bool {
        loop {
            if *self.acknowledged.borrow() {
                return true;
            }
            if self.acknowledged.changed().await.is_err() {
                return false;
            }
        }
    }

    pub(crate) fn is_acknowledged(&self) -> bool {
        *self.acknowledged.borrow()
    }

    pub(crate) fn is_expired(&self) -> bool {
        self.deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
    }

    pub(crate) fn remaining(&self) -> Duration {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
            .unwrap_or(APPLICATION_ACK_REPLAY_DEADLINE)
    }

    pub(crate) fn replay_deadline_remaining(&self) -> Option<Duration> {
        self.deadline
            .map(|deadline| deadline.saturating_duration_since(Instant::now()))
    }

    pub(crate) fn start_replay_window(&mut self) {
        self.deadline
            .get_or_insert_with(|| Instant::now() + APPLICATION_ACK_REPLAY_DEADLINE);
    }

    pub(crate) fn subscribe(&self) -> watch::Receiver<bool> {
        self.acknowledged.clone()
    }

    #[cfg(test)]
    pub(crate) fn set_replay_deadline_after(&mut self, duration: Duration) {
        self.deadline = Some(Instant::now() + duration);
    }
}

impl Drop for ApplicationAckHandle {
    fn drop(&mut self) {
        self.registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.message_id);
    }
}

#[cfg(test)]
pub(crate) struct ApplicationAckProbe {
    registry: Arc<ApplicationAckRegistry>,
    message_id: MessageId,
    node: NodeId,
}

#[cfg(test)]
impl ApplicationAckProbe {
    pub(crate) fn acknowledge(&self) -> bool {
        self.registry.acknowledge(self.message_id, self.node)
    }
}

#[cfg(test)]
pub(crate) fn test_application_ack(
    message_id: MessageId,
    node: NodeId,
) -> (ApplicationAckHandle, ApplicationAckProbe) {
    let registry = Arc::new(ApplicationAckRegistry::new(1));
    let handle = registry
        .try_register(message_id, node)
        .expect("test application ACK must fit");
    (
        handle,
        ApplicationAckProbe {
            registry,
            message_id,
            node,
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct ReplayKey {
    peer_node_id: u64,
    message_id: u64,
}

pub(crate) struct ReplayEntry {
    fingerprint: [u8; 32],
    completed: watch::Sender<Option<(ResponseContent, Instant)>>,
}

impl ReplayEntry {
    pub(crate) async fn wait(&self) -> ResponseContent {
        let mut completed = self.completed.subscribe();
        loop {
            if let Some((response, _)) = completed.borrow().as_ref() {
                return response.clone();
            }
            if completed.changed().await.is_err() {
                return ResponseContent::Error(
                    crate::distributed::message::ClientError::ResponseTimeout,
                );
            }
        }
    }
}

struct ReplayCacheState {
    entries: HashMap<ReplayKey, Arc<ReplayEntry>>,
    completed_order: VecDeque<(ReplayKey, Weak<ReplayEntry>)>,
}

struct ReplayCache {
    state: Mutex<ReplayCacheState>,
    max_entries: usize,
    ttl: Duration,
}

impl ReplayCache {
    fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            state: Mutex::new(ReplayCacheState {
                entries: HashMap::new(),
                completed_order: VecDeque::new(),
            }),
            max_entries,
            ttl,
        }
    }

    fn begin(self: &Arc<Self>, key: ReplayKey, fingerprint: [u8; 32]) -> ReplayDecision {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while let Some((oldest, recorded)) = state.completed_order.front().cloned() {
            let Some(recorded) = recorded.upgrade() else {
                state.completed_order.pop_front();
                continue;
            };
            let is_current_generation = state
                .entries
                .get(&oldest)
                .is_some_and(|current| Arc::ptr_eq(current, &recorded));
            if !is_current_generation {
                state.completed_order.pop_front();
                continue;
            }
            let expired = recorded
                .completed
                .borrow()
                .as_ref()
                .is_some_and(|(_, completed_at)| {
                    now.saturating_duration_since(*completed_at) >= self.ttl
                });
            if !expired {
                break;
            }
            state.completed_order.pop_front();
            state.entries.remove(&oldest);
        }

        if let Some(entry) = state.entries.get(&key).cloned() {
            if entry.fingerprint != fingerprint {
                return ReplayDecision::Conflict;
            }
            let cached_response = entry
                .completed
                .borrow()
                .as_ref()
                .map(|(response, _)| response.clone());
            if let Some(response) = cached_response {
                entry.completed.send_if_modified(|completed| {
                    let Some((_, completed_at)) = completed else {
                        return false;
                    };
                    *completed_at = now;
                    true
                });
                state.completed_order.retain(|(recorded_key, recorded)| {
                    if *recorded_key != key {
                        return true;
                    }
                    recorded
                        .upgrade()
                        .is_some_and(|recorded| !Arc::ptr_eq(&recorded, &entry))
                });
                state
                    .completed_order
                    .push_back((key, Arc::downgrade(&entry)));
                return ReplayDecision::Cached(response);
            }
            return ReplayDecision::Wait(entry);
        }
        if state.entries.len() >= self.max_entries {
            return ReplayDecision::Saturated;
        }

        let (completed, _) = watch::channel(None);
        let entry = Arc::new(ReplayEntry {
            fingerprint,
            completed,
        });
        state.entries.insert(key, entry.clone());
        ReplayDecision::Execute(ReplayExecutionGuard {
            cache: self.clone(),
            key,
            entry,
            finished: false,
        })
    }

    fn finish(&self, key: ReplayKey, entry: &Arc<ReplayEntry>, response: ResponseContent) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state
            .entries
            .get(&key)
            .is_some_and(|current| Arc::ptr_eq(current, entry))
        {
            return;
        }
        if !entry.completed.send_if_modified(|completed| {
            if completed.is_some() {
                return false;
            }
            *completed = Some((response.clone(), Instant::now()));
            true
        }) {
            return;
        }
        state
            .completed_order
            .push_back((key, Arc::downgrade(entry)));
    }
}

pub(crate) enum ReplayDecision {
    Execute(ReplayExecutionGuard),
    Wait(Arc<ReplayEntry>),
    Cached(ResponseContent),
    Conflict,
    Saturated,
}

pub(crate) struct ReplayExecutionGuard {
    cache: Arc<ReplayCache>,
    key: ReplayKey,
    entry: Arc<ReplayEntry>,
    finished: bool,
}

impl ReplayExecutionGuard {
    pub(crate) fn finish(mut self, response: ResponseContent) -> ResponseContent {
        let response = bounded_replay_response(response);
        self.cache.finish(self.key, &self.entry, response.clone());
        self.finished = true;
        response
    }
}

fn bounded_replay_response(response: ResponseContent) -> ResponseContent {
    fn truncate(mut message: String) -> String {
        if message.len() <= MAX_REPLAY_ERROR_BYTES {
            return message;
        }
        let mut end = MAX_REPLAY_ERROR_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message
    }

    match response {
        ResponseContent::Error(error) => ResponseContent::Error(match error {
            crate::distributed::message::ClientError::Unexpected(message) => {
                crate::distributed::message::ClientError::Unexpected(truncate(message))
            }
            crate::distributed::message::ClientError::Connection(message) => {
                crate::distributed::message::ClientError::Connection(truncate(message))
            }
            crate::distributed::message::ClientError::DeliveryBackpressure(message) => {
                crate::distributed::message::ClientError::DeliveryBackpressure(truncate(message))
            }
            crate::distributed::message::ClientError::DeliveryTooLarge(message) => {
                crate::distributed::message::ClientError::DeliveryTooLarge(truncate(message))
            }
            crate::distributed::message::ClientError::DeliveryRejected(message) => {
                crate::distributed::message::ClientError::DeliveryRejected(truncate(message))
            }
            other => other,
        }),
        other => other,
    }
}

impl Drop for ReplayExecutionGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.cache.finish(
                self.key,
                &self.entry,
                ResponseContent::Error(crate::distributed::message::ClientError::ResponseTimeout),
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct InboundRouteKey {
    peer_node_id: u64,
    environment_id: u64,
    process_id: u64,
}

struct InboundRouteSequencer {
    routes: Mutex<HashMap<InboundRouteKey, Weak<AsyncMutex<()>>>>,
}

impl InboundRouteSequencer {
    fn new() -> Self {
        Self {
            routes: Mutex::new(HashMap::new()),
        }
    }

    async fn acquire(self: &Arc<Self>, key: InboundRouteKey) -> InboundRouteExecutionGuard {
        let route = {
            let mut routes = self
                .routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match routes.get(&key).and_then(Weak::upgrade) {
                Some(route) => route,
                None => {
                    let route = Arc::new(AsyncMutex::new(()));
                    routes.insert(key, Arc::downgrade(&route));
                    route
                }
            }
        };
        let lock = route.clone().lock_owned().await;
        InboundRouteExecutionGuard {
            sequencer: self.clone(),
            key,
            route,
            lock: Some(lock),
        }
    }
}

pub(crate) struct InboundRouteExecutionGuard {
    sequencer: Arc<InboundRouteSequencer>,
    key: InboundRouteKey,
    route: Arc<AsyncMutex<()>>,
    lock: Option<OwnedMutexGuard<()>>,
}

impl Drop for InboundRouteExecutionGuard {
    fn drop(&mut self) {
        drop(self.lock.take());
        let mut routes = self
            .sequencer
            .routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if Arc::strong_count(&self.route) == 1
            && routes
                .get(&self.key)
                .is_some_and(|route| Weak::ptr_eq(route, &Arc::downgrade(&self.route)))
        {
            routes.remove(&self.key);
        }
    }
}

struct IncomingResponse {
    expected_node: NodeId,
    response: AsyncCell<ResponseContent>,
    created_at: Instant,
}

impl IncomingResponse {
    fn new(expected_node: NodeId) -> Self {
        Self {
            expected_node,
            response: AsyncCell::new(),
            created_at: Instant::now(),
        }
    }
}

/// Removes a response waiter if the operation is cancelled or returns early.
///
/// Normal completion also removes the entry through [`Client::await_response`];
/// the second removal in `Drop` is intentionally harmless.
struct ResponseWaiterGuard<'a> {
    responses: &'a DashMap<MessageId, Arc<IncomingResponse>>,
    message_id: MessageId,
}

impl<'a> ResponseWaiterGuard<'a> {
    fn insert(
        responses: &'a DashMap<MessageId, Arc<IncomingResponse>>,
        message_id: MessageId,
        expected_node: NodeId,
    ) -> Self {
        responses.insert(message_id, Arc::new(IncomingResponse::new(expected_node)));
        Self {
            responses,
            message_id,
        }
    }
}

impl Drop for ResponseWaiterGuard<'_> {
    fn drop(&mut self) {
        self.responses.remove(&self.message_id);
    }
}

#[derive(Clone)]
pub struct Client {
    pub node_id: NodeId,
    pub inner: Arc<Inner>,
}

/// Owns the distributed-registry registrations associated with one process.
///
/// The final owner schedules owner-conditional cluster cleanup. Sharing the
/// guard across hot-reload replacement states prevents the old instance from
/// unregistering names that the replacement still owns.
pub struct RegistryProcessRegistration {
    client: Client,
    global_pid: super::GlobalProcessId,
    cleaned: AtomicBool,
}

#[derive(Debug)]
struct RegistryCleanupRetry {
    in_flight: bool,
    failures: u32,
    next_attempt: Instant,
}

impl RegistryCleanupRetry {
    fn ready() -> Self {
        Self {
            in_flight: false,
            failures: 0,
            next_attempt: Instant::now(),
        }
    }
}

fn registry_cleanup_backoff(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(6);
    let multiplier = 1_u32 << exponent;
    (REGISTRY_CLEANUP_RETRY_TICK * multiplier).min(MAX_REGISTRY_CLEANUP_BACKOFF)
}

impl RegistryProcessRegistration {
    fn trigger_cleanup(&self) {
        if self.cleaned.swap(true, atomic::Ordering::AcqRel) {
            return;
        }
        let has_global_names = !self
            .client
            .registry()
            .global_names_for_process(self.global_pid)
            .is_empty();
        self.client
            .registry()
            .remove_local_registrations(self.global_pid);
        if !has_global_names {
            return;
        }
        {
            let mut pending = self
                .client
                .inner
                .registry_cleanup_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !pending.contains_key(&self.global_pid)
                && pending.len() >= self.client.inner.limits.registry.max_entries
            {
                log::error!(
                    "Registry cleanup pending limit reached; retaining registrations for {}",
                    self.global_pid
                );
                return;
            }
            pending
                .entry(self.global_pid)
                .or_insert_with(RegistryCleanupRetry::ready);
        }
        if let Err(error) = self
            .client
            .inner
            .registry_cleanup_tx
            .try_send(self.global_pid)
        {
            log::warn!(
                "Registry process-cleanup wake queue rejected {}; periodic retry retains it: {error}",
                self.global_pid
            );
        }
    }
}

impl lunatic_process::env::ProcessExitHook for RegistryProcessRegistration {
    fn process_exited(&self) {
        self.trigger_cleanup();
    }
}

impl Drop for RegistryProcessRegistration {
    fn drop(&mut self) {
        self.trigger_cleanup();
    }
}

pub struct Inner {
    control_client: control::Client,
    node_client: quic::Client,
    pub next_message_id: AtomicU64,
    next_process_queue_generation: AtomicU64,
    // Across Environments and ProcessId's track message queues
    pub(crate) buf_rx: DashMap<EnvironmentId, DashMap<ProcessQueueRoute, ProcessQueueReceiver>>,
    // Sending part of the message queue
    pub(crate) buf_tx: DashMap<(EnvironmentId, ProcessQueueRoute), ProcessQueueSender>,
    // Kept for public API compatibility with the legacy source-keyed scheduler.
    pub in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    // Holds one admitted message per exact source-to-destination route.
    pub(crate) route_in_progress: DashMap<(EnvironmentId, ProcessQueueRoute), AdmittedMessage>,
    pub(crate) nodes_queues: DashMap<NodeId, NodeQueue>,
    responses: DashMap<MessageId, Arc<IncomingResponse>>,
    pub response_tx: Sender<(MessageId, ResponseContent)>,
    registry_cleanup_tx: Sender<super::GlobalProcessId>,
    registry_cleanup_pending: Mutex<HashMap<super::GlobalProcessId, RegistryCleanupRetry>>,
    pub has_messages: Arc<Notify>,
    outbound_budget: Arc<OutboundBudget>,
    control_outbound_budget: Arc<OutboundBudget>,
    application_acks: Arc<ApplicationAckRegistry>,
    replay_cache: Arc<ReplayCache>,
    inbound_routes: Arc<InboundRouteSequencer>,
    node_queue_admission: AsyncMutex<()>,
    topology_nodes: Mutex<HashSet<u64>>,
    limits: DistributedLimits,
    // Distributed process registry
    pub registry: Arc<DistributedRegistry>,
    // Registry coordinator for cross-node coordination
    pub coordinator: Arc<RegistryCoordinator>,
}

fn process_queue_is_current(
    queues: &DashMap<(EnvironmentId, ProcessQueueRoute), ProcessQueueSender>,
    key: (EnvironmentId, ProcessQueueRoute),
    generation: u64,
    sender: &Sender<AdmittedMessage>,
) -> bool {
    queues
        .get(&key)
        .map(|queue| queue.generation == generation && queue.sender.same_channel(sender))
        .unwrap_or(false)
}

impl Client {
    pub fn new(node_id: u64, control_client: control::Client, node_client: quic::Client) -> Self {
        Self::new_with_limits(
            node_id,
            control_client,
            node_client,
            DistributedLimits::default(),
        )
    }

    pub fn new_with_limits(
        node_id: u64,
        control_client: control::Client,
        node_client: quic::Client,
        limits: DistributedLimits,
    ) -> Self {
        let (send, recv) = tokio::sync::mpsc::channel(1000);
        let cleanup_capacity = limits.registry.max_entries.max(1);
        let (registry_cleanup_tx, registry_cleanup_rx) =
            tokio::sync::mpsc::channel(cleanup_capacity);
        let registry = Arc::new(DistributedRegistry::with_limits(node_id, limits.registry));
        let coordinator = Arc::new(RegistryCoordinator::new(registry.clone(), node_id));
        let mut topology_nodes = control_client
            .node_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        topology_nodes.insert(node_id);
        let outbound = limits.outbound;
        // A random incarnation prefix prevents a restarted node from colliding with the receiver's
        // bounded replay cache while preserving a monotonically increasing counter per process.
        let message_id_epoch = (uuid::Uuid::new_v4().as_u128() as u64) & 0xffff_ffff_0000_0000;

        let client = Self {
            node_id: NodeId(node_id),
            inner: Arc::new(Inner {
                control_client,
                node_client,
                next_message_id: AtomicU64::new(message_id_epoch | 1),
                next_process_queue_generation: AtomicU64::new(1),
                buf_rx: DashMap::new(),
                buf_tx: DashMap::new(),
                in_progress: DashMap::new(),
                route_in_progress: DashMap::new(),
                nodes_queues: DashMap::new(),
                responses: DashMap::new(),
                response_tx: send,
                registry_cleanup_tx,
                registry_cleanup_pending: Mutex::new(HashMap::new()),
                has_messages: Arc::new(Notify::new()),
                outbound_budget: Arc::new(OutboundBudget::new(
                    outbound.max_messages,
                    outbound.max_bytes,
                )),
                control_outbound_budget: Arc::new(OutboundBudget::new(
                    MAX_CONTROL_OUTBOUND_MESSAGES,
                    MAX_CONTROL_OUTBOUND_BYTES,
                )),
                application_acks: Arc::new(ApplicationAckRegistry::new(MAX_APPLICATION_ACKS)),
                replay_cache: Arc::new(ReplayCache::new(
                    MAX_INBOUND_REPLAY_ENTRIES,
                    INBOUND_REPLAY_TTL,
                )),
                inbound_routes: Arc::new(InboundRouteSequencer::new()),
                node_queue_admission: AsyncMutex::new(()),
                topology_nodes: Mutex::new(topology_nodes),
                limits,
                registry,
                coordinator,
            }),
        };
        client.inner.coordinator.attach_client(&client);
        tokio::spawn(congestion::congestion_control_worker(client.clone()));
        tokio::spawn(process_responses(client.clone(), recv));
        tokio::spawn(registry_sync_worker(client.clone()));
        tokio::spawn(registry_cleanup_worker(client.clone(), registry_cleanup_rx));
        client
    }

    /// Get a reference to the distributed process registry
    pub fn registry(&self) -> &DistributedRegistry {
        &self.inner.registry
    }

    /// Get a reference to the registry coordinator
    pub fn coordinator(&self) -> &RegistryCoordinator {
        &self.inner.coordinator
    }

    pub fn register_process_owner(
        &self,
        global_pid: super::GlobalProcessId,
    ) -> Arc<RegistryProcessRegistration> {
        Arc::new(RegistryProcessRegistration {
            client: self.clone(),
            global_pid,
            cleaned: AtomicBool::new(false),
        })
    }

    pub(crate) fn registry_node_ids(&self) -> Vec<u64> {
        self.inner.control_client.node_ids()
    }

    pub(crate) fn is_active_node(&self, node_id: u64) -> bool {
        self.inner.control_client.node_info(node_id).is_some()
    }

    /// Register a cluster-wide process name through the registry quorum.
    pub async fn register_global(
        &self,
        name: impl Into<super::registry::ProcessName>,
        global_pid: super::GlobalProcessId,
    ) -> Result<()> {
        self.inner
            .coordinator
            .register_global_coordinated(name, global_pid, self.inner.control_client.node_count())
            .await
    }

    /// Pull an authoritative registry snapshot from the cluster coordinator.
    pub async fn synchronize_registry(&self) -> Result<()> {
        self.inner.coordinator.synchronize_from_coordinator().await
    }

    pub(crate) async fn send_registry_coordination(
        &self,
        node_id: u64,
        coordination: RegistryCoordinationMessage,
    ) -> Result<()> {
        if !self.inner.control_client.node_ids().contains(&node_id) {
            return Err(anyhow!("Registry target node {node_id} does not exist"));
        }
        let node = self
            .inner
            .control_client
            .node_info(node_id)
            .ok_or_else(|| anyhow!("Registry target node {node_id} does not exist"))?;
        let message = Request::Registry {
            node_id: self.node_id.0,
            message: coordination,
        };
        let data = message::serialize_message(&message)?;
        if data.len() > quic::MAX_WIRE_MESSAGE_BYTES {
            return Err(anyhow!(
                "Registry coordination message size {} exceeds the transport limit {}",
                data.len(),
                quic::MAX_WIRE_MESSAGE_BYTES
            ));
        }
        let _outbound_lease = self
            .inner
            .outbound_budget
            .try_reserve(data.capacity())
            .map_err(anyhow::Error::new)?;
        let message_id = self.next_message_id().0;
        self.inner
            .node_client
            .send_message(node.address, &node.name, node.id, message_id, data.into())
            .await
    }

    pub(crate) async fn handle_registry_message(
        &self,
        source_node_id: VerifiedNodeId,
        claimed_node_id: u64,
        message: RegistryCoordinationMessage,
    ) -> Result<()> {
        if !self.is_active_node(source_node_id.get()) {
            audit_verified_peer_protocol_denial(source_node_id);
            return Err(anyhow!(
                "Authenticated registry peer is not an active topology member"
            ));
        }
        if claimed_node_id != source_node_id.get() {
            audit_verified_peer_protocol_denial(source_node_id);
            return Err(anyhow!(
                "Registry request source did not match authenticated peer"
            ));
        }
        self.inner
            .coordinator
            .handle_message(source_node_id, message)
            .await
    }

    fn next_message_id(&self) -> MessageId {
        MessageId(
            self.inner
                .next_message_id
                .fetch_add(1, atomic::Ordering::Relaxed),
        )
    }

    pub(crate) fn begin_inbound_replay(
        &self,
        peer_node_id: u64,
        message_id: u64,
        request: &Request,
    ) -> Result<ReplayDecision> {
        struct DigestWriter(Sha256);

        impl std::io::Write for DigestWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.update(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut digest = DigestWriter(Sha256::new());
        rmp_serde::encode::write(&mut digest, request)?;
        let fingerprint: [u8; 32] = digest.0.finalize().into();
        Ok(self.inner.replay_cache.begin(
            ReplayKey {
                peer_node_id,
                message_id,
            },
            fingerprint,
        ))
    }

    pub(crate) async fn lock_inbound_route(
        &self,
        peer_node_id: u64,
        environment_id: u64,
        process_id: u64,
    ) -> InboundRouteExecutionGuard {
        self.inner
            .inbound_routes
            .acquire(InboundRouteKey {
                peer_node_id,
                environment_id,
                process_id,
            })
            .await
    }

    pub(crate) async fn ensure_node_queue(
        &self,
        node: NodeId,
    ) -> std::result::Result<NodeQueueSender, OutboundEnqueueError> {
        if let Some(queue_ref) = self.inner.nodes_queues.get(&node) {
            let sender = queue_ref.sender();
            drop(queue_ref);
            if !sender.is_closed() {
                return Ok(sender);
            }
            if let Some((_, stale)) = self
                .inner
                .nodes_queues
                .remove_if(&node, |_, current| current.sender.same_channel(&sender))
            {
                stale.abort();
            }
        }

        let mut is_member = self.inner.control_client.node_ids().contains(&node.0);
        let mut node_info = self.inner.control_client.node_info(node.0);
        if node_info.is_none() || !is_member {
            // Refresh once before declaring the route absent.
            self.inner.control_client.refresh_nodes().await.ok();
            is_member = self.inner.control_client.node_ids().contains(&node.0);
            node_info = self.inner.control_client.node_info(node.0);
        }
        let node_info = node_info.filter(|_| is_member).ok_or_else(|| {
            OutboundEnqueueError::new(
                SendErrorKind::NodeNotFound,
                format!("Node {} does not exist", node.0),
            )
        })?;

        let _admission = self.inner.node_queue_admission.lock().await;
        if let Some(queue_ref) = self.inner.nodes_queues.get(&node) {
            let sender = queue_ref.sender();
            let closed = sender.is_closed();
            drop(queue_ref);
            if !closed {
                return Ok(sender);
            }
            if let Some((_, stale)) = self
                .inner
                .nodes_queues
                .remove_if(&node, |_, current| current.sender.same_channel(&sender))
            {
                stale.abort();
            }
        }

        if self.inner.nodes_queues.len() >= self.inner.limits.registry.max_topology_nodes {
            return Err(OutboundEnqueueError::new(
                SendErrorKind::Backpressure,
                format!(
                    "Distributed node queue limit reached ({})",
                    self.inner.limits.registry.max_topology_nodes
                ),
            ));
        }

        let (send, streams) = node_queue_channels(10);
        let task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info,
            client: self.inner.node_client.clone(),
            message_streams: streams,
        }));
        self.inner.nodes_queues.insert(
            node,
            NodeQueue {
                sender: send.clone(),
                manager: task.abort_handle(),
            },
        );
        Ok(send)
    }

    pub(crate) async fn reconcile_topology(&self) {
        let mut active_nodes = self
            .inner
            .control_client
            .node_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        active_nodes.insert(self.node_id.0);
        self.reconcile_topology_members(active_nodes).await;
    }

    async fn reconcile_topology_members(&self, active_nodes: HashSet<u64>) {
        let removed_nodes = {
            let mut previous = self
                .inner
                .topology_nodes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let removed = previous
                .difference(&active_nodes)
                .copied()
                .collect::<Vec<_>>();
            *previous = active_nodes.clone();
            removed
        };

        let stale_queues = self
            .inner
            .nodes_queues
            .iter()
            .filter_map(|entry| (!active_nodes.contains(&entry.key().0)).then_some(*entry.key()))
            .collect::<Vec<_>>();
        for node in stale_queues {
            if let Some((_, queue)) = self.inner.nodes_queues.remove(&node) {
                queue.abort();
            }
        }

        if !removed_nodes.is_empty() {
            self.inner
                .coordinator
                .reconcile_topology(&active_nodes, &removed_nodes);
        }
    }

    async fn new_message(
        &self,
        params: NewMessageParams,
    ) -> std::result::Result<MessageId, OutboundEnqueueError> {
        let NewMessageParams {
            message_id,
            env,
            src,
            node,
            dest,
            class,
            data,
        } = params;
        if data.len() > quic::MAX_WIRE_MESSAGE_BYTES {
            return Err(OutboundEnqueueError::new(
                SendErrorKind::MessageTooLarge,
                format!(
                    "Distributed message size {} exceeds the transport limit {}",
                    data.len(),
                    quic::MAX_WIRE_MESSAGE_BYTES
                ),
            ));
        }
        let retained_bytes = data.capacity().max(1);
        let budget = match class {
            OutboundClass::Data => &self.inner.outbound_budget,
            OutboundClass::Control => &self.inner.control_outbound_budget,
        };
        let outbound_lease = budget.try_reserve(retained_bytes).map_err(|error| {
            OutboundEnqueueError::new(SendErrorKind::Backpressure, error.to_string())
        })?;
        let application_ack = match class {
            OutboundClass::Data => {
                Some(self.inner.application_acks.try_register(message_id, node)?)
            }
            OutboundClass::Control => None,
        };

        let node_queue = self.ensure_node_queue(node).await?;
        let route = ProcessQueueRoute::new(src, node, dest);
        let queue_key = (env, route);

        // Lazy-initialize exactly one process queue/receiver pair under the sender-map entry lock.
        // The worker can win the admission gate and retire a newly observed empty queue before
        // this producer reaches it. That is a benign internal race, so retry with a fresh
        // generation without surfacing QueueClosed to the guest.
        loop {
            let (queue_generation, tx, admission_gate) = match self.inner.buf_tx.entry(queue_key) {
                DashEntry::Occupied(entry) => (
                    entry.get().generation,
                    entry.get().sender.clone(),
                    entry.get().admission_gate.clone(),
                ),
                DashEntry::Vacant(entry) => {
                    let (send, recv) = tokio::sync::mpsc::channel(OUTBOUND_PROCESS_QUEUE_CAPACITY);
                    let generation = self
                        .inner
                        .next_process_queue_generation
                        .fetch_add(1, atomic::Ordering::Relaxed);
                    let admission_gate = Arc::new(AsyncMutex::new(()));
                    match self.inner.buf_rx.entry(env) {
                        DashEntry::Occupied(env_queue) => {
                            env_queue.get().insert(
                                route,
                                ProcessQueueReceiver {
                                    generation,
                                    receiver: RwLock::new(recv),
                                    admission_gate: admission_gate.clone(),
                                },
                            );
                        }
                        DashEntry::Vacant(env_queue) => {
                            let queue = DashMap::new();
                            queue.insert(
                                route,
                                ProcessQueueReceiver {
                                    generation,
                                    receiver: RwLock::new(recv),
                                    admission_gate: admission_gate.clone(),
                                },
                            );
                            env_queue.insert(queue);
                        }
                    }
                    entry.insert(ProcessQueueSender {
                        generation,
                        sender: send.clone(),
                        admission_gate: admission_gate.clone(),
                    });
                    (generation, send, admission_gate)
                }
            };

            let admission_guard = admission_gate.lock().await;
            let current_generation =
                process_queue_is_current(&self.inner.buf_tx, queue_key, queue_generation, &tx);
            if !current_generation {
                drop(admission_guard);
                continue;
            }

            let message = MessageCtx {
                message_id,
                env,
                src,
                node,
                dest,
                queue_generation,
                offset: AtomicUsize::new(0),
                chunk_id: AtomicU64::new(0),
                data: data.into(),
                retained_bytes,
                class,
                application_ack,
                outbound_lease,
            };
            let admitted = match node_queue.try_admit(message) {
                AdmissionResult::Admitted(admitted) => admitted,
                AdmissionResult::Full(_message) => {
                    return Err(OutboundEnqueueError::new(
                        SendErrorKind::Backpressure,
                        "Distributed outbound stream memory limit reached",
                    ));
                }
                AdmissionResult::Closed(_message) => {
                    return Err(OutboundEnqueueError::new(
                        SendErrorKind::QueueClosed,
                        "Distributed outbound node queue is closed",
                    ));
                }
            };
            match tx.try_send(admitted) {
                Ok(()) => {}
                Err(TrySendError::Full(_message)) => {
                    return Err(OutboundEnqueueError::new(
                        SendErrorKind::Backpressure,
                        format!(
                            "Distributed outbound process queue is full (capacity {})",
                            OUTBOUND_PROCESS_QUEUE_CAPACITY
                        ),
                    ));
                }
                Err(TrySendError::Closed(_message)) => {
                    drop(admission_guard);
                    self.remove_process_resources_if_generation(env, route, queue_generation);
                    return Err(OutboundEnqueueError::new(
                        SendErrorKind::QueueClosed,
                        "Distributed outbound process queue is closed",
                    ));
                }
            }
            self.inner.has_messages.notify_one();
            return Ok(message_id);
        }
    }

    pub fn remove_process_resources(&self, env: EnvironmentId, process_id: ProcessId) {
        let mut routes = self
            .inner
            .buf_tx
            .iter()
            .filter_map(|entry| {
                let (queue_env, route) = *entry.key();
                (queue_env == env && route.src == process_id).then_some(route)
            })
            .collect::<HashSet<_>>();
        routes.extend(self.inner.route_in_progress.iter().filter_map(|entry| {
            let (queue_env, route) = *entry.key();
            (queue_env == env && route.src == process_id).then_some(route)
        }));
        if let Some(env_queue) = self.inner.buf_rx.get(&env) {
            routes.extend(
                env_queue
                    .iter()
                    .filter_map(|entry| (entry.key().src == process_id).then_some(*entry.key())),
            );
        }
        for route in routes {
            self.inner.buf_tx.remove(&(env, route));
            self.inner.route_in_progress.remove(&(env, route));
            if let Some(env_queue) = self.inner.buf_rx.get(&env) {
                env_queue.remove(&route);
            }
        }
        self.inner
            .buf_rx
            .remove_if(&env, |_, queue| queue.is_empty());
    }

    pub(crate) fn remove_process_resources_if_generation(
        &self,
        env: EnvironmentId,
        route: ProcessQueueRoute,
        generation: u64,
    ) -> bool {
        if self
            .inner
            .buf_tx
            .remove_if(&(env, route), |_, queue| queue.generation == generation)
            .is_none()
        {
            return false;
        }
        self.cleanup_process_generation(env, route, generation);
        true
    }

    pub(crate) fn remove_idle_process_resources_if_generation(
        &self,
        env: EnvironmentId,
        route: ProcessQueueRoute,
        generation: u64,
    ) -> bool {
        if self
            .inner
            .buf_tx
            .remove_if(&(env, route), |_, queue| queue.generation == generation)
            .is_none()
        {
            return false;
        }
        self.cleanup_process_generation(env, route, generation);
        true
    }

    fn cleanup_process_generation(
        &self,
        env: EnvironmentId,
        route: ProcessQueueRoute,
        generation: u64,
    ) {
        self.inner
            .route_in_progress
            .remove_if(&(env, route), |_, message| {
                message.queue_generation == generation
            });
        if let Some(env_queue) = self.inner.buf_rx.get(&env) {
            env_queue.remove_if(&route, |_, receiver| receiver.generation == generation);
            let remove_environment = env_queue.is_empty();
            drop(env_queue);
            if remove_environment {
                self.inner
                    .buf_rx
                    .remove_if(&env, |_, queue| queue.is_empty());
            }
        }
    }

    // Send distributed message
    pub async fn send(&self, params: SendParams) -> std::result::Result<MessageId, SendError> {
        self.send_with_response_timeout(params, None).await
    }

    /// Send a distributed message and bound the time spent waiting for the
    /// destination's mailbox-admission acknowledgement.
    pub async fn send_with_timeout(
        &self,
        params: SendParams,
        response_timeout: Duration,
    ) -> std::result::Result<MessageId, SendError> {
        self.send_with_response_timeout(params, Some(response_timeout))
            .await
    }

    async fn send_with_response_timeout(
        &self,
        params: SendParams,
        response_timeout: Option<Duration>,
    ) -> std::result::Result<MessageId, SendError> {
        let SendParams {
            source_env,
            target_env,
            src,
            node,
            dest,
            tag,
            data,
        } = params;
        let message = Request::Message {
            node_id: self.node_id.0,
            environment_id: target_env.0,
            process_id: dest.0,
            tag,
            data,
        };
        let serialized = match message::serialize_message(&message) {
            Ok(serialized) => serialized,
            Err(error) => {
                let Request::Message { data, .. } = message else {
                    unreachable!()
                };
                return Err(SendError::new(
                    SendErrorKind::Serialization,
                    format!("Error serializing distributed message: {error}"),
                    data,
                ));
            }
        };
        let message_id = self.next_message_id();
        // Install the waiter before outbound admission. A fast receiver can
        // otherwise return the mailbox result before this node can correlate it.
        let _waiter = ResponseWaiterGuard::insert(&self.inner.responses, message_id, node);
        if let Err(error) = self
            .new_message(NewMessageParams {
                message_id,
                env: source_env,
                src,
                node,
                dest,
                class: OutboundClass::Data,
                data: serialized,
            })
            .await
        {
            let Request::Message { data, .. } = message else {
                unreachable!()
            };
            return Err(SendError::new(error.kind, error.message, data));
        }

        let response_result = if let Some(response_timeout) = response_timeout {
            match tokio::time::timeout(response_timeout, self.await_response(message_id)).await {
                Ok(result) => result.map_err(|error| {
                    (
                        SendErrorKind::UnexpectedResponse,
                        format!("Remote delivery response disappeared: {error}"),
                    )
                }),
                Err(_) => Err((
                    SendErrorKind::ResponseTimeout,
                    "Timed out waiting for remote mailbox admission".to_string(),
                )),
            }
        } else {
            self.await_response(message_id).await.map_err(|error| {
                (
                    SendErrorKind::UnexpectedResponse,
                    format!("Remote delivery response disappeared: {error}"),
                )
            })
        };
        let Request::Message { data, .. } = message else {
            unreachable!()
        };
        match response_result {
            Err((kind, error)) => Err(SendError::new(kind, error, data)),
            Ok(ResponseContent::Sent) => Ok(message_id),
            Ok(ResponseContent::Error(error)) => Err(remote_send_error(error, data)),
            Ok(other) => Err(SendError::new(
                SendErrorKind::UnexpectedResponse,
                format!(
                    "Remote delivery returned an unexpected {} response",
                    other.kind()
                ),
                data,
            )),
        }
    }

    // Send distributed spawn message
    pub async fn spawn(&self, params: SpawnParams) -> Result<MessageId> {
        let message = Request::Spawn(params.spawn);
        let data = message::serialize_message(&message).unwrap_or_else(|_| {
            unreachable!("lunatic::distributed::client::spawn serialize_message")
        });
        let message_id = self.next_message_id();
        self.inner
            .responses
            .insert(message_id, Arc::new(IncomingResponse::new(params.node)));
        if let Err(error) = self
            .new_message(NewMessageParams {
                message_id,
                env: params.env,
                src: params.src,
                node: params.node,
                dest: ProcessId(0),
                class: OutboundClass::Data,
                data,
            })
            .await
        {
            self.inner.responses.remove(&message_id);
            return Err(anyhow::Error::new(error));
        }
        Ok(message_id)
    }

    // Send distributed response message
    pub async fn send_response(&self, params: ResponseParams) -> Result<MessageId> {
        let message = Request::Response(params.response);
        let data = message::serialize_message(&message).unwrap_or_else(|_| {
            unreachable!("lunatic::distributed::client::send_response serialize_message")
        });
        let message_id = self.next_message_id();
        self.new_message(NewMessageParams {
            message_id,
            env: EnvironmentId(0),
            src: ProcessId(0),
            node: params.node_id,
            dest: ProcessId(0),
            class: OutboundClass::Control,
            data,
        })
        .await
        .map_err(anyhow::Error::new)
    }

    // Receive response
    pub(crate) async fn recv_response(
        &self,
        source_node_id: VerifiedNodeId,
        response: Response,
    ) -> Result<()> {
        let message_id = MessageId(response.message_id);
        let source_node = NodeId(source_node_id.get());
        let ack_expected = self.inner.application_acks.expected_node(message_id);
        let waiter_expected = self
            .inner
            .responses
            .get(&message_id)
            .map(|waiter| waiter.expected_node);
        let Some(expected_node) = ack_expected.or(waiter_expected) else {
            log::warn!(
                "Dropping distributed response for unknown message {}",
                message_id.0
            );
            return Ok(());
        };
        if expected_node != source_node {
            audit_verified_peer_protocol_denial(source_node_id);
            return Err(anyhow!(
                "Distributed response source did not match the expected authenticated peer"
            ));
        }
        self.inner
            .application_acks
            .acknowledge(message_id, source_node);
        if waiter_expected.is_none() {
            // The public caller may already have timed out or been cancelled. The independent
            // scheduler ACK still commits the retained message and releases every permit.
            return Ok(());
        }
        if let Err(error) = self
            .inner
            .response_tx
            .send((message_id, response.content))
            .await
        {
            let (message_id, _) = error.0;
            log::warn!(
                "Failed to enqueue distributed response for message {}: response worker closed",
                message_id.0
            );
        }
        Ok(())
    }

    pub async fn await_response(&self, message_id: MessageId) -> Result<ResponseContent> {
        let response_cell = self
            .inner
            .responses
            .get(&message_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| anyhow!("message does not exist"))?;
        // Never retain a DashMap shard guard while waiting for a remote response.
        let response = response_cell.response.take().await;
        self.inner.responses.remove(&message_id);
        Ok(response)
    }
}

async fn registry_sync_worker(client: Client) -> ! {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.tick().await;
    loop {
        interval.tick().await;
        client.reconcile_topology().await;
        if let Err(error) = client.synchronize_registry().await {
            log::trace!("Periodic registry synchronization failed: {error}");
        }
    }
}

fn take_due_registry_cleanups(client: &Client, limit: usize) -> Vec<super::GlobalProcessId> {
    let now = Instant::now();
    let mut pending = client
        .inner
        .registry_cleanup_pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let available = limit.saturating_sub(pending.values().filter(|retry| retry.in_flight).count());
    let mut due = Vec::with_capacity(available);
    for (global_pid, retry) in pending.iter_mut() {
        if due.len() >= available {
            break;
        }
        if !retry.in_flight && retry.next_attempt <= now {
            retry.in_flight = true;
            due.push(*global_pid);
        }
    }
    due
}

fn finish_registry_cleanup_attempt(
    client: &Client,
    global_pid: super::GlobalProcessId,
    result: Result<Vec<ProcessName>>,
) {
    match result {
        Ok(_) => {
            // NotFound and OwnerChanged are terminal successes for this old
            // owner too: the coordinator already proved that this process no
            // longer owns the name.
            client.registry().unregister_process(global_pid);
            client
                .inner
                .registry_cleanup_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&global_pid);
        }
        Err(error) => {
            let mut pending = client
                .inner
                .registry_cleanup_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(retry) = pending.get_mut(&global_pid) {
                retry.in_flight = false;
                retry.failures = retry.failures.saturating_add(1);
                retry.next_attempt = Instant::now() + registry_cleanup_backoff(retry.failures);
                log::warn!(
                    "Failed to coordinate registry cleanup for process {global_pid}; retry {} scheduled: {error}",
                    retry.failures
                );
            }
            // Keep the global entry and reverse mapping until a terminal
            // coordinator result. They are the bounded durable retry record.
        }
    }
}

async fn registry_cleanup_worker(client: Client, mut cleanup_rx: Receiver<super::GlobalProcessId>) {
    let mut retry_tick = tokio::time::interval(REGISTRY_CLEANUP_RETRY_TICK);
    retry_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut attempts = tokio::task::JoinSet::new();
    loop {
        for global_pid in take_due_registry_cleanups(&client, MAX_REGISTRY_CLEANUP_IN_FLIGHT) {
            let attempt_client = client.clone();
            attempts.spawn(async move {
                let result = attempt_client
                    .inner
                    .coordinator
                    .unregister_process_registrations(global_pid)
                    .await;
                (global_pid, result)
            });
        }

        tokio::select! {
            _ = retry_tick.tick() => {}
            wake = cleanup_rx.recv() => {
                if wake.is_none() && attempts.is_empty() {
                    return;
                }
            }
            Some(attempt) = attempts.join_next(), if !attempts.is_empty() => {
                match attempt {
                    Ok((global_pid, result)) => {
                        finish_registry_cleanup_attempt(&client, global_pid, result);
                    }
                    Err(error) => {
                        log::error!("Registry cleanup attempt task failed: {error}");
                        // Coordinator futures are not expected to panic. If one does,
                        // cancel the remaining batch before making every in-flight
                        // marker retryable. This prevents overlapping duplicate attempts.
                        attempts.abort_all();
                        while attempts.join_next().await.is_some() {}
                        let mut pending = client
                            .inner
                            .registry_cleanup_pending
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        for retry in pending.values_mut() {
                            retry.in_flight = false;
                            retry.next_attempt = Instant::now() + REGISTRY_CLEANUP_RETRY_TICK;
                        }
                    }
                }
            }
        }
    }
}

pub async fn process_responses(
    client: Client,
    mut recv: Receiver<(MessageId, ResponseContent)>,
) -> ! {
    const TIMEOUT: Duration = Duration::from_secs(5);
    loop {
        tokio::select! {
           r =  recv.recv() => {
            if let Some((message_id, response)) = r {
                deliver_response(&client.inner.responses, message_id, response);
            }
           },
           _ = tokio::time::sleep(TIMEOUT) => {
            expire_responses(&client.inner.responses, Instant::now(), TIMEOUT);
           }
        };
    }
}

fn deliver_response(
    responses: &DashMap<MessageId, Arc<IncomingResponse>>,
    message_id: MessageId,
    response: ResponseContent,
) -> bool {
    if let Some(cell) = responses.get(&message_id) {
        cell.response.set(response);
        true
    } else {
        log::warn!(
            "Dropping distributed response for unknown message {}",
            message_id.0
        );
        false
    }
}

fn expire_responses(
    responses: &DashMap<MessageId, Arc<IncomingResponse>>,
    now: Instant,
    timeout: Duration,
) {
    let mut completed = Vec::new();
    for entry in responses.iter() {
        if now.saturating_duration_since(entry.created_at) <= timeout {
            continue;
        }
        if entry.response.is_set() {
            completed.push(*entry.key());
        } else {
            entry.response.set(ResponseContent::Error(
                crate::distributed::message::ClientError::ResponseTimeout,
            ));
        }
    }
    // DashMap references hold shard read locks, so mutate only after the iterator is gone.
    for message_id in completed {
        responses.remove(&message_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunatic_control::NodeInfo;
    use tokio::sync::mpsc::error::TryRecvError;

    fn test_client_without_workers() -> Client {
        test_client_without_workers_with(DistributedLimits::default(), Vec::new())
    }

    fn test_client_without_workers_with(limits: DistributedLimits, nodes: Vec<NodeInfo>) -> Client {
        let root = crate::control::cert::test_root_cert().expect("test root certificate");
        let root_pem = root.certificate_pem().to_owned();
        let node_certificate = crate::distributed::server::gen_node_cert("client-unit-test")
            .expect("test node certificate");
        let certificate_pem = node_certificate
            .serialize_pem_with_signer(&root)
            .expect("signed test node certificate");
        let key_pem = node_certificate.serialize_private_key_pem();
        let node_client =
            quic::new_quic_client(&root_pem, &certificate_pem, &key_pem).expect("test QUIC client");
        let registration = crate::control::RegistrationMetadata {
            node_name: uuid::Uuid::from_u128(1),
            cert_pem_chain: vec![certificate_pem],
            root_cert: root_pem,
            envs: vec![],
            is_privileged: true,
        };
        let control_client = control::Client::from_static_nodes(registration, 1, nodes);
        let (response_tx, _response_rx) = tokio::sync::mpsc::channel(1);
        let (registry_cleanup_tx, _registry_cleanup_rx) = tokio::sync::mpsc::channel(1);
        let registry = Arc::new(DistributedRegistry::with_limits(1, limits.registry));
        let coordinator = Arc::new(RegistryCoordinator::new(registry.clone(), 1));
        let mut topology_nodes = control_client
            .node_ids()
            .into_iter()
            .collect::<HashSet<_>>();
        topology_nodes.insert(1);
        let client = Client {
            node_id: NodeId(1),
            inner: Arc::new(Inner {
                control_client,
                node_client,
                next_message_id: AtomicU64::new(1),
                next_process_queue_generation: AtomicU64::new(2),
                buf_rx: DashMap::new(),
                buf_tx: DashMap::new(),
                in_progress: DashMap::new(),
                route_in_progress: DashMap::new(),
                nodes_queues: DashMap::new(),
                responses: DashMap::new(),
                response_tx,
                registry_cleanup_tx,
                registry_cleanup_pending: Mutex::new(HashMap::new()),
                has_messages: Arc::new(Notify::new()),
                outbound_budget: Arc::new(OutboundBudget::new(
                    limits.outbound.max_messages,
                    limits.outbound.max_bytes,
                )),
                control_outbound_budget: Arc::new(OutboundBudget::new(
                    MAX_CONTROL_OUTBOUND_MESSAGES,
                    MAX_CONTROL_OUTBOUND_BYTES,
                )),
                application_acks: Arc::new(ApplicationAckRegistry::new(MAX_APPLICATION_ACKS)),
                replay_cache: Arc::new(ReplayCache::new(
                    MAX_INBOUND_REPLAY_ENTRIES,
                    INBOUND_REPLAY_TTL,
                )),
                inbound_routes: Arc::new(InboundRouteSequencer::new()),
                node_queue_admission: AsyncMutex::new(()),
                topology_nodes: Mutex::new(topology_nodes),
                limits,
                registry,
                coordinator,
            }),
        };
        client.inner.coordinator.attach_client(&client);
        client
    }

    fn install_send_route(
        client: &Client,
        source_env: EnvironmentId,
        source: ProcessId,
        node: NodeId,
        dest: ProcessId,
    ) -> (
        Arc<AsyncMutex<()>>,
        tokio::sync::mpsc::Receiver<AdmittedMessage>,
    ) {
        let (node_sender, node_receivers) = node_queue_channels(10);
        let manager = tokio::spawn(async move {
            let _node_receivers = node_receivers;
            std::future::pending::<()>().await
        });
        client.inner.nodes_queues.insert(
            node,
            NodeQueue {
                sender: node_sender,
                manager: manager.abort_handle(),
            },
        );
        let (process_sender, process_receiver) =
            tokio::sync::mpsc::channel(OUTBOUND_PROCESS_QUEUE_CAPACITY);
        let admission_gate = Arc::new(AsyncMutex::new(()));
        let route = ProcessQueueRoute::new(source, node, dest);
        client.inner.buf_tx.insert(
            (source_env, route),
            ProcessQueueSender {
                generation: 1,
                sender: process_sender,
                admission_gate: admission_gate.clone(),
            },
        );
        (admission_gate, process_receiver)
    }

    #[test]
    fn outbound_budget_enforces_message_and_byte_limits_and_releases_exactly() {
        let budget = Arc::new(OutboundBudget::new(2, 10));
        let first = budget.try_reserve(6).expect("first reservation must fit");
        let first_clone = first.clone();
        let second = budget.try_reserve(4).expect("exact byte limit must fit");
        assert_eq!(budget.current_usage(), (2, 10));

        let message_limit = budget.try_reserve(0);
        assert!(matches!(
            message_limit,
            Err(OutboundSaturationError {
                kind: OutboundLimitKind::Messages,
                ..
            })
        ));
        assert_eq!(budget.current_usage(), (2, 10));

        drop(first);
        assert_eq!(
            budget.current_usage(),
            (2, 10),
            "cloned staging leases must keep the whole message charged"
        );
        drop(first_clone);
        assert_eq!(budget.current_usage(), (1, 4));
        drop(second);
        assert_eq!(budget.current_usage(), (0, 0));

        let byte_budget = Arc::new(OutboundBudget::new(3, 10));
        let reservation = byte_budget.try_reserve(8).expect("reservation must fit");
        assert!(matches!(
            byte_budget.try_reserve(3),
            Err(OutboundSaturationError {
                kind: OutboundLimitKind::Bytes,
                ..
            })
        ));
        assert_eq!(byte_budget.current_usage(), (1, 8));
        drop(reservation);
        assert_eq!(byte_budget.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn registry_direct_send_uses_shared_outbound_budget_and_recovers() {
        let limits = DistributedLimits {
            outbound: OutboundLimits {
                max_messages: 1,
                max_bytes: 1_024,
            },
            registry: RegistryLimits::default(),
        };
        let client = test_client_without_workers_with(
            limits,
            vec![NodeInfo {
                id: 2,
                name: "node-2.invalid".into(),
                address: "127.0.0.1:9".parse().unwrap(),
            }],
        );
        let queued_data = client
            .inner
            .outbound_budget
            .try_reserve(1)
            .expect("data-plane reservation must fit");

        let error = client
            .send_registry_coordination(
                2,
                RegistryCoordinationMessage::RegistryHeartbeat {
                    node_id: 1,
                    timestamp: 0,
                },
            )
            .await
            .expect_err("registry traffic must observe the shared message ceiling");
        assert_eq!(
            error
                .downcast_ref::<OutboundSaturationError>()
                .expect("saturation type must survive anyhow")
                .kind(),
            OutboundLimitKind::Messages
        );
        assert_eq!(client.inner.outbound_budget.current_usage(), (1, 1));

        drop(queued_data);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
        let recovered = client
            .inner
            .outbound_budget
            .try_reserve(1_024)
            .expect("released direct-send capacity must be reusable");
        drop(recovered);
    }

    #[tokio::test]
    async fn cancelling_failed_registry_transport_releases_its_lease() {
        let client = test_client_without_workers_with(
            DistributedLimits {
                outbound: OutboundLimits {
                    max_messages: 1,
                    max_bytes: 1_024,
                },
                registry: RegistryLimits::default(),
            },
            vec![NodeInfo {
                id: 2,
                name: "node-2.invalid".into(),
                address: "127.0.0.1:9".parse().unwrap(),
            }],
        );

        let _ = tokio::time::timeout(
            Duration::from_millis(20),
            client.send_registry_coordination(
                2,
                RegistryCoordinationMessage::RegistryHeartbeat {
                    node_id: 1,
                    timestamp: 0,
                },
            ),
        )
        .await;
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
        let lease = client
            .inner
            .outbound_budget
            .try_reserve(1_024)
            .expect("transport cancellation must restore the full budget");
        drop(lease);
    }

    #[tokio::test]
    async fn topology_churn_aborts_managers_and_removes_node_owned_registry_state() {
        let client = test_client_without_workers();
        for node_id in 2..102_u64 {
            let name = format!("service-{node_id}");
            let owner = super::super::GlobalProcessId::new(node_id, 1, 1);
            client
                .registry()
                .apply_global(name.clone(), owner, node_id)
                .unwrap();
            *client
                .inner
                .topology_nodes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = HashSet::from([1, node_id]);

            let (lease, usage) = test_outbound_lease(8);
            let (sender, receivers) =
                congestion::NodeQueueSender::with_limits(1, 1, quic::MAX_WIRE_MESSAGE_BYTES);
            let manager = tokio::spawn(async move {
                let _lease = lease;
                let _receivers = receivers;
                std::future::pending::<()>().await;
            });
            client.inner.nodes_queues.insert(
                NodeId(node_id),
                NodeQueue {
                    sender,
                    manager: manager.abort_handle(),
                },
            );
            tokio::task::yield_now().await;
            assert_eq!(usage.current_usage(), (1, 8));

            client.reconcile_topology_members(HashSet::from([1])).await;
            tokio::time::timeout(Duration::from_secs(1), async {
                while usage.current_usage() != (0, 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("removed node manager must release retained chunks");
            assert!(!client.inner.nodes_queues.contains_key(&NodeId(node_id)));
            assert!(client.registry().lookup_global(name).is_none());
        }
        assert_eq!(client.inner.nodes_queues.len(), 0);
        assert_eq!(client.registry().global_count(), 0);
    }

    #[tokio::test]
    async fn process_exit_hook_is_idempotent_and_retains_global_names_until_quorum_cleanup() {
        let client = test_client_without_workers();
        let owner = super::super::GlobalProcessId::new(1, 7, 9);
        client.registry().register_local("local", owner).unwrap();
        client.registry().register_global("global", owner).unwrap();
        let registration = client.register_process_owner(owner);

        lunatic_process::env::ProcessExitHook::process_exited(registration.as_ref());

        assert!(client.registry().lookup_local("local").is_none());
        assert_eq!(
            client
                .registry()
                .lookup_global("global")
                .unwrap()
                .global_pid,
            owner
        );
        assert!(client
            .inner
            .registry_cleanup_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&owner));
        lunatic_process::env::ProcessExitHook::process_exited(registration.as_ref());

        finish_registry_cleanup_attempt(
            &client,
            owner,
            Err(anyhow!("simulated registry partition")),
        );
        assert!(client.registry().lookup_global("global").is_some());
        {
            let mut pending = client
                .inner
                .registry_cleanup_pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let retry = pending.get_mut(&owner).unwrap();
            assert_eq!(retry.failures, 1);
            assert!(!retry.in_flight);
            retry.next_attempt = Instant::now() - Duration::from_millis(1);
        }
        assert_eq!(take_due_registry_cleanups(&client, 1), vec![owner]);

        finish_registry_cleanup_attempt(&client, owner, Ok(vec!["global".into()]));
        assert_eq!(client.registry().usage().entries, 0);
        assert!(!client
            .inner
            .registry_cleanup_pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(&owner));
    }

    #[test]
    fn send_error_returns_the_original_data_allocation() {
        let mut data = Vec::with_capacity(128);
        data.extend_from_slice(b"recover me");
        let original_ptr = data.as_ptr();
        let original_capacity = data.capacity();

        let error = SendError::new(SendErrorKind::Backpressure, "queue full", data);
        assert_eq!(error.kind(), SendErrorKind::Backpressure);
        let recovered = error.into_data();

        assert_eq!(recovered, b"recover me");
        assert_eq!(recovered.as_ptr(), original_ptr);
        assert_eq!(recovered.capacity(), original_capacity);
    }

    #[tokio::test]
    async fn send_waiter_precedes_admission_and_remote_error_preserves_payload() {
        let client = test_client_without_workers();
        let source_env = EnvironmentId(5);
        let target_env = EnvironmentId(8);
        let source = ProcessId(6);
        let node = NodeId(2);
        let (admission_gate, process_receiver) =
            install_send_route(&client, source_env, source, node, ProcessId(9));
        let admission_guard = admission_gate.clone().lock_owned().await;

        let mut data = Vec::with_capacity(128);
        data.extend_from_slice(b"remote payload");
        let original_ptr = data.as_ptr();
        let original_capacity = data.capacity();
        let sending_client = client.clone();
        let send_task = tokio::spawn(async move {
            sending_client
                .send(SendParams {
                    source_env,
                    target_env,
                    src: source,
                    node,
                    dest: ProcessId(9),
                    tag: Some(10),
                    data,
                })
                .await
        });

        let message_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(entry) = client.inner.responses.iter().next() {
                    break *entry.key();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("send must install its waiter before admission");
        assert!(!send_task.is_finished());
        assert!(deliver_response(
            &client.inner.responses,
            message_id,
            ResponseContent::Error(message::ClientError::EnvironmentNotFound),
        ));

        drop(admission_guard);
        let error = send_task
            .await
            .expect("send task must finish")
            .expect_err("missing remote environment must fail delivery");
        assert_eq!(error.kind(), SendErrorKind::EnvironmentNotFound);
        let recovered = error.into_data();
        assert_eq!(recovered, b"remote payload");
        assert_eq!(recovered.as_ptr(), original_ptr);
        assert_eq!(recovered.capacity(), original_capacity);
        assert!(!client.inner.responses.contains_key(&message_id));

        drop(process_receiver);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn send_uses_target_environment_and_waits_for_sent_ack() {
        let client = test_client_without_workers();
        let source_env = EnvironmentId(5);
        let target_env = EnvironmentId(8);
        let source = ProcessId(6);
        let node = NodeId(2);
        let (_admission_gate, mut process_receiver) =
            install_send_route(&client, source_env, source, node, ProcessId(9));
        let sending_client = client.clone();
        let send_task = tokio::spawn(async move {
            sending_client
                .send(SendParams {
                    source_env,
                    target_env,
                    src: source,
                    node,
                    dest: ProcessId(9),
                    tag: Some(10),
                    data: b"mailbox data".to_vec(),
                })
                .await
        });

        let queued = tokio::time::timeout(Duration::from_secs(1), process_receiver.recv())
            .await
            .expect("message admission must complete")
            .expect("process queue must remain open");
        let request: Request = message::deserialize_message(&queued.data).unwrap();
        assert!(matches!(
            request,
            Request::Message {
                environment_id: 8,
                process_id: 9,
                ..
            }
        ));
        assert!(!send_task.is_finished());
        assert!(deliver_response(
            &client.inner.responses,
            queued.message_id,
            ResponseContent::Sent,
        ));
        let delivered = send_task
            .await
            .expect("send task must finish")
            .expect("Sent acknowledgement must complete delivery");
        assert_eq!(delivered, queued.message_id);
        assert!(!client.inner.responses.contains_key(&delivered));

        drop(queued);
        drop(process_receiver);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn send_timeout_and_unexpected_response_are_typed_and_remove_waiters() {
        for (response, expected_kind) in [
            (
                ResponseContent::Error(message::ClientError::ResponseTimeout),
                SendErrorKind::ResponseTimeout,
            ),
            (
                ResponseContent::Spawned(99),
                SendErrorKind::UnexpectedResponse,
            ),
        ] {
            let client = test_client_without_workers();
            let source_env = EnvironmentId(5);
            let source = ProcessId(6);
            let node = NodeId(2);
            let (_admission_gate, mut process_receiver) =
                install_send_route(&client, source_env, source, node, ProcessId(9));
            let sending_client = client.clone();
            let send_task = tokio::spawn(async move {
                sending_client
                    .send(SendParams {
                        source_env,
                        target_env: EnvironmentId(8),
                        src: source,
                        node,
                        dest: ProcessId(9),
                        tag: None,
                        data: vec![1, 2, 3],
                    })
                    .await
            });
            let queued = process_receiver.recv().await.unwrap();
            let message_id = queued.message_id;
            assert!(deliver_response(
                &client.inner.responses,
                message_id,
                response,
            ));
            let error = send_task.await.unwrap().expect_err("delivery must fail");
            assert_eq!(error.kind(), expected_kind);
            assert_eq!(error.into_data(), vec![1, 2, 3]);
            assert!(!client.inner.responses.contains_key(&message_id));
            drop(queued);
            drop(process_receiver);
            assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
        }
    }

    #[tokio::test]
    async fn explicit_send_timeout_returns_payload_and_removes_waiter() {
        let client = test_client_without_workers();
        let source_env = EnvironmentId(5);
        let source = ProcessId(6);
        let node = NodeId(2);
        let (_admission_gate, mut process_receiver) =
            install_send_route(&client, source_env, source, node, ProcessId(9));
        let error = client
            .send_with_timeout(
                SendParams {
                    source_env,
                    target_env: EnvironmentId(8),
                    src: source,
                    node,
                    dest: ProcessId(9),
                    tag: None,
                    data: vec![1, 2, 3],
                },
                Duration::from_millis(10),
            )
            .await
            .expect_err("missing acknowledgement must time out");
        assert_eq!(error.kind(), SendErrorKind::ResponseTimeout);
        assert_eq!(error.into_data(), vec![1, 2, 3]);
        assert!(client.inner.responses.is_empty());

        let queued = process_receiver
            .try_recv()
            .expect("timed-out delivery was admitted locally");
        drop(queued);
        drop(process_receiver);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn cancelling_send_removes_waiter_and_releases_outbound_budget() {
        let client = test_client_without_workers();
        let source_env = EnvironmentId(5);
        let source = ProcessId(6);
        let node = NodeId(2);
        let (admission_gate, process_receiver) =
            install_send_route(&client, source_env, source, node, ProcessId(9));
        let admission_guard = admission_gate.clone().lock_owned().await;
        let sending_client = client.clone();
        let send_task = tokio::spawn(async move {
            sending_client
                .send(SendParams {
                    source_env,
                    target_env: EnvironmentId(8),
                    src: source,
                    node,
                    dest: ProcessId(9),
                    tag: None,
                    data: vec![1, 2, 3],
                })
                .await
        });
        let message_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(entry) = client.inner.responses.iter().next() {
                    break *entry.key();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("send must install its waiter");

        send_task.abort();
        let _ = send_task.await;
        assert!(!client.inner.responses.contains_key(&message_id));
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
        drop(admission_guard);
        drop(process_receiver);
    }

    #[tokio::test]
    async fn admission_gate_prevents_enqueue_after_idle_cleanup() {
        let env = EnvironmentId(7);
        let process = ProcessId(11);
        let route = ProcessQueueRoute::new(process, NodeId(2), ProcessId(13));
        let generation = 3;
        let queues = Arc::new(DashMap::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let admission_gate = Arc::new(AsyncMutex::new(()));
        queues.insert(
            (env, route),
            ProcessQueueSender {
                generation,
                sender: sender.clone(),
                admission_gate: admission_gate.clone(),
            },
        );

        // Model the worker's empty observation while it exclusively owns cleanup admission.
        let cleanup_guard = admission_gate.clone().lock_owned().await;
        assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));

        let producer_queues = queues.clone();
        let producer_gate = admission_gate.clone();
        let producer_sender = sender.clone();
        let (node_sender, _node_receivers) = congestion::NodeQueueSender::with_limits(1, 1, 3);
        let (lease, usage) = test_outbound_lease(3);
        let producer = tokio::spawn(async move {
            let _admission_guard = producer_gate.lock().await;
            if !process_queue_is_current(
                &producer_queues,
                (env, route),
                generation,
                &producer_sender,
            ) {
                return false;
            }
            let message = MessageCtx {
                message_id: MessageId(1),
                env,
                src: process,
                node: NodeId(2),
                dest: ProcessId(13),
                queue_generation: generation,
                chunk_id: AtomicU64::new(0),
                offset: AtomicUsize::new(0),
                data: Bytes::from_static(b"abc"),
                retained_bytes: 3,
                class: OutboundClass::Data,
                application_ack: None,
                outbound_lease: lease,
            };
            let AdmissionResult::Admitted(admitted) = node_sender.try_admit(message) else {
                return false;
            };
            producer_sender.try_send(admitted).is_ok()
        });

        tokio::task::yield_now().await;
        assert!(queues
            .remove_if(&(env, route), |_, queue| queue.generation == generation)
            .is_some());
        drop(cleanup_guard);

        assert!(!producer.await.expect("producer task must finish"));
        drop(sender);
        assert!(matches!(
            receiver.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        assert_eq!(usage.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn new_message_retries_a_generation_retired_during_admission() {
        let client = test_client_without_workers();
        let env = EnvironmentId(7);
        let source = ProcessId(11);
        let node = NodeId(2);
        let route = ProcessQueueRoute::new(source, node, ProcessId(13));
        let (admission_gate, old_receiver) =
            install_send_route(&client, env, source, node, route.dest);
        let cleanup_guard = admission_gate.clone().lock_owned().await;
        let sending_client = client.clone();
        let send_task = tokio::spawn(async move {
            sending_client
                .new_message(NewMessageParams {
                    message_id: MessageId(9),
                    env,
                    src: source,
                    node,
                    dest: ProcessId(13),
                    class: OutboundClass::Data,
                    data: vec![1, 2, 3],
                })
                .await
        });

        tokio::task::yield_now().await;
        assert!(client.inner.buf_tx.remove(&(env, route)).is_some());
        drop(cleanup_guard);
        let admitted = tokio::time::timeout(Duration::from_secs(1), send_task)
            .await
            .expect("benign queue retirement must be retried")
            .unwrap()
            .unwrap();
        assert_eq!(admitted, MessageId(9));

        let env_queue = client.inner.buf_rx.get(&env).unwrap();
        let receiver = env_queue.get(&route).unwrap();
        assert_ne!(receiver.generation, 1);
        let queued = receiver.receiver.write().await.try_recv().unwrap();
        drop(receiver);
        drop(env_queue);
        drop(queued);
        drop(old_receiver);
        client.remove_process_resources(env, source);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn spawn_waiter_exists_before_queue_admission_can_complete() {
        let client = test_client_without_workers();
        let env = EnvironmentId(5);
        let source = ProcessId(6);
        let node = NodeId(2);
        let route = ProcessQueueRoute::new(source, node, ProcessId(0));
        let generation = 1;

        let (node_sender, node_receivers) = node_queue_channels(10);
        let manager = tokio::spawn(async move {
            let _node_receivers = node_receivers;
            std::future::pending::<()>().await
        });
        client.inner.nodes_queues.insert(
            node,
            NodeQueue {
                sender: node_sender,
                manager: manager.abort_handle(),
            },
        );
        let (process_sender, process_receiver) =
            tokio::sync::mpsc::channel(OUTBOUND_PROCESS_QUEUE_CAPACITY);
        let admission_gate = Arc::new(AsyncMutex::new(()));
        client.inner.buf_tx.insert(
            (env, route),
            ProcessQueueSender {
                generation,
                sender: process_sender,
                admission_gate: admission_gate.clone(),
            },
        );

        let admission_guard = admission_gate.clone().lock_owned().await;
        let spawning_client = client.clone();
        let spawn_task = tokio::spawn(async move {
            spawning_client
                .spawn(SpawnParams {
                    env,
                    src: source,
                    node,
                    spawn: Spawn {
                        response_node_id: 1,
                        environment_id: env.0,
                        module_id: 9,
                        function: "run".to_string(),
                        params: Vec::new(),
                        config: Vec::new(),
                    },
                })
                .await
        });

        let message_id = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Some(entry) = client.inner.responses.iter().next() {
                    break *entry.key();
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("spawn must install its waiter before admission");
        assert!(!spawn_task.is_finished());
        assert!(deliver_response(
            &client.inner.responses,
            message_id,
            ResponseContent::Spawned(42),
        ));

        drop(admission_guard);
        let admitted_id = spawn_task
            .await
            .expect("spawn task must finish")
            .expect("spawn must be admitted");
        assert_eq!(admitted_id, message_id);
        match client
            .await_response(message_id)
            .await
            .expect("early response must remain available")
        {
            ResponseContent::Spawned(process_id) => assert_eq!(process_id, 42),
            other => panic!("unexpected spawn response: {other:?}"),
        }

        drop(process_receiver);
        client.remove_process_resources(env, source);
        assert_eq!(client.inner.outbound_budget.current_usage(), (0, 0));
    }

    #[test]
    fn response_delivery_reports_missing_waiter_and_expiry_is_two_phase() {
        let responses = DashMap::new();
        assert!(!deliver_response(
            &responses,
            MessageId(404),
            ResponseContent::Sent
        ));

        let now = Instant::now();
        let stale_completed = MessageId(1);
        let stale_pending = MessageId(2);
        let completed = Arc::new(IncomingResponse {
            expected_node: NodeId(2),
            response: AsyncCell::new_with(ResponseContent::Sent),
            created_at: now - Duration::from_secs(10),
        });
        let pending = Arc::new(IncomingResponse {
            expected_node: NodeId(2),
            response: AsyncCell::new(),
            created_at: now - Duration::from_secs(10),
        });
        responses.insert(stale_completed, completed);
        responses.insert(stale_pending, pending.clone());

        expire_responses(&responses, now, Duration::from_secs(5));

        assert!(!responses.contains_key(&stale_completed));
        assert!(responses.contains_key(&stale_pending));
        assert!(
            pending.response.is_set(),
            "pending waiter must receive a timeout"
        );

        expire_responses(&responses, now, Duration::from_secs(5));
        assert!(!responses.contains_key(&stale_pending));
    }

    #[tokio::test]
    async fn response_from_wrong_authenticated_node_keeps_expected_waiter() {
        let client = test_client_without_workers();
        let message_id = MessageId(77);
        let waiter = Arc::new(IncomingResponse::new(NodeId(2)));
        client.inner.responses.insert(message_id, waiter.clone());

        let result = client
            .recv_response(
                VerifiedNodeId::for_test(3),
                Response {
                    message_id: message_id.0,
                    content: ResponseContent::Spawned(999),
                },
            )
            .await;

        assert!(result.is_err());
        assert!(client.inner.responses.contains_key(&message_id));
        assert!(!waiter.response.is_set());
        assert!(deliver_response(
            &client.inner.responses,
            message_id,
            ResponseContent::Spawned(42),
        ));
        assert!(waiter.response.is_set());
    }

    #[tokio::test]
    async fn application_ack_is_independent_from_the_public_response_waiter() {
        let registry = Arc::new(ApplicationAckRegistry::new(1));
        let message_id = MessageId(91);
        let mut handle = registry
            .try_register(message_id, NodeId(2))
            .expect("one acknowledgement fits");

        assert!(!registry.acknowledge(message_id, NodeId(3)));
        assert!(!handle.is_acknowledged());
        assert!(registry.acknowledge(message_id, NodeId(2)));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), handle.wait())
                .await
                .expect("ack wait must not lose its wakeup")
        );
        drop(handle);
        assert!(registry
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty());
    }

    #[tokio::test]
    async fn replay_cache_deduplicates_waiters_and_expires_completed_entries() {
        let cache = Arc::new(ReplayCache::new(2, Duration::from_secs(30)));
        let key = ReplayKey {
            peer_node_id: 2,
            message_id: 7,
        };
        let guard = match cache.begin(key, [1; 32]) {
            ReplayDecision::Execute(guard) => guard,
            _ => panic!("first request must execute"),
        };
        let waiter = match cache.begin(key, [1; 32]) {
            ReplayDecision::Wait(waiter) => waiter,
            _ => panic!("concurrent duplicate must wait"),
        };
        assert!(matches!(
            cache.begin(key, [2; 32]),
            ReplayDecision::Conflict
        ));

        assert_eq!(guard.finish(ResponseContent::Sent), ResponseContent::Sent);
        assert_eq!(waiter.wait().await, ResponseContent::Sent);
        assert!(matches!(
            cache.begin(key, [1; 32]),
            ReplayDecision::Cached(ResponseContent::Sent)
        ));
        assert_eq!(
            cache
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .completed_order
                .len(),
            1
        );

        let expiring = Arc::new(ReplayCache::new(2, Duration::ZERO));
        let guard = match expiring.begin(key, [1; 32]) {
            ReplayDecision::Execute(guard) => guard,
            _ => panic!("fresh expiring entry must execute"),
        };
        guard.finish(ResponseContent::Sent);
        let new_guard = match expiring.begin(key, [2; 32]) {
            ReplayDecision::Execute(guard) => guard,
            _ => panic!("expired entry must admit a new generation"),
        };
        assert!(matches!(
            expiring.begin(key, [2; 32]),
            ReplayDecision::Wait(_)
        ));
        drop(new_guard);
    }

    #[test]
    fn replay_tombstone_outlives_the_complete_replay_delivery_window() {
        assert!(
            INBOUND_REPLAY_TTL
                >= quic::REQUEST_MESSAGE_REASSEMBLY_TIMEOUT
                    + quic::REQUEST_MESSAGE_REASSEMBLY_TIMEOUT
        );
        assert!(
            INBOUND_REPLAY_TTL
                > APPLICATION_ACK_REPLAY_DEADLINE
                    + quic::REQUEST_MESSAGE_REASSEMBLY_TIMEOUT
                    + quic::REQUEST_STREAM_IDLE_TIMEOUT * 3
        );
    }

    #[tokio::test]
    async fn production_route_scheduler_bypasses_a_saturated_sibling_lane() {
        let client = test_client_without_workers();
        let env = EnvironmentId(5);
        let src = ProcessId(1);
        let node = NodeId(2);
        let (node_sender, mut receivers) = congestion::NodeQueueSender::with_limits(3, 1, 16);
        let _control_receiver = receivers.remove(0);
        let mut slow_receiver = receivers.remove(0);
        let mut fast_receiver = receivers.remove(0);
        let manager = tokio::spawn(std::future::pending::<()>());
        client.inner.nodes_queues.insert(
            node,
            NodeQueue {
                sender: node_sender,
                manager: manager.abort_handle(),
            },
        );
        let worker = tokio::spawn(congestion::congestion_control_worker(client.clone()));

        client
            .new_message(NewMessageParams {
                message_id: MessageId(100),
                env,
                src,
                node,
                dest: ProcessId(1),
                class: OutboundClass::Data,
                data: vec![0; 4],
            })
            .await
            .expect("slow route is initially admitted");
        let slow = tokio::time::timeout(Duration::from_millis(100), slow_receiver.recv())
            .await
            .expect("slow route reaches its production lane")
            .expect("slow lane remains open");

        let saturated = client
            .new_message(NewMessageParams {
                message_id: MessageId(101),
                env,
                src,
                node,
                dest: ProcessId(1),
                class: OutboundClass::Data,
                data: vec![1; 4],
            })
            .await
            .expect_err("the occupied lane must reject cap + 1");
        assert_eq!(saturated.kind(), SendErrorKind::Backpressure);

        client
            .new_message(NewMessageParams {
                message_id: MessageId(102),
                env,
                src,
                node,
                dest: ProcessId(2),
                class: OutboundClass::Data,
                data: vec![2; 4],
            })
            .await
            .expect("sibling route retains independent admission");
        let fast = tokio::time::timeout(Duration::from_millis(100), fast_receiver.recv())
            .await
            .expect("healthy production route must bypass the saturated route")
            .expect("healthy lane remains open");
        assert_eq!(fast.message_id, MessageId(102));

        drop(fast);
        drop(slow);
        worker.abort();
        let _ = worker.await;
    }
}
