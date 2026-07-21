use std::{
    fmt,
    sync::{
        atomic::{self, AtomicU64, AtomicUsize},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use async_cell::sync::AsyncCell;
use bytes::Bytes;

use crate::distributed::message;
use dashmap::mapref::entry::Entry as DashEntry;
use dashmap::DashMap;
use tokio::sync::{
    mpsc::{error::TrySendError, Receiver, Sender},
    Mutex as AsyncMutex, Notify, RwLock,
};

use crate::{
    congestion::{self, node_connection_manager, MessageChunk, NodeConnectionManager},
    control,
    distributed::message::{Request, ResponseContent, Spawn},
    distributed::registry::DistributedRegistry,
    distributed::registry_coordination::{RegistryCoordinationMessage, RegistryCoordinator},
    quic,
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
    pub env: EnvironmentId,
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
    pub(crate) outbound_lease: OutboundMessageLease,
}

pub(crate) struct ProcessQueueSender {
    pub(crate) generation: u64,
    pub(crate) sender: Sender<MessageCtx>,
    pub(crate) admission_gate: Arc<AsyncMutex<()>>,
}

pub(crate) struct ProcessQueueReceiver {
    pub(crate) generation: u64,
    pub(crate) receiver: RwLock<Receiver<MessageCtx>>,
    pub(crate) admission_gate: Arc<AsyncMutex<()>>,
}

pub const MAX_OUTBOUND_IN_FLIGHT_MESSAGES: usize = 1_024;
pub const MAX_OUTBOUND_IN_FLIGHT_BYTES: usize = 32 * 1024 * 1024;
pub const OUTBOUND_PROCESS_QUEUE_CAPACITY: usize = 64;
pub const OUTBOUND_NODE_QUEUE_CAPACITY: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendErrorKind {
    NodeNotFound,
    Backpressure,
    MessageTooLarge,
    QueueClosed,
    Serialization,
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
    max_messages: usize,
    max_bytes: usize,
    usage: Mutex<OutboundBudgetUsage>,
}

impl OutboundBudget {
    fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            max_messages,
            max_bytes,
            usage: Mutex::new(OutboundBudgetUsage::default()),
        }
    }

    fn try_reserve(
        self: &Arc<Self>,
        bytes: usize,
    ) -> Result<OutboundMessageLease, OutboundEnqueueError> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_bytes = usage.bytes.checked_add(bytes).ok_or_else(|| {
            OutboundEnqueueError::new(
                SendErrorKind::Backpressure,
                "Distributed outbound byte accounting overflow",
            )
        })?;
        if usage.messages >= self.max_messages || next_bytes > self.max_bytes {
            return Err(OutboundEnqueueError::new(
                SendErrorKind::Backpressure,
                format!(
                    "Distributed outbound queue limit reached ({} messages / {} bytes)",
                    self.max_messages, self.max_bytes
                ),
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
        Self::new(
            MAX_OUTBOUND_IN_FLIGHT_MESSAGES,
            MAX_OUTBOUND_IN_FLIGHT_BYTES,
        )
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

type IncomingResponse = (AsyncCell<ResponseContent>, Instant);

#[derive(Clone)]
pub struct Client {
    pub node_id: NodeId,
    pub inner: Arc<Inner>,
}

pub struct Inner {
    control_client: control::Client,
    node_client: quic::Client,
    pub next_message_id: AtomicU64,
    next_process_queue_generation: AtomicU64,
    // Across Environments and ProcessId's track message queues
    pub(crate) buf_rx: DashMap<EnvironmentId, DashMap<ProcessId, ProcessQueueReceiver>>,
    // Sending part of the message queue
    pub(crate) buf_tx: DashMap<(EnvironmentId, ProcessId), ProcessQueueSender>,
    // Holds the message while its being chunked
    pub in_progress: DashMap<(EnvironmentId, ProcessId), MessageCtx>,
    pub nodes_queues: DashMap<NodeId, Sender<MessageChunk>>,
    pub responses: DashMap<MessageId, Arc<IncomingResponse>>,
    pub response_tx: Sender<(MessageId, ResponseContent)>,
    pub has_messages: Arc<Notify>,
    outbound_budget: Arc<OutboundBudget>,
    // Distributed process registry
    pub registry: Arc<DistributedRegistry>,
    // Registry coordinator for cross-node coordination
    pub coordinator: Arc<RegistryCoordinator>,
}

fn process_queue_is_current(
    queues: &DashMap<(EnvironmentId, ProcessId), ProcessQueueSender>,
    env: EnvironmentId,
    src: ProcessId,
    generation: u64,
    sender: &Sender<MessageCtx>,
) -> bool {
    queues
        .get(&(env, src))
        .map(|queue| queue.generation == generation && queue.sender.same_channel(sender))
        .unwrap_or(false)
}

impl Client {
    pub fn new(node_id: u64, control_client: control::Client, node_client: quic::Client) -> Self {
        let (send, recv) = tokio::sync::mpsc::channel(1000);
        let registry = Arc::new(DistributedRegistry::new(node_id));
        let coordinator = Arc::new(RegistryCoordinator::new(registry.clone(), node_id));

        let client = Self {
            node_id: NodeId(node_id),
            inner: Arc::new(Inner {
                control_client,
                node_client,
                next_message_id: AtomicU64::new(1),
                next_process_queue_generation: AtomicU64::new(1),
                buf_rx: DashMap::new(),
                buf_tx: DashMap::new(),
                in_progress: DashMap::new(),
                nodes_queues: DashMap::new(),
                responses: DashMap::new(),
                response_tx: send,
                has_messages: Arc::new(Notify::new()),
                outbound_budget: Arc::new(OutboundBudget::default()),
                registry,
                coordinator,
            }),
        };
        client.inner.coordinator.attach_client(&client);
        tokio::spawn(congestion::congestion_control_worker(client.clone()));
        tokio::spawn(process_responses(client.clone(), recv));
        tokio::spawn(registry_sync_worker(client.clone()));
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

    pub(crate) fn registry_node_ids(&self) -> Vec<u64> {
        self.inner.control_client.node_ids()
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
        let message_id = self.next_message_id().0;
        self.inner
            .node_client
            .send_message(node.address, &node.name, message_id, data.into())
            .await
    }

    pub async fn handle_registry_message(
        &self,
        source_node_id: u64,
        message: RegistryCoordinationMessage,
    ) -> Result<()> {
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

    pub(crate) async fn ensure_node_queue(
        &self,
        node: NodeId,
    ) -> std::result::Result<Sender<MessageChunk>, OutboundEnqueueError> {
        if let Some(sender_ref) = self.inner.nodes_queues.get(&node) {
            let sender = sender_ref.clone();
            drop(sender_ref);
            if !sender.is_closed() {
                return Ok(sender);
            }
            self.inner
                .nodes_queues
                .remove_if(&node, |_, current| current.same_channel(&sender));
        }

        let mut node_info = self.inner.control_client.node_info(node.0);
        if node_info.is_none() {
            // Refresh once before declaring the route absent.
            self.inner.control_client.refresh_nodes().await.ok();
            node_info = self.inner.control_client.node_info(node.0);
        }
        let node_info = node_info.ok_or_else(|| {
            OutboundEnqueueError::new(
                SendErrorKind::NodeNotFound,
                format!("Node {} does not exist", node.0),
            )
        })?;

        match self.inner.nodes_queues.entry(node) {
            DashEntry::Occupied(mut entry) if entry.get().is_closed() => {
                let (send, recv) = tokio::sync::mpsc::channel(OUTBOUND_NODE_QUEUE_CAPACITY);
                entry.insert(send.clone());
                tokio::spawn(node_connection_manager(NodeConnectionManager {
                    streams: 10,
                    node_info,
                    client: self.inner.node_client.clone(),
                    message_chunks: recv,
                }));
                Ok(send)
            }
            DashEntry::Occupied(entry) => Ok(entry.get().clone()),
            DashEntry::Vacant(entry) => {
                let (send, recv) = tokio::sync::mpsc::channel(OUTBOUND_NODE_QUEUE_CAPACITY);
                entry.insert(send.clone());
                tokio::spawn(node_connection_manager(NodeConnectionManager {
                    streams: 10,
                    node_info,
                    client: self.inner.node_client.clone(),
                    message_chunks: recv,
                }));
                Ok(send)
            }
        }
    }

    async fn new_message(
        &self,
        message_id: MessageId,
        env: EnvironmentId,
        src: ProcessId,
        node: NodeId,
        dest: ProcessId,
        data: Vec<u8>,
    ) -> std::result::Result<MessageId, OutboundEnqueueError> {
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
        let outbound_lease = self.inner.outbound_budget.try_reserve(data.capacity())?;

        self.ensure_node_queue(node).await?;

        // Lazy-initialize exactly one process queue/receiver pair under the sender-map entry lock.
        let (queue_generation, tx, admission_gate) = match self.inner.buf_tx.entry((env, src)) {
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
                            src,
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
                            src,
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
            process_queue_is_current(&self.inner.buf_tx, env, src, queue_generation, &tx);
        if !current_generation {
            return Err(OutboundEnqueueError::new(
                SendErrorKind::QueueClosed,
                "Distributed outbound process queue was replaced during admission",
            ));
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
            outbound_lease,
        };
        match tx.try_send(message) {
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
                self.remove_process_resources_if_generation(env, src, queue_generation);
                return Err(OutboundEnqueueError::new(
                    SendErrorKind::QueueClosed,
                    "Distributed outbound process queue is closed",
                ));
            }
        }
        self.inner.has_messages.notify_one();
        Ok(message_id)
    }

    pub fn remove_process_resources(&self, env: EnvironmentId, process_id: ProcessId) {
        self.inner.buf_tx.remove(&(env, process_id));
        self.inner.in_progress.remove(&(env, process_id));
        if let Some(env_queue) = self.inner.buf_rx.get(&env) {
            env_queue.remove(&process_id);
            let remove_environment = env_queue.is_empty();
            drop(env_queue);
            if remove_environment {
                self.inner
                    .buf_rx
                    .remove_if(&env, |_, queue| queue.is_empty());
            }
        }
    }

    pub(crate) fn remove_process_resources_if_generation(
        &self,
        env: EnvironmentId,
        process_id: ProcessId,
        generation: u64,
    ) -> bool {
        if self
            .inner
            .buf_tx
            .remove_if(&(env, process_id), |_, queue| {
                queue.generation == generation
            })
            .is_none()
        {
            return false;
        }
        self.cleanup_process_generation(env, process_id, generation);
        true
    }

    pub(crate) fn remove_idle_process_resources_if_generation(
        &self,
        env: EnvironmentId,
        process_id: ProcessId,
        generation: u64,
    ) -> bool {
        if self
            .inner
            .buf_tx
            .remove_if(&(env, process_id), |_, queue| {
                queue.generation == generation
            })
            .is_none()
        {
            return false;
        }
        self.cleanup_process_generation(env, process_id, generation);
        true
    }

    fn cleanup_process_generation(
        &self,
        env: EnvironmentId,
        process_id: ProcessId,
        generation: u64,
    ) {
        self.inner
            .in_progress
            .remove_if(&(env, process_id), |_, message| {
                message.queue_generation == generation
            });
        if let Some(env_queue) = self.inner.buf_rx.get(&env) {
            env_queue.remove_if(&process_id, |_, receiver| receiver.generation == generation);
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
        let SendParams {
            env,
            src,
            node,
            dest,
            tag,
            data,
        } = params;
        let message = Request::Message {
            node_id: self.node_id.0,
            environment_id: env.0,
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
        match self
            .new_message(message_id, env, src, node, dest, serialized)
            .await
        {
            Ok(message_id) => Ok(message_id),
            Err(error) => {
                let Request::Message { data, .. } = message else {
                    unreachable!()
                };
                Err(SendError::new(error.kind, error.message, data))
            }
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
            .insert(message_id, Arc::new((AsyncCell::new(), Instant::now())));
        if let Err(error) = self
            .new_message(
                message_id,
                params.env,
                params.src,
                params.node,
                ProcessId(0),
                data,
            )
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
        self.new_message(
            message_id,
            EnvironmentId(0),
            ProcessId(0),
            params.node_id,
            ProcessId(0),
            data,
        )
        .await
        .map_err(anyhow::Error::new)
    }

    // Receive response
    pub async fn recv_response(&self, response: Response) {
        if let Err(error) = self
            .inner
            .response_tx
            .send((MessageId(response.message_id), response.content))
            .await
        {
            let (message_id, _) = error.0;
            log::warn!(
                "Failed to enqueue distributed response for message {}: response worker closed",
                message_id.0
            );
        }
    }

    pub async fn await_response(&self, message_id: MessageId) -> Result<ResponseContent> {
        let response_cell = self
            .inner
            .responses
            .get(&message_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| anyhow!("message does not exist"))?;
        // Never retain a DashMap shard guard while waiting for a remote response.
        let response = response_cell.0.take().await;
        self.inner.responses.remove(&message_id);
        Ok(response)
    }
}

async fn registry_sync_worker(client: Client) -> ! {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.tick().await;
    loop {
        interval.tick().await;
        if let Err(error) = client.synchronize_registry().await {
            log::trace!("Periodic registry synchronization failed: {error}");
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
        cell.0.set(response);
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
        if now.saturating_duration_since(entry.1) <= timeout {
            continue;
        }
        if entry.0.is_set() {
            completed.push(*entry.key());
        } else {
            entry.0.set(ResponseContent::Error(
                crate::distributed::message::ClientError::Unexpected(
                    "Response timeout.".to_string(),
                ),
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
    use lunatic_control::api::{ControlUrls, Registration};
    use tokio::sync::mpsc::error::TryRecvError;

    fn test_client_without_workers() -> Client {
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
        let unused_url = "http://127.0.0.1:1/".to_string();
        let registration = Registration {
            node_name: uuid::Uuid::from_u128(1),
            cert_pem_chain: vec![certificate_pem],
            authentication_token: "test".to_string(),
            root_cert: root_pem,
            urls: ControlUrls {
                api_base: unused_url.clone(),
                nodes: unused_url.clone(),
                node_started: unused_url.clone(),
                node_stopped: unused_url.clone(),
                get_module: unused_url.clone(),
                add_module: unused_url.clone(),
                get_nodes: unused_url,
            },
            envs: vec![],
            is_privileged: true,
        };
        let control_client = control::Client::from_static_nodes(registration, 1, Vec::new());
        let (response_tx, _response_rx) = tokio::sync::mpsc::channel(1);
        let registry = Arc::new(DistributedRegistry::new(1));
        let coordinator = Arc::new(RegistryCoordinator::new(registry.clone(), 1));
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
                nodes_queues: DashMap::new(),
                responses: DashMap::new(),
                response_tx,
                has_messages: Arc::new(Notify::new()),
                outbound_budget: Arc::new(OutboundBudget::default()),
                registry,
                coordinator,
            }),
        };
        client.inner.coordinator.attach_client(&client);
        client
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
            Err(OutboundEnqueueError {
                kind: SendErrorKind::Backpressure,
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
            Err(OutboundEnqueueError {
                kind: SendErrorKind::Backpressure,
                ..
            })
        ));
        assert_eq!(byte_budget.current_usage(), (1, 8));
        drop(reservation);
        assert_eq!(byte_budget.current_usage(), (0, 0));
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
    async fn admission_gate_prevents_enqueue_after_idle_cleanup() {
        let env = EnvironmentId(7);
        let process = ProcessId(11);
        let generation = 3;
        let queues = Arc::new(DashMap::new());
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let admission_gate = Arc::new(AsyncMutex::new(()));
        queues.insert(
            (env, process),
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
        let (lease, usage) = test_outbound_lease(3);
        let producer = tokio::spawn(async move {
            let _admission_guard = producer_gate.lock().await;
            if !process_queue_is_current(
                &producer_queues,
                env,
                process,
                generation,
                &producer_sender,
            ) {
                return false;
            }
            producer_sender
                .try_send(MessageCtx {
                    message_id: MessageId(1),
                    env,
                    src: process,
                    node: NodeId(2),
                    dest: ProcessId(13),
                    queue_generation: generation,
                    chunk_id: AtomicU64::new(0),
                    offset: AtomicUsize::new(0),
                    data: Bytes::from_static(b"abc"),
                    outbound_lease: lease,
                })
                .is_ok()
        });

        tokio::task::yield_now().await;
        assert!(queues
            .remove_if(&(env, process), |_, queue| queue.generation == generation)
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
    async fn spawn_waiter_exists_before_queue_admission_can_complete() {
        let client = test_client_without_workers();
        let env = EnvironmentId(5);
        let source = ProcessId(6);
        let node = NodeId(2);
        let generation = 1;

        let (node_sender, _node_receiver) =
            tokio::sync::mpsc::channel(OUTBOUND_NODE_QUEUE_CAPACITY);
        client.inner.nodes_queues.insert(node, node_sender);
        let (process_sender, process_receiver) =
            tokio::sync::mpsc::channel(OUTBOUND_PROCESS_QUEUE_CAPACITY);
        let admission_gate = Arc::new(AsyncMutex::new(()));
        client.inner.buf_tx.insert(
            (env, source),
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
        let completed = Arc::new((
            AsyncCell::new_with(ResponseContent::Sent),
            now - Duration::from_secs(10),
        ));
        let pending = Arc::new((AsyncCell::new(), now - Duration::from_secs(10)));
        responses.insert(stale_completed, completed);
        responses.insert(stale_pending, pending.clone());

        expire_responses(&responses, now, Duration::from_secs(5));

        assert!(!responses.contains_key(&stale_completed));
        assert!(responses.contains_key(&stale_pending));
        assert!(pending.0.is_set(), "pending waiter must receive a timeout");

        expire_responses(&responses, now, Duration::from_secs(5));
        assert!(!responses.contains_key(&stale_pending));
    }
}
