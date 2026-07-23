/// Congestion control module for distributed message chunking between competing processes.
///
/// When a process send out a message to another process on a different node
/// the message is forwarded to the Congestion control worker via a queue.
///
/// Congestion control worker picks up each message sent and routes chunks
/// to  node connection managers. For each node there exists only one connection
/// manager.
///
/// The node connection manager is responsible for routing message chunks based on
/// the source process_id and destination process_id to appropriate quic stream.
/// This ensures that all process to process messages come in order in which they
/// are being sent.
///
/// Stream task manages quic stream and writes multiple message chunks.
///
/// Topology illustration:
///
///  -----       -----
/// |  P  | ... |  P  | - Processes
///  -----       -----
///    |      /
///    |    /
///  -----
/// |  C  | - Congestion control worker
///  -----
///    | \
///    |   \
///    |     \
///    |       \
///  -----       -----
/// |  N  | ... |  N  | - Node connection managers
///  -----       -----
///    |         | \
///    |         ...
///    |
///    |
///  -----       -----
/// |  S  | ... |  S  | - Stream tasks
///  -----       -----
///
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};

use anyhow::Result;
use lunatic_control::NodeInfo;
use tokio::sync::{
    mpsc::{self, error::TryRecvError, error::TrySendError, Receiver, Sender},
    watch, Mutex, OwnedSemaphorePermit, Semaphore,
};
use tokio::task::JoinSet;

use crate::{
    distributed::{
        self,
        client::{
            MessageCtx, OutboundClass, OutboundMessageLease, ProcessId, ProcessQueueRoute,
            SendErrorKind,
        },
    },
    quic,
};

pub struct MessageChunk {
    src: ProcessId,
    dest: ProcessId,
    message_id: u64,
    message_size: u32,
    chunk_id: u64,
    data: bytes::Bytes,
    _budget: OutboundMessageLease,
}

// TODO: move to configuration
const CHUNK_SIZE: usize = quic::MESSAGE_CHUNK_SIZE;
const STREAM_BUFFER_CHUNKS: usize = 64;
const STREAM_INGRESS_MESSAGES: usize = 64;
const OUTBOUND_STREAM_CANCELLED_ERROR_CODE: u32 = 2;
const CLOSED_NODE_RETRY_DELAY: Duration = Duration::from_millis(10);
const NODE_QUEUE_RECOVERY_RETRY_DELAY: Duration = Duration::from_millis(250);

#[derive(Debug, PartialEq, Eq)]
enum WorkerAction {
    Continue,
    WaitForMessage,
    RetryUnavailableNode,
}

fn next_worker_action(made_progress: bool, in_progress_empty: bool) -> WorkerAction {
    if made_progress {
        WorkerAction::Continue
    } else if in_progress_empty {
        WorkerAction::WaitForMessage
    } else {
        WorkerAction::RetryUnavailableNode
    }
}

#[derive(Debug, PartialEq, Eq)]
enum EnqueueResult<T> {
    Sent,
    Full(T),
    Closed(T),
}

#[cfg(test)]
fn try_enqueue<T>(sender: &Sender<T>, value: T) -> EnqueueResult<T> {
    match sender.try_send(value) {
        Ok(()) => EnqueueResult::Sent,
        Err(TrySendError::Full(value)) => EnqueueResult::Full(value),
        Err(TrySendError::Closed(value)) => EnqueueResult::Closed(value),
    }
}

struct StreamIngressSender {
    sender: Sender<AdmittedMessage>,
    message_slots: Arc<Semaphore>,
    byte_slots: Arc<Semaphore>,
}

struct NodeQueueSenderInner {
    streams: Box<[StreamIngressSender]>,
    message_slots: Arc<Semaphore>,
    byte_slots: Arc<Semaphore>,
}

/// Cloneable, non-blocking ingress for one remote node.
///
/// Routing happens before any shared FIFO so a full logical stream cannot hide work for another
/// stream behind it. The permits stay with the whole message while it is queued, written, or held
/// for reconnect, making both the per-stream and aggregate ceilings include in-flight work.
#[derive(Clone)]
pub(crate) struct NodeQueueSender {
    inner: Arc<NodeQueueSenderInner>,
}

struct AdmissionPermits {
    _stream_message_slot: OwnedSemaphorePermit,
    _stream_byte_slots: OwnedSemaphorePermit,
    _node_message_slot: OwnedSemaphorePermit,
    _node_byte_slots: OwnedSemaphorePermit,
}

pub(crate) struct AdmittedMessage {
    // Permits precede the global lease-bearing message so observing a released outbound lease also
    // means every narrower stream/node reservation has already been returned.
    _permits: AdmissionPermits,
    owner: NodeQueueSender,
    message: MessageCtx,
}

impl AdmittedMessage {
    pub(crate) fn into_message(self) -> MessageCtx {
        self.message
    }

    fn belongs_to(&self, sender: &NodeQueueSender) -> bool {
        self.owner.same_channel(sender)
    }
}

impl std::ops::Deref for AdmittedMessage {
    type Target = MessageCtx;

    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

pub(crate) enum AdmissionResult {
    Admitted(AdmittedMessage),
    Full(MessageCtx),
    Closed(MessageCtx),
}

enum RehomeResult {
    Ready(AdmittedMessage),
    Full(AdmittedMessage),
    Closed(AdmittedMessage),
}

impl NodeQueueSender {
    pub(crate) fn with_limits(
        streams: usize,
        stream_message_capacity: usize,
        stream_byte_capacity: usize,
    ) -> (Self, Vec<Receiver<AdmittedMessage>>) {
        assert!(streams > 0, "node queue needs at least one stream");
        assert!(
            stream_message_capacity > 0,
            "stream message capacity must be non-zero"
        );
        assert!(
            stream_byte_capacity > 0,
            "stream byte capacity must be non-zero"
        );

        let node_message_capacity = streams
            .checked_mul(stream_message_capacity)
            .expect("node message capacity overflow");
        let node_byte_capacity = streams
            .checked_mul(stream_byte_capacity)
            .expect("node byte capacity overflow");
        let node_message_slots = Arc::new(Semaphore::new(node_message_capacity));
        let node_byte_slots = Arc::new(Semaphore::new(node_byte_capacity));
        let mut senders = Vec::with_capacity(streams);
        let mut receivers = Vec::with_capacity(streams);

        for _ in 0..streams {
            let (sender, receiver) = mpsc::channel(stream_message_capacity);
            senders.push(StreamIngressSender {
                sender,
                message_slots: Arc::new(Semaphore::new(stream_message_capacity)),
                byte_slots: Arc::new(Semaphore::new(stream_byte_capacity)),
            });
            receivers.push(receiver);
        }

        (
            Self {
                inner: Arc::new(NodeQueueSenderInner {
                    streams: senders.into_boxed_slice(),
                    message_slots: node_message_slots,
                    byte_slots: node_byte_slots,
                }),
            },
            receivers,
        )
    }

    fn stream_index(&self, message: &MessageCtx) -> usize {
        if message.class == OutboundClass::Control || self.inner.streams.len() == 1 {
            return 0;
        }
        let data_streams = self.inner.streams.len() - 1;
        1 + ((message.src.0 ^ message.dest.0) % data_streams as u64) as usize
    }

    fn try_reserve(&self, message: &MessageCtx) -> Option<AdmissionPermits> {
        let stream_index = self.stream_index(message);
        let stream = &self.inner.streams[stream_index];
        let byte_count = message.retained_bytes;
        let byte_count = u32::try_from(byte_count).ok()?;

        let stream_message_slot = stream.message_slots.clone().try_acquire_owned().ok()?;
        let stream_byte_slots = stream
            .byte_slots
            .clone()
            .try_acquire_many_owned(byte_count)
            .ok()?;
        let node_message_slot = self.inner.message_slots.clone().try_acquire_owned().ok()?;
        let node_byte_slots = self
            .inner
            .byte_slots
            .clone()
            .try_acquire_many_owned(byte_count)
            .ok()?;

        Some(AdmissionPermits {
            _stream_message_slot: stream_message_slot,
            _stream_byte_slots: stream_byte_slots,
            _node_message_slot: node_message_slot,
            _node_byte_slots: node_byte_slots,
        })
    }

    pub(crate) fn try_admit(&self, message: MessageCtx) -> AdmissionResult {
        if self.is_closed() {
            return AdmissionResult::Closed(message);
        }
        let Some(permits) = self.try_reserve(&message) else {
            return AdmissionResult::Full(message);
        };

        AdmissionResult::Admitted(AdmittedMessage {
            _permits: permits,
            owner: self.clone(),
            message,
        })
    }

    fn try_enqueue_admitted(&self, admitted: AdmittedMessage) -> EnqueueResult<AdmittedMessage> {
        debug_assert!(admitted.belongs_to(self));
        let stream_index = self.stream_index(&admitted.message);
        match self.inner.streams[stream_index].sender.try_send(admitted) {
            Ok(()) => EnqueueResult::Sent,
            Err(TrySendError::Full(admitted)) => EnqueueResult::Full(admitted),
            Err(TrySendError::Closed(admitted)) => EnqueueResult::Closed(admitted),
        }
    }

    fn try_rehome(&self, admitted: AdmittedMessage) -> RehomeResult {
        if admitted.belongs_to(self) {
            return RehomeResult::Ready(admitted);
        }
        if self.is_closed() {
            return RehomeResult::Closed(admitted);
        }
        let Some(permits) = self.try_reserve(&admitted.message) else {
            return RehomeResult::Full(admitted);
        };
        let message = admitted.into_message();
        let rehomed = AdmittedMessage {
            _permits: permits,
            owner: self.clone(),
            message,
        };
        RehomeResult::Ready(rehomed)
    }

    #[cfg(test)]
    fn try_send(&self, message: MessageCtx) -> EnqueueResult<MessageCtx> {
        let admitted = match self.try_admit(message) {
            AdmissionResult::Admitted(admitted) => admitted,
            AdmissionResult::Full(message) => return EnqueueResult::Full(message),
            AdmissionResult::Closed(message) => return EnqueueResult::Closed(message),
        };

        match self.try_enqueue_admitted(admitted) {
            EnqueueResult::Sent => EnqueueResult::Sent,
            EnqueueResult::Full(admitted) => EnqueueResult::Full(admitted.into_message()),
            EnqueueResult::Closed(admitted) => EnqueueResult::Closed(admitted.into_message()),
        }
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.inner
            .streams
            .iter()
            .any(|stream| stream.sender.is_closed())
    }

    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

pub(crate) fn node_queue_channels(
    streams: usize,
) -> (NodeQueueSender, Vec<Receiver<AdmittedMessage>>) {
    NodeQueueSender::with_limits(
        streams,
        STREAM_INGRESS_MESSAGES,
        quic::MAX_WIRE_MESSAGE_BYTES,
    )
}

enum NodeRecoveryAction<T> {
    Drop(T),
    Retry,
}

fn node_recovery_action<T>(
    kind: SendErrorKind,
    remove_message: impl FnOnce() -> T,
) -> NodeRecoveryAction<T> {
    if matches!(kind, SendErrorKind::NodeNotFound) {
        NodeRecoveryAction::Drop(remove_message())
    } else {
        NodeRecoveryAction::Retry
    }
}

fn requeue_in_progress(
    state: &distributed::Client,
    key: (distributed::client::EnvironmentId, ProcessQueueRoute),
    queue_generation: u64,
    message: AdmittedMessage,
) {
    let Some(queue) = state.inner.buf_tx.get(&key) else {
        return;
    };
    if queue.generation == queue_generation {
        // Retain the sender-map guard through insertion. Generation cleanup removes the sender
        // first, so it will either win before this check or wait and then remove this message.
        state.inner.route_in_progress.entry(key).or_insert(message);
    }
}

pub async fn congestion_control_worker(state: distributed::Client) -> ! {
    log::trace!("starting congestion control worker");
    let mut retry_after = HashMap::new();
    loop {
        // Register before scanning so a notification racing with an empty pass is retained.
        let notified = state.inner.has_messages.notified();
        tokio::pin!(notified);
        let process_queues: Vec<_> = state
            .inner
            .buf_rx
            .iter()
            .flat_map(|env| {
                let env_id = *env.key();
                env.iter()
                    .map(|process| (env_id, *process.key(), process.generation))
                    .collect::<Vec<_>>()
            })
            .collect();
        let active_keys: HashSet<_> = process_queues.iter().copied().collect();
        retry_after.retain(|key, _| active_keys.contains(key));
        let mut made_progress = false;

        for (env_id, route, queue_generation) in process_queues {
            let key = (env_id, route);
            let retry_key = (env_id, route, queue_generation);
            if retry_after
                .get(&retry_key)
                .is_some_and(|deadline| *deadline > Instant::now())
            {
                continue;
            }
            retry_after.remove(&retry_key);
            let finished = if let Some(message) = state.inner.route_in_progress.get(&key) {
                if message.queue_generation != queue_generation {
                    continue;
                }
                let node = message.node;
                let message_id = message.message_id;
                let node_queue = state
                    .inner
                    .nodes_queues
                    .get(&node)
                    .map(|queue| queue.sender());
                let Some(node_queue) = node_queue else {
                    drop(message);
                    match state.ensure_node_queue(node).await {
                        Ok(_) => made_progress = true,
                        Err(error) => {
                            let now = Instant::now();
                            match node_recovery_action(error.kind(), || {
                                state.inner.route_in_progress.remove_if(&key, |_, message| {
                                    message.queue_generation == queue_generation
                                        && message.message_id == message_id
                                })
                            }) {
                                NodeRecoveryAction::Drop(removed) => {
                                    retry_after.remove(&retry_key);
                                    if removed.is_some() {
                                        made_progress = true;
                                        log::warn!(
                                            "Dropping distributed message {} because node={} queue cannot be recovered: {error}",
                                            message_id.0,
                                            node.0
                                        );
                                    }
                                }
                                NodeRecoveryAction::Retry => {
                                    log::trace!(
                                        "Cannot recreate outbound queue for node={}: {error}",
                                        node.0
                                    );
                                    retry_after
                                        .insert(retry_key, now + NODE_QUEUE_RECOVERY_RETRY_DELAY);
                                }
                            }
                        }
                    }
                    continue;
                };
                drop(message);

                let Some((_, message)) =
                    state.inner.route_in_progress.remove_if(&key, |_, message| {
                        message.queue_generation == queue_generation
                            && message.message_id == message_id
                    })
                else {
                    continue;
                };
                let src = message.src;
                let dest = message.dest;
                let message = match node_queue.try_rehome(message) {
                    RehomeResult::Ready(message) => message,
                    RehomeResult::Full(message) => {
                        requeue_in_progress(&state, key, queue_generation, message);
                        retry_after.insert(retry_key, Instant::now() + CLOSED_NODE_RETRY_DELAY);
                        continue;
                    }
                    RehomeResult::Closed(message) => {
                        requeue_in_progress(&state, key, queue_generation, message);
                        if let Some((_, stale)) = state
                            .inner
                            .nodes_queues
                            .remove_if(&node, |_, queue| queue.sender().same_channel(&node_queue))
                        {
                            stale.abort();
                        }
                        retry_after.insert(retry_key, Instant::now() + CLOSED_NODE_RETRY_DELAY);
                        continue;
                    }
                };
                match node_queue.try_enqueue_admitted(message) {
                    EnqueueResult::Sent => {
                        made_progress = true;
                        log::trace!(
                            "congestion::message::forwarded message_id={} stream={}",
                            message_id.0,
                            ((src.0 ^ dest.0) % node_queue.inner.streams.len() as u64)
                        );
                        true
                    }
                    EnqueueResult::Full(message) => {
                        requeue_in_progress(&state, key, queue_generation, message);
                        retry_after.insert(retry_key, Instant::now() + CLOSED_NODE_RETRY_DELAY);
                        false
                    }
                    EnqueueResult::Closed(message) => {
                        log::warn!(
                            "Cannot forward message from pid={} to node={} dest_pid={}: node queue closed",
                            src.0,
                            node.0,
                            dest.0,
                        );
                        requeue_in_progress(&state, key, queue_generation, message);
                        if let Some((_, stale)) = state
                            .inner
                            .nodes_queues
                            .remove_if(&node, |_, queue| queue.sender().same_channel(&node_queue))
                        {
                            stale.abort();
                        }
                        retry_after.insert(retry_key, Instant::now() + CLOSED_NODE_RETRY_DELAY);
                        false
                    }
                }
            } else {
                true
            };
            if finished {
                let (receive_result, admission_guard) = {
                    let Some(env_queue) = state.inner.buf_rx.get(&env_id) else {
                        continue;
                    };
                    let Some(receiver) = env_queue.get(&route) else {
                        continue;
                    };
                    if receiver.generation != queue_generation {
                        continue;
                    }
                    let admission_guard = receiver.admission_gate.clone().lock_owned().await;
                    let result = receiver.receiver.write().await.try_recv();
                    (result, admission_guard)
                };
                match receive_result {
                    // Push message into in progress space
                    Ok(new_msg_ctx) => {
                        made_progress = true;
                        log::trace!(
                            "congestion::message::received message_id={}",
                            new_msg_ctx.message_id.0
                        );
                        let new_key = (
                            new_msg_ctx.env,
                            ProcessQueueRoute {
                                src: new_msg_ctx.src,
                                node: new_msg_ctx.node,
                                dest: new_msg_ctx.dest,
                            },
                        );
                        if let Some(sender) = state.inner.buf_tx.get(&new_key) {
                            if sender.generation == new_msg_ctx.queue_generation {
                                state.inner.route_in_progress.insert(new_key, new_msg_ctx);
                            }
                        }
                        drop(admission_guard);
                    }
                    // No new messages
                    Err(TryRecvError::Empty) => {
                        if state.remove_idle_process_resources_if_generation(
                            env_id,
                            route,
                            queue_generation,
                        ) {
                            retry_after.remove(&retry_key);
                        }
                        drop(admission_guard);
                    }
                    // Process finished; release every queue/map entry after dropping map guards.
                    Err(TryRecvError::Disconnected) => {
                        retry_after.remove(&retry_key);
                        state.remove_process_resources_if_generation(
                            env_id,
                            route,
                            queue_generation,
                        );
                        drop(admission_guard);
                    }
                };
            }
        }

        match next_worker_action(made_progress, state.inner.route_in_progress.is_empty()) {
            WorkerAction::Continue => {}
            WorkerAction::WaitForMessage => notified.await,
            // A closed/missing node queue must neither spin nor lose its final chunk.
            WorkerAction::RetryUnavailableNode => tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await,
        }
    }
}

struct BufferedChunk<T> {
    value: T,
    _slot: OwnedSemaphorePermit,
}

struct BoundedBuffer<T> {
    queue: Mutex<VecDeque<BufferedChunk<T>>>,
    slots: Arc<Semaphore>,
}

impl<T> BoundedBuffer<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "stream buffer capacity must be non-zero");
        Self {
            queue: Mutex::new(VecDeque::with_capacity(capacity)),
            slots: Arc::new(Semaphore::new(capacity)),
        }
    }

    #[cfg(test)]
    async fn push(&self, value: T) {
        let slot = self.reserve_slot().await;
        self.push_reserved(value, slot).await;
    }

    async fn reserve_slot(&self) -> OwnedSemaphorePermit {
        self.slots
            .clone()
            .acquire_owned()
            .await
            .expect("stream buffer semaphore is never closed")
    }

    async fn push_reserved(&self, value: T, slot: OwnedSemaphorePermit) {
        self.queue
            .lock()
            .await
            .push_front(BufferedChunk { value, _slot: slot });
    }

    async fn pop_oldest(&self) -> Option<BufferedChunk<T>> {
        self.queue.lock().await.pop_back()
    }

    async fn retry_oldest(&self, value: BufferedChunk<T>) {
        self.queue.lock().await.push_back(value);
    }

    #[cfg(test)]
    async fn len(&self) -> usize {
        self.queue.lock().await.len()
    }
}

type StreamBuffer = Arc<BoundedBuffer<MessageChunk>>;

pub struct NodeConnectionManager {
    pub streams: usize,
    pub node_info: NodeInfo,
    pub client: quic::Client,
    pub message_chunks: Receiver<MessageChunk>,
}

pub async fn node_connection_manager(mut manager: NodeConnectionManager) -> Result<()> {
    anyhow::ensure!(
        manager.streams > 0,
        "node manager needs at least one stream"
    );
    let node_info = manager.node_info;
    log::trace!(
        "congestion::node_connection_manager::started node={} address={}",
        node_info.id,
        node_info.address
    );
    // Setup stream buffer
    let mut buffers: Vec<StreamBuffer> = Vec::with_capacity(manager.streams);
    for _ in 0..manager.streams {
        buffers.push(Arc::new(BoundedBuffer::new(STREAM_BUFFER_CHUNKS)));
    }
    let mut pending_chunk = None;

    'connections: loop {
        if pending_chunk.is_none()
            && manager.message_chunks.is_closed()
            && manager.message_chunks.is_empty()
        {
            return Ok(());
        }
        // Setup conn or fail
        let conn = match manager
            .client
            .try_connect_node(node_info.address, &node_info.name, node_info.id, 3)
            .await
        {
            Ok(conn) => conn,
            Err(e) => {
                log::error!("congestion::node_connection_manager Connection failed: {e}");
                tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
                continue;
            }
        };
        log::trace!(
            "node={} name={} address={}",
            node_info.id,
            node_info.name,
            node_info.address
        );
        let mut quic_streams = Vec::with_capacity(manager.streams);
        for _ in 0..manager.streams {
            match conn.open_uni().await {
                Ok(stream) => quic_streams.push(stream),
                Err(e) => {
                    log::error!("congestion::node_connection_manager Stream open failed: {e}");
                    conn.close(quinn::VarInt::from_u32(0), b"stream open failed");
                    tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
                    continue 'connections;
                }
            }
        }

        // Start stream tasks only after all streams are available, keeping indexes aligned.
        let (dead_stream_notifier, mut dead_stream_waker) = mpsc::channel::<()>(1);
        let mut stream_tasks = Vec::new();
        let mut stream_wakers = Vec::new();
        for (buffer, stream) in buffers.iter().zip(quic_streams) {
            let (send, recv) = mpsc::channel::<()>(1);
            stream_wakers.push(send);
            stream_tasks.push(tokio::spawn(stream_task(StreamTask {
                quic_stream: stream,
                action: recv,
                manager_notifier: dead_stream_notifier.clone(),
                buffer: buffer.clone(),
            })));
        }
        // Working chunk passing loop
        let mut input_closed = false;
        'forward_chunks: loop {
            let chunk = if let Some(chunk) = pending_chunk.take() {
                chunk
            } else {
                tokio::select! {
                    chunk = manager.message_chunks.recv() => match chunk {
                        Some(chunk) => chunk,
                        None => {
                            input_closed = true;
                            break 'forward_chunks;
                        }
                    },
                    _ = dead_stream_waker.recv() => break 'forward_chunks,
                }
            };
            log::trace!(
                "congestion::node_connection_manager::msg_id {}",
                chunk.message_id
            );
            let src = chunk.src.0;
            let dest = chunk.dest.0;
            // Preserve ordering by routing each process pair to one stable stream.
            let stream_index = ((src ^ dest) % manager.streams as u64) as usize;
            let reserve_slot = buffers[stream_index].reserve_slot();
            tokio::pin!(reserve_slot);
            let slot = tokio::select! {
                slot = &mut reserve_slot => slot,
                _ = dead_stream_waker.recv() => {
                    pending_chunk = Some(chunk);
                    break 'forward_chunks;
                }
            };
            buffers[stream_index].push_reserved(chunk, slot).await;
            // A single coalesced wake is sufficient; buffer capacity provides backpressure.
            stream_wakers[stream_index].try_send(()).ok();
        }
        conn.close(quinn::VarInt::from_u32(0), b"reconnect streams");
        // Closing the action channels wakes idle streams. Closing QUIC wakes stalled writers.
        drop(stream_wakers);
        for task in stream_tasks {
            task.await.ok();
        }
        if input_closed {
            return Ok(());
        }
    }
}

struct StreamTask {
    quic_stream: quinn::SendStream,
    action: Receiver<()>,
    manager_notifier: Sender<()>,
    buffer: StreamBuffer,
}

async fn stream_task(mut state: StreamTask) {
    log::trace!("congestion::stream_task::start {}", state.quic_stream.id());
    loop {
        let Some(chunk) = state.buffer.pop_oldest().await else {
            match state.action.recv().await {
                Some(()) => continue,
                None => break,
            }
        };
        let mut data = quic::frame_message_chunk(
            chunk.value.message_id,
            chunk.value.message_size,
            chunk.value.chunk_id,
            chunk.value.data.clone(),
        );
        // Try to send data
        match state.quic_stream.write_all_chunks(&mut data).await {
            Ok(_) => {
                log::trace!("congestion::stream_task::write");
            }
            Err(_) => {
                // Connection is dead; return the chunk as the oldest buffered item.
                // Its slot (and outbound message lease) remain held for the retry.
                state.buffer.retry_oldest(chunk).await;
                // Notify manager that connection has died
                state.manager_notifier.try_send(()).ok();
                break;
            }
        };
    }
}

pub(crate) struct FairNodeConnectionManager {
    pub(crate) node_info: NodeInfo,
    pub(crate) client: quic::Client,
    pub(crate) message_streams: Vec<Receiver<AdmittedMessage>>,
}

struct FairStreamState {
    receiver: Mutex<Receiver<AdmittedMessage>>,
    retry_front: StdMutex<VecDeque<AdmittedMessage>>,
}

impl FairStreamState {
    fn new(receiver: Receiver<AdmittedMessage>) -> Self {
        Self {
            receiver: Mutex::new(receiver),
            retry_front: StdMutex::new(VecDeque::new()),
        }
    }

    fn retry_queue(&self) -> std::sync::MutexGuard<'_, VecDeque<AdmittedMessage>> {
        self.retry_front
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn retry(&self, message: AdmittedMessage) {
        self.retry_queue().push_front(message);
    }

    fn reap_terminal_retries(&self) {
        self.retry_queue().retain(|message| {
            let Some(application_ack) = message.message.application_ack.as_ref() else {
                return true;
            };
            if application_ack.is_acknowledged() {
                log::trace!(
                    "dropping disconnected retry message_id={} after application acknowledgement",
                    message.message.message_id.0
                );
                return false;
            }
            if application_ack.is_expired() {
                log::error!(
                    "dropping disconnected retry message_id={} after its replay window elapsed; delivery outcome is unknown",
                    message.message.message_id.0
                );
                return false;
            }
            true
        });
    }

    async fn next_message(
        &self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> std::result::Result<AdmittedMessage, NextMessageExit> {
        if let Some(message) = self.retry_queue().pop_front() {
            return Ok(message);
        }
        if *shutdown.borrow() {
            return Err(NextMessageExit::Shutdown);
        }

        tokio::select! {
            biased;
            _ = wait_for_shutdown(shutdown) => Err(NextMessageExit::Shutdown),
            message = async {
                self.receiver.lock().await.recv().await
            } => match message {
                Some(message) => Ok(message),
                None => Err(NextMessageExit::InputClosed),
            },
        }
    }

    async fn is_drained(&self) -> bool {
        if !self.retry_queue().is_empty() {
            return false;
        }
        let receiver = self.receiver.lock().await;
        receiver.is_closed() && receiver.is_empty()
    }
}

enum NextMessageExit {
    InputClosed,
    Shutdown,
}

struct InFlightMessage {
    state: Arc<FairStreamState>,
    message: Option<AdmittedMessage>,
}

impl InFlightMessage {
    fn new(state: Arc<FairStreamState>, message: AdmittedMessage) -> Self {
        Self {
            state,
            message: Some(message),
        }
    }

    fn message(&self) -> &MessageCtx {
        &self
            .message
            .as_ref()
            .expect("in-flight message is present until commit")
            .message
    }

    fn application_ack(&mut self) -> Option<&mut distributed::client::ApplicationAckHandle> {
        self.message
            .as_mut()
            .and_then(|message| message.message.application_ack.as_mut())
    }

    fn commit(mut self) {
        self.message.take();
    }
}

impl Drop for InFlightMessage {
    fn drop(&mut self) {
        if let Some(message) = self.message.take() {
            self.state.retry(message);
        }
    }
}

enum StreamTaskExit {
    InputClosed,
    Shutdown,
    ConnectionFailed(anyhow::Error),
}

enum SendMessageOutcome {
    Acknowledged,
    ApplicationAcknowledged,
    ReplayWindowElapsed,
    Shutdown,
    StreamFailed(anyhow::Error),
    ConnectionFailed(anyhow::Error),
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    if *shutdown.borrow() {
        return;
    }
    let _ = shutdown.changed().await;
}

async fn wait_for_application_ack(acknowledged: &mut watch::Receiver<bool>) {
    loop {
        if *acknowledged.borrow() {
            return;
        }
        if acknowledged.changed().await.is_err() {
            // The in-flight message owns the registry handle, so closure without dropping this
            // future is not expected. Stay pending and let the transport branch make progress.
            std::future::pending::<()>().await;
        }
    }
}

async fn wait_for_replay_deadline(remaining: Option<Duration>) {
    match remaining {
        Some(remaining) => tokio::time::sleep(remaining).await,
        None => std::future::pending::<()>().await,
    }
}

struct ResetOnDropSendStream {
    stream: quinn::SendStream,
    armed: bool,
}

impl ResetOnDropSendStream {
    fn new(stream: quinn::SendStream) -> Self {
        Self {
            stream,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl std::ops::Deref for ResetOnDropSendStream {
    type Target = quinn::SendStream;

    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

impl std::ops::DerefMut for ResetOnDropSendStream {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}

impl Drop for ResetOnDropSendStream {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.stream.reset(quinn::VarInt::from_u32(
                OUTBOUND_STREAM_CANCELLED_ERROR_CODE,
            ));
        }
    }
}

async fn write_message_frame(
    stream: &mut quinn::SendStream,
    message_id: u64,
    message_size: u32,
    chunk_id: u64,
    data: bytes::Bytes,
) -> SendMessageOutcome {
    let mut framed = quic::frame_message_chunk(message_id, message_size, chunk_id, data);
    match tokio::time::timeout(
        quic::REQUEST_STREAM_IDLE_TIMEOUT,
        stream.write_all_chunks(&mut framed),
    )
    .await
    {
        Err(_) => SendMessageOutcome::StreamFailed(anyhow::anyhow!(
            "outbound stream made no chunk progress for {:?}",
            quic::REQUEST_STREAM_IDLE_TIMEOUT,
        )),
        Ok(Ok(())) => SendMessageOutcome::Acknowledged,
        Ok(Err(quinn::WriteError::ConnectionLost(error))) => {
            SendMessageOutcome::ConnectionFailed(error.into())
        }
        Ok(Err(error)) => SendMessageOutcome::StreamFailed(error.into()),
    }
}

async fn send_message_on_fresh_stream_inner(
    connection: &quinn::Connection,
    message: &MessageCtx,
) -> SendMessageOutcome {
    let _keep_outbound_lease_alive = &message.outbound_lease;
    let stream = match tokio::time::timeout(
        quic::REQUEST_STREAM_IDLE_TIMEOUT,
        connection.open_uni(),
    )
    .await
    {
        Err(_) => {
            return SendMessageOutcome::StreamFailed(anyhow::anyhow!(
                "opening outbound stream exceeded {:?}",
                quic::REQUEST_STREAM_IDLE_TIMEOUT,
            ))
        }
        Ok(Ok(stream)) => stream,
        Ok(Err(error)) => return SendMessageOutcome::ConnectionFailed(error.into()),
    };
    // Quinn normally finishes a send stream when it is dropped. Arm an explicit reset until the
    // peer acknowledges the complete stream so cancellation cannot leave a partial or
    // unacknowledged replay alive past its absolute deadline.
    let mut stream = ResetOnDropSendStream::new(stream);
    let Ok(message_size) = u32::try_from(message.data.len()) else {
        return SendMessageOutcome::StreamFailed(anyhow::anyhow!(
            "outbound message length does not fit in u32"
        ));
    };

    if message.data.is_empty() {
        let outcome = write_message_frame(
            &mut stream,
            message.message_id.0,
            message_size,
            0,
            message.data.slice(0..0),
        )
        .await;
        if !matches!(outcome, SendMessageOutcome::Acknowledged) {
            return outcome;
        }
    } else {
        for (chunk_id, offset) in (0..message.data.len()).step_by(CHUNK_SIZE).enumerate() {
            let end = (offset + CHUNK_SIZE).min(message.data.len());
            let outcome = write_message_frame(
                &mut stream,
                message.message_id.0,
                message_size,
                chunk_id as u64,
                message.data.slice(offset..end),
            )
            .await;
            if !matches!(outcome, SendMessageOutcome::Acknowledged) {
                return outcome;
            }
        }
    }

    if let Err(error) = stream.finish() {
        return SendMessageOutcome::StreamFailed(error.into());
    }
    match tokio::time::timeout(quic::REQUEST_STREAM_IDLE_TIMEOUT, stream.stopped()).await {
        Err(_) => SendMessageOutcome::StreamFailed(anyhow::anyhow!(
            "peer did not acknowledge the finished stream within {:?}",
            quic::REQUEST_STREAM_IDLE_TIMEOUT,
        )),
        Ok(Ok(None)) => {
            stream.disarm();
            SendMessageOutcome::Acknowledged
        }
        Ok(Ok(Some(code))) => SendMessageOutcome::StreamFailed(anyhow::anyhow!(
            "peer stopped outbound message stream with code {code:?}"
        )),
        Ok(Err(quinn::StoppedError::ConnectionLost(error))) => {
            SendMessageOutcome::ConnectionFailed(error.into())
        }
        Ok(Err(error)) => SendMessageOutcome::StreamFailed(error.into()),
    }
}

async fn send_message_on_fresh_stream(
    connection: &quinn::Connection,
    message: &MessageCtx,
    shutdown: &mut watch::Receiver<bool>,
) -> SendMessageOutcome {
    tokio::select! {
        biased;
        _ = wait_for_shutdown(shutdown) => SendMessageOutcome::Shutdown,
        result = send_message_on_fresh_stream_inner(connection, message) => result,
    }
}

async fn fair_stream_task(
    state: Arc<FairStreamState>,
    connection: quinn::Connection,
    mut shutdown: watch::Receiver<bool>,
) -> StreamTaskExit {
    loop {
        let message = match state.next_message(&mut shutdown).await {
            Ok(message) => message,
            Err(NextMessageExit::InputClosed) => return StreamTaskExit::InputClosed,
            Err(NextMessageExit::Shutdown) => return StreamTaskExit::Shutdown,
        };
        let mut in_flight = InFlightMessage::new(state.clone(), message);
        let ack_state = in_flight
            .application_ack()
            .map(|ack| (ack.is_acknowledged(), ack.is_expired()));
        if let Some((acknowledged, expired)) = ack_state {
            if acknowledged {
                in_flight.commit();
                continue;
            }
            if expired {
                log::error!(
                    "application acknowledgement replay window elapsed; delivery outcome is unknown"
                );
                in_flight.commit();
                continue;
            }
        }
        if let Some(application_ack) = in_flight.application_ack() {
            // Starting this once immediately before the first connected send attempt closes the
            // case where the peer executes a request but its transport acknowledgement is lost.
            // The same absolute deadline remains attached across stream and connection retries.
            application_ack.start_replay_window();
        }
        let (ack_during_send, replay_deadline_remaining) = in_flight
            .application_ack()
            .map(|application_ack| {
                (
                    Some(application_ack.subscribe()),
                    application_ack.replay_deadline_remaining(),
                )
            })
            .unwrap_or((None, None));
        let send_outcome = if let Some(mut acknowledged) = ack_during_send {
            tokio::select! {
                biased;
                _ = wait_for_application_ack(&mut acknowledged) => {
                    // A response to an earlier attempt makes this replay unnecessary. Dropping
                    // the send future resets an unfinished QUIC stream; the receiver tombstone
                    // still covers a fully delivered attempt racing with this cancellation.
                    SendMessageOutcome::ApplicationAcknowledged
                }
                _ = wait_for_replay_deadline(replay_deadline_remaining) => {
                    // The receiver's finite replay tombstone must outlive every transport
                    // attempt. Cancelling this future resets any unfinished stream and prevents
                    // an attempt that started just before the deadline from executing later.
                    SendMessageOutcome::ReplayWindowElapsed
                }
                outcome = send_message_on_fresh_stream(
                    &connection,
                    in_flight.message(),
                    &mut shutdown,
                ) => outcome,
            }
        } else {
            send_message_on_fresh_stream(&connection, in_flight.message(), &mut shutdown).await
        };
        match send_outcome {
            SendMessageOutcome::ApplicationAcknowledged => in_flight.commit(),
            SendMessageOutcome::ReplayWindowElapsed => {
                log::error!(
                    "application acknowledgement replay window elapsed during send; delivery outcome is unknown"
                );
                in_flight.commit();
            }
            SendMessageOutcome::Acknowledged => {
                if let Some(application_ack) = in_flight.application_ack() {
                    let ack_wait = application_ack
                        .remaining()
                        .min(quic::REQUEST_STREAM_IDLE_TIMEOUT);
                    let acknowledged = tokio::select! {
                        biased;
                        acknowledged = application_ack.wait() => acknowledged,
                        _ = wait_for_shutdown(&mut shutdown) => {
                            return StreamTaskExit::Shutdown;
                        }
                        _ = tokio::time::sleep(ack_wait) => false,
                    };
                    if !acknowledged {
                        let replay_window_elapsed = in_flight
                            .application_ack()
                            .is_some_and(|ack| ack.is_expired());
                        if replay_window_elapsed {
                            log::error!(
                                "application acknowledgement replay window elapsed; delivery outcome is unknown"
                            );
                            in_flight.commit();
                            continue;
                        }
                        log::warn!("application acknowledgement timed out; replaying message");
                        drop(in_flight);
                        tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
                        continue;
                    }
                }
                in_flight.commit();
            }
            SendMessageOutcome::Shutdown => return StreamTaskExit::Shutdown,
            SendMessageOutcome::StreamFailed(error) => {
                log::warn!("outbound stream failed without closing its connection: {error}");
                drop(in_flight);
                tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
            }
            SendMessageOutcome::ConnectionFailed(error) => {
                return StreamTaskExit::ConnectionFailed(error)
            }
        }
    }
}

/// Production node manager with independently bounded logical stream lanes.
///
/// Each message is written on a fresh QUIC stream and retained until the peer acknowledges every
/// byte and, for data requests, returns an application response. An ACK from an earlier attempt
/// cancels an unfinished replay. Any detected connection failure retries the whole message from
/// chunk zero instead of forwarding an invalid suffix.
pub(crate) async fn fair_node_connection_manager(manager: FairNodeConnectionManager) -> Result<()> {
    anyhow::ensure!(
        !manager.message_streams.is_empty(),
        "fair node manager needs at least one stream"
    );
    let node_info = manager.node_info;
    let states = manager
        .message_streams
        .into_iter()
        .map(|receiver| Arc::new(FairStreamState::new(receiver)))
        .collect::<Vec<_>>();

    loop {
        let mut all_drained = true;
        for state in &states {
            state.reap_terminal_retries();
            all_drained &= state.is_drained().await;
        }
        if all_drained {
            return Ok(());
        }

        let connection_attempt =
            manager
                .client
                .try_connect_node(node_info.address, &node_info.name, node_info.id, 3);
        tokio::pin!(connection_attempt);
        let connection_result = loop {
            tokio::select! {
                result = &mut connection_attempt => break result,
                _ = tokio::time::sleep(CLOSED_NODE_RETRY_DELAY) => {
                    for state in &states {
                        // A failed connected attempt may already have caused the remote side
                        // effect. Release terminal retries while reconnect is slow or impossible;
                        // messages never attempted still have no deadline and remain queued.
                        state.reap_terminal_retries();
                    }
                }
            }
        };
        let connection = match connection_result {
            Ok(connection) => connection,
            Err(error) => {
                log::error!("fair node manager connection failed: {error}");
                tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
                continue;
            }
        };
        let (shutdown, shutdown_rx) = watch::channel(false);
        let mut stream_tasks = JoinSet::new();
        for state in &states {
            stream_tasks.spawn(fair_stream_task(
                state.clone(),
                connection.clone(),
                shutdown_rx.clone(),
            ));
        }

        let mut closed_streams = 0usize;
        let reconnect_reason = loop {
            match stream_tasks.join_next().await {
                Some(Ok(StreamTaskExit::InputClosed)) => {
                    closed_streams += 1;
                    if closed_streams == states.len() {
                        connection.close(quinn::VarInt::from_u32(0), b"node ingress drained");
                        return Ok(());
                    }
                }
                Some(Ok(StreamTaskExit::ConnectionFailed(error))) => break error,
                Some(Ok(StreamTaskExit::Shutdown)) => {
                    break anyhow::anyhow!("stream writer stopped before manager shutdown")
                }
                Some(Err(error)) => break anyhow::anyhow!("stream writer task failed: {error}"),
                None => return Ok(()),
            }
        };

        log::warn!(
            "node={} outbound connection will be recreated: {reconnect_reason}",
            node_info.id
        );
        connection.close(quinn::VarInt::from_u32(0), b"reconnect fair streams");
        let _ = shutdown.send(true);
        if tokio::time::timeout(Duration::from_secs(1), async {
            while stream_tasks.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
        }
        tokio::time::sleep(CLOSED_NODE_RETRY_DELAY).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicU64, AtomicUsize},
            Arc,
        },
        time::Instant,
    };

    use anyhow::Result;
    use bytes::Bytes;
    use tokio::time::{timeout, Duration};
    use tokio::{
        sync::{mpsc, oneshot, watch},
        task::JoinSet,
    };

    use super::{
        fair_node_connection_manager, fair_stream_task, next_worker_action, node_recovery_action,
        try_enqueue, BoundedBuffer, EnqueueResult, FairNodeConnectionManager, FairStreamState,
        InFlightMessage, MessageChunk, NodeQueueSender, NodeRecoveryAction, StreamTaskExit,
        WorkerAction, CHUNK_SIZE,
    };
    use crate::{
        control::cert,
        distributed::{
            client::{
                test_application_ack, test_outbound_lease, ApplicationAckProbe, EnvironmentId,
                MessageCtx, MessageId, NodeId, OutboundBudgetProbe, ProcessId, SendErrorKind,
            },
            server::gen_node_cert,
        },
        quic, CertAttrs,
    };

    fn test_message(
        message_id: u64,
        src: u64,
        dest: u64,
        bytes: usize,
    ) -> (MessageCtx, OutboundBudgetProbe) {
        test_message_with_retained_bytes(message_id, src, dest, bytes, bytes.max(1))
    }

    fn test_message_with_retained_bytes(
        message_id: u64,
        src: u64,
        dest: u64,
        wire_bytes: usize,
        retained_bytes: usize,
    ) -> (MessageCtx, OutboundBudgetProbe) {
        let (outbound_lease, usage) = test_outbound_lease(retained_bytes);
        (
            MessageCtx {
                message_id: MessageId(message_id),
                env: EnvironmentId(1),
                src: ProcessId(src),
                node: NodeId(2),
                dest: ProcessId(dest),
                queue_generation: 1,
                chunk_id: AtomicU64::new(0),
                offset: AtomicUsize::new(0),
                data: Bytes::from(vec![message_id as u8; wire_bytes]),
                retained_bytes,
                class: crate::distributed::client::OutboundClass::Data,
                application_ack: None,
                outbound_lease,
            },
            usage,
        )
    }

    fn signed_test_node(
        root: &cert::CertificateAuthority,
        name: &str,
        node_id: u64,
    ) -> Result<(String, String)> {
        let request = gen_node_cert(name)?;
        let certificate = cert::sign_node_certificate(
            &request.serialize_request_pem()?,
            root,
            name,
            &CertAttrs {
                node_id: Some(node_id),
                allowed_envs: Vec::new(),
                is_privileged: true,
            },
        )?;
        Ok((certificate, request.serialize_private_key_pem()))
    }

    fn test_message_with_ack(
        message_id: u64,
        src: u64,
        dest: u64,
        bytes: usize,
    ) -> (MessageCtx, OutboundBudgetProbe, ApplicationAckProbe) {
        let (mut message, usage) = test_message(message_id, src, dest, bytes);
        let (ack, probe) = test_application_ack(message.message_id, message.node);
        message.application_ack = Some(ack);
        (message, usage, probe)
    }

    async fn wait_for_released_budget(usage: &OutboundBudgetProbe) {
        timeout(Duration::from_secs(2), async {
            while usage.current_usage() != (0, 0) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("outbound budget must be released");
    }

    #[tokio::test]
    async fn saturated_stream_does_not_block_ready_stream() {
        let (sender, mut receivers) = NodeQueueSender::with_limits(3, 2, 16);
        let _control_receiver = receivers.remove(0);
        let mut slow_receiver = receivers.remove(0);
        let mut ready_receiver = receivers.remove(0);

        let (slow_in_flight, slow_in_flight_usage) = test_message(1, 1, 1, 4);
        assert_eq!(sender.stream_index(&slow_in_flight), 1);
        assert!(matches!(
            sender.try_send(slow_in_flight),
            EnqueueResult::Sent
        ));
        let slow_in_flight = slow_receiver.recv().await.expect("first slow message");

        let (slow_queued, slow_queued_usage) = test_message(2, 1, 1, 4);
        assert!(matches!(sender.try_send(slow_queued), EnqueueResult::Sent));
        let (slow_rejected, slow_rejected_usage) = test_message(3, 1, 1, 4);
        let slow_rejected = match sender.try_send(slow_rejected) {
            EnqueueResult::Full(message) => message,
            _ => panic!("saturated stream must return ownership with Full"),
        };

        let (ready, ready_usage) = test_message(4, 1, 2, 4);
        assert_eq!(sender.stream_index(&ready), 2);
        assert!(matches!(sender.try_send(ready), EnqueueResult::Sent));
        let ready = timeout(Duration::from_millis(100), ready_receiver.recv())
            .await
            .expect("ready stream must progress within the fairness threshold")
            .expect("ready stream remains open");
        assert_eq!(ready.message.message_id, MessageId(4));

        drop(ready);
        drop(slow_in_flight);
        drop(slow_receiver.recv().await);
        drop(slow_rejected);
        assert_eq!(slow_in_flight_usage.current_usage(), (0, 0));
        assert_eq!(slow_queued_usage.current_usage(), (0, 0));
        assert_eq!(slow_rejected_usage.current_usage(), (0, 0));
        assert_eq!(ready_usage.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn stream_and_node_byte_limits_are_exact_and_reusable() {
        let (sender, mut receivers) = NodeQueueSender::with_limits(3, 3, 4);
        let mut control_receiver = receivers.remove(0);
        let mut first_receiver = receivers.remove(0);
        let mut second_receiver = receivers.remove(0);

        let (mut control, control_usage) = test_message(9, 0, 0, 4);
        control.class = crate::distributed::client::OutboundClass::Control;
        assert!(matches!(sender.try_send(control), EnqueueResult::Sent));
        let control = control_receiver
            .recv()
            .await
            .expect("control stream message");

        let (first, first_usage) = test_message(10, 2, 2, 4);
        assert!(matches!(sender.try_send(first), EnqueueResult::Sent));
        let first = first_receiver.recv().await.expect("first stream message");

        let (over_limit, over_limit_usage) = test_message(11, 2, 2, 1);
        let over_limit = match sender.try_send(over_limit) {
            EnqueueResult::Full(message) => message,
            _ => panic!("per-stream byte ceiling must reject cap + 1"),
        };

        let (other_stream, other_stream_usage) = test_message(12, 2, 3, 4);
        assert!(matches!(sender.try_send(other_stream), EnqueueResult::Sent));
        let other_stream = second_receiver
            .recv()
            .await
            .expect("reserved capacity for the other stream");

        drop(first);
        assert!(matches!(sender.try_send(over_limit), EnqueueResult::Sent));
        drop(first_receiver.recv().await);
        drop(other_stream);
        drop(control);

        assert_eq!(control_usage.current_usage(), (0, 0));
        assert_eq!(first_usage.current_usage(), (0, 0));
        assert_eq!(over_limit_usage.current_usage(), (0, 0));
        assert_eq!(other_stream_usage.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn stream_limit_counts_retained_allocation_not_only_wire_length() {
        let (sender, mut receivers) = NodeQueueSender::with_limits(1, 1, 4);
        let mut receiver = receivers.remove(0);
        let (oversized, oversized_usage) = test_message_with_retained_bytes(13, 1, 1, 1, 5);
        let oversized = match sender.try_send(oversized) {
            EnqueueResult::Full(message) => message,
            _ => panic!("retained capacity above the byte ceiling must be rejected"),
        };
        drop(oversized);
        assert_eq!(oversized_usage.current_usage(), (0, 0));

        let (fitting, fitting_usage) = test_message_with_retained_bytes(14, 1, 1, 1, 4);
        assert!(matches!(sender.try_send(fitting), EnqueueResult::Sent));
        drop(
            receiver
                .recv()
                .await
                .expect("exact retained capacity must fit"),
        );
        assert_eq!(fitting_usage.current_usage(), (0, 0));
    }

    #[tokio::test]
    async fn cancelled_writer_retries_whole_message_before_lane_fifo() {
        let (sender, mut receivers) = NodeQueueSender::with_limits(1, 2, 16);
        let receiver = receivers.remove(0);
        let state = Arc::new(FairStreamState::new(receiver));
        let (first, first_usage) = test_message(20, 1, 1, 4);
        let (second, second_usage) = test_message(21, 1, 1, 4);
        assert!(matches!(sender.try_send(first), EnqueueResult::Sent));
        assert!(matches!(sender.try_send(second), EnqueueResult::Sent));
        let (_shutdown, mut shutdown) = watch::channel(false);

        let first = match state.next_message(&mut shutdown).await {
            Ok(message) => message,
            _ => panic!("first queued message must be available"),
        };
        assert_eq!(first.message.message_id, MessageId(20));
        drop(InFlightMessage::new(state.clone(), first));
        assert_eq!(first_usage.current_usage(), (1, 4));

        let retried = match state.next_message(&mut shutdown).await {
            Ok(message) => message,
            _ => panic!("cancelled in-flight message must be retried"),
        };
        assert_eq!(retried.message.message_id, MessageId(20));
        InFlightMessage::new(state.clone(), retried).commit();

        let second = match state.next_message(&mut shutdown).await {
            Ok(message) => message,
            _ => panic!("later FIFO message must remain queued"),
        };
        assert_eq!(second.message.message_id, MessageId(21));
        InFlightMessage::new(state, second).commit();
        assert_eq!(first_usage.current_usage(), (0, 0));
        assert_eq!(second_usage.current_usage(), (0, 0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn adversarial_slow_destination_fairness_benchmark() -> Result<()> {
        const FAST_SAMPLES: usize = 16;
        const FAIR_LANE_LATENCY_THRESHOLD: Duration = Duration::from_millis(100);

        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "fair-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "fair-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (slow_accepted_tx, slow_accepted_rx) = oneshot::channel();
        let (fast_frame_tx, mut fast_frame_rx) = mpsc::channel(FAST_SAMPLES);
        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connecting = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("test server closed"))?;
            let connection = connecting.await?;
            let slow_stream = connection.accept_uni().await?;
            let _ = slow_accepted_tx.send(());
            for _ in 0..FAST_SAMPLES {
                let mut fast_stream = connection.accept_uni().await?;
                let fast_frame = fast_stream.read_to_end(1024 * 1024).await?;
                fast_frame_tx
                    .send(fast_frame)
                    .await
                    .map_err(|_| anyhow::anyhow!("fairness sample receiver stopped"))?;
            }
            let _ = server_shutdown_rx.await;
            drop(slow_stream);
            connection.close(quinn::VarInt::from_u32(0), b"fairness test complete");
            server.close(quinn::VarInt::from_u32(0), b"fairness test complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let slow_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, receivers) = NodeQueueSender::with_limits(3, 1, slow_bytes);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "fair-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));

        let (slow, slow_usage) = test_message(100, 1, 1, slow_bytes);
        assert!(matches!(sender.try_send(slow), EnqueueResult::Sent));
        timeout(Duration::from_secs(5), slow_accepted_rx).await??;

        let (rejected, rejected_usage) = test_message(101, 1, 1, 1);
        let rejected = match sender.try_send(rejected) {
            EnqueueResult::Full(message) => message,
            _ => panic!("the stalled lane must retain its sole admission slot"),
        };
        let mut fast_latencies = Vec::with_capacity(FAST_SAMPLES);
        for sample in 0..FAST_SAMPLES {
            let message_id = 102 + sample as u64;
            let (fast, fast_usage) = test_message(message_id, 1, 2, 4);
            let started = Instant::now();
            assert!(matches!(sender.try_send(fast), EnqueueResult::Sent));
            let fast_frame = timeout(FAIR_LANE_LATENCY_THRESHOLD, fast_frame_rx.recv())
                .await
                .expect("healthy lane must arrive within the fairness threshold")
                .ok_or_else(|| anyhow::anyhow!("fairness sample stream closed"))?;
            let latency = started.elapsed();
            assert_eq!(u64::from_le_bytes(fast_frame[0..8].try_into()?), message_id);
            wait_for_released_budget(&fast_usage).await;
            fast_latencies.push(latency);
        }
        fast_latencies.sort_unstable();
        let p99_index = (FAST_SAMPLES * 99).div_ceil(100).saturating_sub(1);
        let p99 = fast_latencies[p99_index];
        eprintln!(
            "adversarial fair-lane p99 over {FAST_SAMPLES} samples: {p99:?} (limit {FAIR_LANE_LATENCY_THRESHOLD:?})"
        );
        println!(
            "LUNATIC_RESILIENCE_EVIDENCE {{\"kind\":\"slow_consumer_fairness\",\"samples\":{FAST_SAMPLES},\"healthy_lane_p99_us\":{},\"limit_us\":{},\"stalled_lane_remained_bounded\":true}}",
            p99.as_micros(),
            FAIR_LANE_LATENCY_THRESHOLD.as_micros()
        );
        assert!(
            p99 <= FAIR_LANE_LATENCY_THRESHOLD,
            "healthy-lane p99 {p99:?} exceeded {FAIR_LANE_LATENCY_THRESHOLD:?}"
        );
        assert_eq!(slow_usage.current_usage(), (1, slow_bytes));

        drop(rejected);
        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        let _ = server_shutdown_tx.send(());
        timeout(Duration::from_secs(5), server_task).await???;
        wait_for_released_budget(&slow_usage).await;
        wait_for_released_budget(&rejected_usage).await;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn application_ack_serializes_one_logical_lane() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "ack-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "ack-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (frames_tx, mut frames_rx) = mpsc::channel(2);
        let server_task = tokio::spawn(async move {
            let connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("ACK test connection missing"))?
                .await?;
            for _ in 0..2 {
                let mut stream = connection.accept_uni().await?;
                let frame = stream.read_to_end(1024 * 1024).await?;
                frames_tx
                    .send(frame)
                    .await
                    .map_err(|_| anyhow::anyhow!("ACK frame receiver stopped"))?;
            }
            connection.close(quinn::VarInt::from_u32(0), b"ACK test complete");
            server.close(quinn::VarInt::from_u32(0), b"ACK test complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let (sender, receivers) = NodeQueueSender::with_limits(1, 2, 16);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "ack-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));
        let (first, first_usage, first_ack) = test_message_with_ack(150, 1, 1, 4);
        let (second, second_usage) = test_message(151, 1, 1, 4);
        assert!(matches!(sender.try_send(first), EnqueueResult::Sent));
        assert!(matches!(sender.try_send(second), EnqueueResult::Sent));

        let first_frame = timeout(Duration::from_secs(5), frames_rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("first ACK frame missing"))?;
        assert_eq!(u64::from_le_bytes(first_frame[0..8].try_into()?), 150);
        assert!(
            timeout(Duration::from_millis(50), frames_rx.recv())
                .await
                .is_err(),
            "later lane message must wait for the first application ACK"
        );
        assert_eq!(first_usage.current_usage(), (1, 4));

        assert!(first_ack.acknowledge());
        let second_frame = timeout(Duration::from_millis(100), frames_rx.recv())
            .await?
            .ok_or_else(|| anyhow::anyhow!("second ACK frame missing"))?;
        assert_eq!(u64::from_le_bytes(second_frame[0..8].try_into()?), 151);
        wait_for_released_budget(&first_usage).await;
        wait_for_released_budget(&second_usage).await;
        timeout(Duration::from_secs(5), server_task).await???;

        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn stream_stop_retries_only_its_lane_on_the_same_connection() -> Result<()> {
        const HEALTHY_LANE_THRESHOLD: Duration = Duration::from_millis(250);

        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "stop-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "stop-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (first_stopped_tx, first_stopped_rx) = oneshot::channel();
        let (observed_tx, mut observed_rx) = watch::channel(0u8);
        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("STOP test connection missing"))?
                .await?;
            let mut failed_stream = connection.accept_uni().await?;
            let mut first_chunk = vec![0u8; 24 + CHUNK_SIZE];
            failed_stream.read_exact(&mut first_chunk).await?;
            anyhow::ensure!(u64::from_le_bytes(first_chunk[0..8].try_into()?) == 250);
            anyhow::ensure!(u64::from_le_bytes(first_chunk[12..20].try_into()?) == 0);
            failed_stream.stop(quinn::VarInt::from_u32(7))?;
            drop(failed_stream);
            let _ = first_stopped_tx.send(());

            // Read the retried and healthy streams concurrently so the test harness itself does
            // not introduce head-of-line blocking between the two independent lanes.
            let mut readers = JoinSet::new();
            for _ in 0..2 {
                let mut stream =
                    timeout(Duration::from_secs(15), connection.accept_uni()).await??;
                let observed_tx = observed_tx.clone();
                readers.spawn(async move {
                    let frame = stream.read_to_end(1024 * 1024).await?;
                    let message_id = u64::from_le_bytes(frame[0..8].try_into()?);
                    let chunk_id = u64::from_le_bytes(frame[12..20].try_into()?);
                    anyhow::ensure!(chunk_id == 0, "every lane retry must start at chunk zero");
                    match message_id {
                        250 => observed_tx.send_modify(|observed| *observed |= 0b01),
                        251 => observed_tx.send_modify(|observed| *observed |= 0b10),
                        other => anyhow::bail!("unexpected STOP-isolation message {other}"),
                    }
                    Ok::<_, anyhow::Error>(message_id)
                });
            }
            let mut retry_seen = false;
            let mut healthy_seen = false;
            while let Some(result) = readers.join_next().await {
                let message_id = result??;
                retry_seen |= message_id == 250;
                healthy_seen |= message_id == 251;
            }
            anyhow::ensure!(retry_seen, "stopped lane was not retried");
            anyhow::ensure!(healthy_seen, "healthy lane did not progress");
            let _ = server_shutdown_rx.await;
            connection.close(quinn::VarInt::from_u32(0), b"STOP isolation complete");
            server.close(quinn::VarInt::from_u32(0), b"STOP isolation complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let failed_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, receivers) = NodeQueueSender::with_limits(3, 1, failed_bytes);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "stop-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));
        let (failed, failed_usage, failed_ack) = test_message_with_ack(250, 1, 1, failed_bytes);
        assert!(matches!(sender.try_send(failed), EnqueueResult::Sent));
        timeout(Duration::from_secs(5), first_stopped_rx).await??;

        let (healthy, healthy_usage) = test_message(251, 1, 2, 4);
        let started = Instant::now();
        assert!(matches!(sender.try_send(healthy), EnqueueResult::Sent));
        timeout(
            HEALTHY_LANE_THRESHOLD,
            observed_rx.wait_for(|observed| *observed & 0b10 != 0),
        )
        .await
        .expect("healthy lane must survive a sibling STOP")?;
        assert!(
            started.elapsed() <= HEALTHY_LANE_THRESHOLD,
            "healthy lane exceeded the fixed STOP-isolation threshold"
        );
        timeout(
            Duration::from_secs(15),
            observed_rx.wait_for(|observed| *observed & 0b01 != 0),
        )
        .await
        .expect("stopped lane must retry on the original connection")?;
        assert!(failed_ack.acknowledge());
        wait_for_released_budget(&failed_usage).await;
        wait_for_released_budget(&healthy_usage).await;

        let _ = server_shutdown_tx.send(());
        timeout(Duration::from_secs(5), server_task).await???;
        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn application_ack_cancels_a_stalled_replay_and_releases_its_lease() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "cancel-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "cancel-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (stream_accepted_tx, stream_accepted_rx) = oneshot::channel();
        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("cancellation test connection missing"))?
                .await?;
            let stalled_stream = connection.accept_uni().await?;
            let _ = stream_accepted_tx.send(());
            let _ = server_shutdown_rx.await;
            drop(stalled_stream);
            connection.close(quinn::VarInt::from_u32(0), b"cancellation test complete");
            server.close(quinn::VarInt::from_u32(0), b"cancellation test complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let stalled_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, receivers) = NodeQueueSender::with_limits(1, 1, stalled_bytes);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "cancel-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));
        let (message, usage, acknowledgement) = test_message_with_ack(275, 1, 1, stalled_bytes);
        assert!(matches!(sender.try_send(message), EnqueueResult::Sent));
        timeout(Duration::from_secs(5), stream_accepted_rx).await??;
        assert_eq!(usage.current_usage(), (1, stalled_bytes));

        assert!(acknowledgement.acknowledge());
        wait_for_released_budget(&usage).await;

        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        let _ = server_shutdown_tx.send(());
        timeout(Duration::from_secs(5), server_task).await???;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replay_deadline_resets_a_stalled_stream_and_releases_its_lease() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "deadline-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "deadline-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (stream_accepted_tx, stream_accepted_rx) = oneshot::channel();
        let (inspect_reset_tx, inspect_reset_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("deadline test connection missing"))?
                .await?;
            let mut stalled_stream = connection.accept_uni().await?;
            let _ = stream_accepted_tx.send(());
            let _ = inspect_reset_rx.await;
            let read = timeout(
                Duration::from_secs(2),
                stalled_stream.read_to_end(quic::QUIC_CONNECTION_RECEIVE_WINDOW_BYTES as usize * 8),
            )
            .await
            .map_err(|_| anyhow::anyhow!("deadline cancellation did not finish the stream"))?;
            anyhow::ensure!(
                read.is_err(),
                "deadline cancellation must reset, not finish, the stalled stream"
            );
            connection.close(quinn::VarInt::from_u32(0), b"deadline test complete");
            server.close(quinn::VarInt::from_u32(0), b"deadline test complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let connection = timeout(
            Duration::from_secs(5),
            client.try_connect_node(listen_addr, "deadline-server", 2, 1),
        )
        .await??;
        let stalled_bytes = quic::QUIC_CONNECTION_RECEIVE_WINDOW_BYTES as usize * 4;
        let (sender, mut receivers) = NodeQueueSender::with_limits(1, 1, stalled_bytes);
        let state = Arc::new(FairStreamState::new(receivers.remove(0)));
        let (mut message, usage, _acknowledgement) =
            test_message_with_ack(276, 1, 1, stalled_bytes);
        message
            .application_ack
            .as_mut()
            .expect("test message has an application acknowledgement")
            .set_replay_deadline_after(Duration::from_secs(2));
        assert!(matches!(sender.try_send(message), EnqueueResult::Sent));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let stream_task = tokio::spawn(fair_stream_task(state, connection, shutdown_rx));

        timeout(Duration::from_secs(2), stream_accepted_rx).await??;
        assert_eq!(usage.current_usage(), (1, stalled_bytes));
        timeout(Duration::from_secs(4), async {
            while usage.current_usage() != (0, 0) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deadline expiry must release every admission and outbound permit");
        let _ = inspect_reset_tx.send(());
        timeout(Duration::from_secs(5), server_task).await???;

        let _ = shutdown_tx.send(true);
        let _ = timeout(Duration::from_secs(2), stream_task).await??;
        drop(sender);
        drop(client);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn connection_loss_preserves_the_absolute_replay_deadline() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "loss-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "loss-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (first_stream_tx, first_stream_rx) = oneshot::channel();
        let (inspect_second_tx, inspect_second_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let first_connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("first loss-test connection missing"))?
                .await?;
            let first_stream = first_connection.accept_uni().await?;
            let _ = first_stream_tx.send(());
            first_connection.close(quinn::VarInt::from_u32(7), b"forced connection loss");
            drop(first_stream);

            let second_connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("second loss-test connection missing"))?
                .await?;
            let _ = inspect_second_rx.await;
            anyhow::ensure!(
                timeout(Duration::from_millis(500), second_connection.accept_uni())
                    .await
                    .is_err(),
                "an expired message must not open a replay stream after reconnect"
            );
            second_connection.close(quinn::VarInt::from_u32(0), b"loss test complete");
            server.close(quinn::VarInt::from_u32(0), b"loss test complete");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let first_connection = timeout(
            Duration::from_secs(5),
            client.try_connect_node(listen_addr, "loss-server", 2, 1),
        )
        .await??;
        let stalled_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, mut receivers) = NodeQueueSender::with_limits(1, 1, stalled_bytes);
        let state = Arc::new(FairStreamState::new(receivers.remove(0)));
        let (message, usage, _acknowledgement) = test_message_with_ack(277, 1, 1, stalled_bytes);
        assert!(matches!(sender.try_send(message), EnqueueResult::Sent));
        let (_first_shutdown_tx, first_shutdown_rx) = watch::channel(false);
        let first_task = tokio::spawn(fair_stream_task(
            state.clone(),
            first_connection,
            first_shutdown_rx,
        ));

        timeout(Duration::from_secs(2), first_stream_rx).await??;
        let first_exit = timeout(Duration::from_secs(5), first_task).await??;
        assert!(matches!(first_exit, StreamTaskExit::ConnectionFailed(_)));
        assert_eq!(usage.current_usage(), (1, stalled_bytes));
        {
            let mut retry_queue = state.retry_queue();
            let retry = retry_queue
                .front_mut()
                .expect("connection loss must retain the message for reconnect");
            let application_ack = retry
                .message
                .application_ack
                .as_mut()
                .expect("test message has an application acknowledgement");
            assert!(
                application_ack.replay_deadline_remaining().is_some(),
                "the first connected send must start the absolute replay deadline"
            );
            application_ack.set_replay_deadline_after(Duration::ZERO);
        }

        let second_connection = timeout(
            Duration::from_secs(5),
            client.try_connect_node(listen_addr, "loss-server", 2, 1),
        )
        .await??;
        let (second_shutdown_tx, second_shutdown_rx) = watch::channel(false);
        let second_task = tokio::spawn(fair_stream_task(
            state,
            second_connection,
            second_shutdown_rx,
        ));
        let _ = inspect_second_tx.send(());
        wait_for_released_budget(&usage).await;
        timeout(Duration::from_secs(5), server_task).await???;

        let _ = second_shutdown_tx.send(true);
        let _ = timeout(Duration::from_secs(2), second_task).await??;
        drop(sender);
        drop(client);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disconnected_manager_reaps_an_expired_retry_without_a_listener() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "reaper-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "reaper-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let (first_stream_tx, first_stream_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            let connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("reaper test connection missing"))?
                .await?;
            let stream = connection.accept_uni().await?;
            let _ = first_stream_tx.send(());
            connection.close(quinn::VarInt::from_u32(7), b"reaper test connection loss");
            drop(stream);
            server.close(quinn::VarInt::from_u32(7), b"reaper listener closed");
            Ok::<_, anyhow::Error>(())
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let stalled_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, receivers) = NodeQueueSender::with_limits(1, 1, stalled_bytes);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "reaper-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));
        let (mut message, usage, acknowledgement) = test_message_with_ack(278, 1, 1, stalled_bytes);
        message
            .application_ack
            .as_mut()
            .expect("test message has an application acknowledgement")
            .set_replay_deadline_after(Duration::from_secs(1));
        assert!(matches!(sender.try_send(message), EnqueueResult::Sent));

        timeout(Duration::from_secs(2), first_stream_rx).await??;
        timeout(Duration::from_secs(3), async {
            while usage.current_usage() != (0, 0) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("disconnected retry expiry must release every retained permit");
        assert!(
            !acknowledgement.acknowledge(),
            "dropping the expired retry must unregister its application ACK"
        );

        timeout(Duration::from_secs(2), server_task).await???;
        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn real_quic_reconnect_replays_message_from_chunk_zero() -> Result<()> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.certificate_pem().to_owned();
        let (server_cert, server_key) = signed_test_node(&root, "replay-server", 2)?;
        let (client_cert, client_key) = signed_test_node(&root, "replay-client", 1)?;
        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server = quic::new_quic_server(
            server_addr,
            vec![server_cert, ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server.local_addr()?;
        let server_task = tokio::spawn(async move {
            let first_connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("first test connection missing"))?
                .await?;
            let mut first_stream = first_connection.accept_uni().await?;
            let mut first_chunk = vec![0u8; 24 + CHUNK_SIZE];
            first_stream.read_exact(&mut first_chunk).await?;
            assert_eq!(u64::from_le_bytes(first_chunk[12..20].try_into()?), 0);
            first_stream.stop(quinn::VarInt::from_u32(7))?;
            first_connection.close(quinn::VarInt::from_u32(7), b"inject reconnect");

            let second_connection = server
                .accept()
                .await
                .ok_or_else(|| anyhow::anyhow!("replay connection missing"))?
                .await?;
            let mut replay_stream = second_connection.accept_uni().await?;
            let replay = replay_stream.read_to_end(1024 * 1024).await?;
            second_connection.close(quinn::VarInt::from_u32(0), b"replay complete");
            server.close(quinn::VarInt::from_u32(0), b"replay complete");
            Ok::<_, anyhow::Error>(replay)
        });

        let client = quic::new_quic_client(&ca_pem, &client_cert, &client_key)?;
        let message_bytes = quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES as usize * 2;
        let (sender, receivers) = NodeQueueSender::with_limits(1, 1, message_bytes);
        let manager_task = tokio::spawn(fair_node_connection_manager(FairNodeConnectionManager {
            node_info: lunatic_control::NodeInfo {
                id: 2,
                address: listen_addr,
                name: "replay-server".to_string(),
            },
            client,
            message_streams: receivers,
        }));
        let (message, usage) = test_message(200, 1, 1, message_bytes);
        assert!(matches!(sender.try_send(message), EnqueueResult::Sent));

        let replay = timeout(Duration::from_secs(10), server_task).await???;
        assert_eq!(
            replay.len(),
            (message_bytes / CHUNK_SIZE) * (24 + CHUNK_SIZE)
        );
        assert_eq!(u64::from_le_bytes(replay[0..8].try_into()?), 200);
        assert_eq!(u64::from_le_bytes(replay[12..20].try_into()?), 0);
        let second_header = 24 + CHUNK_SIZE;
        assert_eq!(
            u64::from_le_bytes(replay[second_header + 12..second_header + 20].try_into()?),
            1
        );
        wait_for_released_budget(&usage).await;

        manager_task.abort();
        let _ = manager_task.await;
        drop(sender);
        Ok(())
    }

    #[tokio::test]
    async fn stream_buffer_blocks_at_capacity_until_writer_completes() {
        let buffer = Arc::new(BoundedBuffer::new(2));
        buffer.push(1_u64).await;
        buffer.push(2_u64).await;

        let mut blocked_push = tokio::spawn({
            let buffer = buffer.clone();
            async move { buffer.push(3_u64).await }
        });
        assert!(timeout(Duration::from_millis(20), &mut blocked_push)
            .await
            .is_err());

        let in_flight = buffer.pop_oldest().await.expect("first buffered chunk");
        assert_eq!(in_flight.value, 1);
        assert!(timeout(Duration::from_millis(20), &mut blocked_push)
            .await
            .is_err());

        // Capacity includes the chunk retained by a stalled writer.
        drop(in_flight);
        timeout(Duration::from_secs(1), blocked_push)
            .await
            .expect("push wakes after writer completion")
            .expect("push task succeeds");
        assert_eq!(buffer.len().await, 2);
    }

    #[tokio::test]
    async fn failed_chunk_is_retried_before_later_chunks() {
        let buffer = BoundedBuffer::new(2);
        buffer.push(10_u64).await;
        buffer.push(20_u64).await;

        let failed = buffer.pop_oldest().await.expect("oldest chunk");
        assert_eq!(failed.value, 10);
        buffer.retry_oldest(failed).await;

        assert_eq!(buffer.pop_oldest().await.unwrap().value, 10);
        assert_eq!(buffer.pop_oldest().await.unwrap().value, 20);
    }

    #[tokio::test]
    async fn outbound_lease_survives_chunk_buffer_and_retry_until_drop() {
        let (message_lease, usage) = test_outbound_lease(4);
        let chunk = MessageChunk {
            src: ProcessId(1),
            dest: ProcessId(2),
            message_id: 3,
            message_size: 4,
            chunk_id: 0,
            data: Bytes::from_static(b"data"),
            _budget: message_lease.clone(),
        };
        drop(message_lease);

        let buffer = BoundedBuffer::new(1);
        buffer.push(chunk).await;
        assert_eq!(usage.current_usage(), (1, 4));

        let in_flight = buffer.pop_oldest().await.expect("writer owns chunk");
        assert_eq!(usage.current_usage(), (1, 4));
        buffer.retry_oldest(in_flight).await;
        assert_eq!(usage.current_usage(), (1, 4));

        drop(buffer.pop_oldest().await);
        assert_eq!(usage.current_usage(), (0, 0));
    }

    #[test]
    fn removed_node_is_not_retried_and_releases_its_lease() {
        let (lease, usage) = test_outbound_lease(8);
        let action = node_recovery_action(SendErrorKind::NodeNotFound, || lease);
        assert!(matches!(&action, NodeRecoveryAction::Drop(_)));
        assert_eq!(usage.current_usage(), (1, 8));
        drop(action);
        assert_eq!(usage.current_usage(), (0, 0));

        let retry: NodeRecoveryAction<()> =
            node_recovery_action(SendErrorKind::QueueClosed, || {
                panic!("retry must not remove the current message")
            });
        assert!(matches!(retry, NodeRecoveryAction::Retry));
    }

    #[tokio::test]
    async fn full_node_queue_returns_final_chunk_for_retry() {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        assert_eq!(try_enqueue(&sender, 1_u64), EnqueueResult::Sent);
        assert_eq!(try_enqueue(&sender, 2_u64), EnqueueResult::Full(2));

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(try_enqueue(&sender, 2_u64), EnqueueResult::Sent);
        assert_eq!(receiver.recv().await, Some(2));

        drop(receiver);
        assert_eq!(try_enqueue(&sender, 3_u64), EnqueueResult::Closed(3));
    }

    #[test]
    fn empty_worker_waits_even_when_no_environment_exists() {
        assert_eq!(
            next_worker_action(false, true),
            WorkerAction::WaitForMessage
        );
        assert_eq!(
            next_worker_action(false, false),
            WorkerAction::RetryUnavailableNode
        );
        assert_eq!(next_worker_action(true, true), WorkerAction::Continue);
    }
}
