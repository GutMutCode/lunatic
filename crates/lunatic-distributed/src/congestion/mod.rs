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
    sync::{atomic, Arc},
    time::{Duration, Instant},
};

use anyhow::Result;
use lunatic_control::NodeInfo;
use tokio::sync::{
    mpsc::{self, error::TryRecvError, error::TrySendError, Receiver, Sender},
    Mutex, OwnedSemaphorePermit, Semaphore,
};

use crate::{
    distributed::{
        self,
        client::{OutboundMessageLease, ProcessId, SendErrorKind},
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

fn try_enqueue<T>(sender: &Sender<T>, value: T) -> EnqueueResult<T> {
    match sender.try_send(value) {
        Ok(()) => EnqueueResult::Sent,
        Err(TrySendError::Full(value)) => EnqueueResult::Full(value),
        Err(TrySendError::Closed(value)) => EnqueueResult::Closed(value),
    }
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

        for (env_id, process_id, queue_generation) in process_queues {
            let key = (env_id, process_id);
            let retry_key = (env_id, process_id, queue_generation);
            if retry_after
                .get(&retry_key)
                .is_some_and(|deadline| *deadline > Instant::now())
            {
                continue;
            }
            retry_after.remove(&retry_key);
            let finished = if let Some(msg_ctx) = state.inner.in_progress.get(&key) {
                if msg_ctx.queue_generation != queue_generation {
                    continue;
                }
                // Chunk data using offset
                let offset = msg_ctx.offset.load(atomic::Ordering::Relaxed);
                let chunk_id = msg_ctx.chunk_id.load(atomic::Ordering::Relaxed);
                let (data, finished) = if msg_ctx.data.len() <= offset + CHUNK_SIZE {
                    // Chunk will be finished after this write
                    (msg_ctx.data.slice(offset..), true)
                } else {
                    (msg_ctx.data.slice(offset..offset + CHUNK_SIZE), false)
                };
                // Create chunk
                let chunk = MessageChunk {
                    src: msg_ctx.src,
                    dest: msg_ctx.dest,
                    message_id: msg_ctx.message_id.0,
                    message_size: msg_ctx.data.len() as u32,
                    chunk_id,
                    data,
                    _budget: msg_ctx.outbound_lease.clone(),
                };
                let node_queue = state
                    .inner
                    .nodes_queues
                    .get(&msg_ctx.node)
                    .map(|queue| queue.sender());
                let Some(node_queue) = node_queue else {
                    let node = msg_ctx.node;
                    let message_id = msg_ctx.message_id;
                    drop(msg_ctx);
                    match state.ensure_node_queue(node).await {
                        Ok(_) => made_progress = true,
                        Err(error) => {
                            let now = Instant::now();
                            match node_recovery_action(error.kind(), || {
                                state.inner.in_progress.remove_if(&key, |_, message| {
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
                match try_enqueue(&node_queue, chunk) {
                    EnqueueResult::Sent => {
                        made_progress = true;
                        log::trace!(
                            "congestion::chunk::sent message_id={} chunk_id={chunk_id}",
                            msg_ctx.message_id.0
                        );
                        // Move to next chunk
                        msg_ctx
                            .offset
                            .store(offset + CHUNK_SIZE, atomic::Ordering::Relaxed);
                        msg_ctx
                            .chunk_id
                            .store(chunk_id + 1, atomic::Ordering::Relaxed);
                        finished
                    }
                    EnqueueResult::Full(_chunk) => {
                        retry_after.insert(retry_key, Instant::now() + CLOSED_NODE_RETRY_DELAY);
                        false
                    }
                    EnqueueResult::Closed(_chunk) => {
                        log::warn!(
                                    "Cannot send next chunk from pid={} to node={} dest_pid={}: node queue closed",
                                    msg_ctx.src.0,
                                    msg_ctx.node.0,
                                    msg_ctx.dest.0,
                                );
                        if let Some((_, stale)) = state
                            .inner
                            .nodes_queues
                            .remove_if(&msg_ctx.node, |_, queue| {
                                queue.sender().same_channel(&node_queue)
                            })
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
                state.inner.in_progress.remove_if(&key, |_, message| {
                    message.queue_generation == queue_generation
                });
                let (receive_result, admission_guard) = {
                    let Some(env_queue) = state.inner.buf_rx.get(&env_id) else {
                        continue;
                    };
                    let Some(receiver) = env_queue.get(&process_id) else {
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
                        let new_key = (new_msg_ctx.env, new_msg_ctx.src);
                        if let Some(sender) = state.inner.buf_tx.get(&new_key) {
                            if sender.generation == new_msg_ctx.queue_generation {
                                state.inner.in_progress.insert(new_key, new_msg_ctx);
                            }
                        }
                        drop(admission_guard);
                    }
                    // No new messages
                    Err(TryRecvError::Empty) => {
                        if state.remove_idle_process_resources_if_generation(
                            env_id,
                            process_id,
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
                            process_id,
                            queue_generation,
                        );
                        drop(admission_guard);
                    }
                };
            }
        }

        match next_worker_action(made_progress, state.inner.in_progress.is_empty()) {
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
            .try_connect(node_info.address, &node_info.name, 3)
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use tokio::time::{timeout, Duration};

    use super::{
        next_worker_action, node_recovery_action, try_enqueue, BoundedBuffer, EnqueueResult,
        MessageChunk, NodeRecoveryAction, WorkerAction,
    };
    use crate::distributed::client::{test_outbound_lease, ProcessId, SendErrorKind};

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
