use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use lunatic_process::{env::Environment, state::ProcessState};
use quinn::{
    crypto::rustls::{QuicClientConfig, QuicServerConfig},
    ClientConfig, Connection, ConnectionError, Endpoint, Incoming, ServerConfig, TransportConfig,
    VarInt,
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    RootCertStore,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use wasmtime::ResourceLimiter;
use x509_parser::{der_parser::oid, oid_registry::asn1_rs::Utf8String, prelude::FromDer};

use crate::{distributed, CertAttrs, DistributedCtx};

pub const MESSAGE_CHUNK_SIZE: usize = 1024;

/// Maximum serialized distributed request accepted by the QUIC transport.
///
/// This transport ceiling is deliberately finite and larger than the default process-message
/// limit because it also carries spawn configuration and registry traffic. Destination-specific
/// limits are still enforced after decoding.
pub const MAX_WIRE_MESSAGE_BYTES: usize = 8 * 1024 * 1024;

/// Maximum number of partially received or currently dispatched messages retained by one server.
pub const MAX_IN_FLIGHT_MESSAGES: usize = 64;

/// Maximum reassembly buffer bytes retained by one server across all connections and streams.
///
/// Admission initially reserves declared size, rather than bytes received so far, so a peer cannot
/// pin many nearly empty messages that advertise a large eventual size. If the allocator gives a
/// buffer additional capacity, that capacity is charged before the buffer is retained.
pub const MAX_IN_FLIGHT_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

/// Maximum active connection and handshake tasks for one server.
pub const MAX_CONCURRENT_CONNECTIONS: usize = 64;

/// Maximum request-stream tasks active across all connections to one server.
pub const MAX_CONCURRENT_REQUEST_STREAMS: usize = 64;

/// Maximum QUIC flow-control credit advertised for one incoming request stream.
pub const QUIC_STREAM_RECEIVE_WINDOW_BYTES: u32 = 64 * 1024;

/// Maximum aggregate QUIC flow-control credit advertised by one connection.
///
/// Across [`MAX_CONCURRENT_CONNECTIONS`], this is at most the server-wide in-flight byte budget.
pub const QUIC_CONNECTION_RECEIVE_WINDOW_BYTES: u32 =
    (MAX_IN_FLIGHT_MESSAGE_BYTES / MAX_CONCURRENT_CONNECTIONS) as u32;

/// Maximum time allowed to receive a complete chunk header or body.
///
/// The deadline applies to each complete read, rather than resetting for individual bytes, so it
/// also bounds slow-header and slow-body attacks. Abandoned partial-message reservations are
/// released when the timed-out stream task exits.
pub const REQUEST_STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum wall-clock time a partial message may retain a reassembly reservation.
///
/// Congestion streams can carry multiple interleaved messages for their entire connection
/// lifetime, so this deadline is tracked per message from admission of its first chunk.
pub const REQUEST_MESSAGE_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(60);

const MESSAGE_CHUNK_HEADER_SIZE: usize = 24;
const STREAM_REJECTED_ERROR_CODE: u32 = 1;
const MAX_MESSAGEPACK_NESTING: usize = 64;
// A sequence slot must cover owned scalar values such as `String` and `Vec<u8>` (24 bytes on
// 64-bit targets), not just pointer-sized values. Map entries need room for both key and value.
const DECODED_SEQUENCE_SLOT_BYTES: usize = 32;
const DECODED_MAP_ENTRY_BYTES: usize = 64;
const DECODED_CONTAINER_OVERHEAD_BYTES: usize = 24;

#[derive(Clone)]
struct ServerAdmission {
    connections: Arc<Semaphore>,
    streams: Arc<Semaphore>,
}

impl ServerAdmission {
    fn new(max_connections: usize, max_streams: usize) -> Self {
        Self {
            connections: Arc::new(Semaphore::new(max_connections)),
            streams: Arc::new(Semaphore::new(max_streams)),
        }
    }

    fn try_admit_connection(&self) -> Option<OwnedSemaphorePermit> {
        self.connections.clone().try_acquire_owned().ok()
    }

    fn try_admit_stream(&self) -> Option<OwnedSemaphorePermit> {
        self.streams.clone().try_acquire_owned().ok()
    }
}

impl Default for ServerAdmission {
    fn default() -> Self {
        Self::new(MAX_CONCURRENT_CONNECTIONS, MAX_CONCURRENT_REQUEST_STREAMS)
    }
}

fn quic_receive_window_limits() -> (u32, u32) {
    (
        QUIC_STREAM_RECEIVE_WINDOW_BYTES,
        QUIC_CONNECTION_RECEIVE_WINDOW_BYTES,
    )
}

fn outbound_only_client_transport_config() -> Arc<TransportConfig> {
    let mut transport = TransportConfig::default();
    transport
        // Distributed client connections only originate unidirectional request streams. Peers
        // deliver their own requests over separately authenticated connections to this node's
        // server endpoint.
        .max_concurrent_bidi_streams(VarInt::from_u32(0))
        .max_concurrent_uni_streams(VarInt::from_u32(0))
        .stream_receive_window(VarInt::from_u32(QUIC_STREAM_RECEIVE_WINDOW_BYTES))
        .receive_window(VarInt::from_u32(QUIC_STREAM_RECEIVE_WINDOW_BYTES))
        .datagram_receive_buffer_size(None);
    Arc::new(transport)
}

#[derive(Clone)]
pub struct Client {
    inner: Endpoint,
}

impl Client {
    pub async fn _connect(&self, addr: SocketAddr, name: &str) -> Result<quinn::Connection> {
        Ok(self.inner.connect(addr, name)?.await?)
    }

    pub async fn try_connect(
        &self,
        addr: SocketAddr,
        name: &str,
        retry: u32,
    ) -> Result<quinn::Connection> {
        for try_num in 1..(retry + 1) {
            match self._connect(addr, name).await {
                Ok(conn) => return Ok(conn),
                Err(e) => {
                    log::error!("Error connecting to {name} at {addr}, try {try_num}. Error: {e}")
                }
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Err(anyhow!("Failed to connect to {name} at {addr}"))
    }

    /// Send one complete distributed protocol message over an authenticated QUIC stream.
    pub(crate) async fn send_message(
        &self,
        addr: SocketAddr,
        name: &str,
        message_id: u64,
        data: Bytes,
    ) -> Result<()> {
        let conn = self._connect(addr, name).await?;
        let mut stream = conn.open_uni().await?;
        write_message(&mut stream, message_id, data).await?;
        stream.finish()?;
        if let Some(error_code) = stream.stopped().await? {
            return Err(anyhow!(
                "Peer stopped distributed message stream with error code {error_code}"
            ));
        }
        Ok(())
    }
}

pub(crate) fn frame_message_chunk(
    message_id: u64,
    message_size: u32,
    chunk_id: u64,
    data: Bytes,
) -> [Bytes; 2] {
    let mut header = Vec::with_capacity(24);
    header.extend_from_slice(&message_id.to_le_bytes());
    header.extend_from_slice(&message_size.to_le_bytes());
    header.extend_from_slice(&chunk_id.to_le_bytes());
    header.extend_from_slice(&(data.len() as u32).to_le_bytes());
    [Bytes::from(header), data]
}

/// Write a serialized distributed request using the production chunk framing.
pub async fn write_message(
    stream: &mut quinn::SendStream,
    message_id: u64,
    data: Bytes,
) -> Result<()> {
    validate_wire_message_size(data.len())?;
    let message_size = data.len() as u32;
    let chunk_count = data.len().max(1).div_ceil(MESSAGE_CHUNK_SIZE);
    let mut framed = Vec::with_capacity(chunk_count * 2);

    if data.is_empty() {
        framed.extend(frame_message_chunk(message_id, message_size, 0, data));
    } else {
        for (chunk_id, offset) in (0..data.len()).step_by(MESSAGE_CHUNK_SIZE).enumerate() {
            let end = (offset + MESSAGE_CHUNK_SIZE).min(data.len());
            framed.extend(frame_message_chunk(
                message_id,
                message_size,
                chunk_id as u64,
                data.slice(offset..end),
            ));
        }
    }

    stream.write_all_chunks(&mut framed).await?;
    Ok(())
}

fn validate_wire_message_size(message_size: usize) -> Result<()> {
    if message_size > MAX_WIRE_MESSAGE_BYTES {
        return Err(anyhow!(
            "Distributed message size {message_size} exceeds the QUIC transport limit of \
             {MAX_WIRE_MESSAGE_BYTES} bytes"
        ));
    }
    Ok(())
}

fn get_cert_attrs(conn: &Connection) -> Result<CertAttrs> {
    let peer_identity = match conn
        .peer_identity()
        .ok_or(anyhow!("Peer must provide an identity."))?
        .downcast::<Vec<CertificateDer<'static>>>()
    {
        Ok(certs) => Ok(certs),
        Err(_) => Err(anyhow!("Failed to downcast peer identity.")),
    }?;
    if peer_identity.len() != 1 {
        return Err(anyhow!("More than one identity certificate detected."));
    }
    let cert = peer_identity
        .first()
        .ok_or_else(|| anyhow!("Peer identity certificate is missing."))?;
    let (_rem, x509) = x509_parser::certificate::X509Certificate::from_der(cert.as_ref())?;
    let oid = oid!(2.5.29 .9);
    let ext = x509
        .get_extension_unique(&oid)?
        .ok_or_else(|| anyhow!("Missing critical Lunatic certificate extension."))?;
    let (_rem, value) = Utf8String::from_der(ext.value)?;
    Ok(serde_json::from_str(&value.string())?)
}

fn read_certificate(pem: &str) -> Result<CertificateDer<'static>> {
    let mut reader = pem.as_bytes();
    let mut certificates = rustls_pemfile::certs(&mut reader);
    let certificate = certificates
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("Certificate PEM is empty or malformed."))?;
    if certificates.next().transpose()?.is_some() {
        return Err(anyhow!("Expected exactly one certificate."));
    }
    Ok(certificate)
}

fn read_private_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader = pem.as_bytes();
    let private_key = rustls_pemfile::private_key(&mut reader)?
        .ok_or_else(|| anyhow!("Private key PEM is empty or malformed."))?;
    if rustls_pemfile::private_key(&mut reader)?.is_some() {
        return Err(anyhow!("Expected exactly one private key."));
    }
    Ok(private_key)
}

pub fn new_quic_client(ca_cert: &str, cert: &str, key: &str) -> Result<Client> {
    let ca_cert = read_certificate(ca_cert)?;
    let mut roots = RootCertStore::empty();
    roots.add(ca_cert)?;

    let pk = read_private_key(key)?;
    let cert = read_certificate(cert)?;
    let cert = vec![cert];

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client_crypto = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_root_certificates(roots)
        .with_client_auth_cert(cert, pk)?;

    let mut client_config = ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    client_config.transport_config(outbound_only_client_transport_config());
    let mut endpoint = Endpoint::client("[::]:0".parse().unwrap())?;
    endpoint.set_default_client_config(client_config);
    Ok(Client { inner: endpoint })
}

pub fn new_quic_server(
    addr: SocketAddr,
    certs: Vec<String>,
    key: &str,
    ca_cert: &str,
) -> Result<Endpoint> {
    let ca_cert = read_certificate(ca_cert)?;
    let mut roots = RootCertStore::empty();
    roots.add(ca_cert)?;

    let pk = read_private_key(key)?;

    let mut cert_chain = Vec::new();
    for (i, cert) in certs.iter().enumerate() {
        let cert = read_certificate(cert)?;
        if i != 0 {
            roots.add(cert.clone())?;
        }
        cert_chain.push(cert);
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone()).build()?;
    let server_crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain, pk)?;
    let mut server_config =
        ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport = Arc::get_mut(&mut server_config.transport).unwrap();
    let (stream_receive_window, connection_receive_window) = quic_receive_window_limits();
    transport
        .keep_alive_interval(Some(Duration::from_millis(100)))
        // The protocol only accepts unidirectional request streams.
        .max_concurrent_bidi_streams(VarInt::from_u32(0))
        .max_concurrent_uni_streams(VarInt::from_u32(MAX_CONCURRENT_REQUEST_STREAMS as u32))
        // Quinn's aggregate receive-window default is VarInt::MAX. Explicit windows keep bytes
        // buffered below the application framing layer within a predictable server-wide bound.
        .stream_receive_window(VarInt::from_u32(stream_receive_window))
        .receive_window(VarInt::from_u32(connection_receive_window))
        // This protocol never consumes QUIC datagrams, so do not advertise or allocate a queue.
        .datagram_receive_buffer_size(None);

    Ok(quinn::Endpoint::server(server_config, addr)?)
}

pub async fn handle_node_server<T, E>(
    quic_server: &mut Endpoint,
    ctx: distributed::server::ServerCtx<T, E>,
) -> Result<()>
where
    T: ProcessState
        + ResourceLimiter
        + DistributedCtx<E>
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    let receive_budget = Arc::new(ReceiveBudget::default());
    let admission = ServerAdmission::default();
    while let Some(conn) = quic_server.accept().await {
        let Some(connection_permit) = admission.try_admit_connection() else {
            log::warn!(
                "Refusing QUIC connection from {}: active connection limit reached",
                conn.remote_address()
            );
            conn.refuse();
            continue;
        };
        tokio::spawn(handle_quic_connection_node(
            ctx.clone(),
            conn,
            receive_budget.clone(),
            admission.clone(),
            connection_permit,
        ));
    }
    Err(anyhow!("Node server exited"))
}

/// Run the registry-only side of the node protocol.
///
/// This uses the same mutual-TLS connection, framing, and `Request` decoding as
/// the full node server and is useful for focused cluster tests and registry-only
/// deployments.
pub async fn handle_registry_server(
    quic_server: &mut Endpoint,
    client: distributed::Client,
) -> Result<()> {
    let receive_budget = Arc::new(ReceiveBudget::default());
    let admission = ServerAdmission::default();
    while let Some(conn) = quic_server.accept().await {
        let Some(connection_permit) = admission.try_admit_connection() else {
            log::warn!(
                "Refusing registry QUIC connection from {}: active connection limit reached",
                conn.remote_address()
            );
            conn.refuse();
            continue;
        };
        let client = client.clone();
        let receive_budget = receive_budget.clone();
        let admission = admission.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_quic_connection_registry(
                client,
                conn,
                receive_budget,
                admission,
                connection_permit,
            )
            .await
            {
                log::warn!("Registry QUIC connection failed: {error}");
            }
        });
    }
    Err(anyhow!("Registry server exited"))
}

async fn handle_quic_connection_registry(
    client: distributed::Client,
    conn: Incoming,
    receive_budget: Arc<ReceiveBudget>,
    admission: ServerAdmission,
    _connection_permit: OwnedSemaphorePermit,
) -> Result<()> {
    let conn = conn.await?;
    get_cert_attrs(&conn)?;
    loop {
        match conn.accept_uni().await {
            Ok(mut recv) => {
                let Some(stream_permit) = admission.try_admit_stream() else {
                    log::warn!("Rejecting registry QUIC stream: active stream limit reached");
                    let _ = recv.stop(VarInt::from_u32(STREAM_REJECTED_ERROR_CODE));
                    continue;
                };
                tokio::spawn(handle_quic_stream_registry(
                    client.clone(),
                    recv,
                    receive_budget.clone(),
                    stream_permit,
                ));
            }
            Err(ConnectionError::LocallyClosed) => break,
            Err(_) => break,
        }
    }
    Ok(())
}

async fn handle_quic_stream_registry(
    client: distributed::Client,
    recv: quinn::RecvStream,
    receive_budget: Arc<ReceiveBudget>,
    _stream_permit: OwnedSemaphorePermit,
) {
    handle_request_stream_with_budget(
        recv,
        receive_budget,
        REQUEST_STREAM_IDLE_TIMEOUT,
        move |_msg_id, request| {
            let client = client.clone();
            async move {
                match request {
                    distributed::message::Request::Registry { node_id, message } => {
                        if let Err(error) = client.handle_registry_message(node_id, message).await {
                            log::warn!("Error handling registry coordination message: {error}");
                        }
                    }
                    other => {
                        log::debug!(
                            "Registry-only server ignored {} distributed request",
                            other.kind()
                        );
                    }
                }
            }
        },
    )
    .await;
}

pub struct NodeEnvPermission(pub Option<HashSet<u64>>);

impl NodeEnvPermission {
    fn new(cert_attrs: CertAttrs) -> Self {
        let some_set: Option<HashSet<u64>> = if cert_attrs.is_privileged {
            None
        } else {
            Some(cert_attrs.allowed_envs.into_iter().collect())
        };
        Self(some_set)
    }
}

async fn handle_quic_connection_node<T, E>(
    ctx: distributed::server::ServerCtx<T, E>,
    conn: Incoming,
    receive_budget: Arc<ReceiveBudget>,
    admission: ServerAdmission,
    _connection_permit: OwnedSemaphorePermit,
) -> Result<()>
where
    T: ProcessState
        + ResourceLimiter
        + DistributedCtx<E>
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    log::info!("New node connection");
    let conn = conn.await?;
    let node_cert_attrs = get_cert_attrs(&conn)?;
    let node_permissions = Arc::new(NodeEnvPermission::new(node_cert_attrs));
    log::info!("Remote {} connected", conn.remote_address());
    loop {
        if let Some(reason) = conn.close_reason() {
            log::info!("Connection {} is closed: {reason}", conn.remote_address());
            break;
        }
        let stream = conn.accept_uni().await;
        log::info!("Stream from remote {} accepted", conn.remote_address());
        match stream {
            Ok(mut recv) => {
                let Some(stream_permit) = admission.try_admit_stream() else {
                    log::warn!("Rejecting node QUIC stream: active stream limit reached");
                    let _ = recv.stop(VarInt::from_u32(STREAM_REJECTED_ERROR_CODE));
                    continue;
                };
                tokio::spawn(handle_quic_stream_node(
                    ctx.clone(),
                    recv,
                    node_permissions.clone(),
                    receive_budget.clone(),
                    stream_permit,
                ));
            }
            Err(ConnectionError::LocallyClosed) => {
                log::trace!("distributed::server::stream locally closed");
                break;
            }
            Err(_) => {}
        }
    }
    log::info!("Connection from remote {} closed", conn.remote_address());
    Ok(())
}

async fn handle_quic_stream_node<T, E>(
    ctx: distributed::server::ServerCtx<T, E>,
    recv: quinn::RecvStream,
    node_permissions: Arc<NodeEnvPermission>,
    receive_budget: Arc<ReceiveBudget>,
    _stream_permit: OwnedSemaphorePermit,
) where
    T: ProcessState
        + ResourceLimiter
        + DistributedCtx<E>
        + Send
        + Sync
        + lunatic_process::reloadable_state::ReloadableState
        + 'static,
    E: Environment + 'static,
{
    log::trace!("distributed::server::handle_quic_stream started");
    handle_request_stream_with_budget(
        recv,
        receive_budget,
        REQUEST_STREAM_IDLE_TIMEOUT,
        move |msg_id, request| {
            let ctx = ctx.clone();
            let node_permissions = node_permissions.clone();
            async move {
                distributed::server::handle_message(ctx, msg_id, request, node_permissions).await;
            }
        },
    )
    .await;
    log::trace!("distributed::server::handle_quic_stream finished");
}

#[derive(Debug)]
struct Chunk {
    message_id: u64,
    message_size: usize,
    chunk_id: u64,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct ReceiveBudgetUsage {
    messages: usize,
    bytes: usize,
}

/// Bounds serialized messages retained before destination-specific limits can be applied.
struct ReceiveBudget {
    max_messages: usize,
    max_bytes: usize,
    usage: Mutex<ReceiveBudgetUsage>,
}

impl ReceiveBudget {
    fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            max_messages,
            max_bytes,
            usage: Mutex::new(ReceiveBudgetUsage::default()),
        }
    }

    fn try_reserve(self: &Arc<Self>, message_size: usize) -> Result<ReceiveReservation> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if usage.messages >= self.max_messages {
            return Err(anyhow!(
                "QUIC transport in-flight message limit {} reached",
                self.max_messages
            ));
        }
        let next_bytes = usage
            .bytes
            .checked_add(message_size)
            .ok_or_else(|| anyhow!("QUIC transport in-flight byte accounting overflow"))?;
        if next_bytes > self.max_bytes {
            return Err(anyhow!(
                "QUIC transport in-flight byte limit {} exceeded by message of {} bytes",
                self.max_bytes,
                message_size
            ));
        }

        usage.messages += 1;
        usage.bytes = next_bytes;
        drop(usage);
        Ok(ReceiveReservation {
            budget: self.clone(),
            bytes: message_size,
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
            .expect("QUIC receive message accounting cannot underflow");
        usage.bytes = usage
            .bytes
            .checked_sub(bytes)
            .expect("QUIC receive byte accounting cannot underflow");
    }

    fn try_grow_reservation(&self, additional_bytes: usize) -> Result<()> {
        let mut usage = self
            .usage
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let next_bytes = usage
            .bytes
            .checked_add(additional_bytes)
            .ok_or_else(|| anyhow!("QUIC transport in-flight byte accounting overflow"))?;
        if next_bytes > self.max_bytes {
            return Err(anyhow!(
                "QUIC transport in-flight byte limit {} exceeded by allocation growth of {} bytes",
                self.max_bytes,
                additional_bytes
            ));
        }
        usage.bytes = next_bytes;
        Ok(())
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

impl Default for ReceiveBudget {
    fn default() -> Self {
        Self::new(MAX_IN_FLIGHT_MESSAGES, MAX_IN_FLIGHT_MESSAGE_BYTES)
    }
}

struct ReceiveReservation {
    budget: Arc<ReceiveBudget>,
    bytes: usize,
}

impl ReceiveReservation {
    fn account_buffer_capacity(&mut self, capacity: usize) -> Result<()> {
        if capacity > self.bytes {
            self.try_charge_additional(capacity - self.bytes)?;
        }
        Ok(())
    }

    fn try_charge_additional(&mut self, additional_bytes: usize) -> Result<()> {
        self.budget.try_grow_reservation(additional_bytes)?;
        self.bytes = self
            .bytes
            .checked_add(additional_bytes)
            .expect("QUIC receive reservation accounting cannot overflow");
        Ok(())
    }
}

impl Drop for ReceiveReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

struct PartialMessage {
    message_size: usize,
    next_chunk_id: u64,
    deadline: Instant,
    data: Vec<u8>,
    reservation: ReceiveReservation,
}

struct ReceivedMessage {
    message_id: u64,
    data: Bytes,
    reservation: ReceiveReservation,
}

struct MessageReassembler {
    chunks: HashMap<u64, PartialMessage>,
    receive_budget: Arc<ReceiveBudget>,
    reassembly_timeout: Duration,
}

impl MessageReassembler {
    fn new(receive_budget: Arc<ReceiveBudget>) -> Self {
        Self::with_timeout(receive_budget, REQUEST_MESSAGE_REASSEMBLY_TIMEOUT)
    }

    fn with_timeout(receive_budget: Arc<ReceiveBudget>, reassembly_timeout: Duration) -> Self {
        Self {
            chunks: HashMap::new(),
            receive_budget,
            reassembly_timeout,
        }
    }

    fn push_chunk(&mut self, chunk: Chunk) -> Result<Option<ReceivedMessage>> {
        self.push_chunk_at(chunk, Instant::now())
    }

    fn push_chunk_at(&mut self, chunk: Chunk, now: Instant) -> Result<Option<ReceivedMessage>> {
        if let Some(message_id) = self
            .chunks
            .iter()
            .find_map(|(message_id, partial)| (now >= partial.deadline).then_some(*message_id))
        {
            self.chunks.clear();
            return Err(anyhow!(
                "QUIC message {message_id} exceeded the {:?} reassembly deadline",
                self.reassembly_timeout
            ));
        }

        if let Err(error) =
            validate_chunk_header(chunk.message_size, chunk.chunk_id, chunk.data.len())
        {
            self.chunks.remove(&chunk.message_id);
            return Err(error);
        }

        match self.chunks.entry(chunk.message_id) {
            Entry::Vacant(entry) => {
                if chunk.chunk_id != 0 {
                    return Err(anyhow!(
                        "Message {} starts with chunk {}, expected chunk 0",
                        chunk.message_id,
                        chunk.chunk_id
                    ));
                }

                let mut reservation = self.receive_budget.try_reserve(chunk.message_size)?;
                let mut data = Vec::new();
                data.try_reserve_exact(chunk.message_size)
                    .map_err(|error| {
                        anyhow!(
                            "Failed to reserve {} bytes for QUIC message {}: {error}",
                            chunk.message_size,
                            chunk.message_id
                        )
                    })?;
                reservation.account_buffer_capacity(data.capacity())?;
                data.extend_from_slice(&chunk.data);

                if data.len() == chunk.message_size {
                    return Ok(Some(ReceivedMessage {
                        message_id: chunk.message_id,
                        data: Bytes::from(data),
                        reservation,
                    }));
                }

                entry.insert(PartialMessage {
                    message_size: chunk.message_size,
                    next_chunk_id: 1,
                    deadline: now
                        .checked_add(self.reassembly_timeout)
                        .ok_or_else(|| anyhow!("QUIC message reassembly deadline overflow"))?,
                    data,
                    reservation,
                });
                Ok(None)
            }
            Entry::Occupied(mut entry) => {
                if entry.get().message_size != chunk.message_size {
                    let expected = entry.get().message_size;
                    drop(entry.remove());
                    return Err(anyhow!(
                        "Message {} changed declared size from {} to {}",
                        chunk.message_id,
                        expected,
                        chunk.message_size
                    ));
                }
                if entry.get().next_chunk_id != chunk.chunk_id {
                    let expected = entry.get().next_chunk_id;
                    drop(entry.remove());
                    return Err(anyhow!(
                        "Message {} received chunk {}, expected chunk {}",
                        chunk.message_id,
                        chunk.chunk_id,
                        expected
                    ));
                }

                let partial = entry.get_mut();
                partial.data.extend_from_slice(&chunk.data);
                partial.next_chunk_id += 1;
                if partial.data.len() == partial.message_size {
                    let partial = entry.remove();
                    return Ok(Some(ReceivedMessage {
                        message_id: chunk.message_id,
                        data: Bytes::from(partial.data),
                        reservation: partial.reservation,
                    }));
                }
                Ok(None)
            }
        }
    }

    #[cfg(test)]
    fn partial_message_count(&self) -> usize {
        self.chunks.len()
    }

    fn earliest_deadline(&self) -> Option<Instant> {
        self.chunks.values().map(|partial| partial.deadline).min()
    }
}

struct RecvCtx {
    recv: quinn::RecvStream,
    reassembler: MessageReassembler,
    idle_timeout: Duration,
}

fn validate_chunk_header(message_size: usize, chunk_id: u64, chunk_size: usize) -> Result<()> {
    validate_wire_message_size(message_size)?;

    if message_size == 0 {
        if chunk_id != 0 || chunk_size != 0 {
            return Err(anyhow!(
                "Empty QUIC message must contain only chunk 0 with an empty body"
            ));
        }
        return Ok(());
    }
    if chunk_size == 0 {
        return Err(anyhow!("Non-empty QUIC message chunk cannot be empty"));
    }

    let chunk_id = usize::try_from(chunk_id)
        .map_err(|_| anyhow!("QUIC message chunk ID does not fit in usize"))?;
    let offset = chunk_id
        .checked_mul(MESSAGE_CHUNK_SIZE)
        .ok_or_else(|| anyhow!("QUIC message chunk offset overflow"))?;
    if offset >= message_size {
        return Err(anyhow!(
            "QUIC message chunk starts at {offset}, beyond declared size {message_size}"
        ));
    }
    let expected_size = MESSAGE_CHUNK_SIZE.min(message_size - offset);
    if chunk_size != expected_size {
        return Err(anyhow!(
            "QUIC message chunk {chunk_id} has size {chunk_size}, expected {expected_size}"
        ));
    }
    Ok(())
}

struct MessagePackCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MessagePackCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let byte = self
            .bytes
            .get(self.offset)
            .copied()
            .ok_or_else(|| anyhow!("Truncated MessagePack value"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| anyhow!("MessagePack length overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| anyhow!("MessagePack value declares data beyond the wire message"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn add_decoded_heap_estimate(estimate: &mut usize, additional: usize) -> Result<()> {
    *estimate = estimate
        .checked_add(additional)
        .ok_or_else(|| anyhow!("Decoded MessagePack heap estimate overflow"))?;
    if *estimate > MAX_IN_FLIGHT_MESSAGE_BYTES {
        return Err(anyhow!(
            "Decoded MessagePack heap estimate {} exceeds transport budget {}",
            *estimate,
            MAX_IN_FLIGHT_MESSAGE_BYTES
        ));
    }
    Ok(())
}

fn push_messagepack_values(
    remaining_values: &mut [usize; MAX_MESSAGEPACK_NESTING],
    depth: &mut usize,
    values: usize,
) -> Result<()> {
    if values == 0 {
        return Ok(());
    }
    if *depth == remaining_values.len() {
        return Err(anyhow!(
            "MessagePack nesting exceeds transport limit {}",
            MAX_MESSAGEPACK_NESTING
        ));
    }
    remaining_values[*depth] = values;
    *depth += 1;
    Ok(())
}

fn account_messagepack_sequence(
    count: usize,
    estimate: &mut usize,
    remaining_values: &mut [usize; MAX_MESSAGEPACK_NESTING],
    depth: &mut usize,
) -> Result<()> {
    let slots = count
        .checked_mul(DECODED_SEQUENCE_SLOT_BYTES)
        .ok_or_else(|| anyhow!("MessagePack sequence allocation estimate overflow"))?;
    add_decoded_heap_estimate(estimate, DECODED_CONTAINER_OVERHEAD_BYTES)?;
    add_decoded_heap_estimate(estimate, slots)?;
    push_messagepack_values(remaining_values, depth, count)
}

fn account_messagepack_map(
    count: usize,
    estimate: &mut usize,
    remaining_values: &mut [usize; MAX_MESSAGEPACK_NESTING],
    depth: &mut usize,
) -> Result<()> {
    let slots = count
        .checked_mul(DECODED_MAP_ENTRY_BYTES)
        .ok_or_else(|| anyhow!("MessagePack map allocation estimate overflow"))?;
    let values = count
        .checked_mul(2)
        .ok_or_else(|| anyhow!("MessagePack map length overflow"))?;
    add_decoded_heap_estimate(estimate, DECODED_CONTAINER_OVERHEAD_BYTES)?;
    add_decoded_heap_estimate(estimate, slots)?;
    push_messagepack_values(remaining_values, depth, values)
}

fn account_messagepack_payload(
    cursor: &mut MessagePackCursor<'_>,
    payload_length: usize,
    estimate: &mut usize,
) -> Result<()> {
    add_decoded_heap_estimate(estimate, payload_length)?;
    cursor.take(payload_length)?;
    Ok(())
}

/// Validates MessagePack topology without invoking Serde or allocating from peer-declared lengths.
///
/// The estimate conservatively charges dynamic sequence/map slots and owned byte/string payloads.
/// It is added to the same server-wide reservation that already owns the serialized buffer before
/// `rmp_serde` is allowed to allocate decoded values.
fn preflight_messagepack_decode(bytes: &[u8]) -> Result<usize> {
    let mut cursor = MessagePackCursor::new(bytes);
    let mut remaining_values = [0usize; MAX_MESSAGEPACK_NESTING];
    remaining_values[0] = 1;
    let mut depth = 1;
    let mut decoded_heap_estimate = 0usize;

    while depth != 0 {
        if remaining_values[depth - 1] == 0 {
            depth -= 1;
            continue;
        }
        remaining_values[depth - 1] -= 1;

        let marker = cursor.read_u8()?;
        match marker {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => {}
            0x80..=0x8f => account_messagepack_map(
                usize::from(marker & 0x0f),
                &mut decoded_heap_estimate,
                &mut remaining_values,
                &mut depth,
            )?,
            0x90..=0x9f => account_messagepack_sequence(
                usize::from(marker & 0x0f),
                &mut decoded_heap_estimate,
                &mut remaining_values,
                &mut depth,
            )?,
            0xa0..=0xbf => account_messagepack_payload(
                &mut cursor,
                usize::from(marker & 0x1f),
                &mut decoded_heap_estimate,
            )?,
            0xc1 => return Err(anyhow!("Reserved MessagePack marker 0xc1 is invalid")),
            0xc4 | 0xd9 => {
                let length = usize::from(cursor.read_u8()?);
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xc5 | 0xda => {
                let length = usize::from(cursor.read_u16()?);
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xc6 | 0xdb => {
                let length = usize::try_from(cursor.read_u32()?)
                    .map_err(|_| anyhow!("MessagePack payload length does not fit in usize"))?;
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xc7 => {
                let length = usize::from(cursor.read_u8()?);
                cursor.take(1)?;
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xc8 => {
                let length = usize::from(cursor.read_u16()?);
                cursor.take(1)?;
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xc9 => {
                let length = usize::try_from(cursor.read_u32()?)
                    .map_err(|_| anyhow!("MessagePack extension length does not fit in usize"))?;
                cursor.take(1)?;
                account_messagepack_payload(&mut cursor, length, &mut decoded_heap_estimate)?;
            }
            0xca => {
                cursor.take(4)?;
            }
            0xcb => {
                cursor.take(8)?;
            }
            0xcc | 0xd0 => {
                cursor.take(1)?;
            }
            0xcd | 0xd1 => {
                cursor.take(2)?;
            }
            0xce | 0xd2 => {
                cursor.take(4)?;
            }
            0xcf | 0xd3 => {
                cursor.take(8)?;
            }
            0xd4 => {
                cursor.take(2)?;
                add_decoded_heap_estimate(&mut decoded_heap_estimate, 1)?;
            }
            0xd5 => {
                cursor.take(3)?;
                add_decoded_heap_estimate(&mut decoded_heap_estimate, 2)?;
            }
            0xd6 => {
                cursor.take(5)?;
                add_decoded_heap_estimate(&mut decoded_heap_estimate, 4)?;
            }
            0xd7 => {
                cursor.take(9)?;
                add_decoded_heap_estimate(&mut decoded_heap_estimate, 8)?;
            }
            0xd8 => {
                cursor.take(17)?;
                add_decoded_heap_estimate(&mut decoded_heap_estimate, 16)?;
            }
            0xdc => account_messagepack_sequence(
                usize::from(cursor.read_u16()?),
                &mut decoded_heap_estimate,
                &mut remaining_values,
                &mut depth,
            )?,
            0xdd => {
                let count = usize::try_from(cursor.read_u32()?)
                    .map_err(|_| anyhow!("MessagePack sequence length does not fit in usize"))?;
                account_messagepack_sequence(
                    count,
                    &mut decoded_heap_estimate,
                    &mut remaining_values,
                    &mut depth,
                )?;
            }
            0xde => account_messagepack_map(
                usize::from(cursor.read_u16()?),
                &mut decoded_heap_estimate,
                &mut remaining_values,
                &mut depth,
            )?,
            0xdf => {
                let count = usize::try_from(cursor.read_u32()?)
                    .map_err(|_| anyhow!("MessagePack map length does not fit in usize"))?;
                account_messagepack_map(
                    count,
                    &mut decoded_heap_estimate,
                    &mut remaining_values,
                    &mut depth,
                )?;
            }
        }
    }

    if !cursor.is_finished() {
        return Err(anyhow!("Trailing bytes after top-level MessagePack value"));
    }
    Ok(decoded_heap_estimate)
}

fn preflight_nested_request_decode(request: &distributed::message::Request) -> Result<usize> {
    match request {
        distributed::message::Request::Spawn(spawn) => preflight_messagepack_decode(&spawn.config)
            .map_err(|error| {
                anyhow!("Invalid nested MessagePack in distributed spawn config: {error}")
            }),
        _ => Ok(0),
    }
}

async fn read_stream_bytes_with_timeout(
    recv: &mut quinn::RecvStream,
    buffer: &mut [u8],
    idle_timeout: Duration,
    frame_part: &str,
) -> Result<()> {
    tokio::time::timeout(idle_timeout, recv.read_exact(buffer))
        .await
        .map_err(|_| {
            anyhow!("Timed out after {idle_timeout:?} while reading QUIC message {frame_part}")
        })?
        .map_err(|error| anyhow!("{error} failed to read QUIC message {frame_part}"))
}

async fn read_next_stream_chunk(
    recv: &mut quinn::RecvStream,
    idle_timeout: Duration,
) -> Result<Chunk> {
    let mut header = [0u8; MESSAGE_CHUNK_HEADER_SIZE];
    read_stream_bytes_with_timeout(recv, &mut header, idle_timeout, "chunk header").await?;

    let message_id = u64::from_le_bytes(header[0..8].try_into().unwrap());
    let message_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let chunk_id = u64::from_le_bytes(header[12..20].try_into().unwrap());
    let chunk_size = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
    validate_chunk_header(message_size, chunk_id, chunk_size)?;

    // Chunk geometry is validated before allocation, so a peer cannot use `chunk_size` to request
    // more than one transport chunk of memory.
    let mut data = Vec::new();
    data.try_reserve_exact(chunk_size)
        .map_err(|error| anyhow!("Failed to reserve QUIC chunk buffer: {error}"))?;
    data.resize(chunk_size, 0);
    read_stream_bytes_with_timeout(recv, &mut data, idle_timeout, "chunk body").await?;
    log::trace!("read message_id={message_id} chunk_id={chunk_id}");
    Ok(Chunk {
        message_id,
        message_size,
        chunk_id,
        data,
    })
}

async fn read_next_stream_message(ctx: &mut RecvCtx) -> Result<ReceivedMessage> {
    loop {
        let earliest_deadline = ctx.reassembler.earliest_deadline();
        let read_chunk = read_next_stream_chunk(&mut ctx.recv, ctx.idle_timeout);
        let chunk = match earliest_deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, read_chunk)
                .await
                .map_err(|_| anyhow!("QUIC message reassembly deadline exceeded"))??,
            None => read_chunk.await?,
        };
        if let Some(message) = ctx.reassembler.push_chunk(chunk)? {
            log::trace!("Finished collecting message_id={}", message.message_id);
            return Ok(message);
        }
    }
}

/// Reassemble, decode, and dispatch requests from a production node stream.
pub async fn handle_request_stream<F, Fut>(recv: quinn::RecvStream, dispatch: F)
where
    F: FnMut(u64, distributed::message::Request) -> Fut,
    Fut: Future<Output = ()>,
{
    handle_request_stream_with_timeout(recv, REQUEST_STREAM_IDLE_TIMEOUT, dispatch).await;
}

/// Reassemble and dispatch a request stream with a caller-selected frame-read timeout.
///
/// Production node servers use [`REQUEST_STREAM_IDLE_TIMEOUT`]. This variant lets embedded users
/// apply a stricter deadline while retaining all framing and memory admission checks.
pub async fn handle_request_stream_with_timeout<F, Fut>(
    recv: quinn::RecvStream,
    idle_timeout: Duration,
    dispatch: F,
) where
    F: FnMut(u64, distributed::message::Request) -> Fut,
    Fut: Future<Output = ()>,
{
    handle_request_stream_with_budget(
        recv,
        Arc::new(ReceiveBudget::default()),
        idle_timeout,
        dispatch,
    )
    .await;
}

async fn handle_request_stream_with_budget<F, Fut>(
    recv: quinn::RecvStream,
    receive_budget: Arc<ReceiveBudget>,
    idle_timeout: Duration,
    mut dispatch: F,
) where
    F: FnMut(u64, distributed::message::Request) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut recv_ctx = RecvCtx {
        recv,
        reassembler: MessageReassembler::new(receive_budget),
        idle_timeout,
    };
    loop {
        let mut received = match read_next_stream_message(&mut recv_ctx).await {
            Ok(received) => received,
            Err(error) => {
                log::debug!("Rejected distributed request stream: {error}");
                return;
            }
        };
        let decoded_heap_estimate = match preflight_messagepack_decode(&received.data) {
            Ok(estimate) => estimate,
            Err(error) => {
                log::debug!("Rejected distributed MessagePack payload: {error}");
                continue;
            }
        };
        if let Err(error) = received
            .reservation
            .try_charge_additional(decoded_heap_estimate)
        {
            log::debug!("Rejected distributed MessagePack allocation: {error}");
            continue;
        }
        let request = match distributed::message::deserialize_message::<distributed::message::Request>(
            &received.data,
        ) {
            Ok(request) => request,
            Err(error) => {
                log::debug!("Error deserializing distributed request: {error}");
                continue;
            }
        };
        let nested_heap_estimate = match preflight_nested_request_decode(&request) {
            Ok(estimate) => estimate,
            Err(error) => {
                log::debug!("Rejected nested distributed payload: {error}");
                continue;
            }
        };
        if let Err(error) = received
            .reservation
            .try_charge_additional(nested_heap_estimate)
        {
            log::debug!("Rejected nested distributed allocation: {error}");
            continue;
        }
        dispatch(received.message_id, request).await;
        // `received` retains serialized capacity plus the conservative decoded-heap estimate
        // (including nested spawn configuration) through deserialization and dispatch.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_chunk(message_id: u64, message_size: usize, chunk_id: u64, byte: u8) -> Chunk {
        let offset = usize::try_from(chunk_id).unwrap() * MESSAGE_CHUNK_SIZE;
        let chunk_size = MESSAGE_CHUNK_SIZE.min(message_size.saturating_sub(offset));
        Chunk {
            message_id,
            message_size,
            chunk_id,
            data: vec![byte; chunk_size],
        }
    }

    #[test]
    fn wire_message_and_chunk_boundaries_are_enforced() {
        assert!(validate_wire_message_size(MAX_WIRE_MESSAGE_BYTES).is_ok());
        assert!(validate_wire_message_size(MAX_WIRE_MESSAGE_BYTES + 1).is_err());

        assert!(validate_chunk_header(0, 0, 0).is_ok());
        assert!(validate_chunk_header(0, 0, 1).is_err());
        assert!(validate_chunk_header(0, 1, 0).is_err());
        assert!(validate_chunk_header(1, 0, 1).is_ok());
        assert!(validate_chunk_header(1, 0, 0).is_err());
        assert!(validate_chunk_header(1, 0, MESSAGE_CHUNK_SIZE).is_err());

        assert!(validate_chunk_header(MESSAGE_CHUNK_SIZE + 1, 0, MESSAGE_CHUNK_SIZE).is_ok());
        assert!(validate_chunk_header(MESSAGE_CHUNK_SIZE + 1, 1, 1).is_ok());
        assert!(validate_chunk_header(MESSAGE_CHUNK_SIZE + 1, 1, 2).is_err());
        assert!(validate_chunk_header(MESSAGE_CHUNK_SIZE + 1, 2, 1).is_err());

        let final_chunk_id = (MAX_WIRE_MESSAGE_BYTES / MESSAGE_CHUNK_SIZE - 1) as u64;
        assert!(
            validate_chunk_header(MAX_WIRE_MESSAGE_BYTES, final_chunk_id, MESSAGE_CHUNK_SIZE)
                .is_ok()
        );
        assert!(
            validate_chunk_header(MAX_WIRE_MESSAGE_BYTES, u64::MAX, u32::MAX as usize).is_err()
        );
        assert!(validate_chunk_header(MAX_WIRE_MESSAGE_BYTES + 1, 0, MESSAGE_CHUNK_SIZE).is_err());
    }

    #[test]
    fn quic_receive_windows_fit_the_server_wide_budget() {
        let (stream_receive_window, connection_receive_window) = quic_receive_window_limits();
        assert!(stream_receive_window <= connection_receive_window);
        assert_eq!(
            connection_receive_window as usize * MAX_CONCURRENT_CONNECTIONS,
            MAX_IN_FLIGHT_MESSAGE_BYTES
        );
    }

    #[test]
    fn messagepack_preflight_accepts_requests_and_rejects_allocation_bombs() {
        let request = distributed::message::Request::Message {
            node_id: 1,
            environment_id: 2,
            process_id: 3,
            tag: Some(4),
            data: (0..4_097).map(|value| (value % 251) as u8).collect(),
        };
        let encoded = distributed::message::serialize_message(&request).unwrap();
        let estimate = preflight_messagepack_decode(&encoded).unwrap();
        assert!(estimate >= 4_097);
        assert!(
            distributed::message::deserialize_message::<distributed::message::Request>(&encoded)
                .is_ok()
        );

        let mut huge_string = vec![0xdb];
        huge_string.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(preflight_messagepack_decode(&huge_string).is_err());

        let mut huge_sequence = vec![0xdd];
        huge_sequence.extend_from_slice(&u32::MAX.to_be_bytes());
        assert!(preflight_messagepack_decode(&huge_sequence).is_err());

        let mut excessive_nesting = vec![0x91; MAX_MESSAGEPACK_NESTING + 1];
        excessive_nesting.push(0xc0);
        assert!(preflight_messagepack_decode(&excessive_nesting).is_err());

        assert!(preflight_messagepack_decode(&[0xc6, 0, 0, 0, 4, 0]).is_err());
        assert!(preflight_messagepack_decode(&[0xc0, 0xc0]).is_err());
        assert!(preflight_messagepack_decode(&[0xc1]).is_err());

        let valid_config = rmp_serde::to_vec(&vec!["PATH=/bin", "MODE=cluster"]).unwrap();
        let valid_spawn = distributed::message::Request::Spawn(distributed::message::Spawn {
            response_node_id: 1,
            environment_id: 2,
            module_id: 3,
            function: "entry".to_string(),
            params: Vec::new(),
            config: valid_config,
        });
        assert!(preflight_nested_request_decode(&valid_spawn).unwrap() > 0);

        let invalid_spawn = distributed::message::Request::Spawn(distributed::message::Spawn {
            response_node_id: 1,
            environment_id: 2,
            module_id: 3,
            function: "entry".to_string(),
            params: Vec::new(),
            config: huge_sequence,
        });
        assert!(preflight_nested_request_decode(&invalid_spawn).is_err());
    }

    #[test]
    fn nested_empty_string_sequence_is_conservatively_charged() {
        // This is small on the wire (one byte per empty string), but decoding it as a
        // `Vec<String>` needs a 24-byte String slot for every element on 64-bit targets.
        // The chosen count keeps the structural estimate below the byte ceiling by itself while
        // making serialized bytes plus the estimate exceed the shared transport budget.
        const EMPTY_STRING_COUNT: usize = 1_020_000;
        let mut config = Vec::with_capacity(5 + EMPTY_STRING_COUNT);
        config.push(0xdd);
        config.extend_from_slice(&(EMPTY_STRING_COUNT as u32).to_be_bytes());
        config.resize(5 + EMPTY_STRING_COUNT, 0xa0);

        let spawn = distributed::message::Request::Spawn(distributed::message::Spawn {
            response_node_id: 1,
            environment_id: 2,
            module_id: 3,
            function: "entry".to_string(),
            params: Vec::new(),
            config,
        });
        let estimate = preflight_nested_request_decode(&spawn).unwrap();
        assert_eq!(
            estimate,
            DECODED_CONTAINER_OVERHEAD_BYTES + EMPTY_STRING_COUNT * DECODED_SEQUENCE_SLOT_BYTES
        );
        assert!(estimate >= EMPTY_STRING_COUNT * std::mem::size_of::<String>());

        let distributed::message::Request::Spawn(spawn) = spawn else {
            unreachable!()
        };
        let budget = Arc::new(ReceiveBudget::new(1, MAX_IN_FLIGHT_MESSAGE_BYTES));
        let mut reservation = budget.try_reserve(spawn.config.len()).unwrap();
        assert!(reservation.try_charge_additional(estimate).is_err());
        assert_eq!(budget.current_usage(), (1, spawn.config.len()));
    }

    #[test]
    fn decoded_heap_charge_is_transactional_and_reusable() {
        let budget = Arc::new(ReceiveBudget::new(1, 1_024));
        let mut reservation = budget.try_reserve(100).unwrap();
        reservation.try_charge_additional(500).unwrap();
        assert_eq!(budget.current_usage(), (1, 600));

        assert!(reservation.try_charge_additional(500).is_err());
        assert_eq!(budget.current_usage(), (1, 600));

        drop(reservation);
        assert_eq!(budget.current_usage(), (0, 0));
        let reused = budget.try_reserve(1_024).unwrap();
        drop(reused);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn valid_interleaved_messages_reassemble_and_hold_budget_until_drop() {
        let budget = Arc::new(ReceiveBudget::new(4, 4 * MESSAGE_CHUNK_SIZE));
        let mut reassembler = MessageReassembler::new(budget.clone());

        assert!(reassembler
            .push_chunk(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 0, 0x11))
            .unwrap()
            .is_none());
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));

        let second = reassembler
            .push_chunk(valid_chunk(2, MESSAGE_CHUNK_SIZE, 0, 0x22))
            .unwrap()
            .expect("single-chunk message should complete");
        assert_eq!(budget.current_usage(), (2, 3 * MESSAGE_CHUNK_SIZE));
        assert_eq!(second.data.len(), MESSAGE_CHUNK_SIZE);
        drop(second);
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));

        let first = reassembler
            .push_chunk(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 1, 0x33))
            .unwrap()
            .expect("second chunk should complete message");
        assert_eq!(first.data.len(), 2 * MESSAGE_CHUNK_SIZE);
        assert!(first.data[..MESSAGE_CHUNK_SIZE]
            .iter()
            .all(|byte| *byte == 0x11));
        assert!(first.data[MESSAGE_CHUNK_SIZE..]
            .iter()
            .all(|byte| *byte == 0x33));
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));
        drop(first);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn inconsistent_size_or_chunk_order_cleans_state_immediately() {
        let budget = Arc::new(ReceiveBudget::new(2, 4 * MESSAGE_CHUNK_SIZE));
        let mut reassembler = MessageReassembler::new(budget.clone());

        reassembler
            .push_chunk(valid_chunk(7, 2 * MESSAGE_CHUNK_SIZE, 0, 0x10))
            .unwrap();
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));
        assert!(reassembler
            .push_chunk(valid_chunk(7, 3 * MESSAGE_CHUNK_SIZE, 1, 0x20))
            .is_err());
        assert_eq!(reassembler.partial_message_count(), 0);
        assert_eq!(budget.current_usage(), (0, 0));

        reassembler
            .push_chunk(valid_chunk(7, 2 * MESSAGE_CHUNK_SIZE, 0, 0x30))
            .unwrap();
        assert!(reassembler
            .push_chunk(valid_chunk(7, 2 * MESSAGE_CHUNK_SIZE, 0, 0x40))
            .is_err());
        assert_eq!(reassembler.partial_message_count(), 0);
        assert_eq!(budget.current_usage(), (0, 0));

        reassembler
            .push_chunk(valid_chunk(7, 2 * MESSAGE_CHUNK_SIZE, 0, 0x50))
            .unwrap();
        assert!(reassembler
            .push_chunk(Chunk {
                message_id: 7,
                message_size: 2 * MESSAGE_CHUNK_SIZE,
                chunk_id: 1,
                data: vec![0; 1],
            })
            .is_err());
        assert_eq!(reassembler.partial_message_count(), 0);
        assert_eq!(budget.current_usage(), (0, 0));

        let restarted = reassembler
            .push_chunk(valid_chunk(7, MESSAGE_CHUNK_SIZE, 0, 0x60))
            .unwrap()
            .expect("cleaned message ID should be reusable");
        drop(restarted);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn in_flight_message_limit_rejects_without_retaining_chunk() {
        let budget = Arc::new(ReceiveBudget::new(2, 10 * MESSAGE_CHUNK_SIZE));
        let mut reassembler = MessageReassembler::new(budget.clone());

        for message_id in [1, 2] {
            assert!(reassembler
                .push_chunk(valid_chunk(message_id, 2 * MESSAGE_CHUNK_SIZE, 0, 0x10))
                .unwrap()
                .is_none());
        }
        assert_eq!(budget.current_usage(), (2, 4 * MESSAGE_CHUNK_SIZE));
        assert!(reassembler
            .push_chunk(valid_chunk(3, 2 * MESSAGE_CHUNK_SIZE, 0, 0x20))
            .is_err());
        assert_eq!(reassembler.partial_message_count(), 2);
        assert_eq!(budget.current_usage(), (2, 4 * MESSAGE_CHUNK_SIZE));

        drop(reassembler);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn in_flight_byte_limit_is_exact_and_capacity_is_reusable() {
        let budget = Arc::new(ReceiveBudget::new(10, 2 * MESSAGE_CHUNK_SIZE));
        let mut first_stream = MessageReassembler::new(budget.clone());
        let mut second_stream = MessageReassembler::new(budget.clone());

        first_stream
            .push_chunk(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 0, 0x10))
            .unwrap();
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));
        assert!(second_stream
            .push_chunk(valid_chunk(2, MESSAGE_CHUNK_SIZE, 0, 0x20))
            .is_err());
        assert_eq!(second_stream.partial_message_count(), 0);
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));

        let completed = first_stream
            .push_chunk(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 1, 0x30))
            .unwrap()
            .expect("message should complete at the exact byte limit");
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));
        drop(completed);
        assert_eq!(budget.current_usage(), (0, 0));

        let reused = second_stream
            .push_chunk(valid_chunk(2, MESSAGE_CHUNK_SIZE, 0, 0x40))
            .unwrap()
            .expect("released byte capacity should be reusable");
        drop(reused);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn dropping_partial_reassembly_releases_all_declared_bytes() {
        let budget = Arc::new(ReceiveBudget::new(4, 8 * MESSAGE_CHUNK_SIZE));
        {
            let mut reassembler = MessageReassembler::new(budget.clone());
            reassembler
                .push_chunk(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 0, 0x10))
                .unwrap();
            reassembler
                .push_chunk(valid_chunk(2, 3 * MESSAGE_CHUNK_SIZE, 0, 0x20))
                .unwrap();
            assert_eq!(budget.current_usage(), (2, 5 * MESSAGE_CHUNK_SIZE));
        }
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn partial_message_reassembly_deadline_releases_reserved_bytes() {
        let budget = Arc::new(ReceiveBudget::new(2, 4 * MESSAGE_CHUNK_SIZE));
        let reassembly_timeout = Duration::from_millis(50);
        let mut reassembler = MessageReassembler::with_timeout(budget.clone(), reassembly_timeout);
        let started_at = Instant::now();

        assert!(reassembler
            .push_chunk_at(valid_chunk(1, 2 * MESSAGE_CHUNK_SIZE, 0, 0x11), started_at,)
            .unwrap()
            .is_none());
        assert_eq!(budget.current_usage(), (1, 2 * MESSAGE_CHUNK_SIZE));
        assert_eq!(
            reassembler.earliest_deadline(),
            Some(started_at + reassembly_timeout)
        );

        let error = match reassembler.push_chunk_at(
            valid_chunk(2, MESSAGE_CHUNK_SIZE, 0, 0x22),
            started_at + reassembly_timeout,
        ) {
            Err(error) => error,
            Ok(_) => panic!("expired partial message should reject the stream"),
        };
        assert!(error.to_string().contains("reassembly deadline"));
        assert_eq!(reassembler.partial_message_count(), 0);
        assert_eq!(budget.current_usage(), (0, 0));
    }

    #[test]
    fn server_admission_limits_connection_and_stream_tasks_exactly() {
        let admission = ServerAdmission::new(2, 3);

        let first_connection = admission.try_admit_connection().unwrap();
        let second_connection = admission.try_admit_connection().unwrap();
        assert!(admission.try_admit_connection().is_none());
        drop(first_connection);
        let reused_connection = admission.try_admit_connection().unwrap();
        assert!(admission.try_admit_connection().is_none());

        let first_stream = admission.try_admit_stream().unwrap();
        let second_stream = admission.try_admit_stream().unwrap();
        let third_stream = admission.try_admit_stream().unwrap();
        assert!(admission.try_admit_stream().is_none());
        drop(second_stream);
        let reused_stream = admission.try_admit_stream().unwrap();
        assert!(admission.try_admit_stream().is_none());

        drop((second_connection, reused_connection));
        drop((first_stream, third_stream, reused_stream));
    }
}
