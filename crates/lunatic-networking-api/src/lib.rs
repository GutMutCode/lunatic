mod dns;
mod tcp;
mod tls_tcp;
mod udp;

use std::convert::TryInto;
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use hash_map_id::HashMapId;
use lunatic_error_api::ErrorCtx;
use tokio::io::{split, ReadHalf, WriteHalf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsStream;
use wasmtime::{Caller, Linker, Memory};

use lunatic_common_api::IntoTrap;

pub use dns::DnsIterator;

pub struct TcpConnection {
    pub reader: Mutex<OwnedReadHalf>,
    pub writer: Mutex<OwnedWriteHalf>,
    pub read_timeout: Mutex<Option<Duration>>,
    pub write_timeout: Mutex<Option<Duration>>,
    pub peek_timeout: Mutex<Option<Duration>>,
    pub peer_addr: Option<SocketAddr>,
    pub local_addr: Option<SocketAddr>,
}

/// This encapsulates the TCP-level connection, some connection
/// state, and the underlying TLS-level session.
pub struct TlsConnection {
    pub reader: Mutex<ReadHalf<TlsStream<TcpStream>>>,
    pub writer: Mutex<WriteHalf<TlsStream<TcpStream>>>,
    pub closing: bool,
    pub clean_closure: bool,
    pub read_timeout: Mutex<Option<Duration>>,
    pub write_timeout: Mutex<Option<Duration>>,
    pub peek_timeout: Mutex<Option<Duration>>,
    /// Descriptive client metadata (None for server-accepted connections).
    ///
    /// This is useful for diagnostics and serialized snapshots, but is not
    /// sufficient to recreate the original TLS/application byte stream.
    pub client_metadata: Option<TlsClientConnectionMetadata>,
}

/// Descriptive metadata associated with a TLS client stream.
#[derive(Debug, Clone)]
pub struct TlsClientConnectionMetadata {
    pub server_name: String,
    pub port: u16,
    pub peer_addr: Option<SocketAddr>,
    pub local_addr: Option<SocketAddr>,
    /// Custom root certificates (empty = use system defaults)
    pub custom_root_certs: Vec<Vec<u8>>,
}

pub struct TlsListener {
    pub listener: TcpListener,
    pub certs: CertificateDer<'static>,
    pub keys: PrivateKeyDer<'static>,
}

impl TlsConnection {
    pub fn new(sock: TlsStream<TcpStream>) -> TlsConnection {
        let (read_half, write_half) = split(sock);
        TlsConnection {
            reader: Mutex::new(read_half),
            writer: Mutex::new(write_half),
            closing: false,
            clean_closure: false,
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            peek_timeout: Mutex::new(None),
            client_metadata: None,
        }
    }

    pub fn with_client_metadata(
        sock: TlsStream<TcpStream>,
        metadata: TlsClientConnectionMetadata,
    ) -> TlsConnection {
        let (read_half, write_half) = split(sock);
        TlsConnection {
            reader: Mutex::new(read_half),
            writer: Mutex::new(write_half),
            closing: false,
            clean_closure: false,
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            peek_timeout: Mutex::new(None),
            client_metadata: Some(metadata),
        }
    }
}

impl TcpConnection {
    pub fn new(stream: TcpStream) -> Self {
        let peer_addr = stream.peer_addr().ok();
        let local_addr = stream.local_addr().ok();
        let (read_half, write_half) = stream.into_split();
        TcpConnection {
            reader: Mutex::new(read_half),
            writer: Mutex::new(write_half),
            read_timeout: Mutex::new(None),
            write_timeout: Mutex::new(None),
            peek_timeout: Mutex::new(None),
            peer_addr,
            local_addr,
        }
    }

    pub fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}

pub type TcpListenerResources = HashMapId<TcpListener>;
pub type TlsListenerResources = HashMapId<TlsListener>;
pub type TcpStreamResources = HashMapId<Arc<TcpConnection>>;
pub type TlsStreamResources = HashMapId<Arc<TlsConnection>>;
pub type UdpResources = HashMapId<Arc<UdpSocket>>;
pub type DnsResources = HashMapId<DnsIterator>;

/// Shared accounting for guest-visible network handles.
///
/// Implementations must keep the descriptor and network counters in one
/// atomic transaction. The shared object can outlive a process state while a
/// socket is attached to a queued message, so both operations use `&self` and
/// must provide their own synchronization.
pub trait NetworkHandleQuota: Send + Sync {
    fn reserve(&self) -> Result<()>;
    fn release(&self) -> Result<()>;
}

/// Owns exactly one already-reserved network-handle quota unit.
///
/// This lease is moved together with a TCP/TLS/UDP resource while it is in a
/// message. Dropping the message releases the sender's reservation. Moving the
/// resource to another process first reserves the destination and only then
/// releases the source, so a failed transfer leaves the original reservation
/// intact.
#[must_use = "dropping a network handle lease releases its quota reservation"]
pub struct NetworkHandleLease {
    quota: Option<Arc<dyn NetworkHandleQuota>>,
}

impl NetworkHandleLease {
    /// Reserves a new quota unit and returns its RAII owner.
    ///
    /// If any later operation fails or is cancelled, dropping the returned
    /// lease rolls the reservation back automatically.
    pub fn reserve_new(quota: Arc<dyn NetworkHandleQuota>) -> Result<Self> {
        quota.reserve()?;
        Ok(Self { quota: Some(quota) })
    }

    /// Adopts a reservation that was already made for a resource-table entry.
    pub fn from_existing(quota: Arc<dyn NetworkHandleQuota>) -> Self {
        Self { quota: Some(quota) }
    }

    /// Transfers this reservation to `target` without an unaccounted window.
    ///
    /// `NetworkingCtx::network_handle_quota` must return clones of one stable
    /// `Arc`; pointer equality is used to make same-process transfers a no-op.
    pub fn transfer_to(&mut self, target: Arc<dyn NetworkHandleQuota>) -> Result<()> {
        let source = self
            .quota
            .as_ref()
            .expect("an active network handle lease always has an owner");
        if Arc::ptr_eq(source, &target) {
            return Ok(());
        }

        target.reserve()?;
        if let Err(source_error) = source.release() {
            return match target.release() {
                Ok(()) => Err(source_error),
                Err(rollback_error) => Err(anyhow!(
                    "failed to release source network quota ({source_error}); \
                     destination rollback also failed ({rollback_error})"
                )),
            };
        }

        self.quota = Some(target);
        Ok(())
    }

    /// Converts the lease back into the implicit reservation owned by a
    /// resource-table entry. No counter is changed.
    pub fn into_table_reservation(mut self) {
        self.quota.take();
    }
}

impl fmt::Debug for NetworkHandleLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NetworkHandleLease")
            .field("active", &self.quota.is_some())
            .finish()
    }
}

impl Drop for NetworkHandleLease {
    fn drop(&mut self) {
        if let Some(quota) = self.quota.take() {
            // Drop cannot report an accounting invariant failure. Explicit
            // table operations still surface such failures through `Result`.
            let _ = quota.release();
        }
    }
}

/// Shared accounting for guest-visible DNS iterator resources.
///
/// Implementations must enforce a finite per-process ceiling and make each
/// reservation/release atomic. The quota is shared because a [`DnsIterator`]
/// owns its reservation for its entire lifetime, including while process
/// resources are moved during hot reload.
pub trait DnsIteratorQuota: Send + Sync {
    fn reserve(&self) -> Result<()>;
    fn release(&self) -> Result<()>;
}

/// Owns exactly one DNS iterator quota reservation.
///
/// Runtime-created [`DnsIterator`] values retain this lease while they are in
/// the resource table. Failed operations drop the lease automatically, and
/// removing or dropping the iterator releases the reservation exactly once.
#[must_use = "dropping a DNS iterator lease releases its quota reservation"]
pub struct DnsIteratorLease {
    quota: Option<Arc<dyn DnsIteratorQuota>>,
}

impl DnsIteratorLease {
    /// Reserves one iterator slot and returns its RAII owner.
    pub fn reserve_new(quota: Arc<dyn DnsIteratorQuota>) -> Result<Self> {
        quota.reserve()?;
        Ok(Self { quota: Some(quota) })
    }
}

impl fmt::Debug for DnsIteratorLease {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DnsIteratorLease")
            .field("active", &self.quota.is_some())
            .finish()
    }
}

impl Drop for DnsIteratorLease {
    fn drop(&mut self) {
        if let Some(quota) = self.quota.take() {
            // Drop cannot report a quota invariant failure. Implementations
            // must make release atomic and reject underflow.
            let _ = quota.release();
        }
    }
}

pub trait NetworkingCtx {
    fn tcp_listener_resources(&self) -> &TcpListenerResources;
    fn tcp_listener_resources_mut(&mut self) -> &mut TcpListenerResources;
    fn tcp_stream_resources(&self) -> &TcpStreamResources;
    fn tcp_stream_resources_mut(&mut self) -> &mut TcpStreamResources;
    fn tls_listener_resources(&self) -> &TlsListenerResources;
    fn tls_listener_resources_mut(&mut self) -> &mut TlsListenerResources;
    fn tls_stream_resources(&self) -> &TlsStreamResources;
    fn tls_stream_resources_mut(&mut self) -> &mut TlsStreamResources;
    fn udp_resources(&self) -> &UdpResources;
    fn udp_resources_mut(&mut self) -> &mut UdpResources;
    fn dns_resources(&self) -> &DnsResources;
    fn dns_resources_mut(&mut self) -> &mut DnsResources;

    /// Returns this process's stable, shared network quota owner.
    ///
    /// Every successful call for one process must return a clone of the same
    /// `Arc`. The owner must remain valid after the process state is dropped
    /// because a queued message can retain one of its leases. The default
    /// keeps legacy contexts source-compatible but makes new network-handle
    /// admission and network-resource message transfer fail closed.
    fn network_handle_quota(&self) -> Option<Arc<dyn NetworkHandleQuota>> {
        None
    }

    /// Returns this process's stable, shared DNS iterator quota owner.
    ///
    /// Every successful call for one process must clone the same `Arc`, and
    /// the implementation must enforce a finite ceiling. The default keeps
    /// legacy contexts source-compatible while making host-created DNS
    /// iterators fail closed.
    fn dns_iterator_quota(&self) -> Option<Arc<dyn DnsIteratorQuota>> {
        None
    }

    fn can_open_network_connection(&mut self) -> Result<()> {
        Ok(())
    }

    fn close_network_connection(&mut self) {}

    /// Atomically reserves a transferable network handle lease.
    ///
    /// Contexts without a stable shared quota fail closed before performing
    /// any OS network operation.
    fn reserve_network_handle_lease(&self) -> Result<NetworkHandleLease> {
        let quota = self
            .network_handle_quota()
            .ok_or_else(|| anyhow!("transferable network handle quota unavailable"))?;
        NetworkHandleLease::reserve_new(quota)
    }

    /// Atomically reserves a DNS iterator lease.
    ///
    /// Contexts without a stable finite quota fail before DNS lookup, socket
    /// accept, datagram receive, or address inspection can create an iterator.
    fn reserve_dns_iterator_lease(&self) -> Result<DnsIteratorLease> {
        let quota = self
            .dns_iterator_quota()
            .ok_or_else(|| anyhow!("DNS iterator quota unavailable"))?;
        DnsIteratorLease::reserve_new(quota)
    }

    /// Reserves one guest-visible network handle.
    ///
    /// A handle represents one entry in any of the TCP/TLS listener, TCP/TLS
    /// stream, or UDP socket resource tables. Implementations should check the
    /// descriptor and network ceilings atomically so a failed reservation
    /// changes neither counter.
    fn reserve_network_handle(&mut self) -> Result<()> {
        self.network_handle_quota()
            .ok_or_else(|| anyhow!("transferable network handle quota unavailable"))?
            .reserve()
    }

    /// Releases one previously reserved guest-visible network handle.
    ///
    /// Implementations should report underflow instead of silently saturating;
    /// invalid resource IDs never reach this hook.
    fn release_network_handle(&mut self) -> Result<()> {
        self.network_handle_quota()
            .ok_or_else(|| anyhow!("transferable network handle quota unavailable"))?
            .release()
    }
}

// Register the networking APIs to the linker
pub fn register<T: NetworkingCtx + ErrorCtx + Send + 'static>(
    linker: &mut Linker<T>,
) -> Result<()> {
    dns::register(linker)?;
    tcp::register(linker)?;
    tls_tcp::register(linker)?;
    udp::register(linker)?;
    Ok(())
}

/// Validates a guest-memory output range before a host operation acquires an
/// RAII resource reservation or performs irreversible I/O.
///
/// Wasm memories cannot shrink, so a range that is valid before the operation
/// remains writable while the guest is suspended in that host call.
fn validate_memory_range<T>(
    caller: &Caller<T>,
    memory: &Memory,
    ptr: u32,
    len: usize,
    operation: &str,
) -> Result<()> {
    let start = ptr as usize;
    let end = start
        .checked_add(len)
        .ok_or_else(|| anyhow!("guest memory range overflow"))?;
    memory
        .data(caller)
        .get(start..end)
        .map(|_| ())
        .or_trap(operation)
}

fn socket_address<T: NetworkingCtx>(
    caller: &Caller<T>,
    memory: &Memory,
    addr_type: u32,
    addr_u8_ptr: u32,
    port: u32,
    flow_info: u32,
    scope_id: u32,
) -> Result<SocketAddr> {
    Ok(match addr_type {
        4 => {
            let ip = memory
                .data(caller)
                .get(addr_u8_ptr as usize..(addr_u8_ptr + 4) as usize)
                .or_trap("lunatic::network::socket_address*")?;
            let addr = <Ipv4Addr as From<[u8; 4]>>::from(ip.try_into().expect("exactly 4 bytes"));
            SocketAddrV4::new(addr, port as u16).into()
        }
        6 => {
            let ip = memory
                .data(caller)
                .get(addr_u8_ptr as usize..(addr_u8_ptr + 16) as usize)
                .or_trap("lunatic::network::socket_address*")?;
            let addr = <Ipv6Addr as From<[u8; 16]>>::from(ip.try_into().expect("exactly 16 bytes"));
            SocketAddrV6::new(addr, port as u16, flow_info, scope_id).into()
        }
        _ => return Err(anyhow!("Unsupported address type in socket_address*")),
    })
}
