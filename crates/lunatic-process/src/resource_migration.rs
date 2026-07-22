use anyhow::{anyhow, Result};
use bincode::Options as _;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

const RESOURCE_SNAPSHOT_MAGIC: [u8; 8] = *b"LUNRSNP\0";
const RESOURCE_SNAPSHOT_VERSION: u8 = 2;

/// An opaque reference to TLS listener credentials held by a host provider.
///
/// The handle is intentionally serializable so a resource snapshot can name
/// credentials without containing their private-key bytes. It is only a
/// locator: providers must additionally validate the requesting runtime scope.
/// The value is redacted from `Debug` output as defense in depth.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TlsCredentialHandle([u8; 16]);

impl TlsCredentialHandle {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl fmt::Debug for TlsCredentialHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TlsCredentialHandle([REDACTED])")
    }
}

/// Represents a serializable resource snapshot for migration
#[derive(Clone, Serialize, Deserialize)]
pub enum ResourceSnapshot {
    /// TCP connection: store peer address for reconnection
    TcpConnection {
        peer_addr: String,
        local_addr: String,
    },
    /// TCP listener: store bound address
    TcpListener { local_addr: String },
    /// Metadata describing a TLS client connection.
    ///
    /// This does not contain cryptographic session state and cannot recreate the
    /// original byte stream. In-process hot reload transfers the live host
    /// resource instead.
    TlsClientConnectionMetadata {
        server_name: String,
        port: u16,
        peer_addr: Option<String>,
        local_addr: Option<String>,
        custom_root_certs: Vec<Vec<u8>>,
        read_timeout_ms: Option<u64>,
        write_timeout_ms: Option<u64>,
    },
    /// Metadata describing a server-accepted TLS connection.
    ///
    /// A server cannot reconnect this stream from serialized metadata.
    TlsServerConnectionMetadata {
        requires_peer_reconnect: bool,
        reason: String,
    },
    /// TLS listener metadata and an opaque credential-provider reference.
    ///
    /// The referenced certificate and private key are never part of this
    /// serializable value.
    TlsListener {
        local_addr: String,
        credential_handle: TlsCredentialHandle,
    },
    /// UDP socket: store bound address
    UdpSocket { local_addr: String },
    /// Generic placeholder for resources that can't be migrated
    NonMigratable {
        resource_type: String,
        reason: String,
    },
}

impl fmt::Debug for ResourceSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TcpConnection { .. } => formatter
                .debug_struct("TcpConnection")
                .field("addresses", &"[REDACTED]")
                .finish(),
            Self::TcpListener { .. } => formatter
                .debug_struct("TcpListener")
                .field("local_addr", &"[REDACTED]")
                .finish(),
            Self::TlsClientConnectionMetadata {
                port,
                custom_root_certs,
                read_timeout_ms,
                write_timeout_ms,
                ..
            } => formatter
                .debug_struct("TlsClientConnectionMetadata")
                .field("endpoint", &"[REDACTED]")
                .field("port", port)
                .field("custom_root_cert_count", &custom_root_certs.len())
                .field("read_timeout_ms", read_timeout_ms)
                .field("write_timeout_ms", write_timeout_ms)
                .finish(),
            Self::TlsServerConnectionMetadata { .. } => formatter
                .debug_struct("TlsServerConnectionMetadata")
                .field("reason", &"[REDACTED]")
                .finish(),
            Self::TlsListener { .. } => formatter
                .debug_struct("TlsListener")
                .field("local_addr", &"[REDACTED]")
                .field("credential_handle", &"[REDACTED]")
                .finish(),
            Self::UdpSocket { .. } => formatter
                .debug_struct("UdpSocket")
                .field("local_addr", &"[REDACTED]")
                .finish(),
            Self::NonMigratable { .. } => formatter
                .debug_struct("NonMigratable")
                .field("resource_type", &"[REDACTED]")
                .field("reason", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Counts of live host resources transferred between Wasm instances.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResourceTransferReport {
    pub tcp_listeners: usize,
    pub tcp_streams: usize,
    pub tls_listeners: usize,
    pub tls_streams: usize,
    pub udp_sockets: usize,
    pub dns_iterators: usize,
}

impl ResourceTransferReport {
    pub fn from_snapshot(snapshot: &ResourceMigrationSnapshot) -> Self {
        Self {
            tcp_listeners: snapshot.tcp_listeners.len(),
            tcp_streams: snapshot.tcp_streams.len(),
            tls_listeners: snapshot.tls_listeners.len(),
            tls_streams: snapshot.tls_streams.len(),
            udp_sockets: snapshot.udp_sockets.len(),
            dns_iterators: 0,
        }
    }

    pub fn total(&self) -> usize {
        self.tcp_listeners
            + self.tcp_streams
            + self.tls_listeners
            + self.tls_streams
            + self.udp_sockets
            + self.dns_iterators
    }
}

/// Collection of resource snapshots by resource ID
#[derive(Debug, Clone, Default)]
pub struct ResourceMigrationSnapshot {
    pub tcp_listeners: HashMap<u64, ResourceSnapshot>,
    pub tcp_streams: HashMap<u64, ResourceSnapshot>,
    pub tls_listeners: HashMap<u64, ResourceSnapshot>,
    pub tls_streams: HashMap<u64, ResourceSnapshot>,
    pub udp_sockets: HashMap<u64, ResourceSnapshot>,
}

#[derive(Serialize)]
struct ResourceMigrationSnapshotRef<'a> {
    tcp_listeners: &'a HashMap<u64, ResourceSnapshot>,
    tcp_streams: &'a HashMap<u64, ResourceSnapshot>,
    tls_listeners: &'a HashMap<u64, ResourceSnapshot>,
    tls_streams: &'a HashMap<u64, ResourceSnapshot>,
    udp_sockets: &'a HashMap<u64, ResourceSnapshot>,
}

impl<'a> From<&'a ResourceMigrationSnapshot> for ResourceMigrationSnapshotRef<'a> {
    fn from(snapshot: &'a ResourceMigrationSnapshot) -> Self {
        Self {
            tcp_listeners: &snapshot.tcp_listeners,
            tcp_streams: &snapshot.tcp_streams,
            tls_listeners: &snapshot.tls_listeners,
            tls_streams: &snapshot.tls_streams,
            udp_sockets: &snapshot.udp_sockets,
        }
    }
}

#[derive(Deserialize)]
struct ResourceMigrationSnapshotWire {
    tcp_listeners: HashMap<u64, ResourceSnapshot>,
    tcp_streams: HashMap<u64, ResourceSnapshot>,
    tls_listeners: HashMap<u64, ResourceSnapshot>,
    tls_streams: HashMap<u64, ResourceSnapshot>,
    udp_sockets: HashMap<u64, ResourceSnapshot>,
}

impl From<ResourceMigrationSnapshotWire> for ResourceMigrationSnapshot {
    fn from(snapshot: ResourceMigrationSnapshotWire) -> Self {
        Self {
            tcp_listeners: snapshot.tcp_listeners,
            tcp_streams: snapshot.tcp_streams,
            tls_listeners: snapshot.tls_listeners,
            tls_streams: snapshot.tls_streams,
            udp_sockets: snapshot.udp_sockets,
        }
    }
}

impl ResourceMigrationSnapshot {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_tcp_listener(&mut self, id: u64, snapshot: ResourceSnapshot) {
        self.tcp_listeners.insert(id, snapshot);
    }

    pub fn add_tcp_stream(&mut self, id: u64, snapshot: ResourceSnapshot) {
        self.tcp_streams.insert(id, snapshot);
    }

    pub fn add_tls_listener(&mut self, id: u64, snapshot: ResourceSnapshot) {
        self.tls_listeners.insert(id, snapshot);
    }

    pub fn add_tls_stream(&mut self, id: u64, snapshot: ResourceSnapshot) {
        self.tls_streams.insert(id, snapshot);
    }

    pub fn add_udp_socket(&mut self, id: u64, snapshot: ResourceSnapshot) {
        self.udp_sockets.insert(id, snapshot);
    }

    /// Serialize to bytes for storage
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let payload = bincode::serialize(&ResourceMigrationSnapshotRef::from(self))?;
        let mut bytes = Vec::with_capacity(
            RESOURCE_SNAPSHOT_MAGIC.len() + std::mem::size_of::<u8>() + payload.len(),
        );
        bytes.extend_from_slice(&RESOURCE_SNAPSHOT_MAGIC);
        bytes.push(RESOURCE_SNAPSHOT_VERSION);
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if !bytes.starts_with(&RESOURCE_SNAPSHOT_MAGIC) {
            anyhow::bail!(
                "legacy unversioned resource snapshots are unsupported; create a fresh snapshot"
            );
        }
        let Some(version) = bytes.get(RESOURCE_SNAPSHOT_MAGIC.len()) else {
            anyhow::bail!("resource snapshot version is missing");
        };
        if *version != RESOURCE_SNAPSHOT_VERSION {
            anyhow::bail!("unsupported resource snapshot version");
        }

        let wire: ResourceMigrationSnapshotWire = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .reject_trailing_bytes()
            .deserialize(&bytes[RESOURCE_SNAPSHOT_MAGIC.len() + 1..])
            .map_err(|_| anyhow!("resource snapshot payload is invalid"))?;
        Ok(wire.into())
    }

    /// Check if there are any resources to migrate
    pub fn is_empty(&self) -> bool {
        self.tcp_listeners.is_empty()
            && self.tcp_streams.is_empty()
            && self.tls_listeners.is_empty()
            && self.tls_streams.is_empty()
            && self.udp_sockets.is_empty()
    }

    /// Get total count of resources
    pub fn count(&self) -> usize {
        self.tcp_listeners.len()
            + self.tcp_streams.len()
            + self.tls_listeners.len()
            + self.tls_streams.len()
            + self.udp_sockets.len()
    }
}

/// Trait for resources that can be snapshotted for migration
pub trait MigratableResource {
    /// Create a snapshot of this resource
    fn snapshot(&self) -> Result<ResourceSnapshot>;

    /// Restore from a snapshot (create new resource from saved data)
    fn restore(snapshot: &ResourceSnapshot) -> Result<Self>
    where
        Self: Sized;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_snapshot_serialization() {
        let mut snapshot = ResourceMigrationSnapshot::new();

        snapshot.add_tcp_stream(
            1,
            ResourceSnapshot::TcpConnection {
                peer_addr: "127.0.0.1:8080".to_string(),
                local_addr: "127.0.0.1:12345".to_string(),
            },
        );

        snapshot.add_tcp_listener(
            2,
            ResourceSnapshot::TcpListener {
                local_addr: "0.0.0.0:9000".to_string(),
            },
        );

        // Serialize
        let bytes = snapshot.to_bytes().unwrap();
        assert!(!bytes.is_empty());

        // Deserialize
        let restored = ResourceMigrationSnapshot::from_bytes(&bytes).unwrap();
        assert_eq!(restored.tcp_streams.len(), 1);
        assert_eq!(restored.tcp_listeners.len(), 1);
        assert_eq!(restored.count(), 2);
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = ResourceMigrationSnapshot::new();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.count(), 0);
    }

    #[test]
    fn snapshot_debug_redacts_addresses_and_credential_handles() {
        let credential_handle = TlsCredentialHandle::from_bytes([0x5a; 16]);
        let snapshot = ResourceSnapshot::TlsListener {
            local_addr: "address-sentinel".to_owned(),
            credential_handle,
        };

        let debug = format!("{snapshot:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("address-sentinel"));
        assert!(!debug.contains(&format!("{:?}", credential_handle.as_bytes())));

        let non_migratable = ResourceSnapshot::NonMigratable {
            resource_type: "private-key-marker".to_owned(),
            reason: "private-key-reason-marker".to_owned(),
        };
        let debug = format!("{non_migratable:?}");
        assert!(!debug.contains("private-key-marker"));
        assert!(!debug.contains("private-key-reason-marker"));
    }

    #[test]
    fn legacy_unversioned_snapshot_is_rejected_without_echoing_secret_bytes() {
        #[derive(Serialize)]
        enum LegacyResourceSnapshot {
            TlsListener {
                local_addr: String,
                cert_pem: Vec<u8>,
                key_pem: Vec<u8>,
            },
        }

        #[derive(Serialize)]
        struct LegacyResourceMigrationSnapshot {
            tcp_listeners: HashMap<u64, LegacyResourceSnapshot>,
            tcp_streams: HashMap<u64, LegacyResourceSnapshot>,
            tls_listeners: HashMap<u64, LegacyResourceSnapshot>,
            tls_streams: HashMap<u64, LegacyResourceSnapshot>,
            udp_sockets: HashMap<u64, LegacyResourceSnapshot>,
        }

        let key_marker = b"legacy-private-key-marker";
        let legacy = LegacyResourceMigrationSnapshot {
            tcp_listeners: HashMap::new(),
            tcp_streams: HashMap::new(),
            tls_listeners: HashMap::from([(
                7,
                LegacyResourceSnapshot::TlsListener {
                    local_addr: "127.0.0.1:8443".to_owned(),
                    cert_pem: b"legacy-certificate".to_vec(),
                    key_pem: key_marker.to_vec(),
                },
            )]),
            tls_streams: HashMap::new(),
            udp_sockets: HashMap::new(),
        };
        let bytes = bincode::serialize(&legacy).unwrap();
        assert!(bytes
            .windows(key_marker.len())
            .any(|window| window == key_marker));

        let error = ResourceMigrationSnapshot::from_bytes(&bytes).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("legacy unversioned resource snapshots are unsupported"));
        assert!(!message.contains("legacy-private-key-marker"));
    }

    #[test]
    fn unknown_and_truncated_versions_are_rejected_without_payload_details() {
        let marker = b"private-key-marker";
        let mut unknown = RESOURCE_SNAPSHOT_MAGIC.to_vec();
        unknown.push(RESOURCE_SNAPSHOT_VERSION + 1);
        unknown.extend_from_slice(marker);

        let error = ResourceMigrationSnapshot::from_bytes(&unknown).unwrap_err();
        assert_eq!(error.to_string(), "unsupported resource snapshot version");
        assert!(!error.to_string().contains("private-key-marker"));

        let error = ResourceMigrationSnapshot::from_bytes(&RESOURCE_SNAPSHOT_MAGIC).unwrap_err();
        assert_eq!(error.to_string(), "resource snapshot version is missing");
    }

    #[test]
    fn trailing_payload_is_rejected_without_echoing_secret_bytes() {
        let marker = b"appended-private-key-marker";
        let mut bytes = ResourceMigrationSnapshot::new().to_bytes().unwrap();
        bytes.extend_from_slice(marker);

        let error = ResourceMigrationSnapshot::from_bytes(&bytes).unwrap_err();
        assert_eq!(error.to_string(), "resource snapshot payload is invalid");
        assert!(!error.to_string().contains("appended-private-key-marker"));
    }
}
