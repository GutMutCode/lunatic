use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a serializable resource snapshot for migration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResourceSnapshot {
    /// TCP connection: store peer address for reconnection
    TcpConnection {
        peer_addr: String,
        local_addr: String,
    },
    /// TCP listener: store bound address
    TcpListener { local_addr: String },
    /// TLS client connection: store reconnection metadata
    TlsClientConnection {
        server_name: String,
        port: u16,
        peer_addr: Option<String>,
        local_addr: Option<String>,
        custom_root_certs: Vec<Vec<u8>>,
        read_timeout_ms: Option<u64>,
        write_timeout_ms: Option<u64>,
    },
    /// TLS server connection: graceful shutdown (client must reconnect)
    TlsServerConnection {
        graceful_shutdown: bool,
        reason: String,
    },
    /// TLS listener: store bound address and certificate info
    TlsListener {
        local_addr: String,
        cert_pem: Vec<u8>,
        key_pem: Vec<u8>,
    },
    /// UDP socket: store bound address
    UdpSocket { local_addr: String },
    /// Generic placeholder for resources that can't be migrated
    NonMigratable {
        resource_type: String,
        reason: String,
    },
}

/// Collection of resource snapshots by resource ID
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResourceMigrationSnapshot {
    pub tcp_listeners: HashMap<u64, ResourceSnapshot>,
    pub tcp_streams: HashMap<u64, ResourceSnapshot>,
    pub tls_listeners: HashMap<u64, ResourceSnapshot>,
    pub tls_streams: HashMap<u64, ResourceSnapshot>,
    pub udp_sockets: HashMap<u64, ResourceSnapshot>,
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
        Ok(bincode::serialize(self)?)
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes)?)
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
}
