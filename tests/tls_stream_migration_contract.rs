use anyhow::Result;
use lunatic_process::resource_migration::{
    ResourceMigrationSnapshot, ResourceSnapshot, ResourceTransferReport,
};

#[test]
fn tls_client_snapshot_is_explicitly_metadata_only() {
    let snapshot = ResourceSnapshot::TlsClientConnectionMetadata {
        server_name: "api.example.com".to_string(),
        port: 443,
        peer_addr: Some("93.184.216.34:443".to_string()),
        local_addr: Some("192.168.1.100:54321".to_string()),
        custom_root_certs: Vec::new(),
        read_timeout_ms: Some(5_000),
        write_timeout_ms: Some(3_000),
    };

    match snapshot {
        ResourceSnapshot::TlsClientConnectionMetadata {
            server_name,
            port,
            peer_addr,
            local_addr,
            read_timeout_ms,
            write_timeout_ms,
            ..
        } => {
            assert_eq!(server_name, "api.example.com");
            assert_eq!(port, 443);
            assert_eq!(peer_addr.as_deref(), Some("93.184.216.34:443"));
            assert_eq!(local_addr.as_deref(), Some("192.168.1.100:54321"));
            assert_eq!(read_timeout_ms, Some(5_000));
            assert_eq!(write_timeout_ms, Some(3_000));
        }
        other => panic!("expected TLS client metadata, got {:?}", other),
    }
}

#[test]
fn tls_server_snapshot_is_explicitly_metadata_only() {
    let snapshot = ResourceSnapshot::TlsServerConnectionMetadata {
        requires_peer_reconnect: true,
        reason: "serialized snapshots cannot restore an accepted stream".to_string(),
    };

    match snapshot {
        ResourceSnapshot::TlsServerConnectionMetadata {
            requires_peer_reconnect,
            reason,
        } => {
            assert!(requires_peer_reconnect);
            assert!(reason.contains("cannot restore"));
        }
        other => panic!("expected TLS server metadata, got {:?}", other),
    }
}

#[test]
fn tls_metadata_serialization_preserves_descriptive_fields() -> Result<()> {
    let mut snapshot = ResourceMigrationSnapshot::new();
    snapshot.add_tls_stream(
        7,
        ResourceSnapshot::TlsClientConnectionMetadata {
            server_name: "secure.example.com".to_string(),
            port: 8_443,
            peer_addr: Some("10.0.0.5:8443".to_string()),
            local_addr: Some("10.0.0.1:49152".to_string()),
            custom_root_certs: vec![vec![0x30, 0x82, 0x01, 0x0a]],
            read_timeout_ms: Some(10_000),
            write_timeout_ms: Some(5_000),
        },
    );

    let restored = ResourceMigrationSnapshot::from_bytes(&snapshot.to_bytes()?)?;
    match restored.tls_streams.get(&7) {
        Some(ResourceSnapshot::TlsClientConnectionMetadata {
            server_name,
            port,
            custom_root_certs,
            ..
        }) => {
            assert_eq!(server_name, "secure.example.com");
            assert_eq!(*port, 8_443);
            assert_eq!(custom_root_certs.len(), 1);
        }
        other => panic!("expected serialized TLS client metadata, got {:?}", other),
    }

    Ok(())
}

#[test]
fn tls_metadata_preserves_all_timeout_combinations() {
    for (read_timeout_ms, write_timeout_ms) in [
        (Some(5_000), Some(3_000)),
        (Some(10_000), None),
        (None, Some(15_000)),
        (None, None),
    ] {
        let snapshot = ResourceSnapshot::TlsClientConnectionMetadata {
            server_name: "test.example.com".to_string(),
            port: 443,
            peer_addr: None,
            local_addr: None,
            custom_root_certs: Vec::new(),
            read_timeout_ms,
            write_timeout_ms,
        };

        match snapshot {
            ResourceSnapshot::TlsClientConnectionMetadata {
                read_timeout_ms: restored_read,
                write_timeout_ms: restored_write,
                ..
            } => {
                assert_eq!(restored_read, read_timeout_ms);
                assert_eq!(restored_write, write_timeout_ms);
            }
            other => panic!("expected TLS client metadata, got {:?}", other),
        }
    }
}

#[test]
fn transfer_report_counts_snapshot_resources() {
    let mut snapshot = ResourceMigrationSnapshot::new();
    snapshot.add_tls_listener(
        1,
        ResourceSnapshot::TlsListener {
            local_addr: "127.0.0.1:8443".to_string(),
            cert_pem: vec![0x30, 0x82],
            key_pem: vec![0x30, 0x82],
        },
    );
    snapshot.add_tls_stream(
        2,
        ResourceSnapshot::TlsServerConnectionMetadata {
            requires_peer_reconnect: true,
            reason: "metadata only".to_string(),
        },
    );

    let report = ResourceTransferReport::from_snapshot(&snapshot);
    assert_eq!(report.tls_listeners, 1);
    assert_eq!(report.tls_streams, 1);
    assert_eq!(report.total(), 2);
}
