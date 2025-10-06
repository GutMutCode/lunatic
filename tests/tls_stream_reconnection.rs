use anyhow::Result;
use lunatic_process::resource_migration::{ResourceMigrationSnapshot, ResourceSnapshot};
use std::time::Duration;

#[tokio::test]
async fn test_tls_client_connection_snapshot() -> Result<()> {
    // Create a TLS client connection snapshot
    let snapshot = ResourceSnapshot::TlsClientConnection {
        server_name: "api.example.com".to_string(),
        port: 443,
        peer_addr: Some("93.184.216.34:443".to_string()),
        local_addr: Some("192.168.1.100:54321".to_string()),
        custom_root_certs: vec![],
        read_timeout_ms: Some(5000),
        write_timeout_ms: Some(3000),
    };

    println!("✓ Created TLS client connection snapshot");

    // Verify snapshot structure
    match &snapshot {
        ResourceSnapshot::TlsClientConnection {
            server_name,
            port,
            peer_addr,
            local_addr,
            read_timeout_ms,
            write_timeout_ms,
            ..
        } => {
            assert_eq!(server_name, "api.example.com");
            assert_eq!(*port, 443);
            assert_eq!(peer_addr.as_ref().unwrap(), "93.184.216.34:443");
            assert_eq!(local_addr.as_ref().unwrap(), "192.168.1.100:54321");
            assert_eq!(*read_timeout_ms, Some(5000));
            assert_eq!(*write_timeout_ms, Some(3000));
            println!("✓ Client connection snapshot data verified");
            println!("  - Server: {}:{}", server_name, port);
            println!("  - Timeouts: read={}ms, write={}ms",
                     read_timeout_ms.unwrap(), write_timeout_ms.unwrap());
        }
        _ => panic!("Expected TlsClientConnection snapshot"),
    }

    Ok(())
}

#[tokio::test]
async fn test_tls_server_connection_snapshot() -> Result<()> {
    // Server-accepted connections use graceful shutdown
    let snapshot = ResourceSnapshot::TlsServerConnection {
        graceful_shutdown: true,
        reason: "Server-accepted TLS connections require client reconnection after hot reload"
            .to_string(),
    };

    println!("✓ Created TLS server connection snapshot");

    match &snapshot {
        ResourceSnapshot::TlsServerConnection {
            graceful_shutdown,
            reason,
        } => {
            assert!(*graceful_shutdown);
            assert!(reason.contains("client reconnection"));
            println!("✓ Server connection marked for graceful shutdown");
            println!("  - Reason: {}", reason);
        }
        _ => panic!("Expected TlsServerConnection snapshot"),
    }

    Ok(())
}

#[tokio::test]
async fn test_tls_client_snapshot_serialization() -> Result<()> {
    let mut migration_snapshot = ResourceMigrationSnapshot::new();

    // Add client connection snapshot
    migration_snapshot.add_tls_stream(
        1,
        ResourceSnapshot::TlsClientConnection {
            server_name: "secure.example.com".to_string(),
            port: 8443,
            peer_addr: Some("10.0.0.5:8443".to_string()),
            local_addr: Some("10.0.0.1:49152".to_string()),
            custom_root_certs: vec![vec![0x30, 0x82, 0x01, 0x0a]], // Mock cert
            read_timeout_ms: Some(10000),
            write_timeout_ms: Some(5000),
        },
    );

    // Add server connection snapshot
    migration_snapshot.add_tls_stream(
        2,
        ResourceSnapshot::TlsServerConnection {
            graceful_shutdown: true,
            reason: "Hot reload in progress".to_string(),
        },
    );

    // Serialize
    let bytes = migration_snapshot.to_bytes()?;
    assert!(!bytes.is_empty());
    println!("✓ Serialized TLS migration snapshot to {} bytes", bytes.len());

    // Deserialize
    let restored = ResourceMigrationSnapshot::from_bytes(&bytes)?;
    assert_eq!(restored.tls_streams.len(), 2);
    println!("✓ Deserialized {} TLS stream snapshots", restored.tls_streams.len());

    // Verify client connection
    let client_snapshot = restored.tls_streams.get(&1).unwrap();
    match client_snapshot {
        ResourceSnapshot::TlsClientConnection {
            server_name,
            port,
            custom_root_certs,
            ..
        } => {
            assert_eq!(server_name, "secure.example.com");
            assert_eq!(*port, 8443);
            assert_eq!(custom_root_certs.len(), 1);
            println!("✓ Client connection data preserved: {}:{}", server_name, port);
        }
        _ => panic!("Expected TlsClientConnection"),
    }

    // Verify server connection
    let server_snapshot = restored.tls_streams.get(&2).unwrap();
    match server_snapshot {
        ResourceSnapshot::TlsServerConnection {
            graceful_shutdown, ..
        } => {
            assert!(*graceful_shutdown);
            println!("✓ Server connection graceful shutdown preserved");
        }
        _ => panic!("Expected TlsServerConnection"),
    }

    Ok(())
}

#[tokio::test]
async fn test_tls_reconnection_metadata() -> Result<()> {
    // Test with custom root certificates
    let custom_cert = vec![0x30, 0x82, 0x03, 0x21]; // Mock PEM data
    let snapshot = ResourceSnapshot::TlsClientConnection {
        server_name: "internal.corp.com".to_string(),
        port: 9443,
        peer_addr: Some("172.16.0.10:9443".to_string()),
        local_addr: None,
        custom_root_certs: vec![custom_cert.clone()],
        read_timeout_ms: None,
        write_timeout_ms: Some(30000),
    };

    match &snapshot {
        ResourceSnapshot::TlsClientConnection {
            custom_root_certs, ..
        } => {
            assert_eq!(custom_root_certs.len(), 1);
            assert_eq!(custom_root_certs[0], custom_cert);
            println!("✓ Custom root certificates preserved in snapshot");
            println!("  - Cert count: {}", custom_root_certs.len());
        }
        _ => panic!("Expected TlsClientConnection"),
    }

    Ok(())
}

#[test]
fn test_tls_snapshot_timeout_handling() {
    // Test various timeout configurations
    let test_cases = vec![
        (Some(5000), Some(3000), "Both timeouts"),
        (Some(10000), None, "Read timeout only"),
        (None, Some(15000), "Write timeout only"),
        (None, None, "No timeouts"),
    ];

    for (read_ms, write_ms, description) in test_cases {
        let snapshot = ResourceSnapshot::TlsClientConnection {
            server_name: "test.example.com".to_string(),
            port: 443,
            peer_addr: None,
            local_addr: None,
            custom_root_certs: vec![],
            read_timeout_ms: read_ms,
            write_timeout_ms: write_ms,
        };

        match snapshot {
            ResourceSnapshot::TlsClientConnection {
                read_timeout_ms: r,
                write_timeout_ms: w,
                ..
            } => {
                assert_eq!(r, read_ms);
                assert_eq!(w, write_ms);
                println!("✓ Timeout configuration preserved: {}", description);
            }
            _ => panic!("Expected TlsClientConnection"),
        }
    }
}

#[tokio::test]
async fn test_migration_snapshot_count() -> Result<()> {
    let mut snapshot = ResourceMigrationSnapshot::new();

    // Add various TLS resources
    snapshot.add_tls_listener(
        1,
        ResourceSnapshot::TlsListener {
            local_addr: "0.0.0.0:8443".to_string(),
            cert_pem: vec![0x30, 0x82],
            key_pem: vec![0x30, 0x82],
        },
    );

    snapshot.add_tls_stream(
        2,
        ResourceSnapshot::TlsClientConnection {
            server_name: "api1.example.com".to_string(),
            port: 443,
            peer_addr: None,
            local_addr: None,
            custom_root_certs: vec![],
            read_timeout_ms: None,
            write_timeout_ms: None,
        },
    );

    snapshot.add_tls_stream(
        3,
        ResourceSnapshot::TlsClientConnection {
            server_name: "api2.example.com".to_string(),
            port: 443,
            peer_addr: None,
            local_addr: None,
            custom_root_certs: vec![],
            read_timeout_ms: None,
            write_timeout_ms: None,
        },
    );

    snapshot.add_tls_stream(
        4,
        ResourceSnapshot::TlsServerConnection {
            graceful_shutdown: true,
            reason: "Test".to_string(),
        },
    );

    assert_eq!(snapshot.tls_listeners.len(), 1);
    assert_eq!(snapshot.tls_streams.len(), 3);
    assert_eq!(snapshot.count(), 4);
    println!("✓ Migration snapshot resource count: {}", snapshot.count());
    println!("  - TLS listeners: {}", snapshot.tls_listeners.len());
    println!("  - TLS streams: {}", snapshot.tls_streams.len());

    Ok(())
}
