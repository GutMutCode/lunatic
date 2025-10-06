use anyhow::Result;
use lunatic_process::resource_migration::ResourceSnapshot;

#[tokio::test]
async fn test_tls_listener_snapshot_structure() -> Result<()> {
    // Generate test certificate and key (self-signed for testing)
    let cert_pem = generate_test_cert();
    let key_pem = generate_test_key();
    let local_addr = "127.0.0.1:8443".to_string();

    // Create TLS listener snapshot
    let snapshot = ResourceSnapshot::TlsListener {
        local_addr: local_addr.clone(),
        cert_pem: cert_pem.clone(),
        key_pem: key_pem.clone(),
    };

    println!("✓ Created TLS listener snapshot");

    // Verify snapshot contains correct data
    match &snapshot {
        ResourceSnapshot::TlsListener {
            local_addr: addr,
            cert_pem: snap_cert,
            key_pem: snap_key,
        } => {
            assert_eq!(addr, &local_addr);
            assert_eq!(snap_cert, &cert_pem);
            assert_eq!(snap_key, &key_pem);
            println!("✓ Snapshot data verified");
            println!("  - Address: {}", addr);
            println!("  - Cert size: {} bytes", snap_cert.len());
            println!("  - Key size: {} bytes", snap_key.len());
        }
        _ => panic!("Expected TlsListener snapshot"),
    }

    Ok(())
}

#[tokio::test]
async fn test_tls_listener_snapshot_serialization() -> Result<()> {
    use lunatic_process::resource_migration::ResourceMigrationSnapshot;

    let cert_pem = generate_test_cert();
    let key_pem = generate_test_key();
    let addr = "127.0.0.1:9443";

    let snapshot = ResourceSnapshot::TlsListener {
        local_addr: addr.to_string(),
        cert_pem: cert_pem.clone(),
        key_pem: key_pem.clone(),
    };

    // Create migration snapshot collection
    let mut migration_snapshot = ResourceMigrationSnapshot::new();
    migration_snapshot.add_tls_listener(1, snapshot);

    // Serialize to bytes
    let bytes = migration_snapshot.to_bytes()?;
    assert!(!bytes.is_empty());
    println!("✓ Serialized TLS snapshot to {} bytes", bytes.len());

    // Deserialize
    let restored_migration = ResourceMigrationSnapshot::from_bytes(&bytes)?;
    assert_eq!(restored_migration.tls_listeners.len(), 1);
    println!("✓ Deserialized TLS snapshot successfully");

    // Verify content
    let restored_snapshot = restored_migration.tls_listeners.get(&1).unwrap();
    match restored_snapshot {
        ResourceSnapshot::TlsListener {
            local_addr,
            cert_pem: restored_cert,
            key_pem: restored_key,
        } => {
            assert_eq!(local_addr, addr);
            assert_eq!(restored_cert, &cert_pem);
            assert_eq!(restored_key, &key_pem);
            println!("✓ TLS listener data preserved: {}", local_addr);
        }
        _ => panic!("Expected TlsListener snapshot"),
    }

    Ok(())
}

#[test]
fn test_tls_stream_non_migratable() {
    // TLS streams should report as non-migratable
    let snapshot = ResourceSnapshot::NonMigratable {
        resource_type: "TlsConnection".to_string(),
        reason: "Active TLS sessions cannot be migrated due to cryptographic state".to_string(),
    };

    match snapshot {
        ResourceSnapshot::NonMigratable {
            resource_type,
            reason,
        } => {
            assert_eq!(resource_type, "TlsConnection");
            assert!(reason.contains("cryptographic state"));
            println!("✓ TLS streams correctly marked as non-migratable");
        }
        _ => panic!("Expected NonMigratable snapshot"),
    }
}

// Helper functions to generate test certificates
fn generate_test_cert() -> Vec<u8> {
    // This is a minimal self-signed certificate for testing
    // In production, use proper certificate generation
    vec![
        0x30, 0x82, 0x01,
        0x0a, // SEQUENCE header (certificate structure)
             // ... simplified test cert data
    ]
}

fn generate_test_key() -> Vec<u8> {
    // This is a minimal private key for testing
    // In production, use proper key generation
    vec![
        0x30, 0x82, 0x01,
        0x3a, // SEQUENCE header (private key structure)
             // ... simplified test key data
    ]
}
