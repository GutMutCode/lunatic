use anyhow::Result;
use lunatic_process::resource_migration::{
    ResourceMigrationSnapshot, ResourceSnapshot, TlsCredentialHandle,
};

#[test]
fn test_tls_listener_snapshot_structure() -> Result<()> {
    let local_addr = "127.0.0.1:8443".to_string();
    let credential_handle = TlsCredentialHandle::from_bytes([0x5a; 16]);

    let snapshot = ResourceSnapshot::TlsListener {
        local_addr: local_addr.clone(),
        credential_handle,
    };

    match &snapshot {
        ResourceSnapshot::TlsListener {
            local_addr: addr,
            credential_handle: snapshot_handle,
        } => {
            assert_eq!(addr, &local_addr);
            assert_eq!(snapshot_handle.as_bytes(), credential_handle.as_bytes());
        }
        _ => panic!("Expected TlsListener snapshot"),
    }

    Ok(())
}

#[test]
fn test_tls_listener_snapshot_serialization_contains_only_opaque_handle() -> Result<()> {
    let private_key_marker = b"private-key-material-must-never-be-serialized";
    let addr = "127.0.0.1:9443";
    let credential_handle = TlsCredentialHandle::from_bytes([0xa5; 16]);

    let snapshot = ResourceSnapshot::TlsListener {
        local_addr: addr.to_string(),
        credential_handle,
    };

    let mut migration_snapshot = ResourceMigrationSnapshot::new();
    migration_snapshot.add_tls_listener(1, snapshot);

    let bytes = migration_snapshot.to_bytes()?;
    assert!(!bytes.is_empty());
    assert!(!bytes
        .windows(private_key_marker.len())
        .any(|window| window == private_key_marker));

    let restored_migration = ResourceMigrationSnapshot::from_bytes(&bytes)?;
    assert_eq!(restored_migration.tls_listeners.len(), 1);

    let restored_snapshot = restored_migration.tls_listeners.get(&1).unwrap();
    match restored_snapshot {
        ResourceSnapshot::TlsListener {
            local_addr,
            credential_handle: restored_handle,
        } => {
            assert_eq!(local_addr, addr);
            assert_eq!(restored_handle.as_bytes(), credential_handle.as_bytes());
        }
        _ => panic!("Expected TlsListener snapshot"),
    }

    Ok(())
}

#[test]
fn serialized_tls_stream_state_can_be_marked_non_migratable() {
    // Persisted snapshots cannot contain enough state to recreate a live TLS
    // application byte stream. In-process hot reload uses live handle transfer.
    let snapshot = ResourceSnapshot::NonMigratable {
        resource_type: "serialized_tls_stream".to_string(),
        reason: "An active TLS/application byte stream cannot be recreated from a snapshot"
            .to_string(),
    };

    match snapshot {
        ResourceSnapshot::NonMigratable {
            resource_type,
            reason,
        } => {
            assert_eq!(resource_type, "serialized_tls_stream");
            assert!(reason.contains("cannot be recreated"));
        }
        _ => panic!("Expected NonMigratable snapshot"),
    }
}
