use lunatic_distributed::distributed::{
    DistributedRegistry, GlobalProcessId, GlobalRegisterResult, RegistryCoordinationMessage,
    RegistryCoordinator,
};
use std::sync::Arc;

/// Test cross-node registration coordination workflow
///
/// Note: This is a simplified test - in practice, the coordinator's
/// handle_register_response requires a pending request to be set up first
/// via register_global_coordinated. For now, we test the notification path directly.
#[tokio::test]
async fn test_cross_node_registration_workflow() {
    // Simulate 2-node cluster
    let registry1 = Arc::new(DistributedRegistry::new(1));
    let registry2 = Arc::new(DistributedRegistry::new(2));

    let coordinator1 = RegistryCoordinator::new(registry1.clone(), 1);
    let coordinator2 = RegistryCoordinator::new(registry2.clone(), 2);

    let gpid = GlobalProcessId::new(1, 1, 100);

    // Node 1: Request global registration
    let request_id = 1;

    // Node 2: Handle the request
    let response = coordinator2
        .handle_register_request(request_id, 1, "global_service".to_string(), gpid)
        .await;

    // Should get success response (no conflict)
    match response {
        RegistryCoordinationMessage::GlobalRegisterResponse {
            request_id: rid,
            result,
        } => {
            assert_eq!(rid, request_id);
            assert!(matches!(result, GlobalRegisterResult::Success));
        }
        _ => panic!("Expected GlobalRegisterResponse"),
    }

    // In practice, Node 1 would collect responses and then broadcast notification
    // For this test, we directly apply the notification to both nodes

    // Node 1: Apply notification (commit locally)
    coordinator1
        .handle_register_notify("global_service".to_string(), gpid, 123456)
        .await
        .unwrap();

    // Node 2: Apply notification
    coordinator2
        .handle_register_notify("global_service".to_string(), gpid, 123456)
        .await
        .unwrap();

    // Both nodes should have the registration
    assert!(registry1.lookup_global("global_service").is_some());
    assert!(registry2.lookup_global("global_service").is_some());
}

/// Test conflict detection in global registration
#[tokio::test]
async fn test_global_registration_conflict() {
    let registry1 = Arc::new(DistributedRegistry::new(1));
    let registry2 = Arc::new(DistributedRegistry::new(2));

    let _coordinator1 = RegistryCoordinator::new(registry1.clone(), 1);
    let coordinator2 = RegistryCoordinator::new(registry2.clone(), 2);

    // Node 2: Pre-register the service
    let gpid2 = GlobalProcessId::new(2, 1, 200);
    registry2
        .register_global("conflict_service", gpid2)
        .unwrap();

    // Node 1: Try to register same name
    let gpid1 = GlobalProcessId::new(1, 1, 100);
    let request_id = 1;

    // Node 2: Handle request - should detect conflict
    let response = coordinator2
        .handle_register_request(request_id, 1, "conflict_service".to_string(), gpid1)
        .await;

    match response {
        RegistryCoordinationMessage::GlobalRegisterResponse {
            request_id: rid,
            result,
        } => {
            assert_eq!(rid, request_id);
            match result {
                GlobalRegisterResult::AlreadyRegistered { existing_gpid, .. } => {
                    assert_eq!(existing_gpid, gpid2);
                }
                _ => panic!("Expected AlreadyRegistered result"),
            }
        }
        _ => panic!("Expected GlobalRegisterResponse"),
    }
}

/// Test registry synchronization for new nodes
#[tokio::test]
async fn test_registry_sync_for_new_node() {
    let registry1 = Arc::new(DistributedRegistry::new(1));
    let registry2 = Arc::new(DistributedRegistry::new(2)); // New node joining

    let coordinator1 = RegistryCoordinator::new(registry1.clone(), 1);
    let coordinator2 = RegistryCoordinator::new(registry2.clone(), 2);

    // Node 1: Has existing global registrations
    let gpid_a = GlobalProcessId::new(1, 1, 100);
    let gpid_b = GlobalProcessId::new(1, 1, 200);
    registry1.register_global("service_a", gpid_a).unwrap();
    registry1.register_global("service_b", gpid_b).unwrap();

    // Node 2: Request sync
    let request_id = 1;
    let response = coordinator1.handle_sync_request(request_id, 2).await;

    match response {
        RegistryCoordinationMessage::RegistrySyncResponse {
            request_id: rid,
            global_entries,
        } => {
            assert_eq!(rid, request_id);
            assert_eq!(global_entries.len(), 2);

            // Node 2: Apply sync
            coordinator2
                .handle_sync_response(global_entries)
                .await
                .unwrap();
        }
        _ => panic!("Expected RegistrySyncResponse"),
    }

    // Node 2 should now have both services
    assert_eq!(registry2.global_count(), 2);
    assert!(registry2.lookup_global("service_a").is_some());
    assert!(registry2.lookup_global("service_b").is_some());
}

/// Test node failure cleanup
#[tokio::test]
async fn test_node_failure_cleanup() {
    let registry = Arc::new(DistributedRegistry::new(1));
    let coordinator = RegistryCoordinator::new(registry.clone(), 1);

    // Register services from different nodes
    let gpid_node2_a = GlobalProcessId::new(2, 1, 100);
    let gpid_node2_b = GlobalProcessId::new(2, 1, 200);
    let gpid_node3 = GlobalProcessId::new(3, 1, 300);

    registry
        .register_global("node2_service_a", gpid_node2_a)
        .unwrap();
    registry
        .register_global("node2_service_b", gpid_node2_b)
        .unwrap();
    registry
        .register_global("node3_service", gpid_node3)
        .unwrap();

    assert_eq!(registry.global_count(), 3);

    // Node 2 fails - cleanup its registrations
    let cleaned = coordinator.cleanup_node_registrations(2).await;

    assert_eq!(cleaned.len(), 2);
    assert_eq!(registry.global_count(), 1);

    // Node 3 service should still be registered
    assert!(registry.lookup_global("node3_service").is_some());

    // Node 2 services should be gone
    assert!(registry.lookup_global("node2_service_a").is_none());
    assert!(registry.lookup_global("node2_service_b").is_none());
}

/// Test single-node fast path (no coordination needed)
#[tokio::test]
async fn test_single_node_fast_path() {
    let registry = Arc::new(DistributedRegistry::new(1));
    let coordinator = RegistryCoordinator::new(registry.clone(), 1);

    let gpid = GlobalProcessId::new(1, 1, 100);

    // Single node (node_count = 1) should register immediately
    let result = coordinator
        .register_global_coordinated("fast_service", gpid, 1)
        .await;

    assert!(result.is_ok());
    assert!(registry.lookup_global("fast_service").is_some());
}

/// Test idempotent notification handling
#[tokio::test]
async fn test_idempotent_notifications() {
    let registry = Arc::new(DistributedRegistry::new(1));
    let coordinator = RegistryCoordinator::new(registry.clone(), 1);

    let gpid = GlobalProcessId::new(2, 1, 200);

    // First notification
    coordinator
        .handle_register_notify("idempotent_service".to_string(), gpid, 123456)
        .await
        .unwrap();

    assert_eq!(registry.global_count(), 1);

    // Second notification (duplicate) - should be idempotent
    coordinator
        .handle_register_notify("idempotent_service".to_string(), gpid, 123457)
        .await
        .unwrap();

    // Should still be 1 (not duplicated)
    assert_eq!(registry.global_count(), 1);
}

/// Test unregistration workflow
#[tokio::test]
async fn test_unregistration_workflow() {
    let registry1 = Arc::new(DistributedRegistry::new(1));
    let registry2 = Arc::new(DistributedRegistry::new(2));

    let _coordinator1 = RegistryCoordinator::new(registry1.clone(), 1);
    let coordinator2 = RegistryCoordinator::new(registry2.clone(), 2);

    // Both nodes have the service registered
    let gpid = GlobalProcessId::new(1, 1, 100);
    registry1.register_global("temp_service", gpid).unwrap();
    registry2.register_global("temp_service", gpid).unwrap();

    // Node 1: Request unregistration
    let request_id = 1;
    let response = coordinator2
        .handle_unregister_request(request_id, 1, "temp_service".to_string())
        .await;

    // Should get success response
    match response {
        RegistryCoordinationMessage::GlobalUnregisterResponse { .. } => {}
        _ => panic!("Expected GlobalUnregisterResponse"),
    }

    // Node 2: Apply unregister notification
    coordinator2
        .handle_unregister_notify("temp_service".to_string())
        .await
        .unwrap();

    assert!(registry2.lookup_global("temp_service").is_none());
}

/// Simulates a 3-node cluster with concurrent registrations
#[tokio::test]
async fn test_three_node_cluster_simulation() {
    let registry1 = Arc::new(DistributedRegistry::new(1));
    let registry2 = Arc::new(DistributedRegistry::new(2));
    let registry3 = Arc::new(DistributedRegistry::new(3));

    let coordinator1 = RegistryCoordinator::new(registry1.clone(), 1);
    let coordinator2 = RegistryCoordinator::new(registry2.clone(), 2);
    let coordinator3 = RegistryCoordinator::new(registry3.clone(), 3);

    // Each node registers a different service
    let gpid1 = GlobalProcessId::new(1, 1, 100);
    let gpid2 = GlobalProcessId::new(2, 1, 200);
    let gpid3 = GlobalProcessId::new(3, 1, 300);

    // Simulate successful coordinated registrations
    coordinator1
        .handle_register_notify("service1".to_string(), gpid1, 1000)
        .await
        .unwrap();
    coordinator2
        .handle_register_notify("service1".to_string(), gpid1, 1000)
        .await
        .unwrap();
    coordinator3
        .handle_register_notify("service1".to_string(), gpid1, 1000)
        .await
        .unwrap();

    coordinator1
        .handle_register_notify("service2".to_string(), gpid2, 2000)
        .await
        .unwrap();
    coordinator2
        .handle_register_notify("service2".to_string(), gpid2, 2000)
        .await
        .unwrap();
    coordinator3
        .handle_register_notify("service2".to_string(), gpid2, 2000)
        .await
        .unwrap();

    coordinator1
        .handle_register_notify("service3".to_string(), gpid3, 3000)
        .await
        .unwrap();
    coordinator2
        .handle_register_notify("service3".to_string(), gpid3, 3000)
        .await
        .unwrap();
    coordinator3
        .handle_register_notify("service3".to_string(), gpid3, 3000)
        .await
        .unwrap();

    // All nodes should have all 3 services
    assert_eq!(registry1.global_count(), 3);
    assert_eq!(registry2.global_count(), 3);
    assert_eq!(registry3.global_count(), 3);

    // Verify each service is accessible from all nodes
    for service in ["service1", "service2", "service3"] {
        assert!(registry1.lookup_global(service).is_some());
        assert!(registry2.lookup_global(service).is_some());
        assert!(registry3.lookup_global(service).is_some());
    }
}
