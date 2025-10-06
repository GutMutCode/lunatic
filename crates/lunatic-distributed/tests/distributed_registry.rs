use lunatic_distributed::distributed::{
    GlobalProcessId, DistributedRegistry, ProcessName, RegistrationScope,
};

#[test]
fn test_global_process_id_basics() {
    let gpid = GlobalProcessId::new(1, 2, 3);

    assert_eq!(gpid.node_id(), 1);
    assert_eq!(gpid.environment_id(), 2);
    assert_eq!(gpid.process_id(), 3);

    assert!(gpid.is_local(1));
    assert!(!gpid.is_local(2));
}

#[test]
fn test_compact_encoding_preserves_data() {
    let test_cases = vec![
        GlobalProcessId::new(0, 0, 0),
        GlobalProcessId::new(1, 1, 1),
        GlobalProcessId::new(0xFFFF, 0xFFFF, 0xFFFFFFFF),
        GlobalProcessId::new(42, 100, 987654321),
    ];

    for original in test_cases {
        let compact = original.to_compact();
        let decoded = GlobalProcessId::from_compact(compact);

        assert_eq!(original, decoded, "Round-trip encoding failed for {:?}", original);
    }
}

#[test]
fn test_registry_local_registration() {
    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    // Register process
    assert!(registry.register_local("logger", gpid).is_ok());

    // Lookup succeeds
    let entry = registry.lookup("logger").expect("Lookup should succeed");
    assert_eq!(entry.global_pid, gpid);
    assert_eq!(entry.scope, RegistrationScope::Local);

    // Duplicate registration fails
    let gpid2 = GlobalProcessId::new(1, 1, 200);
    assert!(registry.register_local("logger", gpid2).is_err());
}

#[test]
fn test_registry_global_registration() {
    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    // Register globally
    assert!(registry.register_global("database_manager", gpid).is_ok());

    // Lookup in global scope
    let entry = registry.lookup_global("database_manager")
        .expect("Global lookup should succeed");
    assert_eq!(entry.global_pid, gpid);
    assert_eq!(entry.scope, RegistrationScope::Global);

    // Generic lookup also works
    let entry2 = registry.lookup("database_manager")
        .expect("Generic lookup should succeed");
    assert_eq!(entry2.global_pid, gpid);
}

#[test]
fn test_registry_unregister() {
    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    registry.register_local("temp_service", gpid).unwrap();
    assert!(registry.lookup("temp_service").is_some());

    // Unregister
    assert!(registry.unregister("temp_service").is_ok());
    assert!(registry.lookup("temp_service").is_none());

    // Unregister non-existent name fails
    assert!(registry.unregister("non_existent").is_err());
}

#[test]
fn test_registry_unregister_process() {
    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    // Register multiple names for same process
    registry.register_local("alias1", gpid).unwrap();
    registry.register_global("alias2", gpid).unwrap();
    registry.register_local("alias3", gpid).unwrap();

    assert_eq!(registry.local_count(), 2);
    assert_eq!(registry.global_count(), 1);

    // Unregister process removes all names
    registry.unregister_process(gpid);

    assert!(registry.lookup("alias1").is_none());
    assert!(registry.lookup("alias2").is_none());
    assert!(registry.lookup("alias3").is_none());
    assert_eq!(registry.local_count(), 0);
    assert_eq!(registry.global_count(), 0);
}

#[test]
fn test_registry_reverse_lookup() {
    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    registry.register_local("service_a", gpid).unwrap();
    registry.register_global("service_b", gpid).unwrap();

    let names = registry.get_names(gpid);
    assert_eq!(names.len(), 2);

    let name_strs: Vec<&str> = names.iter().map(|n| n.as_str()).collect();
    assert!(name_strs.contains(&"service_a"));
    assert!(name_strs.contains(&"service_b"));
}

#[test]
fn test_registry_local_vs_global_scope() {
    let registry = DistributedRegistry::new(1);
    let gpid_local = GlobalProcessId::new(1, 1, 100);
    let gpid_global = GlobalProcessId::new(2, 1, 200);

    registry.register_local("local_service", gpid_local).unwrap();
    registry.register_global("global_service", gpid_global).unwrap();

    // Local-only lookup
    assert!(registry.lookup_local("local_service").is_some());
    assert!(registry.lookup_local("global_service").is_none());

    // Global-only lookup
    assert!(registry.lookup_global("global_service").is_some());
    assert!(registry.lookup_global("local_service").is_none());

    // Generic lookup finds both
    assert!(registry.lookup("local_service").is_some());
    assert!(registry.lookup("global_service").is_some());
}

#[test]
fn test_registry_counters_and_lists() {
    let registry = DistributedRegistry::new(1);
    let gpid1 = GlobalProcessId::new(1, 1, 100);
    let gpid2 = GlobalProcessId::new(1, 1, 200);
    let gpid3 = GlobalProcessId::new(1, 1, 300);

    registry.register_local("local1", gpid1).unwrap();
    registry.register_local("local2", gpid2).unwrap();
    registry.register_global("global1", gpid3).unwrap();

    assert_eq!(registry.local_count(), 2);
    assert_eq!(registry.global_count(), 1);

    let local_names = registry.local_names();
    assert_eq!(local_names.len(), 2);

    let global_names = registry.global_names();
    assert_eq!(global_names.len(), 1);
}

#[test]
fn test_registry_clear() {
    let registry = DistributedRegistry::new(1);
    let gpid1 = GlobalProcessId::new(1, 1, 100);
    let gpid2 = GlobalProcessId::new(1, 1, 200);

    registry.register_local("name1", gpid1).unwrap();
    registry.register_global("name2", gpid2).unwrap();

    assert_eq!(registry.local_count(), 1);
    assert_eq!(registry.global_count(), 1);

    registry.clear();

    assert_eq!(registry.local_count(), 0);
    assert_eq!(registry.global_count(), 0);
    assert!(registry.lookup("name1").is_none());
    assert!(registry.lookup("name2").is_none());
}

#[test]
fn test_process_name_conversions() {
    let name1: ProcessName = "logger".into();
    let name2: ProcessName = String::from("database").into();
    let name3 = ProcessName::new("cache");

    assert_eq!(name1.as_str(), "logger");
    assert_eq!(name2.as_str(), "database");
    assert_eq!(name3.as_str(), "cache");
}

/// Test that simulates Erlang-style process registration workflow
#[test]
fn test_erlang_style_workflow() {
    let registry = DistributedRegistry::new(1);

    // Spawn multiple processes
    let logger_pid = GlobalProcessId::new(1, 1, 1);
    let db_pid = GlobalProcessId::new(1, 1, 2);
    let cache_pid = GlobalProcessId::new(1, 1, 3);

    // Register with meaningful names
    registry.register_global("logger", logger_pid).unwrap();
    registry.register_global("database", db_pid).unwrap();
    registry.register_local("cache", cache_pid).unwrap();

    // Lookup by name (location transparency)
    let logger_entry = registry.lookup("logger").expect("Logger should be registered");
    assert_eq!(logger_entry.global_pid, logger_pid);

    // Simulate process crash - unregister all names
    registry.unregister_process(logger_pid);
    assert!(registry.lookup("logger").is_none());

    // Other processes unaffected
    assert!(registry.lookup("database").is_some());
    assert!(registry.lookup("cache").is_some());
}

/// Test cross-node process identification
#[test]
fn test_cross_node_identification() {
    let node1_registry = DistributedRegistry::new(1);
    let node2_registry = DistributedRegistry::new(2);

    // Process on node 1
    let pid_node1 = GlobalProcessId::new(1, 1, 100);
    node1_registry.register_local("worker", pid_node1).unwrap();

    // Process on node 2 with same name (different scope)
    let pid_node2 = GlobalProcessId::new(2, 1, 100);
    node2_registry.register_local("worker", pid_node2).unwrap();

    // Each registry has its own local namespace
    let entry1 = node1_registry.lookup("worker").unwrap();
    let entry2 = node2_registry.lookup("worker").unwrap();

    assert_eq!(entry1.global_pid.node_id(), 1);
    assert_eq!(entry2.global_pid.node_id(), 2);
    assert_ne!(entry1.global_pid, entry2.global_pid);
}

/// Test that registry timestamp is set
#[test]
fn test_registration_timestamp() {
    use std::thread;
    use std::time::Duration;

    let registry = DistributedRegistry::new(1);
    let gpid = GlobalProcessId::new(1, 1, 100);

    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    thread::sleep(Duration::from_millis(10));
    registry.register_local("timestamped", gpid).unwrap();
    thread::sleep(Duration::from_millis(10));

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let entry = registry.lookup("timestamped").unwrap();
    assert!(entry.registered_at > before);
    assert!(entry.registered_at < after);
}
