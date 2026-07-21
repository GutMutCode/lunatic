use std::sync::{Arc, Barrier};
use std::thread;

use lunatic_distributed::distributed::registry::{
    RegistryCapacityError, RegistryLimits, RegistryUsage,
};
use lunatic_distributed::distributed::{
    DistributedRegistry, GlobalProcessId, ProcessName, RegistrationScope,
};

fn registry_limits(
    max_name_bytes: usize,
    max_entries: usize,
    max_retained_bytes: usize,
) -> RegistryLimits {
    RegistryLimits {
        max_name_bytes,
        max_entries,
        max_retained_bytes,
        ..RegistryLimits::default()
    }
}

#[test]
fn registry_default_limits_are_finite_and_exposed() {
    let registry = DistributedRegistry::new(1);
    assert_eq!(
        registry.limits(),
        RegistryLimits {
            max_name_bytes: 1_024,
            max_entries: 16_384,
            max_retained_bytes: 4 * 1_024 * 1_024,
            max_name_locks: 1_024,
            max_pending_responses: 4_096,
            max_topology_nodes: 1_024,
        }
    );
}

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

        assert_eq!(
            original, decoded,
            "Round-trip encoding failed for {:?}",
            original
        );
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
    let entry = registry
        .lookup_global("database_manager")
        .expect("Global lookup should succeed");
    assert_eq!(entry.global_pid, gpid);
    assert_eq!(entry.scope, RegistrationScope::Global);

    // Generic lookup also works
    let entry2 = registry
        .lookup("database_manager")
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

    registry
        .register_local("local_service", gpid_local)
        .unwrap();
    registry
        .register_global("global_service", gpid_global)
        .unwrap();

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
    let logger_entry = registry
        .lookup("logger")
        .expect("Logger should be registered");
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

#[test]
fn registry_entry_limit_is_exact_and_capacity_is_reusable() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(32, 2, 128));
    let pid = GlobalProcessId::new(1, 1, 1);

    registry.register_local("a", pid).unwrap();
    registry.register_global("b", pid).unwrap();
    assert_eq!(
        registry.usage(),
        RegistryUsage {
            entries: 2,
            retained_bytes: 4,
        }
    );

    let error = registry.register_local("c", pid).unwrap_err();
    assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
    assert_eq!(registry.usage().entries, 2);

    registry.unregister("a").unwrap();
    registry.register_local("c", pid).unwrap();
    assert_eq!(registry.usage().entries, 2);
}

#[test]
fn registry_retained_byte_limit_is_exact_and_capacity_is_reusable() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(32, 8, 6));
    let pid = GlobalProcessId::new(1, 1, 1);

    registry.register_local("abc", pid).unwrap();
    assert_eq!(registry.usage().retained_bytes, 6);

    let error = registry.register_global("z", pid).unwrap_err();
    assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
    assert_eq!(registry.usage().retained_bytes, 6);

    registry.unregister("abc").unwrap();
    registry.register_global("xyz", pid).unwrap();
    assert_eq!(registry.usage().retained_bytes, 6);
}

#[test]
fn global_prepare_validation_is_idempotent_and_does_not_mutate() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(32, 1, 128));
    let owner = GlobalProcessId::new(1, 1, 1);
    let other = GlobalProcessId::new(2, 1, 2);
    registry.register_global("name", owner).unwrap();
    let usage = registry.usage();

    registry.can_register_global("name", owner).unwrap();
    assert!(registry.can_register_global("name", other).is_err());
    let full = registry.can_register_global("other", other).unwrap_err();
    assert!(full.downcast_ref::<RegistryCapacityError>().is_some());
    assert_eq!(registry.usage(), usage);
    assert_eq!(registry.global_count(), 1);
}

#[test]
fn registry_rejects_oversized_empty_and_control_character_names() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(3, 8, 128));
    let pid = GlobalProcessId::new(1, 1, 1);

    let oversized = registry.register_local("éé", pid).unwrap_err();
    assert!(oversized.downcast_ref::<RegistryCapacityError>().is_some());
    assert!(registry.register_global("", pid).is_err());
    let control = registry.register_global("a\nb", pid).unwrap_err();
    assert!(control.downcast_ref::<RegistryCapacityError>().is_none());
    assert_eq!(registry.usage(), RegistryUsage::default());
}

#[test]
fn concurrent_same_name_registration_has_exactly_one_winner() {
    let registry = Arc::new(DistributedRegistry::new(1));
    let workers = 16;
    let barrier = Arc::new(Barrier::new(workers));
    let handles = (0..workers)
        .map(|process_id| {
            let registry = registry.clone();
            let barrier = barrier.clone();
            thread::spawn(move || {
                barrier.wait();
                registry
                    .register_local("contended", GlobalProcessId::new(1, 1, process_id as u64))
                    .is_ok()
            })
        })
        .collect::<Vec<_>>();

    let winners = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .filter(|winner| *winner)
        .count();
    assert_eq!(winners, 1);
    assert_eq!(registry.local_count(), 1);
    assert_eq!(registry.usage().entries, 1);
}

#[test]
fn process_cleanup_is_scope_and_owner_safe_for_equal_names() {
    let registry = DistributedRegistry::new(1);
    let local_pid = GlobalProcessId::new(1, 1, 1);
    let global_pid = GlobalProcessId::new(2, 1, 2);
    registry.register_local("shared", local_pid).unwrap();
    registry.register_global("shared", global_pid).unwrap();

    let removed = registry.unregister_process(local_pid);
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].1.scope, RegistrationScope::Local);
    assert!(registry.lookup_local("shared").is_none());
    assert_eq!(
        registry.lookup_global("shared").unwrap().global_pid,
        global_pid
    );
    assert_eq!(registry.global_names_for_process(global_pid).len(), 1);
    assert_eq!(registry.usage().entries, 1);
}

#[test]
fn local_process_cleanup_leaves_global_names_for_coordination() {
    let registry = DistributedRegistry::new(1);
    let pid = GlobalProcessId::new(1, 1, 1);
    registry.register_local("local", pid).unwrap();
    registry.register_global("global", pid).unwrap();

    assert_eq!(
        registry.remove_local_registrations(pid),
        vec![ProcessName::new("local")]
    );
    assert!(registry.lookup_local("local").is_none());
    assert_eq!(
        registry.global_names_for_process(pid),
        vec![ProcessName::new("global")]
    );
    assert_eq!(registry.usage().entries, 1);
}

#[test]
fn conditional_global_removal_rejects_an_old_owner_after_reassignment() {
    let registry = DistributedRegistry::new(1);
    let old_pid = GlobalProcessId::new(2, 1, 1);
    let new_pid = GlobalProcessId::new(3, 1, 1);
    registry.register_global("lease", old_pid).unwrap();
    registry.apply_global("lease", new_pid, 99).unwrap();

    assert!(!registry.remove_global_if_owner("lease", old_pid));
    assert_eq!(registry.lookup_global("lease").unwrap().global_pid, new_pid);
    assert!(registry.global_names_for_process(old_pid).is_empty());
    assert_eq!(registry.global_names_for_process(new_pid).len(), 1);
    assert!(registry.remove_global_if_owner("lease", new_pid));
    assert_eq!(registry.usage(), RegistryUsage::default());
}

#[test]
fn snapshot_rejection_is_atomic_for_capacity_and_duplicates() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(16, 2, 32));
    let local_pid = GlobalProcessId::new(1, 1, 1);
    let global_pid = GlobalProcessId::new(2, 1, 1);
    registry.register_local("local", local_pid).unwrap();
    registry.register_global("old", global_pid).unwrap();
    let baseline = registry.global_snapshot();
    let usage = registry.usage();

    let oversized = registry
        .replace_global_snapshot(vec![
            ("one".into(), global_pid, 1),
            ("two".into(), global_pid, 2),
        ])
        .unwrap_err();
    assert!(oversized.downcast_ref::<RegistryCapacityError>().is_some());
    assert_eq!(registry.global_snapshot(), baseline);
    assert_eq!(registry.usage(), usage);

    let oversized_name = registry
        .replace_global_snapshot(vec![("0123456789abcdefg".into(), global_pid, 1)])
        .unwrap_err();
    assert!(oversized_name
        .downcast_ref::<RegistryCapacityError>()
        .is_some());
    assert_eq!(registry.global_snapshot(), baseline);
    assert_eq!(registry.usage(), usage);

    registry
        .replace_global_snapshot(vec![
            ("dup".into(), global_pid, 1),
            ("dup".into(), local_pid, 2),
        ])
        .unwrap_err();
    assert_eq!(registry.global_snapshot(), baseline);
    assert_eq!(registry.usage(), usage);
}

#[test]
fn snapshot_byte_preflight_projects_against_local_state() {
    let registry = DistributedRegistry::with_limits(1, registry_limits(16, 4, 12));
    let local_pid = GlobalProcessId::new(1, 1, 1);
    let global_pid = GlobalProcessId::new(2, 1, 1);
    registry.register_local("aa", local_pid).unwrap();

    registry
        .replace_global_snapshot(vec![("cccc".into(), global_pid, 1)])
        .unwrap();
    assert_eq!(registry.usage().retained_bytes, 12);
    let baseline = registry.global_snapshot();

    let error = registry
        .replace_global_snapshot(vec![("ccccc".into(), global_pid, 2)])
        .unwrap_err();
    assert!(error.downcast_ref::<RegistryCapacityError>().is_some());
    assert_eq!(registry.global_snapshot(), baseline);
    assert_eq!(registry.usage().retained_bytes, 12);
}

#[test]
fn repeated_snapshot_replacement_returns_to_local_usage_baseline() {
    let registry = DistributedRegistry::new(1);
    let local_pid = GlobalProcessId::new(1, 1, 1);
    let first_pid = GlobalProcessId::new(2, 1, 1);
    let second_pid = GlobalProcessId::new(3, 1, 1);
    registry.register_local("shared", local_pid).unwrap();
    let baseline = registry.usage();

    for generation in 0..8 {
        registry
            .replace_global_snapshot(vec![("shared".into(), first_pid, generation)])
            .unwrap();
        registry
            .replace_global_snapshot(vec![("new".into(), second_pid, generation)])
            .unwrap();
        assert!(registry.global_names_for_process(first_pid).is_empty());
        registry.replace_global_snapshot(Vec::new()).unwrap();
        assert_eq!(registry.usage(), baseline);
        assert!(registry.global_names_for_process(second_pid).is_empty());
        assert_eq!(
            registry.lookup_local("shared").unwrap().global_pid,
            local_pid
        );
    }
}

#[test]
fn node_cleanup_removes_only_global_names_for_that_node() {
    let registry = DistributedRegistry::new(1);
    let local_pid = GlobalProcessId::new(7, 1, 1);
    let failed_global = GlobalProcessId::new(7, 1, 2);
    let surviving_global = GlobalProcessId::new(8, 1, 3);
    registry.register_local("same", local_pid).unwrap();
    registry.register_global("same", failed_global).unwrap();
    registry
        .register_global("survivor", surviving_global)
        .unwrap();

    assert_eq!(
        registry.remove_node_registrations(7),
        vec![ProcessName::new("same")]
    );
    assert_eq!(registry.lookup_local("same").unwrap().global_pid, local_pid);
    assert!(registry.lookup_global("same").is_none());
    assert!(registry.lookup_global("survivor").is_some());
}
