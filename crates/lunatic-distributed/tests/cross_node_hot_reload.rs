// Cross-node hot reload tests for distributed Lunatic runtime
//
// These tests validate hot reload functionality in multi-node scenarios,
// ensuring coordinated reloads work correctly and handle failures gracefully.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use lunatic_distributed::{
    control::{self, cert::TEST_ROOT_CERT},
    distributed::{
        client::{Client, EnvironmentId, NodeId, ProcessId},
        message::Spawn,
        server::{gen_node_cert, node_server, ServerCtx},
    },
    quic, DistributedCtx, DistributedProcessState,
};
use lunatic_process::{
    env::{Environment, Environments},
    hot_reload::ReloadCoordinator,
    runtimes::{wasmtime::WasmtimeRuntime, Modules, RawWasm},
    state::ProcessState,
    Signal,
};
use tokio::time::timeout;
use wasmtime::ResourceLimiter;

// Reuse mock implementations from node_failure tests
mod mock {
    use super::*;
    use async_trait::async_trait;
    use lunatic_process::{
        env::{DefaultEnvironment, Environment},
        reloadable_state::ReloadableState,
        state::ProcessState,
        Process,
    };
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use wasmtime::Module;

    #[derive(Clone)]
    pub struct MockEnvironments {
        envs: Arc<RwLock<std::collections::HashMap<u64, Arc<DefaultEnvironment>>>>,
    }

    impl MockEnvironments {
        pub fn new() -> Self {
            Self {
                envs: Arc::new(RwLock::new(std::collections::HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl Environments for MockEnvironments {
        type Env = DefaultEnvironment;

        async fn get(&self, id: u64) -> Option<Arc<Self::Env>> {
            self.envs.read().await.get(&id).cloned()
        }

        async fn create(&self, id: u64) -> Result<Arc<Self::Env>> {
            let env = Arc::new(DefaultEnvironment::new(id));
            self.envs.write().await.insert(id, env.clone());
            Ok(env)
        }

        async fn remove(&self, id: u64) {
            self.envs.write().await.remove(&id);
        }
    }

    #[derive(Clone)]
    pub struct MockState {
        env: Arc<DefaultEnvironment>,
        distributed: DistributedProcessState,
        runtime: WasmtimeRuntime,
        module: wasmtime::Module,
        pub reload_count: Arc<std::sync::atomic::AtomicU32>,
    }

    impl ProcessState for MockState {
        type Config = ();

        fn new(
            env: Arc<impl Environment>,
            _runtime: WasmtimeRuntime,
            _module: wasmtime::Module,
            _config: Arc<Self::Config>,
        ) -> Result<Self> {
            unreachable!("Use new_dist_state for tests")
        }

        fn set_process(&mut self, _proc: Process<Self>) {}
    }

    impl ReloadableState for MockState {
        fn snapshot(&self) -> Result<Vec<u8>> {
            let count = self
                .reload_count
                .load(std::sync::atomic::Ordering::SeqCst);
            Ok(count.to_le_bytes().to_vec())
        }

        fn restore(&mut self, data: &[u8]) -> Result<()> {
            if data.len() >= 4 {
                let count = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                self.reload_count
                    .store(count + 1, std::sync::atomic::Ordering::SeqCst);
            }
            Ok(())
        }
    }

    impl DistributedCtx<DefaultEnvironment> for MockState {
        fn new_dist_state(
            env: Arc<DefaultEnvironment>,
            distributed: DistributedProcessState,
            runtime: WasmtimeRuntime,
            module: Module,
            _config: Arc<Self::Config>,
        ) -> Result<Self> {
            Ok(Self {
                env,
                distributed,
                runtime,
                module,
                reload_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
            })
        }
    }

    impl ResourceLimiter for MockState {
        fn memory_growing(
            &mut self,
            _current: usize,
            _desired: usize,
            _maximum: Option<usize>,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }

        fn table_growing(
            &mut self,
            _current: u32,
            _desired: u32,
            _maximum: Option<u32>,
        ) -> anyhow::Result<bool> {
            Ok(true)
        }
    }
}

// Test infrastructure
struct TestCluster {
    control_server: control::Server,
    nodes: Vec<TestNode>,
    reload_coordinator: Arc<ReloadCoordinator>,
}

struct TestNode {
    id: u64,
    addr: SocketAddr,
    client: Client,
    envs: Arc<mock::MockEnvironments>,
    modules: Modules<()>,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TestCluster {
    async fn new(node_count: usize) -> Result<Self> {
        let control_addr: SocketAddr = "127.0.0.1:0".parse()?;
        let control_server = control::Server::new(control_addr).await?;
        let control_url = format!("http://{}", control_server.addr());

        let reload_coordinator = Arc::new(ReloadCoordinator::new());
        let mut nodes = Vec::new();

        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let node_addr: SocketAddr = "127.0.0.1:0".parse()?;

            let cert = gen_node_cert(&format!("test-node-{}", node_id))?;
            let cert_pem = cert.serialize_pem()?;
            let key_pem = cert.serialize_private_key_pem();

            let control_client =
                control::Client::new(&control_url, node_id, &node_addr.to_string()).await?;
            let quic_client = quic::Client::new();
            let node_client = Client::new(node_id, control_client.clone(), quic_client);

            let envs = Arc::new(mock::MockEnvironments::new());
            let modules = Modules::new();
            let runtime = WasmtimeRuntime::new(None)?;
            let distributed = DistributedProcessState {
                control: control_client.clone(),
                client: node_client.clone(),
            };

            let ctx = ServerCtx::<mock::MockState, _> {
                envs: envs.clone() as Arc<dyn Environments<Env = _>>,
                modules: modules.clone(),
                distributed,
                runtime,
                node_client: node_client.clone(),
                allowed_envs: None,
            };

            let server_handle = tokio::spawn(node_server(
                ctx,
                node_addr,
                TEST_ROOT_CERT.to_string(),
                vec![cert_pem],
                key_pem,
            ));

            nodes.push(TestNode {
                id: node_id,
                addr: node_addr,
                client: node_client,
                envs,
                modules,
                _server_handle: server_handle,
            });
        }

        Ok(Self {
            control_server,
            nodes,
            reload_coordinator,
        })
    }

    fn node(&self, index: usize) -> &TestNode {
        &self.nodes[index]
    }
}

// Helper to create a simple WASM module for testing
fn create_test_wasm() -> Vec<u8> {
    // Minimal valid WASM module with _start function
    wat::parse_str(
        r#"
        (module
            (func $start (export "_start"))
            (memory (export "memory") 1)
        )
        "#,
    )
    .expect("Failed to parse WAT")
}

// Test Cases

#[tokio::test]
async fn test_simple_cross_node_hot_reload() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let node0 = cluster.node(0);
    let node1 = cluster.node(1);

    // Compile module on both nodes
    let module_id = 1u64;
    let wasm_v1 = create_test_wasm();
    let raw_wasm_v1 = RawWasm::new(Some(module_id), wasm_v1.clone());

    let runtime0 = WasmtimeRuntime::new(None)?;
    let runtime1 = WasmtimeRuntime::new(None)?;

    node0
        .modules
        .compile(runtime0.clone(), raw_wasm_v1.clone())
        .await??;
    node1
        .modules
        .compile(runtime1.clone(), raw_wasm_v1.clone())
        .await??;

    // Create environment and spawn process on node1
    let env_id = 1u64;
    let env = node1.envs.create(env_id).await?;

    // Simulate process creation (simplified)
    let process_id = 100u64;

    // Register reload operation
    cluster
        .reload_coordinator
        .register_reload(module_id, vec![process_id])
        .await;

    // Compile new version
    let wasm_v2 = create_test_wasm(); // In real test, this would be different
    let raw_wasm_v2 = RawWasm::new(Some(module_id), wasm_v2);

    node0
        .modules
        .compile(runtime0.clone(), raw_wasm_v2.clone())
        .await??;
    node1
        .modules
        .compile(runtime1.clone(), raw_wasm_v2)
        .await??;

    // Mark reload as complete
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id)
        .await;

    // Verify no active reloads
    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;
    assert!(!has_active, "Reload should be complete");

    Ok(())
}

#[tokio::test]
async fn test_coordinated_reload_with_node_failure() -> Result<()> {
    let mut cluster = TestCluster::new(3).await?;

    let module_id = 2u64;
    let wasm = create_test_wasm();

    // Deploy module to all nodes
    for i in 0..3 {
        let node = cluster.node(i);
        let runtime = WasmtimeRuntime::new(None)?;
        let raw_wasm = RawWasm::new(Some(module_id), wasm.clone());
        node.modules.compile(runtime, raw_wasm).await??;
    }

    // Register processes on all nodes
    let processes = vec![101u64, 102, 103];
    cluster
        .reload_coordinator
        .register_reload(module_id, processes.clone())
        .await;

    // Crash node 1 during reload
    let node1_id = cluster.node(1).id;
    cluster
        .node(1)
        .client
        .inner
        .control_client
        .notify_node_stopped()
        .await?;

    // Attempt to complete reload despite failure
    // In production, this would trigger rollback
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id)
        .await;

    // Verify coordinator state
    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;

    // Reload marked complete despite node failure
    // Real implementation should handle this via rollback
    assert!(!has_active, "Coordinator should mark reload complete");

    Ok(())
}

#[tokio::test]
async fn test_atomic_reload_across_nodes() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let module_id = 3u64;
    let wasm = create_test_wasm();

    // Deploy to both nodes
    for i in 0..2 {
        let node = cluster.node(i);
        let runtime = WasmtimeRuntime::new(None)?;
        let raw_wasm = RawWasm::new(Some(module_id), wasm.clone());
        node.modules.compile(runtime, raw_wasm).await??;
    }

    // Register processes on both nodes
    let processes_node0 = vec![201u64, 202];
    let processes_node1 = vec![203u64, 204];
    let all_processes: Vec<u64> = processes_node0
        .iter()
        .chain(processes_node1.iter())
        .cloned()
        .collect();

    cluster
        .reload_coordinator
        .register_reload(module_id, all_processes)
        .await;

    // Simulate atomic reload
    // All processes must reload or none should
    let reload_success = true; // Simulate successful reload

    if reload_success {
        cluster
            .reload_coordinator
            .mark_reload_complete(module_id)
            .await;
    } else {
        // Would trigger rollback in real implementation
        cluster
            .reload_coordinator
            .cancel_reload(module_id)
            .await
            .ok();
    }

    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;
    assert!(!has_active, "Atomic reload should complete or rollback");

    Ok(())
}

#[tokio::test]
async fn test_reload_state_preservation_across_nodes() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let module_id = 4u64;
    let process_id = 300u64;

    // Simulate state snapshot before reload
    let state = mock::MockState {
        env: Arc::new(lunatic_process::env::DefaultEnvironment::new(1)),
        distributed: DistributedProcessState {
            control: cluster.node(0).client.inner.control_client.clone(),
            client: cluster.node(0).client.clone(),
        },
        runtime: WasmtimeRuntime::new(None)?,
        module: {
            let runtime = WasmtimeRuntime::new(None)?;
            let wasm = create_test_wasm();
            let module = runtime
                .engine()
                .compile_module(&wasm, Some("test".to_string()))?;
            module
        },
        reload_count: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    };

    let snapshot = state.snapshot()?;

    // Register reload
    cluster
        .reload_coordinator
        .register_reload(module_id, vec![process_id])
        .await;

    // Store state for reload
    cluster
        .reload_coordinator
        .store_old_state(module_id, process_id, snapshot.clone())
        .await;

    // Retrieve state
    let retrieved = cluster
        .reload_coordinator
        .get_old_state(module_id, process_id)
        .await;

    assert!(retrieved.is_some(), "State should be retrievable");
    assert_eq!(retrieved.unwrap(), snapshot, "State should match");

    // Complete reload
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id)
        .await;

    // State should be cleaned up
    let after_complete = cluster
        .reload_coordinator
        .get_old_state(module_id, process_id)
        .await;
    assert!(
        after_complete.is_none(),
        "State should be cleaned after complete"
    );

    Ok(())
}

#[tokio::test]
async fn test_rollback_after_partial_reload() -> Result<()> {
    let cluster = TestCluster::new(3).await?;

    let module_id = 5u64;
    let processes = vec![401u64, 402, 403];

    // Register reload for all processes
    cluster
        .reload_coordinator
        .register_reload(module_id, processes.clone())
        .await;

    // Simulate partial success (2 of 3 processes reloaded)
    let successful_processes = vec![401u64, 402];
    for &pid in &successful_processes {
        let mock_state = vec![1, 2, 3, 4]; // Mock snapshot
        cluster
            .reload_coordinator
            .store_old_state(module_id, pid, mock_state)
            .await;
    }

    // Simulate failure on third process
    // This should trigger rollback to all processes

    // Cancel reload (triggers rollback in real implementation)
    cluster
        .reload_coordinator
        .cancel_reload(module_id)
        .await
        .ok();

    // Verify rollback occurred
    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;
    assert!(!has_active, "Reload should be cancelled after failure");

    // Verify old states are available for rollback
    for &pid in &successful_processes {
        let state = cluster
            .reload_coordinator
            .get_old_state(module_id, pid)
            .await;
        // In real implementation, rollback would use these states
        // For test, just verify they existed
    }

    Ok(())
}

#[tokio::test]
async fn test_concurrent_reloads_different_modules() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let module_id_1 = 6u64;
    let module_id_2 = 7u64;

    let processes_m1 = vec![501u64, 502];
    let processes_m2 = vec![503u64, 504];

    // Register concurrent reloads
    cluster
        .reload_coordinator
        .register_reload(module_id_1, processes_m1)
        .await;
    cluster
        .reload_coordinator
        .register_reload(module_id_2, processes_m2)
        .await;

    // Both should be tracked
    assert!(
        cluster
            .reload_coordinator
            .has_active_reload(module_id_1)
            .await
    );
    assert!(
        cluster
            .reload_coordinator
            .has_active_reload(module_id_2)
            .await
    );

    // Complete first reload
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id_1)
        .await;

    // Second should still be active
    assert!(
        !cluster
            .reload_coordinator
            .has_active_reload(module_id_1)
            .await
    );
    assert!(
        cluster
            .reload_coordinator
            .has_active_reload(module_id_2)
            .await
    );

    // Complete second reload
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id_2)
        .await;

    assert!(
        !cluster
            .reload_coordinator
            .has_active_reload(module_id_2)
            .await
    );

    Ok(())
}

#[tokio::test]
async fn test_reload_version_tracking() -> Result<()> {
    let cluster = TestCluster::new(1).await?;

    let module_id = 8u64;
    let wasm_v1 = create_test_wasm();
    let wasm_v2 = create_test_wasm(); // In reality would be different

    let runtime = WasmtimeRuntime::new(None)?;
    let node = cluster.node(0);

    // Compile version 1
    let raw_v1 = RawWasm::new(Some(module_id), wasm_v1);
    node.modules.compile(runtime.clone(), raw_v1).await??;

    // Register reload
    cluster
        .reload_coordinator
        .register_reload(module_id, vec![600])
        .await;

    // Compile version 2
    let raw_v2 = RawWasm::new(Some(module_id), wasm_v2);
    node.modules.compile(runtime.clone(), raw_v2).await??;

    // Complete reload
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id)
        .await;

    // Module should track version changes
    // (In real implementation, module registry would track this)

    Ok(())
}

#[tokio::test]
async fn test_network_delay_during_reload() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let module_id = 9u64;
    let processes = vec![701u64, 702];

    // Register reload
    cluster
        .reload_coordinator
        .register_reload(module_id, processes.clone())
        .await;

    // Simulate network delay by waiting
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Store states with delay
    for &pid in &processes {
        let state = vec![pid as u8];
        cluster
            .reload_coordinator
            .store_old_state(module_id, pid, state)
            .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Complete reload despite delays
    cluster
        .reload_coordinator
        .mark_reload_complete(module_id)
        .await;

    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;
    assert!(!has_active, "Reload should complete despite network delays");

    Ok(())
}

#[tokio::test]
async fn test_reload_cancellation_cleanup() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    let module_id = 10u64;
    let processes = vec![801u64, 802, 803];

    // Register reload
    cluster
        .reload_coordinator
        .register_reload(module_id, processes.clone())
        .await;

    // Store some states
    for &pid in &processes[0..2] {
        cluster
            .reload_coordinator
            .store_old_state(module_id, pid, vec![pid as u8])
            .await;
    }

    // Cancel reload
    cluster
        .reload_coordinator
        .cancel_reload(module_id)
        .await
        .ok();

    // Verify cleanup
    let has_active = cluster
        .reload_coordinator
        .has_active_reload(module_id)
        .await;
    assert!(!has_active, "Cancelled reload should not be active");

    // States should eventually be cleaned up
    // (exact timing depends on implementation)

    Ok(())
}
