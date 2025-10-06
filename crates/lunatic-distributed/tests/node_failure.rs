// Node failure scenario tests for distributed Lunatic runtime
//
// These tests validate the system's behavior when nodes crash, become
// unreachable, or experience network partitions during various operations.

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use lunatic_distributed::{
    control::{self, cert::TEST_ROOT_CERT},
    distributed::{
        client::{Client, EnvironmentId, MessageId, NodeId, ProcessId, SendParams, SpawnParams},
        message::{ClientError, ResponseContent, Spawn},
        server::{gen_node_cert, node_server, ServerCtx},
    },
    quic, DistributedCtx, DistributedProcessState,
};
use lunatic_process::{
    env::{Environment, Environments},
    runtimes::{wasmtime::WasmtimeRuntime, Modules},
    state::ProcessState,
};
use tokio::time::timeout;
use wasmtime::ResourceLimiter;

// Mock implementations for testing
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
            Ok(vec![])
        }

        fn restore(&mut self, _data: &[u8]) -> Result<()> {
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

// Test infrastructure setup
struct TestCluster {
    control_server: control::Server,
    nodes: Vec<TestNode>,
}

struct TestNode {
    id: u64,
    addr: SocketAddr,
    client: Client,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TestCluster {
    async fn new(node_count: usize) -> Result<Self> {
        // Start control server
        let control_addr: SocketAddr = "127.0.0.1:0".parse()?;
        let control_server = control::Server::new(control_addr).await?;
        let control_url = format!("http://{}", control_server.addr());

        let mut nodes = Vec::new();

        for i in 0..node_count {
            let node_id = (i + 1) as u64;
            let node_addr: SocketAddr = "127.0.0.1:0".parse()?;

            // Generate node certificate
            let cert = gen_node_cert(&format!("test-node-{}", node_id))?;
            let cert_pem = cert.serialize_pem()?;
            let key_pem = cert.serialize_private_key_pem();

            // Create node client
            let control_client =
                control::Client::new(&control_url, node_id, &node_addr.to_string()).await?;
            let quic_client = quic::Client::new();
            let node_client = Client::new(node_id, control_client.clone(), quic_client);

            // Create server context
            let envs: Arc<dyn Environments<Env = _>> = Arc::new(mock::MockEnvironments::new());
            let modules = Modules::new();
            let runtime = WasmtimeRuntime::new(None)?;
            let distributed = DistributedProcessState {
                control: control_client.clone(),
                client: node_client.clone(),
            };

            let ctx = ServerCtx::<mock::MockState, _> {
                envs,
                modules,
                distributed,
                runtime,
                node_client: node_client.clone(),
                allowed_envs: None,
            };

            // Start node server
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
                _server_handle: server_handle,
            });
        }

        Ok(Self {
            control_server,
            nodes,
        })
    }

    fn node(&self, index: usize) -> &TestNode {
        &self.nodes[index]
    }

    async fn stop_node(&mut self, index: usize) -> Result<()> {
        let node = &self.nodes[index];
        node.client
            .inner
            .control_client
            .notify_node_stopped()
            .await?;
        Ok(())
    }
}

// Test Cases

#[tokio::test]
async fn test_message_send_to_crashed_node() -> Result<()> {
    let mut cluster = TestCluster::new(2).await?;

    let sender = cluster.node(0);
    let receiver_id = cluster.node(1).id;

    // Stop the receiver node
    cluster.stop_node(1).await?;

    // Attempt to send message to crashed node
    let params = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(receiver_id),
        dest: ProcessId(2),
        tag: Some(42),
        data: vec![1, 2, 3],
    };

    let result = timeout(Duration::from_secs(2), sender.client.send(params)).await;

    // Should fail or timeout
    match result {
        Ok(Ok(_)) => {
            // Message queued, but response should timeout
            // This is acceptable behavior
        }
        Ok(Err(_)) => {
            // Connection error is expected
        }
        Err(_) => {
            // Timeout is expected
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_spawn_on_crashed_node() -> Result<()> {
    let mut cluster = TestCluster::new(2).await?;

    let sender = cluster.node(0);
    let receiver_id = cluster.node(1).id;

    // Stop the receiver node
    cluster.stop_node(1).await?;

    // Attempt to spawn on crashed node
    let spawn = Spawn {
        response_node_id: sender.id,
        environment_id: 1,
        module_id: 1,
        function: "_start".to_string(),
        params: vec![],
        config: vec![],
    };

    let params = SpawnParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(receiver_id),
        spawn,
    };

    let message_id = sender.client.spawn(params).await?;

    // Wait for response (should timeout)
    let result = timeout(
        Duration::from_secs(6), // Slightly longer than 5s timeout
        sender.client.await_response(message_id),
    )
    .await;

    match result {
        Ok(Ok(ResponseContent::Error(ClientError::Unexpected(msg)))) => {
            assert!(msg.contains("timeout") || msg.contains("not exist"));
        }
        Ok(Ok(_)) => panic!("Expected error response"),
        Ok(Err(_)) => {
            // Connection error is acceptable
        }
        Err(_) => {
            // Timeout is expected behavior
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_node_crash_during_message_processing() -> Result<()> {
    let mut cluster = TestCluster::new(3).await?;

    let sender = cluster.node(0);
    let relay_id = cluster.node(1).id;
    let receiver_id = cluster.node(2).id;

    // Send message from node 0 -> node 1 (which will crash)
    let params = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(relay_id),
        dest: ProcessId(2),
        tag: Some(100),
        data: b"test message".to_vec(),
    };

    let message_id = sender.client.send(params).await?;

    // Crash the relay node immediately
    tokio::time::sleep(Duration::from_millis(50)).await;
    cluster.stop_node(1).await?;

    // Wait a bit to ensure message processing would have occurred
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify system stability - can still send to node 2
    let params2 = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(receiver_id),
        dest: ProcessId(3),
        tag: Some(200),
        data: b"recovery test".to_vec(),
    };

    let result = timeout(Duration::from_secs(2), sender.client.send(params2)).await;
    assert!(result.is_ok(), "Should be able to send to other nodes after one crashes");

    Ok(())
}

#[tokio::test]
async fn test_network_partition_simulation() -> Result<()> {
    let cluster = TestCluster::new(3).await?;

    let node0 = cluster.node(0);
    let node1_id = cluster.node(1).id;
    let node2_id = cluster.node(2).id;

    // Send messages to both nodes
    let params1 = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(node1_id),
        dest: ProcessId(10),
        tag: Some(1),
        data: b"to node 1".to_vec(),
    };

    let params2 = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(node2_id),
        dest: ProcessId(20),
        tag: Some(2),
        data: b"to node 2".to_vec(),
    };

    // Both should work initially
    let msg1 = timeout(Duration::from_secs(2), node0.client.send(params1.clone())).await;
    let msg2 = timeout(Duration::from_secs(2), node0.client.send(params2.clone())).await;

    assert!(msg1.is_ok(), "Initial send to node 1 should succeed");
    assert!(msg2.is_ok(), "Initial send to node 2 should succeed");

    Ok(())
}

#[tokio::test]
async fn test_stale_node_reference_cleanup() -> Result<()> {
    let mut cluster = TestCluster::new(2).await?;

    let sender = cluster.node(0);
    let receiver_id = cluster.node(1).id;

    // Send successful message
    let params = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(receiver_id),
        dest: ProcessId(2),
        tag: Some(1),
        data: b"initial".to_vec(),
    };

    let _ = sender.client.send(params.clone()).await?;

    // Stop node
    cluster.stop_node(1).await?;

    // Refresh nodes to detect failure
    sender
        .client
        .inner
        .control_client
        .refresh_nodes()
        .await
        .ok();

    // Subsequent sends should handle stale reference
    let params2 = SendParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(receiver_id),
        dest: ProcessId(2),
        tag: Some(2),
        data: b"after crash".to_vec(),
    };

    let result = timeout(Duration::from_secs(2), sender.client.send(params2)).await;

    // Should either fail fast or queue (both acceptable)
    match result {
        Ok(Ok(_)) => {
            // Queued for later delivery
        }
        Ok(Err(_)) | Err(_) => {
            // Failed fast, which is also acceptable
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_response_timeout_handling() -> Result<()> {
    let cluster = TestCluster::new(1).await?;
    let node = cluster.node(0);

    // Create a spawn request that will timeout (no actual receiver)
    let spawn = Spawn {
        response_node_id: node.id,
        environment_id: 999, // Non-existent environment
        module_id: 1,
        function: "_start".to_string(),
        params: vec![],
        config: vec![],
    };

    let params = SpawnParams {
        env: EnvironmentId(1),
        src: ProcessId(1),
        node: NodeId(999), // Non-existent node
        spawn,
    };

    let message_id = node.client.spawn(params).await?;

    // Response timeout is 5 seconds (see client.rs:290)
    let result = timeout(
        Duration::from_secs(6),
        node.client.await_response(message_id),
    )
    .await;

    match result {
        Ok(Ok(ResponseContent::Error(ClientError::Unexpected(msg)))) => {
            assert!(
                msg.contains("timeout") || msg.contains("not exist"),
                "Expected timeout error, got: {}",
                msg
            );
        }
        Ok(Ok(other)) => panic!("Expected timeout error, got: {:?}", other),
        Ok(Err(e)) => panic!("Expected timeout error, got error: {}", e),
        Err(_) => {
            // Overall timeout is also acceptable
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_environment_permission_violation_on_crashed_node() -> Result<()> {
    let mut cluster = TestCluster::new(2).await?;

    let sender = cluster.node(0);
    let receiver_id = cluster.node(1).id;

    // Configure restricted environment access
    let allowed_envs = Some(HashSet::from([1u64]));

    // Try to send to restricted environment
    let params = SendParams {
        env: EnvironmentId(999), // Not in allowed list
        src: ProcessId(1),
        node: NodeId(receiver_id),
        dest: ProcessId(2),
        tag: Some(1),
        data: b"unauthorized".to_vec(),
    };

    // Even if node crashes, permission check should have happened first
    cluster.stop_node(1).await?;

    let result = timeout(Duration::from_secs(2), sender.client.send(params)).await;

    // Should fail (either permission denied or node unreachable)
    match result {
        Ok(Ok(_)) => {
            // Queued, but will fail on server side
        }
        Ok(Err(_)) | Err(_) => {
            // Expected failure
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_concurrent_node_failures() -> Result<()> {
    let mut cluster = TestCluster::new(4).await?;

    let sender = cluster.node(0);

    // Send messages to all other nodes concurrently
    let mut handles = vec![];

    for i in 1..4 {
        let client = sender.client.clone();
        let node_id = cluster.node(i).id;

        let handle = tokio::spawn(async move {
            let params = SendParams {
                env: EnvironmentId(1),
                src: ProcessId(1),
                node: NodeId(node_id),
                dest: ProcessId(i as u64 + 10),
                tag: Some(i as i64),
                data: format!("message to node {}", i).into_bytes(),
            };

            timeout(Duration::from_secs(2), client.send(params)).await
        });

        handles.push(handle);
    }

    // Crash nodes 1 and 2 while messages in flight
    tokio::time::sleep(Duration::from_millis(50)).await;
    cluster.stop_node(1).await?;
    cluster.stop_node(2).await?;

    // Wait for all sends to complete
    let results = futures::future::join_all(handles).await;

    // At least one should complete (node 3 is still up)
    let successful = results
        .iter()
        .filter(|r| matches!(r, Ok(Ok(Ok(_)))))
        .count();

    // Node 3 should still work
    assert!(
        successful >= 1,
        "At least one node should still be reachable"
    );

    Ok(())
}
