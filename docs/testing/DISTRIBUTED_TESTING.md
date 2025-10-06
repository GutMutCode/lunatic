# Distributed Testing Guide

This guide covers testing strategies for Lunatic's distributed runtime capabilities, including node failure scenarios, cross-node hot reload, and network partition handling.

## Overview

Lunatic's distributed system enables:
- Cross-node process spawning
- Distributed message passing
- Coordinated hot reload across multiple nodes
- Fault tolerance in multi-node deployments

Testing these capabilities requires simulating multi-node environments and failure scenarios that don't occur in single-node testing.

## Test Organization

### Location

Distributed tests are located in:
```
crates/lunatic-distributed/tests/
├── node_failure.rs           # Node crash and network failure tests
├── cross_node_hot_reload.rs  # Hot reload coordination tests
└── README.md                 # This file
```

### Test Categories

#### 1. Node Failure Tests (`node_failure.rs`)

**Purpose**: Validate system behavior when nodes crash, become unreachable, or experience network issues.

**Scenarios Covered**:
- **Message send to crashed node** - Tests timeout and error handling
- **Spawn on crashed node** - Validates failure detection and response
- **Node crash during message processing** - Ensures system stability
- **Network partition simulation** - Tests split-brain scenarios
- **Stale node reference cleanup** - Validates node discovery refresh
- **Response timeout handling** - Tests 5-second timeout mechanism
- **Environment permission violations** - Security under failure conditions
- **Concurrent node failures** - Multiple simultaneous crashes

**Key Test**: `test_message_send_to_crashed_node()`
```rust
#[tokio::test]
async fn test_message_send_to_crashed_node() -> Result<()> {
    let mut cluster = TestCluster::new(2).await?;

    // Stop receiver node
    cluster.stop_node(1).await?;

    // Attempt to send message
    let result = timeout(
        Duration::from_secs(2),
        sender.client.send(params)
    ).await;

    // Should fail or timeout gracefully
    assert!(result.is_err() || matches!(result, Ok(Err(_))));
    Ok(())
}
```

#### 2. Cross-Node Hot Reload Tests (`cross_node_hot_reload.rs`)

**Purpose**: Validate coordinated hot reload functionality across multiple nodes.

**Scenarios Covered**:
- **Simple cross-node hot reload** - Basic 2-node coordinated reload
- **Coordinated reload with node failure** - Partial reload scenarios
- **Atomic reload across nodes** - All-or-nothing semantics
- **Reload state preservation** - State snapshot/restore across nodes
- **Rollback after partial reload** - Automatic recovery mechanism
- **Concurrent reloads** - Multiple modules reloading simultaneously
- **Reload version tracking** - Module version management
- **Network delay during reload** - Latency handling
- **Reload cancellation cleanup** - Resource cleanup on cancel

**Key Test**: `test_atomic_reload_across_nodes()`
```rust
#[tokio::test]
async fn test_atomic_reload_across_nodes() -> Result<()> {
    let cluster = TestCluster::new(2).await?;

    // Deploy module to all nodes
    for node in &cluster.nodes {
        node.modules.compile(runtime, raw_wasm).await??;
    }

    // Register all processes for reload
    coordinator.register_reload(module_id, all_processes).await;

    // Simulate atomic reload
    if all_success {
        coordinator.mark_reload_complete(module_id).await;
    } else {
        coordinator.cancel_reload(module_id).await?; // Rollback
    }

    Ok(())
}
```

## Test Infrastructure

### TestCluster

The `TestCluster` struct simulates a multi-node Lunatic deployment:

```rust
struct TestCluster {
    control_server: control::Server,  // Centralized node registry
    nodes: Vec<TestNode>,              // Individual node instances
    reload_coordinator: Arc<ReloadCoordinator>, // Hot reload coordination
}

impl TestCluster {
    async fn new(node_count: usize) -> Result<Self> {
        // 1. Start control server
        // 2. Generate certificates for each node
        // 3. Create node clients and servers
        // 4. Initialize reload coordinator
    }

    fn node(&self, index: usize) -> &TestNode {
        &self.nodes[index]
    }

    async fn stop_node(&mut self, index: usize) -> Result<()> {
        // Gracefully stop a node to simulate failure
    }
}
```

### TestNode

Each node in the cluster contains:

```rust
struct TestNode {
    id: u64,                          // Unique node identifier
    addr: SocketAddr,                 // Network address
    client: Client,                   // Distributed client for messaging
    envs: Arc<MockEnvironments>,      // Process environments
    modules: Modules<()>,             // WASM module cache
    _server_handle: JoinHandle<()>,   // Server task handle
}
```

### Mock Implementations

Tests use mock implementations for process state:

```rust
#[derive(Clone)]
pub struct MockState {
    env: Arc<DefaultEnvironment>,
    distributed: DistributedProcessState,
    runtime: WasmtimeRuntime,
    module: wasmtime::Module,
    reload_count: Arc<AtomicU32>, // Tracks reload events
}

impl ReloadableState for MockState {
    fn snapshot(&self) -> Result<Vec<u8>> {
        // Serialize reload count
        let count = self.reload_count.load(Ordering::SeqCst);
        Ok(count.to_le_bytes().to_vec())
    }

    fn restore(&mut self, data: &[u8]) -> Result<()> {
        // Deserialize and increment reload count
        let count = u32::from_le_bytes([...]);
        self.reload_count.store(count + 1, Ordering::SeqCst);
        Ok(())
    }
}
```

## Running Tests

### Run All Distributed Tests

```bash
cd crates/lunatic-distributed
cargo test
```

### Run Specific Test Category

```bash
# Node failure tests only
cargo test --test node_failure

# Hot reload tests only
cargo test --test cross_node_hot_reload
```

### Run Individual Test

```bash
cargo test test_message_send_to_crashed_node -- --exact
```

### Enable Logging

```bash
RUST_LOG=debug cargo test -- --nocapture
```

## Writing New Distributed Tests

### Template

```rust
#[tokio::test]
async fn test_your_scenario() -> Result<()> {
    // 1. Create test cluster
    let cluster = TestCluster::new(NODE_COUNT).await?;

    // 2. Set up initial state
    let node0 = cluster.node(0);
    let node1 = cluster.node(1);

    // 3. Perform actions
    let result = node0.client.send(params).await?;

    // 4. Simulate failure (if applicable)
    cluster.stop_node(1).await?;

    // 5. Verify behavior
    assert!(expected_condition, "Error message");

    Ok(())
}
```

### Best Practices

1. **Use Timeouts**: Always wrap potentially blocking operations:
   ```rust
   let result = timeout(Duration::from_secs(2), async_operation()).await;
   ```

2. **Test Both Success and Failure**: Validate happy path and error cases:
   ```rust
   match result {
       Ok(Ok(value)) => { /* success case */ },
       Ok(Err(e)) => { /* expected error */ },
       Err(_) => { /* timeout is acceptable */ },
   }
   ```

3. **Clean Up Resources**: Use `Drop` or explicit cleanup:
   ```rust
   impl Drop for TestCluster {
       fn drop(&mut self) {
           // Stop all nodes
           // Close connections
       }
   }
   ```

4. **Isolate Tests**: Each test should create its own cluster:
   ```rust
   // ✅ Good: isolated cluster
   let cluster = TestCluster::new(2).await?;

   // ❌ Bad: shared global state
   static SHARED_CLUSTER: ...
   ```

5. **Use Meaningful Node Counts**:
   - 2 nodes: Basic client-server scenarios
   - 3 nodes: Network partition tests (minority/majority)
   - 4+ nodes: Complex coordination scenarios

## Known Limitations

### 1. Network Simulation

**Current**: Tests use real network sockets on localhost.

**Limitation**: Cannot simulate true network conditions (latency, packet loss).

**Workaround**: Use `tokio::time::sleep()` to simulate delays:
```rust
tokio::time::sleep(Duration::from_millis(100)).await;
```

**Future Enhancement**: Consider network simulation library (e.g., `comfy-table`, `toxiproxy`).

### 2. Control Server Dependency

**Current**: All tests require a control server for node discovery.

**Limitation**: Cannot test scenarios where control server is unavailable.

**Workaround**: Tests focus on node failures, not control plane failures.

**Future Enhancement**: Add control server failure scenarios.

### 3. WASM Module Complexity

**Current**: Tests use minimal WAT modules:
```wat
(module
    (func $start (export "_start"))
    (memory (export "memory") 1)
)
```

**Limitation**: Doesn't test complex state serialization.

**Workaround**: Mock state implements `ReloadableState` with custom logic.

**Future Enhancement**: Use real compiled Rust WASM modules.

### 4. Timing Sensitivity

**Current**: Tests rely on specific timeouts (e.g., 5-second response timeout).

**Limitation**: May be flaky on slow CI machines.

**Workaround**: Use generous timeouts in tests:
```rust
// Client timeout: 5s
// Test timeout: 6s (allows for margin)
timeout(Duration::from_secs(6), operation).await
```

## Troubleshooting

### Test Hangs

**Symptom**: Test never completes, hangs indefinitely.

**Causes**:
1. Deadlock in async code
2. Server not responding
3. Missing timeout wrapper

**Solution**:
```bash
# Run with timeout
cargo test -- --test-threads=1 --nocapture &
sleep 30 && pkill cargo  # Kill after 30s
```

### Connection Refused Errors

**Symptom**: `Connection refused` or `Address already in use`.

**Causes**:
1. Port conflict with previous test
2. Server not fully started

**Solution**:
```rust
// Use port 0 for automatic allocation
let addr: SocketAddr = "127.0.0.1:0".parse()?;
let server = Server::bind(addr).await?;
let actual_port = server.local_addr().port();
```

### Certificate Errors

**Symptom**: TLS handshake failures, certificate validation errors.

**Causes**:
1. Test certificate mismatch
2. Expired test certificates

**Solution**:
```rust
// Use TEST_ROOT_CERT for all tests
use lunatic_distributed::control::cert::TEST_ROOT_CERT;

let server = quic::new_quic_server(
    socket,
    certs,
    &key,
    &TEST_ROOT_CERT  // ← Consistent root cert
)?;
```

### Flaky Tests

**Symptom**: Tests pass/fail inconsistently.

**Causes**:
1. Race conditions
2. Insufficient delays
3. Shared state between tests

**Solution**:
```rust
// Add explicit synchronization
let (tx, rx) = tokio::sync::oneshot::channel();

// In node 1
perform_action().await;
tx.send(()).unwrap();

// In node 2
rx.await.unwrap();
verify_state().await;
```

## Integration with CI

### GitHub Actions Configuration

Add to `.github/workflows/ci.yml`:

```yaml
distributed-tests:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
    - name: Run distributed tests
      run: |
        cd crates/lunatic-distributed
        cargo test --release -- --test-threads=1
      timeout-minutes: 10
```

### Test Execution Strategy

**Sequential Execution**: Use `--test-threads=1` to avoid port conflicts:
```bash
cargo test -- --test-threads=1
```

**Timeout**: Set global timeout to prevent CI hangs:
```bash
timeout 600 cargo test  # 10-minute timeout
```

**Retry on Failure**: Retry flaky tests up to 3 times:
```bash
cargo test || cargo test || cargo test
```

## Metrics and Coverage

### Test Coverage Goals

- **Node Failure**: 100% of failure modes covered
  - Node crash before operation
  - Node crash during operation
  - Node crash after operation
  - Network timeout
  - Connection refused

- **Hot Reload**: 100% of coordination paths covered
  - Successful coordinated reload
  - Partial failure → rollback
  - Concurrent reloads
  - State preservation

- **Message Passing**: 90% of edge cases covered
  - Send to live node
  - Send to crashed node
  - Send during node crash
  - Timeout handling

### Coverage Measurement

```bash
# Install cargo-tarpaulin
cargo install cargo-tarpaulin

# Run coverage for distributed crate
cd crates/lunatic-distributed
cargo tarpaulin --out Html --output-dir coverage
```

## Future Enhancements

### Planned Test Additions

1. **Chaos Testing**
   - Random node crashes during operations
   - Random network delays
   - Message reordering simulation

2. **Performance Testing**
   - Cross-node messaging latency
   - Hot reload coordination overhead
   - Node failure recovery time

3. **Security Testing**
   - Certificate validation
   - Environment permission enforcement
   - Malicious node behavior

4. **Scalability Testing**
   - 10+ node clusters
   - High message throughput
   - Large module deployments

### Test Infrastructure Improvements

1. **Network Simulation**
   ```rust
   struct NetworkSimulator {
       latency: Duration,
       packet_loss: f32,
       bandwidth_limit: usize,
   }
   ```

2. **Failure Injection**
   ```rust
   trait FailureInjector {
       fn inject_crash(&mut self, node_id: u64);
       fn inject_partition(&mut self, nodes: Vec<u64>);
       fn inject_delay(&mut self, duration: Duration);
   }
   ```

3. **Deterministic Replay**
   ```rust
   struct TestRecorder {
       events: Vec<Event>,
       replay_mode: bool,
   }
   ```

## References

### Related Documentation

- [Hot Reload Implementation](../phases/PHASE7_RESOURCE_MIGRATION.md)
- [Core Values Status](../core_values/status.md)
- [Rollback Semantics](../hot_reload/ROLLBACK.md)

### Source Code

- `crates/lunatic-distributed/src/distributed/client.rs` - Client implementation
- `crates/lunatic-distributed/src/distributed/server.rs` - Server implementation
- `crates/lunatic-process/src/hot_reload.rs` - Reload coordinator

### External Resources

- [Distributed Systems Testing](https://jepsen.io/) - Jepsen testing methodology
- [Chaos Engineering](https://principlesofchaos.org/) - Chaos principles
- [Tokio Testing](https://tokio.rs/tokio/topics/testing) - Async testing patterns

## Contributing

When adding new distributed tests:

1. **Update this documentation** - Add test descriptions and examples
2. **Follow naming conventions** - `test_<scenario>_<condition>`
3. **Add inline comments** - Explain non-obvious test logic
4. **Consider CI impact** - Keep tests fast (<5s per test)
5. **Test locally first** - Verify on your machine before CI

### Example PR Checklist

- [ ] Tests added for new feature/bug
- [ ] Tests pass locally with `cargo test`
- [ ] Documentation updated in this file
- [ ] No flaky behavior observed (run 10 times)
- [ ] Timeout values are reasonable
- [ ] Cleanup code is present
- [ ] Error messages are descriptive

---

**Last Updated**: 2025-10-06
**Maintainer**: Lunatic Development Team
**Status**: Active Development
