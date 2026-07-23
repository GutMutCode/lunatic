use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context, Result};
use lunatic_control::NodeInfo;
use lunatic_distributed::{
    control::{self, cert::CertificateAuthority},
    distributed::{
        client::{EnvironmentId, NodeId, ProcessId, SpawnParams},
        message::{self, Request, Response, ResponseContent, Spawn},
        server::ServerCtx,
        Client, GlobalProcessId,
    },
    quic, CertAttrs, DistributedProcessState, SUBJECT_DIR_ATTRS,
};
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironment, LunaticEnvironments},
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
        Modules, RawWasm,
    },
    state::SignalSendError,
    wasm::spawn_wasm,
    Process, Signal,
};
use lunatic_runtime::{DefaultProcessConfig, DefaultProcessState};
use quinn::{Connection, Endpoint, VarInt};
use rcgen::{CertificateParams, CustomExtension, DnType};
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    task::JoinHandle,
    time::timeout,
};
use wasmtime::Val;

const ENVIRONMENT_ID: u64 = 41;
const SERVICE_NAME: &str = "guest-service";
const CROSS_ENVIRONMENT_NAME: &str = "cross-environment";
const GUEST_ROUND_TRIPS: usize = 16;
const TEST_TIMEOUT: Duration = Duration::from_secs(15);

// The distributed connection manager keys peers by numeric node ID. Keep the
// independent localhost clusters in this file from overlapping those IDs.
static TEST_CLUSTER_LOCK: Mutex<()> = Mutex::const_new(());

const REGISTRY_GUEST: &str = r#"
(module
    (import "lunatic::distributed" "node_id" (func $node_id (result i64)))
    (import "lunatic::distributed" "send" (func $distributed_send (param i64 i64) (result i32)))
    (import "lunatic::distributed" "send_receive_skip_search"
        (func $distributed_send_receive (param i64 i64 i64 i64) (result i32)))
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "data_size" (func $data_size (result i64)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "read_data" (func $read_data (param i32 i32) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "send" (func $local_send (param i64) (result i32)))
    (import "lunatic::message" "write_data" (func $write_data (param i32 i32) (result i32)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::registry" "get" (func $registry_get (param i32 i32 i32 i32) (result i32)))
    (import "lunatic::registry" "put" (func $registry_put (param i32 i32 i64 i64)))
    (import "lunatic::registry" "remove" (func $registry_remove (param i32 i32)))

    (memory (export "memory") 1)
    (data (i32.const 0) "guest-service")
    (data (i32.const 32) "cross-environment")
    (data (i32.const 64) "pong")

    (func $assert_i32 (param $actual i32) (param $expected i32)
        (if (i32.ne (local.get $actual) (local.get $expected)) (then unreachable)))

    (func $assert_i64 (param $actual i64) (param $expected i64)
        (if (i64.ne (local.get $actual) (local.get $expected)) (then unreachable)))

    (func $signal (param $observer i64) (param $tag i64)
        (call $create_data (local.get $tag) (i64.const 0))
        (call $assert_i32 (call $local_send (local.get $observer)) (i32.const 0)))

    ;; The owner publishes itself, serves a bounded cross-node request burst, then proves
    ;; explicit removal before re-registering for process-exit cleanup.
    (func (export "owner") (param $observer i64)
        (local $round i32)
        (call $registry_put
            (i32.const 0)
            (i32.const 13)
            (call $node_id)
            (call $process_id))
        (call $signal (local.get $observer) (i64.const 10))

        (loop $serve
            (call $assert_i32
                (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
                (i32.const 0))
            (call $assert_i64 (call $get_tag) (i64.const 42))
            (call $assert_i64 (call $data_size) (i64.const 16))
            (call $assert_i32
                (call $read_data (i32.const 128) (i32.const 16))
                (i32.const 16))

            (call $create_data (i64.const 42) (i64.const 4))
            (call $assert_i32
                (call $write_data (i32.const 64) (i32.const 4))
                (i32.const 4))
            (call $assert_i32
                (call $distributed_send
                    (i64.load (i32.const 128))
                    (i64.load (i32.const 136)))
                (i32.const 0))
            (local.set $round (i32.add (local.get $round) (i32.const 1)))
            (br_if $serve
                (i32.lt_u (local.get $round) (i32.const __GUEST_ROUND_TRIPS__))))
        (call $signal (local.get $observer) (i64.const 11))

        ;; Host barrier before the explicit remove/re-register lifecycle step.
        (call $assert_i32
            (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
            (i32.const 0))
        (call $registry_remove (i32.const 0) (i32.const 13))
        (call $assert_i32
            (call $registry_get (i32.const 0) (i32.const 13) (i32.const 96) (i32.const 104))
            (i32.const 1))
        (call $signal (local.get $observer) (i64.const 12))

        (call $registry_put
            (i32.const 0)
            (i32.const 13)
            (call $node_id)
            (call $process_id))
        (call $signal (local.get $observer) (i64.const 13))

        ;; Returning after this barrier exercises owner-exit cleanup.
        (call $assert_i32
            (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
            (i32.const 0)))

    ;; The requester first observes a missing remote environment. Once the
    ;; owner is started in the same environment, it resolves the global name
    ;; and completes a request/reply over the live remote mailbox.
    (func (export "requester") (param $observer i64) (param $remote_node i64)
        (local $round i32)
        ;; A cross-environment registry entry must be hidden by the legacy ABI,
        ;; which returns only (node, process) and sends in the caller's env.
        (call $assert_i32
            (call $registry_get (i32.const 32) (i32.const 17) (i32.const 96) (i32.const 104))
            (i32.const 1))

        (call $create_data (i64.const 90) (i64.const 0))
        (call $assert_i32
            (call $distributed_send (local.get $remote_node) (i64.const 9999999))
            (i32.const 1))
        (call $signal (local.get $observer) (i64.const 20))

        (loop $round_trip
            ;; The host releases one measured request at a time so its duration starts
            ;; before this guest lookup and cannot collapse into observer-queue drain time.
            (call $assert_i32
                (call $receive (i32.const 0) (i32.const 0) (i64.const -1))
                (i32.const 0))
            ;; Include the production global-registry lookup in every measured request.
            (call $assert_i32
                (call $registry_get (i32.const 0) (i32.const 13) (i32.const 96) (i32.const 104))
                (i32.const 0))
            (i64.store (i32.const 128) (call $node_id))
            (i64.store (i32.const 136) (call $process_id))
            (call $create_data (i64.const 42) (i64.const 16))
            (call $assert_i32
                (call $write_data (i32.const 128) (i32.const 16))
                (i32.const 16))
            (call $assert_i32
                (call $distributed_send_receive
                    (i64.load (i32.const 96))
                    (i64.load (i32.const 104))
                    (i64.const 42)
                    (i64.const 5000))
                (i32.const 0))
            (call $assert_i64 (call $get_tag) (i64.const 42))
            (call $assert_i64 (call $data_size) (i64.const 4))
            (call $assert_i32
                (call $read_data (i32.const 160) (i32.const 4))
                (i32.const 4))
            (call $assert_i32 (i32.load (i32.const 160)) (i32.const 0x676e6f70))
            (local.set $round (i32.add (local.get $round) (i32.const 1)))
            (call $signal
                (local.get $observer)
                (i64.add (i64.const 100) (i64.extend_i32_u (local.get $round))))
            (br_if $round_trip
                (i32.lt_u (local.get $round) (i32.const __GUEST_ROUND_TRIPS__))))
        (call $signal (local.get $observer) (i64.const 22))

        ;; The remote node exists and the environment exists, but the process does not.
        (call $create_data (i64.const 91) (i64.const 0))
        (call $assert_i32
            (call $distributed_send (i64.load (i32.const 96)) (i64.const 9999999))
            (i32.const 1))
        (call $signal (local.get $observer) (i64.const 21)))
)
"#;

struct TagObserver {
    id: u64,
    tags: mpsc::UnboundedSender<i64>,
}

impl Process for TagObserver {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
        if let Signal::Message(Message::Data(message)) = signal {
            let _ = self.tags.send(message.tag.unwrap_or(0));
        }
        Ok(())
    }
}

struct TestNode {
    name: String,
    address: std::net::SocketAddr,
    cert: String,
    key: String,
    client: Client,
    distributed: DistributedProcessState,
    envs: LunaticEnvironments,
    endpoint: Option<Endpoint>,
    server_task: Option<JoinHandle<()>>,
}

struct TestCluster {
    root_cert: String,
    nodes: Vec<TestNode>,
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
}

impl TestCluster {
    async fn new(node_count: usize) -> Result<Self> {
        let root = control::cert::test_root_cert()?;
        let root_cert = root.certificate_pem().to_owned();
        let mut materials = Vec::with_capacity(node_count);

        for id in 1..=node_count as u64 {
            let name = format!("guest-node-{id}.lunatic.test");
            let (cert, key) = node_certificate(&root, &name, id)?;
            let endpoint =
                quic::new_quic_server("[::1]:0".parse()?, vec![cert.clone()], &key, &root_cert)?;
            let address = endpoint.local_addr()?;
            materials.push((id, name, address, cert, key, endpoint));
        }

        let node_infos = materials
            .iter()
            .map(|(id, name, address, _, _, _)| NodeInfo {
                id: *id,
                name: name.clone(),
                address: *address,
            })
            .collect::<Vec<_>>();
        let runtime = WasmtimeRuntime::new(&default_config())?;
        let module = Arc::new(runtime.compile_module::<DefaultProcessState>(RawWasm::new(
            Some(1),
            wat::parse_str(
                REGISTRY_GUEST.replace("__GUEST_ROUND_TRIPS__", &GUEST_ROUND_TRIPS.to_string()),
            )?,
        ))?);
        let mut nodes = Vec::with_capacity(node_count);

        for (id, name, address, cert, key, mut endpoint) in materials {
            let control_client = control::Client::from_static_nodes(
                test_registration(id, &root_cert, &cert),
                id,
                node_infos.clone(),
            );
            let quic_client = quic::new_quic_client(&root_cert, &cert, &key)?;
            let client = Client::new(id, control_client.clone(), quic_client);
            let distributed =
                DistributedProcessState::new(id, control_client, client.clone()).await?;
            let envs = LunaticEnvironments::default();
            let ctx: ServerCtx<DefaultProcessState, LunaticEnvironment> = ServerCtx {
                envs: Arc::new(envs.clone()),
                modules: Modules::default(),
                distributed: distributed.clone(),
                runtime: runtime.clone(),
                node_client: client.clone(),
                allowed_envs: None,
            };
            let server_endpoint = endpoint.clone();
            let server_task = tokio::spawn(async move {
                if let Err(error) = quic::handle_node_server(&mut endpoint, ctx).await {
                    if !error.to_string().contains("Node server exited") {
                        eprintln!("guest registry test server stopped: {error:#}");
                    }
                }
            });
            nodes.push(TestNode {
                name,
                address,
                cert,
                key,
                client,
                distributed,
                envs,
                endpoint: Some(server_endpoint),
                server_task: Some(server_task),
            });
        }

        Ok(Self {
            root_cert,
            nodes,
            runtime,
            module,
        })
    }

    async fn create_environment(&self, index: usize) -> Result<Arc<LunaticEnvironment>> {
        self.nodes[index].envs.create(ENVIRONMENT_ID).await
    }

    async fn spawn_guest(
        &self,
        index: usize,
        environment: Arc<LunaticEnvironment>,
        function: &str,
        params: Vec<Val>,
    ) -> Result<(JoinHandle<Result<DefaultProcessState>>, Arc<dyn Process>)> {
        let state = DefaultProcessState::new(
            environment.clone(),
            Some(self.nodes[index].distributed.clone()),
            self.runtime.clone(),
            self.module.clone(),
            Arc::new(DefaultProcessConfig::default()),
            Arc::new(RwLock::new(HashMap::new())),
        )?;
        spawn_wasm(
            environment,
            self.runtime.clone(),
            &self.module,
            state,
            function,
            params,
            None,
        )
        .await
    }

    async fn wait_for_registry(&self, name: &str, expected: Option<GlobalProcessId>) -> Result<()> {
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            let matches = self.nodes.iter().all(|node| {
                node.client
                    .registry()
                    .lookup_global(name)
                    .map(|entry| entry.global_pid)
                    == expected
            });
            if matches {
                return Ok::<(), anyhow::Error>(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "registry value '{name}' did not converge to {expected:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn stall_node_server(&mut self, index: usize) {
        if let Some(task) = self.nodes[index].server_task.take() {
            task.abort();
            let _ = task.await;
        }
        assert!(
            self.nodes[index].endpoint.is_some(),
            "stalled node server must keep its QUIC endpoint alive"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    async fn send_request_as(
        &self,
        source_index: usize,
        target_index: usize,
        wire_message_id: u64,
        request: Request,
    ) -> Result<(quic::Client, Connection)> {
        self.send_requests_as(source_index, target_index, vec![(wire_message_id, request)])
            .await
    }

    async fn send_requests_as(
        &self,
        source_index: usize,
        target_index: usize,
        requests: Vec<(u64, Request)>,
    ) -> Result<(quic::Client, Connection)> {
        let source = &self.nodes[source_index];
        let target = &self.nodes[target_index];
        let raw_client = quic::new_quic_client(&self.root_cert, &source.cert, &source.key)?;
        let connection = raw_client._connect(target.address, &target.name).await?;
        let mut stream = connection.open_uni().await?;
        for (wire_message_id, request) in requests {
            let data = message::serialize_message(&request)?;
            quic::write_message(&mut stream, wire_message_id, data.into()).await?;
        }
        stream.finish()?;
        let _ = stream.stopped().await?;
        drop(stream);
        // `stopped()` confirms transport acknowledgement, not application dispatch. Keep both
        // the endpoint and connection alive until the caller observes its application barrier.
        Ok((raw_client, connection))
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(endpoint) = node.endpoint.take() {
                endpoint.close(VarInt::from_u32(0), b"test complete");
            }
            if let Some(task) = node.server_task.take() {
                task.abort();
            }
        }
    }
}

fn add_observer(
    environment: &Arc<LunaticEnvironment>,
) -> Result<(Arc<dyn Process>, mpsc::UnboundedReceiver<i64>)> {
    let id = environment.get_next_process_id();
    let (tags, receiver) = mpsc::unbounded_channel();
    let observer: Arc<dyn Process> = Arc::new(TagObserver { id, tags });
    environment.add_process(id, observer.clone())?;
    Ok((observer, receiver))
}

async fn wait_for_tag(receiver: &mut mpsc::UnboundedReceiver<i64>, expected: i64) -> Result<()> {
    timeout(TEST_TIMEOUT, async {
        loop {
            let tag = receiver
                .recv()
                .await
                .ok_or_else(|| anyhow!("observer channel closed before tag {expected}"))?;
            if tag == expected {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await
    .with_context(|| format!("guest did not emit phase tag {expected}"))??;
    Ok(())
}

fn send_trigger(process: &Arc<dyn Process>, tag: i64) -> Result<()> {
    process
        .send(Signal::Message(Message::Data(DataMessage::new(
            Some(tag),
            0,
        ))))
        .map_err(|error| anyhow!(error.to_string()))
}

fn node_certificate(
    root: &CertificateAuthority,
    name: &str,
    node_id: u64,
) -> Result<(String, String)> {
    let mut params = CertificateParams::new(vec![name.to_string()])?;
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Lunatic Inc.");
    params.distinguished_name.push(DnType::CommonName, "Node");
    let attributes = serde_json::to_string(&CertAttrs {
        node_id: Some(node_id),
        allowed_envs: vec![],
        is_privileged: true,
    })?;
    params
        .custom_extensions
        .push(CustomExtension::from_oid_content(
            &SUBJECT_DIR_ATTRS,
            der_utf8_string(&attributes),
        ));
    let cert = control::cert::CertificateRequest::new(params)?;
    Ok((
        cert.serialize_pem_with_signer(root)?,
        cert.serialize_private_key_pem(),
    ))
}

fn der_utf8_string(value: &str) -> Vec<u8> {
    assert!(
        value.len() < 128,
        "test certificate attributes are too long"
    );
    let mut encoded = Vec::with_capacity(value.len() + 2);
    encoded.push(0x0c);
    encoded.push(value.len() as u8);
    encoded.extend_from_slice(value.as_bytes());
    encoded
}

fn test_registration(node_id: u64, root_cert: &str, cert: &str) -> control::RegistrationMetadata {
    control::RegistrationMetadata {
        node_name: uuid::Uuid::from_u128(node_id as u128),
        cert_pem_chain: vec![cert.to_string()],
        root_cert: root_cert.to_string(),
        envs: vec![],
        is_privileged: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn guest_registry_resolves_a_live_remote_mailbox_and_cleans_up_owner_exit() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let cluster = TestCluster::new(2).await?;

    // Seed a different-environment entry through the same cluster quorum. The
    // requester must not observe it through the legacy two-field guest ABI.
    let cross_environment_pid = GlobalProcessId::new(1, ENVIRONMENT_ID + 1, 777);
    cluster.nodes[0]
        .client
        .register_global(CROSS_ENVIRONMENT_NAME, cross_environment_pid)
        .await?;
    cluster
        .wait_for_registry(CROSS_ENVIRONMENT_NAME, Some(cross_environment_pid))
        .await?;

    // Only node 2 initially has the guest environment. This makes the first
    // guest send exercise the remote EnvironmentNotFound acknowledgement.
    let requester_environment = cluster.create_environment(1).await?;
    let (requester_observer, mut requester_tags) = add_observer(&requester_environment)?;
    let (requester_join, requester) = cluster
        .spawn_guest(
            1,
            requester_environment,
            "requester",
            vec![Val::I64(requester_observer.id() as i64), Val::I64(1)],
        )
        .await?;
    wait_for_tag(&mut requester_tags, 20).await?;

    // Add the exact same environment to node 1 and start the named owner.
    let owner_environment = cluster.create_environment(0).await?;
    let (owner_observer, mut owner_tags) = add_observer(&owner_environment)?;
    let (owner_join, owner) = cluster
        .spawn_guest(
            0,
            owner_environment,
            "owner",
            vec![Val::I64(owner_observer.id() as i64)],
        )
        .await?;
    wait_for_tag(&mut owner_tags, 10).await?;
    let owner_pid = GlobalProcessId::new(1, ENVIRONMENT_ID, owner.id());
    cluster
        .wait_for_registry(SERVICE_NAME, Some(owner_pid))
        .await?;

    let round_trip_started = Instant::now();
    let mut round_trip_latencies = Vec::with_capacity(GUEST_ROUND_TRIPS);
    for round in 1..=GUEST_ROUND_TRIPS {
        let request_started = Instant::now();
        send_trigger(&requester, 60 + round as i64)?;
        wait_for_tag(&mut requester_tags, 100 + round as i64).await?;
        round_trip_latencies.push(request_started.elapsed());
    }
    wait_for_tag(&mut requester_tags, 22).await?;
    let round_trip_elapsed = round_trip_started.elapsed();
    wait_for_tag(&mut requester_tags, 21).await?;
    timeout(TEST_TIMEOUT, requester_join)
        .await
        .context("requester guest did not exit")???;
    wait_for_tag(&mut owner_tags, 11).await?;
    round_trip_latencies.sort_unstable();
    let percentile = |percent: usize| {
        let rank = (round_trip_latencies.len() * percent).div_ceil(100).max(1);
        round_trip_latencies[rank - 1].as_micros()
    };
    println!(
        "LUNATIC_RESILIENCE_EVIDENCE {{\"kind\":\"guest_wasm_cross_node_round_trip\",\"samples\":{GUEST_ROUND_TRIPS},\"elapsed_us\":{},\"average_us\":{:.3},\"p50_us\":{},\"p95_us\":{},\"p99_us\":{},\"rate_per_sec\":{:.3},\"loopback_mtls\":true}}",
        round_trip_elapsed.as_micros(),
        round_trip_elapsed.as_secs_f64() * 1_000_000.0 / GUEST_ROUND_TRIPS as f64,
        percentile(50),
        percentile(95),
        percentile(99),
        GUEST_ROUND_TRIPS as f64 / round_trip_elapsed.as_secs_f64()
    );

    // The guest itself removes the name, verifies a miss, and publishes it
    // again so its normal process exit must clean every replica.
    send_trigger(&owner, 61)?;
    wait_for_tag(&mut owner_tags, 12).await?;
    wait_for_tag(&mut owner_tags, 13).await?;
    cluster
        .wait_for_registry(SERVICE_NAME, Some(owner_pid))
        .await?;

    send_trigger(&owner, 62)?;
    timeout(TEST_TIMEOUT, owner_join)
        .await
        .context("owner guest did not exit")???;
    cluster.wait_for_registry(SERVICE_NAME, None).await?;

    cluster.nodes[0]
        .client
        .coordinator()
        .unregister_global_coordinated(CROSS_ENVIRONMENT_NAME, cross_environment_pid)
        .await?;
    cluster
        .wait_for_registry(CROSS_ENVIRONMENT_NAME, None)
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn spawn_response_waiter_accepts_only_the_intended_mtls_peer() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let mut cluster = TestCluster::new(3).await?;
    let barrier_environment = cluster.create_environment(0).await?;
    let (barrier_process, mut barrier_tags) = add_observer(&barrier_environment)?;
    cluster.stall_node_server(1).await;

    // Node 1 creates a waiter for a spawn response from node 2. Keeping node
    // 2's endpoint alive but removing its accept loop prevents a real response
    // while preserving a reachable topology target.
    let node_one = cluster.nodes[0].client.clone();
    let message_id = node_one
        .spawn(SpawnParams {
            env: EnvironmentId(ENVIRONMENT_ID),
            src: ProcessId(7_001),
            node: NodeId(2),
            spawn: Spawn {
                response_node_id: 1,
                environment_id: ENVIRONMENT_ID,
                module_id: 1,
                function: "spoof-target".to_string(),
                params: Vec::new(),
                config: Vec::new(),
            },
        })
        .await?;

    // Node 3 knows the message ID but is not the authenticated peer retained
    // by the waiter. Its response must neither complete nor remove that waiter.
    let spoof_transport_guard = cluster
        .send_requests_as(
            2,
            0,
            vec![
                (
                    8_001,
                    Request::Response(Response {
                        message_id: message_id.0,
                        content: ResponseContent::Spawned(999),
                    }),
                ),
                (
                    8_002,
                    Request::Message {
                        node_id: 3,
                        environment_id: ENVIRONMENT_ID,
                        process_id: barrier_process.id(),
                        tag: Some(93),
                        data: Vec::new(),
                    },
                ),
            ],
        )
        .await?;
    wait_for_tag(&mut barrier_tags, 93).await?;
    assert!(
        timeout(
            Duration::from_millis(50),
            node_one.await_response(message_id)
        )
        .await
        .is_err(),
        "spawn waiter accepted a response authenticated as node 3"
    );
    drop(spoof_transport_guard);

    let intended_transport_guard = cluster
        .send_request_as(
            1,
            0,
            8_003,
            Request::Response(Response {
                message_id: message_id.0,
                content: ResponseContent::Spawned(42),
            }),
        )
        .await?;
    let response = timeout(Duration::from_secs(2), node_one.await_response(message_id))
        .await
        .context("node-2 spawn response did not complete its waiter")??;
    assert_eq!(response, ResponseContent::Spawned(42));
    drop(intended_transport_guard);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn replayed_message_executes_server_side_effect_once_across_reconnect() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let mut cluster = TestCluster::new(2).await?;
    let target_environment = cluster.create_environment(1).await?;
    let (target, mut delivered_tags) = add_observer(&target_environment)?;

    // Keep node 1's QUIC endpoint alive while stopping its application accept loop. Node 2 can
    // enqueue responses, but node 1 cannot consume them, so no response can end the replay test
    // before the same transport ID is resent on a fresh mTLS connection.
    cluster.stall_node_server(0).await;
    let original = Request::Message {
        node_id: 1,
        environment_id: ENVIRONMENT_ID,
        process_id: target.id(),
        tag: Some(70),
        data: vec![1, 2, 3, 4],
    };
    let first_transport = cluster
        .send_requests_as(
            0,
            1,
            vec![
                (9_001, original.clone()),
                (
                    9_002,
                    Request::Message {
                        node_id: 1,
                        environment_id: ENVIRONMENT_ID,
                        process_id: target.id(),
                        tag: Some(71),
                        data: Vec::new(),
                    },
                ),
            ],
        )
        .await?;
    let first = timeout(TEST_TIMEOUT, delivered_tags.recv())
        .await
        .context("original replay-test message was not delivered")?
        .ok_or_else(|| anyhow!("replay-test observer closed after the original"))?;
    let first_barrier = timeout(TEST_TIMEOUT, delivered_tags.recv())
        .await
        .context("original replay-test barrier was not delivered")?
        .ok_or_else(|| anyhow!("replay-test observer closed before the first barrier"))?;
    assert_eq!((first, first_barrier), (70, 71));
    drop(first_transport);

    let replay_transport = cluster
        .send_requests_as(
            0,
            1,
            vec![
                (9_001, original),
                (
                    9_003,
                    Request::Message {
                        node_id: 1,
                        environment_id: ENVIRONMENT_ID,
                        process_id: target.id(),
                        tag: Some(72),
                        data: Vec::new(),
                    },
                ),
            ],
        )
        .await?;
    let replay_barrier = timeout(TEST_TIMEOUT, delivered_tags.recv())
        .await
        .context("replay-test reconnect barrier was not delivered")?
        .ok_or_else(|| anyhow!("replay-test observer closed before the reconnect barrier"))?;
    assert_eq!(
        replay_barrier, 72,
        "the duplicate transport ID executed its mailbox side effect again"
    );
    drop(replay_transport);
    Ok(())
}
