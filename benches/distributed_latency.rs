use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, ensure, Context, Result};
use criterion::{criterion_group, criterion_main, Criterion};
use lunatic_control::NodeInfo;
use lunatic_distributed::{
    control::{
        self,
        cert::{CertificateAuthority, CertificateRequest},
    },
    distributed::{
        client::{EnvironmentId, NodeId, ProcessId, SendParams},
        server::{gen_node_cert, ServerCtx},
        Client, GlobalProcessId,
    },
    quic, CertAttrs, DistributedProcessState, SUBJECT_DIR_ATTRS,
};
use lunatic_process::{
    env::{Environments, LunaticEnvironment, LunaticEnvironments},
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{default_config, WasmtimeRuntime},
        Modules,
    },
    spawn_native, NativeProcess, Process, Signal,
};
use lunatic_runtime::DefaultProcessState;
use quinn::Endpoint;
use rcgen::{CertificateParams, CustomExtension, DnType};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use url::Url;
use uuid::Uuid;

const BENCH_ENVIRONMENT_ID: u64 = 1;
const REGISTRY_NAME: &str = "benchmark-live-mailbox";
const ROUND_TRIP_TAG: i64 = 7;
// The production replay cache retains completed message IDs for 180 seconds and
// admits 16,384 entries per node. Keep this correctness-oriented live benchmark
// short enough that Criterion cannot turn one run into a replay-cache soak test.
// At the observed 70-130 us round-trip range this window uses roughly 1,000
// entries, while still collecting hundreds of real production-path samples.
const LIVE_MAILBOX_WARM_UP: Duration = Duration::from_millis(10);
const LIVE_MAILBOX_MEASUREMENT: Duration = Duration::from_millis(50);

fn control_lookup_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("control_lookup_nodes", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let control = ControlHandle::new().await.expect("control server to start");

            let mut nodes = Vec::new();
            for _ in 0..2 {
                let node_addr: SocketAddr = "127.0.0.1:0".parse().expect("socket addr");
                let node = register_node(&control, node_addr).await;
                nodes.push(node.expect("node registration"));
            }

            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let (query_id, _) = nodes[0].lookup_nodes("").await.expect("lookup to succeed");
                total += start.elapsed();
                nodes[0]
                    .query_result(&query_id)
                    .expect("lookup result to remain available");
            }

            for node in &nodes {
                node.notify_node_stopped().await.ok();
            }
            control.shutdown().await;

            total
        });
    });
}

fn distributed_registry_live_mailbox_benchmark(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let rt = Runtime::new().expect("tokio runtime");
    let harness = rt
        .block_on(LiveMailboxHarness::new())
        .expect("live distributed mailbox harness");
    let harness = Arc::new(Mutex::new(harness));

    c.bench_function("distributed_registry_live_mailbox_round_trip", |b| {
        let harness = Arc::clone(&harness);
        b.to_async(&rt).iter_custom(move |iters| {
            let harness = Arc::clone(&harness);
            async move {
                let mut harness = harness.lock().await;
                let mut total = Duration::ZERO;

                for _ in 0..iters {
                    let start = Instant::now();
                    harness
                        .round_trip()
                        .await
                        .expect("registry-routed mailbox round trip");
                    total += start.elapsed();
                }

                total
            }
        });
    });

    let harness = Arc::try_unwrap(harness)
        .unwrap_or_else(|_| panic!("Criterion retained the live mailbox harness"))
        .into_inner();
    rt.block_on(harness.shutdown());
}

criterion_group!(control_benches, control_lookup_benchmark);
criterion_group! {
    name = live_mailbox_benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(LIVE_MAILBOX_WARM_UP)
        .measurement_time(LIVE_MAILBOX_MEASUREMENT);
    targets = distributed_registry_live_mailbox_benchmark
}
criterion_main!(control_benches, live_mailbox_benches);

struct BenchNode {
    client: Client,
    environment: Arc<LunaticEnvironment>,
    endpoint: Endpoint,
    server_task: JoinHandle<()>,
}

struct LiveMailboxHarness {
    nodes: Vec<BenchNode>,
    receiver: NativeProcess,
    echo: NativeProcess,
    process_tasks: Vec<JoinHandle<Result<()>>>,
    replies: mpsc::Receiver<DataMessage>,
    registered_echo: GlobalProcessId,
    next_sequence: u64,
}

impl LiveMailboxHarness {
    async fn new() -> Result<Self> {
        let root = control::cert::test_root_cert()?;
        let root_cert = root.certificate_pem().to_owned();
        let mut materials = Vec::with_capacity(2);

        for id in 1..=2u64 {
            let name = format!("bench-node-{id}.lunatic.test");
            let (cert, key) = node_certificate(&root, &name, id)?;
            let endpoint =
                quic::new_quic_server(bench_bind_addr(), vec![cert.clone()], &key, &root_cert)?;
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
        let mut nodes = Vec::with_capacity(2);

        for (id, _name, _address, cert, key, endpoint) in materials {
            let control_client = control::Client::from_static_nodes(
                test_registration(id, &root_cert, &cert),
                id,
                node_infos.clone(),
            );
            let quic_client = quic::new_quic_client(&root_cert, &cert, &key)?;
            let client = Client::new(id, control_client.clone(), quic_client);
            let distributed =
                DistributedProcessState::new(id, control_client, client.clone()).await?;
            let environments = Arc::new(LunaticEnvironments::default());
            let environment = environments.create(BENCH_ENVIRONMENT_ID).await?;
            let server_ctx: ServerCtx<DefaultProcessState, LunaticEnvironment> = ServerCtx {
                envs: environments,
                modules: Modules::default(),
                distributed,
                runtime: runtime.clone(),
                node_client: client.clone(),
                allowed_envs: None,
            };
            let endpoint_handle = endpoint.clone();
            let server_task = tokio::spawn(async move {
                let mut endpoint = endpoint;
                if let Err(error) = quic::handle_node_server(&mut endpoint, server_ctx).await {
                    log::debug!("distributed latency benchmark server stopped: {error:#}");
                }
            });
            nodes.push(BenchNode {
                client,
                environment,
                endpoint: endpoint_handle,
                server_task,
            });
        }

        // Let both accept loops start before quorum registration. This is setup,
        // deliberately outside the measured registry-to-mailbox path.
        tokio::task::yield_now().await;

        let (reply_tx, replies) = mpsc::channel(16);
        let (receiver_task, receiver) = spawn_native(
            nodes[0].environment.clone(),
            move |_process, mailbox| async move {
                loop {
                    if let Message::Data(message) = mailbox.pop(None).await {
                        reply_tx
                            .send(message)
                            .await
                            .map_err(|_| anyhow!("benchmark reply observer stopped"))?;
                    }
                }
            },
        )?;

        let response_client = nodes[1].client.clone();
        let receiver_id = receiver.id();
        let (echo_task, echo) = spawn_native(
            nodes[1].environment.clone(),
            move |process, mailbox| async move {
                loop {
                    let Message::Data(message) = mailbox.pop(None).await else {
                        continue;
                    };
                    response_client
                        .send(SendParams {
                            source_env: EnvironmentId(BENCH_ENVIRONMENT_ID),
                            target_env: EnvironmentId(BENCH_ENVIRONMENT_ID),
                            src: ProcessId(process.id()),
                            node: NodeId(1),
                            dest: ProcessId(receiver_id),
                            tag: message.tag,
                            data: message.buffer,
                        })
                        .await
                        .context("echo process failed to deliver its reply")?;
                }
            },
        )?;

        let registered_echo = GlobalProcessId::new(2, BENCH_ENVIRONMENT_ID, echo.id());
        nodes[1]
            .client
            .register_global(REGISTRY_NAME, registered_echo)
            .await
            .context("failed to register the live echo process through quorum")?;
        wait_for_registry(&nodes[0].client, registered_echo).await?;

        Ok(Self {
            nodes,
            receiver,
            echo,
            process_tasks: vec![receiver_task, echo_task],
            replies,
            registered_echo,
            next_sequence: 1,
        })
    }

    async fn round_trip(&mut self) -> Result<()> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let entry = self.nodes[0]
            .client
            .registry()
            .lookup_global(REGISTRY_NAME)
            .ok_or_else(|| anyhow!("global registry entry disappeared"))?;
        ensure!(
            entry.global_pid == self.registered_echo,
            "global registry resolved an unexpected process"
        );
        let payload = sequence.to_le_bytes().to_vec();

        self.nodes[0]
            .client
            .send(SendParams {
                source_env: EnvironmentId(BENCH_ENVIRONMENT_ID),
                target_env: EnvironmentId(entry.global_pid.environment_id()),
                src: ProcessId(self.receiver.id()),
                node: NodeId(entry.global_pid.node_id()),
                dest: ProcessId(entry.global_pid.process_id()),
                tag: Some(ROUND_TRIP_TAG),
                data: payload.clone(),
            })
            .await
            .context("failed to deliver request to registry-resolved process")?;

        let reply = tokio::time::timeout(Duration::from_secs(5), self.replies.recv())
            .await
            .context("timed out waiting for live mailbox reply")?
            .ok_or_else(|| anyhow!("live mailbox reply process stopped"))?;
        ensure!(reply.tag == Some(ROUND_TRIP_TAG), "reply tag changed");
        ensure!(reply.buffer == payload, "reply payload changed");
        Ok(())
    }

    async fn shutdown(mut self) {
        let _ = self.receiver.send(Signal::Kill);
        let _ = self.echo.send(Signal::Kill);
        for task in self.process_tasks.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(1), task).await;
        }
        for node in &self.nodes {
            node.endpoint
                .close(0u32.into(), b"distributed latency benchmark complete");
            node.server_task.abort();
        }
        for node in self.nodes {
            let _ = node.server_task.await;
        }
    }
}

fn bench_bind_addr() -> SocketAddr {
    if cfg!(windows) {
        "[::1]:0".parse().expect("IPv6 loopback address")
    } else {
        "127.0.0.1:0".parse().expect("IPv4 loopback address")
    }
}

async fn wait_for_registry(client: &Client, expected: GlobalProcessId) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if client
            .registry()
            .lookup_global(REGISTRY_NAME)
            .map(|entry| entry.global_pid == expected)
            .unwrap_or(false)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(anyhow!("global registry did not converge before benchmark"));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
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
    let cert = CertificateRequest::new(params)?;
    Ok((
        cert.serialize_pem_with_signer(root)?,
        cert.serialize_private_key_pem(),
    ))
}

fn der_utf8_string(value: &str) -> Vec<u8> {
    assert!(
        value.len() < 128,
        "benchmark certificate attributes are too long"
    );
    let mut encoded = Vec::with_capacity(value.len() + 2);
    encoded.push(0x0c);
    encoded.push(value.len() as u8);
    encoded.extend_from_slice(value.as_bytes());
    encoded
}

fn test_registration(node_id: u64, root_cert: &str, cert: &str) -> control::RegistrationMetadata {
    control::RegistrationMetadata {
        node_name: Uuid::from_u128(node_id as u128),
        cert_pem_chain: vec![cert.to_string()],
        root_cert: root_cert.to_string(),
        envs: vec![],
        is_privileged: true,
    }
}

struct ControlHandle {
    addr: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

impl ControlHandle {
    async fn new() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        listener
            .set_nonblocking(true)
            .context("failed to set control listener non-blocking")?;

        let task = tokio::spawn(async move {
            if let Err(err) = lunatic_control_axum::server::control_server_from_tcp(listener).await
            {
                log::error!("control server stopped: {err:?}");
            }
        });

        Ok(Self { addr, task })
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn shutdown(self) {
        self.task.abort();
    }
}

async fn register_node(control: &ControlHandle, node_addr: SocketAddr) -> Result<control::Client> {
    let node_name = Uuid::new_v4();
    let node_cert = gen_node_cert(&node_name.hyphenated().to_string())?;
    let csr_pem = node_cert
        .serialize_request_pem()
        .context("failed to serialize node CSR")?;

    let registration =
        control::Client::register(Url::parse(&control.base_url())?, node_name, csr_pem).await?;

    let mut attributes = HashMap::new();
    attributes.insert("bench".to_string(), "true".to_string());

    control::Client::new(registration, node_addr, attributes).await
}
