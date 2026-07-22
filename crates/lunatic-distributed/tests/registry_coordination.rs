use anyhow::{anyhow, Context, Result};
use bytes::Bytes;
use lunatic_control::{
    api::{ControlUrls, Registration},
    NodeInfo,
};
use lunatic_distributed::{
    control::{
        self,
        cert::{CertificateAuthority, CertificateRequest},
    },
    distributed::{
        message::{self, Request},
        Client, GlobalProcessId, RegistryCoordinationMessage,
    },
    quic, CertAttrs, SUBJECT_DIR_ATTRS,
};
use quinn::{Endpoint, VarInt};
use rcgen::{CertificateParams, CustomExtension, DnType};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, Mutex};
use tokio::task::JoinHandle;

// The QUIC coordination harness uses the same logical node identities. Keep
// clusters from separate tests from overlapping and routing messages into a
// concurrently running cluster with the same node IDs.
static TEST_CLUSTER_LOCK: Mutex<()> = Mutex::const_new(());

struct TestNode {
    address: SocketAddr,
    cert: String,
    key: String,
    endpoint: Option<Endpoint>,
    server_task: Option<JoinHandle<()>>,
    client: Client,
}

struct TestCluster {
    root_cert: String,
    nodes: Vec<TestNode>,
}

impl TestCluster {
    async fn new(node_count: usize) -> Result<Self> {
        let root = control::cert::test_root_cert()?;
        let root_cert = root.certificate_pem().to_owned();
        let mut materials = Vec::with_capacity(node_count);

        for id in 1..=node_count as u64 {
            let name = format!("node-{id}.lunatic.test");
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
        let mut nodes = Vec::with_capacity(node_count);

        for (id, _name, address, cert, key, endpoint) in materials {
            let control_client = control::Client::from_static_nodes(
                test_registration(id, &root_cert, &cert),
                id,
                node_infos.clone(),
            );
            let quic_client = quic::new_quic_client(&root_cert, &cert, &key)?;
            let client = Client::new(id, control_client, quic_client);
            let server_task = spawn_registry_server(endpoint.clone(), client.clone());
            nodes.push(TestNode {
                address,
                cert,
                key,
                endpoint: Some(endpoint),
                server_task: Some(server_task),
                client,
            });
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(Self { root_cert, nodes })
    }

    fn client(&self, index: usize) -> Client {
        self.nodes[index].client.clone()
    }

    async fn pause(&mut self, index: usize) {
        if let Some(endpoint) = self.nodes[index].endpoint.take() {
            endpoint.close(VarInt::from_u32(0), b"test partition");
            let _ = tokio::time::timeout(Duration::from_secs(1), endpoint.wait_idle()).await;
        }
        if let Some(task) = self.nodes[index].server_task.take() {
            task.abort();
            let _ = task.await;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    async fn resume(&mut self, index: usize) -> Result<()> {
        let node = &mut self.nodes[index];
        let endpoint = quic::new_quic_server(
            node.address,
            vec![node.cert.clone()],
            &node.key,
            &self.root_cert,
        )?;
        node.server_task = Some(spawn_registry_server(endpoint.clone(), node.client.clone()));
        node.endpoint = Some(endpoint);
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    async fn stall_registry_server(&mut self, index: usize) {
        if let Some(task) = self.nodes[index].server_task.take() {
            task.abort();
            let _ = task.await;
        }
        assert!(
            self.nodes[index].endpoint.is_some(),
            "stalled server must keep its QUIC endpoint alive"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    async fn wait_for_value(
        &self,
        name: &str,
        expected: GlobalProcessId,
        timeout: Duration,
    ) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let converged = self.nodes.iter().all(|node| {
                node.client
                    .registry()
                    .lookup_global(name)
                    .map(|entry| entry.global_pid == expected)
                    .unwrap_or(false)
            });
            if converged {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(anyhow!(
                    "Registry value '{name}' did not converge to {expected}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

fn spawn_registry_server(mut endpoint: Endpoint, client: Client) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(error) = quic::handle_registry_server(&mut endpoint, client).await {
            eprintln!("registry test server stopped: {error:#}");
        }
    })
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

async fn send_registry_message_with_claim(
    cluster: &TestCluster,
    certificate_node_index: usize,
    target_node_index: usize,
    claimed_node_id: u64,
    message: RegistryCoordinationMessage,
) -> Result<()> {
    send_registry_requests_as(
        cluster,
        certificate_node_index,
        target_node_index,
        vec![Request::Registry {
            node_id: claimed_node_id,
            message,
        }],
    )
    .await
}

async fn send_registry_requests_as(
    cluster: &TestCluster,
    certificate_node_index: usize,
    target_node_index: usize,
    requests: Vec<Request>,
) -> Result<()> {
    let certificate_node = &cluster.nodes[certificate_node_index];
    let target_node = &cluster.nodes[target_node_index];
    let attacker = quic::new_quic_client(
        &cluster.root_cert,
        &certificate_node.cert,
        &certificate_node.key,
    )?;
    let target_name = format!("node-{}.lunatic.test", target_node_index + 1);
    let connection = attacker._connect(target_node.address, &target_name).await?;
    let mut stream = connection.open_uni().await?;
    for (offset, request) in requests.into_iter().enumerate() {
        let data = Bytes::from(message::serialize_message(&request)?);
        quic::write_message(&mut stream, 7_001 + offset as u64, data).await?;
    }
    stream.finish()?;
    let _ = stream.stopped().await?;
    Ok(())
}

async fn capture_registry_sync_request(
    endpoint: Endpoint,
    expected_requesting_node_id: u64,
) -> Result<u64> {
    loop {
        let incoming = endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow!("leader endpoint closed before synchronization request"))?;
        let connection = incoming.await?;
        let recv = connection.accept_uni().await?;
        let (captured_tx, mut captured_rx) = tokio::sync::mpsc::unbounded_channel();
        quic::handle_request_stream(recv, move |_message_id, request| {
            let _ = captured_tx.send(request);
            async {}
        })
        .await;
        let Some(request) = captured_rx.recv().await else {
            continue;
        };
        if let Request::Registry {
            message:
                RegistryCoordinationMessage::RegistrySyncRequest {
                    request_id,
                    requesting_node_id,
                },
            ..
        } = request
        {
            if requesting_node_id == expected_requesting_node_id {
                return Ok(request_id);
            }
        }
    }
}

async fn capture_registry_register_decision(
    endpoint: Endpoint,
    expected_request_id: u64,
) -> Result<()> {
    loop {
        let incoming = endpoint
            .accept()
            .await
            .ok_or_else(|| anyhow!("node endpoint closed before registry decision"))?;
        let connection = incoming.await?;
        let recv = connection.accept_uni().await?;
        let (captured_tx, mut captured_rx) = tokio::sync::mpsc::unbounded_channel();
        quic::handle_request_stream(recv, move |_message_id, request| {
            let _ = captured_tx.send(request);
            async {}
        })
        .await;
        while let Some(request) = captured_rx.recv().await {
            if matches!(
                request,
                Request::Registry {
                    message: RegistryCoordinationMessage::GlobalRegisterDecision {
                        request_id,
                        ..
                    },
                    ..
                } if request_id == expected_request_id
            ) {
                return Ok(());
            }
        }
    }
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

fn test_registration(node_id: u64, root_cert: &str, cert: &str) -> Registration {
    let unused_url = "http://127.0.0.1:1/".to_string();
    Registration {
        node_name: uuid::Uuid::from_u128(node_id as u128),
        cert_pem_chain: vec![cert.to_string()],
        authentication_token: "test".to_string(),
        root_cert: root_cert.to_string(),
        urls: ControlUrls {
            api_base: unused_url.clone(),
            nodes: unused_url.clone(),
            node_started: unused_url.clone(),
            node_stopped: unused_url.clone(),
            get_module: unused_url.clone(),
            add_module: unused_url.clone(),
            get_nodes: unused_url,
        },
        envs: vec![],
        is_privileged: true,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn same_name_concurrent_registration_has_one_winner_in_2_3_5_node_clusters() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    for node_count in [2usize, 3, 5] {
        let cluster = TestCluster::new(node_count).await?;
        let barrier = Arc::new(Barrier::new(node_count));
        let mut registrations = Vec::new();

        for index in 0..node_count {
            let client = cluster.client(index);
            let barrier = barrier.clone();
            registrations.push(tokio::spawn(async move {
                let gpid = GlobalProcessId::new((index + 1) as u64, 1, 100 + index as u64);
                barrier.wait().await;
                (gpid, client.register_global("shared-service", gpid).await)
            }));
        }

        let mut winner = None;
        let mut successes = 0;
        for registration in registrations {
            let (gpid, result) = registration.await?;
            if result.is_ok() {
                successes += 1;
                winner = Some(gpid);
            }
        }

        assert_eq!(
            successes, 1,
            "{node_count}-node cluster returned more than one successful owner"
        );
        cluster
            .wait_for_value(
                "shared-service",
                winner.expect("one registration must win"),
                Duration::from_secs(5),
            )
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn request_ids_from_different_nodes_do_not_collide_at_the_coordinator() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let cluster = TestCluster::new(3).await?;
    let barrier = Arc::new(Barrier::new(3));
    let mut registrations = Vec::new();

    for index in 0..3 {
        let client = cluster.client(index);
        let barrier = barrier.clone();
        registrations.push(tokio::spawn(async move {
            let name = format!("independent-service-{index}");
            let gpid = GlobalProcessId::new((index + 1) as u64, 1, 300 + index as u64);
            barrier.wait().await;
            client.register_global(name, gpid).await
        }));
    }

    for registration in registrations {
        registration.await??;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn registration_does_not_succeed_before_a_majority_responds() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let mut cluster = TestCluster::new(3).await?;
    cluster.pause(1).await;
    cluster.pause(2).await;

    let client = cluster.client(0);
    let gpid = GlobalProcessId::new(1, 1, 500);
    let mut registration =
        tokio::spawn(async move { client.register_global("delayed-quorum", gpid).await });

    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut registration)
            .await
            .is_err(),
        "registration completed while only one of three nodes was reachable"
    );

    cluster.resume(1).await?;
    tokio::time::timeout(Duration::from_secs(4), registration).await???;
    cluster.resume(2).await?;
    cluster
        .wait_for_value("delayed-quorum", gpid, Duration::from_secs(5))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn recovered_node_resynchronizes_a_commit_missed_during_partition() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let mut cluster = TestCluster::new(3).await?;
    cluster.pause(2).await;

    let gpid = GlobalProcessId::new(1, 1, 900);
    cluster
        .client(0)
        .register_global("partitioned-service", gpid)
        .await?;
    assert!(
        cluster.nodes[2]
            .client
            .registry()
            .lookup_global("partitioned-service")
            .is_none(),
        "partitioned node unexpectedly observed the commit"
    );

    cluster.resume(2).await?;
    cluster
        .wait_for_value("partitioned-service", gpid, Duration::from_secs(6))
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn certificate_identity_cannot_spoof_registry_leader_claim() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let cluster = TestCluster::new(3).await?;
    let forged_name = "forged-leader-commit";
    let forged_owner = GlobalProcessId::new(1, 1, 9_001);
    let barrier_name = "authenticated-node-two-barrier";
    let barrier_owner = GlobalProcessId::new(2, 1, 9_011);

    // Node 2 owns the mTLS certificate on this connection, but the redundant
    // wire field claims node 1, the elected coordinator. Without the transport
    // identity comparison this leader-only notification would mutate leader node 1.
    send_registry_requests_as(
        &cluster,
        1,
        0,
        vec![
            Request::Registry {
                node_id: 1,
                message: RegistryCoordinationMessage::GlobalRegisterNotify {
                    name: forged_name.to_string(),
                    global_pid: forged_owner,
                    registered_at: 1,
                },
            },
            Request::Registry {
                node_id: 2,
                message: RegistryCoordinationMessage::GlobalRegisterRequest {
                    request_id: 55_001,
                    requesting_node_id: 2,
                    name: barrier_name.to_string(),
                    global_pid: barrier_owner,
                },
            },
        ],
    )
    .await?;

    cluster
        .wait_for_value(barrier_name, barrier_owner, Duration::from_secs(5))
        .await?;
    assert!(
        cluster.nodes[0]
            .client
            .registry()
            .lookup_global(forged_name)
            .is_none(),
        "registry state changed after a certificate/payload node-ID mismatch"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn certificate_identity_cannot_inject_a_leader_snapshot() -> Result<()> {
    let _test_guard = TEST_CLUSTER_LOCK.lock().await;
    let mut cluster = TestCluster::new(3).await?;
    cluster.stall_registry_server(0).await;
    cluster.stall_registry_server(1).await;

    // Capture node 3's actual request ID from the production-framed stream so background
    // synchronization timing cannot make this regression pass without reaching the pending call.
    let leader_endpoint = cluster.nodes[0]
        .endpoint
        .as_ref()
        .expect("stalled leader endpoint")
        .clone();
    let capture = tokio::spawn(capture_registry_sync_request(leader_endpoint, 3));
    let node_three = cluster.client(2);
    let sync = tokio::spawn(async move { node_three.synchronize_registry().await });
    let request_id = tokio::time::timeout(Duration::from_secs(2), capture)
        .await
        .context("node-3 synchronization request was not captured")???;
    assert!(
        !sync.is_finished(),
        "synchronization must still have its captured request pending"
    );

    let forged_name = "forged-registry-snapshot";
    let forged_owner = GlobalProcessId::new(1, 1, 9_002);
    let barrier_request_id = 55_002;
    let barrier_endpoint = cluster.nodes[1]
        .endpoint
        .as_ref()
        .expect("stalled node-2 endpoint")
        .clone();
    let barrier = tokio::spawn(capture_registry_register_decision(
        barrier_endpoint,
        barrier_request_id,
    ));
    send_registry_requests_as(
        &cluster,
        1,
        2,
        vec![
            Request::Registry {
                node_id: 2,
                message: RegistryCoordinationMessage::RegistrySyncResponse {
                    request_id,
                    global_entries: vec![(forged_name.to_string(), forged_owner, 1)],
                },
            },
            Request::Registry {
                node_id: 2,
                message: RegistryCoordinationMessage::GlobalRegisterRequest {
                    request_id: barrier_request_id,
                    requesting_node_id: 2,
                    name: "snapshot-processing-barrier".to_string(),
                    global_pid: GlobalProcessId::new(2, 1, 9_003),
                },
            },
        ],
    )
    .await?;

    tokio::time::timeout(Duration::from_secs(2), barrier)
        .await
        .context("node 3 did not finish processing the rejected snapshot")???;
    assert!(
        !sync.is_finished(),
        "a non-coordinator snapshot must not consume the pending synchronization"
    );
    assert!(
        cluster.nodes[2]
            .client
            .registry()
            .lookup_global(forged_name)
            .is_none(),
        "registry snapshot from a non-coordinator certificate was applied"
    );

    let legitimate_name = "legitimate-registry-snapshot";
    let legitimate_owner = GlobalProcessId::new(1, 1, 9_004);
    send_registry_message_with_claim(
        &cluster,
        0,
        2,
        1,
        RegistryCoordinationMessage::RegistrySyncResponse {
            request_id,
            global_entries: vec![(legitimate_name.to_string(), legitimate_owner, 2)],
        },
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(2), sync)
        .await
        .context("legitimate leader snapshot did not complete synchronization")???;
    assert_eq!(
        cluster.nodes[2]
            .client
            .registry()
            .lookup_global(legitimate_name)
            .map(|entry| entry.global_pid),
        Some(legitimate_owner)
    );
    Ok(())
}
