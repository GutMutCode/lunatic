use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Result;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lunatic_distributed::{
    control::cert,
    distributed::{message::Request, server::gen_node_cert},
    quic::{new_quic_client, new_quic_server, Client as QuicClient},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
    sync::oneshot,
};

struct QuicRoundTripHarness {
    client: QuicClient,
    connection: quinn::Connection,
    shutdown: Option<oneshot::Sender<()>>,
    server_task: tokio::task::JoinHandle<()>,
}

impl QuicRoundTripHarness {
    async fn new() -> Result<Self> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.serialize_pem()?;
        let (server_cert, server_key) = cert::default_server_certificates(&root)?;
        let node_cert = gen_node_cert("bench-node")?;
        let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
        let node_key_pem = node_cert.serialize_private_key_pem();

        let server_addr: SocketAddr = "127.0.0.1:0".parse()?;
        let server_endpoint = new_quic_server(
            server_addr,
            vec![server_cert.clone(), ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server_endpoint.local_addr()?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let server_task = tokio::spawn(async move {
            let server_endpoint = server_endpoint;
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => {
                        server_endpoint.close(0u32.into(), b"quic bench shutdown");
                        break;
                    }
                    maybe_conn = server_endpoint.accept() => {
                        match maybe_conn {
                            Some(connecting) => {
                                tokio::spawn(async move {
                                    if let Ok(connection) = connecting.await {
                                        loop {
                                            match connection.accept_bi().await {
                                                Ok((mut send, mut recv)) => {
                                                    if let Err(err) = handle_echo_stream(&mut send, &mut recv).await {
                                                        log::debug!("quic bench stream handler error: {err:?}");
                                                        break;
                                                    }
                                                }
                                                Err(_) => break,
                                            }
                                        }
                                    }
                                });
                            }
                            None => break,
                        }
                    }
                }
            }
        });

        let client = new_quic_client(&ca_pem, &node_cert_pem, &node_key_pem)?;
        let connection = client.try_connect(listen_addr, "bench-node", 1).await?;

        Ok(Self {
            client,
            connection,
            shutdown: Some(shutdown_tx),
            server_task,
        })
    }

    async fn round_trip(&self, payload: &[u8]) -> Result<()> {
        let (mut send, mut recv) = self.connection.open_bi().await?;
        send.write_u32_le(payload.len() as u32).await?;
        send.write_all(payload).await?;
        send.finish().await?;

        let resp_len = recv.read_u32_le().await? as usize;
        let mut buf = vec![0u8; resp_len];
        recv.read_exact(&mut buf).await?;
        Ok(())
    }

    async fn shutdown(mut self) {
        self.connection.close(0u32.into(), b"bench complete");
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.server_task.await;
        drop(self.client);
    }
}

async fn handle_echo_stream(
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
) -> Result<()> {
    let size = recv.read_u32_le().await? as usize;
    let mut buf = vec![0u8; size];
    recv.read_exact(&mut buf).await?;
    send.write_u32_le(size as u32).await?;
    send.write_all(&buf).await?;
    send.finish().await?;
    Ok(())
}

fn bench_request_encode_decode(c: &mut Criterion) {
    // Measure serialization cost for a representative distributed message payload
    c.bench_function("distributed_request_encode_1kb", |b| {
        b.iter_batched(
            || vec![0u8; 1024],
            |payload| {
                let request = Request::Message {
                    node_id: 1,
                    environment_id: 42,
                    process_id: 99,
                    tag: Some(7),
                    data: payload,
                };
                let encoded = rmp_serde::to_vec(&request).expect("encode");
                black_box(encoded);
            },
            BatchSize::SmallInput,
        );
    });

    let template = Request::Message {
        node_id: 1,
        environment_id: 42,
        process_id: 99,
        tag: Some(7),
        data: vec![0u8; 1024],
    };
    let encoded = rmp_serde::to_vec(&template).expect("encode template");

    c.bench_function("distributed_request_decode_1kb", |b| {
        b.iter(|| {
            let decoded: Request = rmp_serde::from_slice(black_box(&encoded)).expect("decode");
            black_box(decoded);
        });
    });
}

fn bench_quic_round_trip(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("distributed_quic_round_trip", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let harness = QuicRoundTripHarness::new().await.expect("quic harness");
            let payload = vec![0u8; 512];
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let start = Instant::now();
                harness
                    .round_trip(&payload)
                    .await
                    .expect("round trip succeeds");
                total += start.elapsed();
            }

            harness.shutdown().await;
            total
        });
    });
}

criterion_group!(benches, bench_request_encode_decode, bench_quic_round_trip);
criterion_main!(benches);
