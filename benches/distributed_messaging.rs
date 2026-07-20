use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::Result;
use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lunatic_distributed::{
    control::cert,
    distributed::{
        message::{self, Request},
        server::gen_node_cert,
    },
    quic::{
        handle_request_stream, new_quic_client, new_quic_server, write_message,
        Client as QuicClient,
    },
};
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
};

struct QuicDispatchHarness {
    client: QuicClient,
    connection: quinn::Connection,
    send: quinn::SendStream,
    dispatched: mpsc::Receiver<(u64, Vec<u8>)>,
    next_message_id: u64,
    shutdown: Option<oneshot::Sender<()>>,
    server_task: tokio::task::JoinHandle<()>,
}

impl QuicDispatchHarness {
    async fn new() -> Result<Self> {
        let root = cert::test_root_cert()?;
        let ca_pem = root.serialize_pem()?;
        let (server_cert, server_key) = cert::default_server_certificates(&root)?;
        let node_cert = gen_node_cert("bench-node")?;
        let node_cert_pem = node_cert.serialize_pem_with_signer(&root)?;
        let node_key_pem = node_cert.serialize_private_key_pem();

        let server_addr: SocketAddr = if cfg!(windows) {
            "[::1]:0".parse()?
        } else {
            "127.0.0.1:0".parse()?
        };
        let server_endpoint = new_quic_server(
            server_addr,
            vec![server_cert.clone(), ca_pem.clone()],
            &server_key,
            &ca_pem,
        )?;
        let listen_addr = server_endpoint.local_addr()?;

        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let (dispatch_tx, dispatch_rx) = mpsc::channel(64);

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
                                let dispatch_tx = dispatch_tx.clone();
                                tokio::spawn(async move {
                                    match connecting.await {
                                        Ok(connection) => {
                                            loop {
                                                match connection.accept_uni().await {
                                                    Ok(recv) => {
                                                        let dispatch_tx = dispatch_tx.clone();
                                                        tokio::spawn(handle_request_stream(
                                                            recv,
                                                            move |message_id, request| {
                                                                let dispatch_tx = dispatch_tx.clone();
                                                                async move {
                                                                    if let Request::Message { data, .. } = request {
                                                                        let _ = dispatch_tx
                                                                            .send((message_id, data))
                                                                            .await;
                                                                    }
                                                                }
                                                            },
                                                        ));
                                                    }
                                                    Err(_) => break,
                                                }
                                            }
                                        }
                                        Err(error) => {
                                            log::debug!("QUIC benchmark handshake failed: {error}");
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
        let connection = client._connect(listen_addr, cert::CTRL_SERVER_NAME).await?;
        let send = connection.open_uni().await?;

        Ok(Self {
            client,
            connection,
            send,
            dispatched: dispatch_rx,
            next_message_id: 1,
            shutdown: Some(shutdown_tx),
            server_task,
        })
    }

    async fn dispatch_message(&mut self, payload: &[u8]) -> Result<()> {
        let message_id = self.next_message_id;
        self.next_message_id += 1;
        let request = Request::Message {
            node_id: 1,
            environment_id: 42,
            process_id: 99,
            tag: Some(7),
            data: payload.to_vec(),
        };
        let encoded = message::serialize_message(&request)?;
        write_message(&mut self.send, message_id, encoded.into()).await?;

        let (dispatched_id, dispatched_payload) = self
            .dispatched
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("QUIC dispatch task stopped"))?;
        anyhow::ensure!(
            dispatched_id == message_id,
            "dispatched message id {dispatched_id} did not match {message_id}"
        );
        anyhow::ensure!(
            dispatched_payload == payload,
            "dispatched payload did not match"
        );
        Ok(())
    }

    async fn shutdown(mut self) {
        let _ = self.send.finish().await;
        self.connection.close(0u32.into(), b"bench complete");
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        let _ = self.server_task.await;
        drop(self.client);
    }
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

fn bench_quic_message_dispatch(c: &mut Criterion) {
    let _ = env_logger::builder().is_test(true).try_init();
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("distributed_quic_message_dispatch_2kb", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut harness = QuicDispatchHarness::new().await.expect("quic harness");
            let payload = vec![0u8; 2048];
            let mut total = Duration::ZERO;

            for _ in 0..iters {
                let start = Instant::now();
                harness
                    .dispatch_message(&payload)
                    .await
                    .expect("message dispatch succeeds");
                total += start.elapsed();
            }

            harness.shutdown().await;
            total
        });
    });
}

criterion_group!(
    benches,
    bench_request_encode_decode,
    bench_quic_message_dispatch
);
criterion_main!(benches);
