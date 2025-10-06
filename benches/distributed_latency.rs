use std::{
    collections::HashMap,
    net::{SocketAddr, TcpListener},
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use criterion::{criterion_group, criterion_main, Criterion};
use lunatic_distributed::{control, distributed::server::gen_node_cert};
use reqwest::Client as HttpClient;
use tokio::runtime::Runtime;
use url::Url;
use uuid::Uuid;

fn control_lookup_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().expect("tokio runtime");

    c.bench_function("control_lookup_nodes", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let control = ControlHandle::new().await.expect("control server to start");

            let mut nodes = Vec::new();
            for index in 0..2 {
                let node_addr: SocketAddr = "127.0.0.1:0".parse().expect("socket addr");
                let node = register_node(&control, node_addr, format!("bench-node-{index}")).await;
                nodes.push(node.expect("node registration"));
            }

            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                nodes[0].lookup_nodes("").await.expect("lookup to succeed");
                total += start.elapsed();
            }

            for node in &nodes {
                node.notify_node_stopped().await.ok();
            }
            control.shutdown().await;

            total
        });
    });
}

criterion_group!(benches, control_lookup_benchmark);
criterion_main!(benches);

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

async fn register_node(
    control: &ControlHandle,
    node_addr: SocketAddr,
    node_name: String,
) -> Result<control::Client> {
    let http_client = HttpClient::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .context("failed to build control HTTP client")?;

    let node_cert = gen_node_cert(&node_name)?;
    let csr_pem = node_cert
        .serialize_request_pem()
        .context("failed to serialize node CSR")?;

    let registration = control::Client::register(
        &http_client,
        Url::parse(&control.base_url())?,
        Uuid::new_v4(),
        csr_pem,
    )
    .await?;

    let mut attributes = HashMap::new();
    attributes.insert("bench".to_string(), "true".to_string());

    control::Client::new(http_client, registration, node_addr, attributes).await
}
