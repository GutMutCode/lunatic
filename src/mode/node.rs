use std::{
    collections::HashSet,
    net::{SocketAddr, UdpSocket},
    path::PathBuf,
};

use clap::Parser;

use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Context, Result};
use lunatic_distributed::{
    control::{self},
    distributed::{self, server::ServerCtx},
    quic,
};
use lunatic_process::{
    env::{Environments, LunaticEnvironments},
    runtimes::{self, Modules},
};
use lunatic_runtime::DefaultProcessState;
use uuid::Uuid;

use crate::mode::common::{run_wasm, RunWasm};

#[derive(Parser, Debug)]
pub(crate) struct Args {
    /// Control server register URL
    #[arg(
        index = 1,
        value_name = "CONTROL_URL",
        default_value = "http://127.0.0.1:3030/"
    )]
    control: String,

    #[arg(long, value_name = "NODE_SOCKET")]
    bind_socket: Option<SocketAddr>,

    #[arg(long, value_name = "WASM_MODULE")]
    wasm: Option<PathBuf>,

    /// Define key=value variable to store as node information
    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    tag: Vec<(String, String)>,

    /// Maximum node-wide distributed messages retained for outbound delivery.
    #[arg(long, default_value_t = 1_024)]
    outbound_max_messages: usize,

    /// Maximum node-wide bytes retained for outbound distributed delivery.
    #[arg(long, default_value_t = 32 * 1024 * 1024)]
    outbound_max_bytes: usize,

    /// Maximum UTF-8 bytes in one distributed registry name.
    #[arg(long, default_value_t = 1_024)]
    registry_max_name_bytes: usize,

    /// Maximum aggregate local and global distributed registry entries.
    #[arg(long, default_value_t = 16_384)]
    registry_max_entries: usize,

    /// Maximum aggregate bytes retained by distributed registry names.
    #[arg(long, default_value_t = 4 * 1024 * 1024)]
    registry_max_retained_bytes: usize,

    /// Number of fixed registration lock stripes.
    #[arg(long, default_value_t = 1_024)]
    registry_max_name_locks: usize,

    /// Maximum aggregate in-flight registry requests and responses.
    #[arg(long, default_value_t = 4_096)]
    registry_max_pending_responses: usize,

    /// Maximum live nodes admitted to registry topology and outbound managers.
    #[arg(long, default_value_t = 1_024)]
    registry_max_topology_nodes: usize,

    #[cfg(feature = "prometheus")]
    #[command(flatten)]
    prometheus: super::common::PrometheusArgs,
}

pub(crate) async fn start(args: Args) -> Result<()> {
    #[cfg(feature = "prometheus")]
    if args.prometheus.prometheus {
        super::common::prometheus(args.prometheus.prometheus_http, None)?;
    }

    let socket = args
        .bind_socket
        .or_else(get_available_localhost)
        .ok_or_else(|| anyhow!("No available localhost UDP port"))?;
    let http_client = reqwest::Client::new();

    // TODO unwrap, better message
    let node_name = Uuid::new_v4();
    let node_name_str = node_name.as_hyphenated().to_string();
    let node_attributes: HashMap<String, String> = args.tag.clone().into_iter().collect();
    let node_cert = lunatic_distributed::distributed::server::gen_node_cert(&node_name_str)
        .with_context(|| "Failed to generate node CSR and PK")?;
    log::info!("Generate CSR for node name {node_name_str}");

    let reg = control::Client::register(
        &http_client,
        args.control
            .parse()
            .with_context(|| "Parsing control URL")?,
        node_name,
        node_cert.serialize_request_pem()?,
    )
    .await?;

    let allowed_envs = if reg.is_privileged {
        None
    } else {
        Some(
            reg.envs
                .iter()
                .map(|env_id| *env_id as u64)
                .collect::<HashSet<u64>>(),
        )
    };

    let control_client = control::Client::new_with_topology_limit(
        http_client.clone(),
        reg.clone(),
        socket,
        node_attributes,
        args.registry_max_topology_nodes,
    )
    .await?;

    let node_id = control_client.node_id();

    log::info!("Registration successful, node id {}", node_id);

    let quic_client = quic::new_quic_client(
        &reg.root_cert,
        reg.cert_pem_chain
            .first()
            .ok_or_else(|| anyhow!("No certificate available for QUIC client"))?,
        &node_cert.serialize_private_key_pem(),
    )
    .with_context(|| "Failed to create mTLS QUIC client")?;

    let distributed_client = distributed::Client::new_with_limits(
        node_id,
        control_client.clone(),
        quic_client.clone(),
        distributed::DistributedLimits {
            outbound: distributed::OutboundLimits {
                max_messages: args.outbound_max_messages,
                max_bytes: args.outbound_max_bytes,
            },
            registry: distributed::RegistryLimits {
                max_name_bytes: args.registry_max_name_bytes,
                max_entries: args.registry_max_entries,
                max_retained_bytes: args.registry_max_retained_bytes,
                max_name_locks: args.registry_max_name_locks,
                max_pending_responses: args.registry_max_pending_responses,
                max_topology_nodes: args.registry_max_topology_nodes,
            },
        },
    );

    let dist = lunatic_distributed::DistributedProcessState::new(
        node_id,
        control_client.clone(),
        distributed_client.clone(),
    )
    .await?;

    let wasmtime_config = runtimes::wasmtime::default_config();
    let runtime = runtimes::wasmtime::WasmtimeRuntime::new(&wasmtime_config)?;
    let envs = Arc::new(LunaticEnvironments::default());

    let mut node = tokio::task::spawn(lunatic_distributed::distributed::server::node_server(
        ServerCtx {
            envs: envs.clone(),
            modules: Modules::<DefaultProcessState>::default(),
            distributed: dist.clone(),
            runtime: runtime.clone(),
            node_client: distributed_client.clone(),
            allowed_envs,
        },
        socket,
        reg.root_cert,
        reg.cert_pem_chain,
        node_cert.serialize_private_key_pem(),
    ));

    let wasm = if let Some(path) = args.wasm {
        let env = envs.create(1).await?;
        Some(tokio::task::spawn(async move {
            if let Err(e) = run_wasm(RunWasm {
                path,
                wasm_args: vec![],
                dir: vec![],
                runtime,
                envs,
                env,
                distributed: Some(dist),
                initial_module_version: None,
                compiled_module: None,
                spawn_ready: None,
            })
            .await
            {
                log::error!("Error running wasm: {e:?}");
            }
        }))
    } else {
        None
    };

    tokio::select! {
        _ = &mut node => {}
        _ = async_ctrlc::CtrlC::new().unwrap() => {
            log::info!("Shutting down node");
            node.abort();
            let _ = node.await;
        }
    }

    if let Some(wasm) = wasm {
        wasm.abort();
        let _ = wasm.await;
    }

    control_client.notify_node_stopped().await.ok();

    Ok(())
}

fn get_available_localhost() -> Option<SocketAddr> {
    for port in 1025..65535u16 {
        let addr = SocketAddr::new("127.0.0.1".parse().unwrap(), port);
        if UdpSocket::bind(addr).is_ok() {
            return Some(addr);
        }
    }

    None
}

fn parse_key_val(s: &str) -> Result<(String, String)> {
    if let Some((key, value)) = s.split_once('=') {
        Ok((key.to_string(), value.to_string()))
    } else {
        Err(anyhow!(format!("Tag '{s}' is not formatted as key=value")))
    }
}
