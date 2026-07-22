use std::{path::PathBuf, sync::Arc};

use anyhow::{anyhow, Context, Result};
use clap::Args;

use lunatic_distributed::DistributedProcessState;
use lunatic_process::{
    env::{Environment, LunaticEnvironment, LunaticEnvironments},
    runtimes::{
        wasmtime::{
            CompiledModuleLimits, WasmtimeCompiledModule, WasmtimeRuntime,
            DEFAULT_MAX_COMPILED_MODULES, DEFAULT_MAX_COMPILED_MODULE_BYTES,
            DEFAULT_MAX_MODULE_BYTES,
        },
        RawWasm,
    },
    wasm::{spawn_wasm_with_options, WasmSpawnOptions},
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_runtime::{DefaultProcessConfig, DefaultProcessState};

#[derive(Args, Clone, Debug)]
pub struct CompiledModuleArgs {
    /// Maximum compiled modules retained by this runtime.
    #[arg(long, default_value_t = DEFAULT_MAX_COMPILED_MODULES)]
    pub max_compiled_modules: usize,

    /// Maximum aggregate source bytes retained by compiled modules.
    #[arg(long, default_value_t = DEFAULT_MAX_COMPILED_MODULE_BYTES)]
    pub max_compiled_module_bytes: usize,

    /// Maximum source bytes accepted for one compiled module.
    #[arg(long, default_value_t = DEFAULT_MAX_MODULE_BYTES)]
    pub max_single_module_bytes: usize,
}

impl CompiledModuleArgs {
    pub fn limits(&self) -> CompiledModuleLimits {
        CompiledModuleLimits {
            modules: self.max_compiled_modules,
            source_bytes: self.max_compiled_module_bytes,
            single_module_bytes: self.max_single_module_bytes,
        }
    }
}

pub struct RunWasm {
    pub path: PathBuf,
    pub wasm_args: Vec<String>,
    pub dir: Vec<PathBuf>,

    pub runtime: WasmtimeRuntime,
    #[allow(dead_code)]
    pub envs: Arc<LunaticEnvironments>,
    pub env: Arc<LunaticEnvironment>,
    pub distributed: Option<DistributedProcessState>,
    pub initial_module_version: Option<(u64, u32)>,
    /// An exact registry-backed module to launch. Watch-mode restarts use this
    /// instead of recompiling whatever bytes happen to be on disk.
    pub compiled_module: Option<Arc<WasmtimeCompiledModule<DefaultProcessState>>>,
    /// Watch mode waits for this acknowledgement before accepting file events,
    /// closing the gap between task spawn and registry membership.
    pub spawn_ready: Option<tokio::sync::oneshot::Sender<()>>,
}

pub async fn run_wasm(args: RunWasm) -> Result<()> {
    let mut config = DefaultProcessConfig::default();
    // Allow initial process to compile modules, create configurations and spawn sub-processes
    config.set_can_compile_modules(true);
    config.set_can_create_configs(true);
    config.set_can_spawn_processes(true);

    // Path to wasm file
    let path = args.path;

    // Set correct command line arguments for the guest
    let filename = path.file_name().unwrap().to_string_lossy().to_string();
    let mut wasi_args = vec![filename];
    wasi_args.extend(args.wasm_args);
    config.set_command_line_arguments(wasi_args);

    // Inherit environment variables
    config.set_environment_variables(std::env::vars().collect());

    // Always preopen the current dir
    config.preopen_dir(".");
    for dir in args.dir {
        if let Some(s) = dir.as_os_str().to_str() {
            config.preopen_dir(s);
        }
    }

    // Spawn the main process. A watch-mode restart must use the exact module
    // that the registry marked committed, even if the watched file has since
    // changed to a candidate that failed to reload.
    let module = if let Some(module) = args.compiled_module {
        module
    } else {
        let module = std::fs::read(&path).map_err(|err| match err.kind() {
            std::io::ErrorKind::NotFound => anyhow!("Module '{}' not found", path.display()),
            _ => err.into(),
        })?;
        let module: RawWasm = if let Some(dist) = args.distributed.as_ref() {
            dist.control.add_module(module).await?
        } else {
            module.into()
        };
        Arc::new(args.runtime.compile_module::<DefaultProcessState>(module)?)
    };
    let state = DefaultProcessState::new(
        args.env.clone(),
        args.distributed,
        args.runtime.clone(),
        module.clone(),
        Arc::new(config),
        Default::default(),
    )
    .unwrap();

    args.env.can_spawn_next_process().await?;
    let (task, _) = spawn_wasm_with_options(
        args.env,
        args.runtime,
        &module,
        state,
        "_start",
        Vec::new(),
        WasmSpawnOptions {
            link: None,
            initial_module_version: args.initial_module_version,
        },
    )
    .await
    .context(format!(
        "Failed to spawn process from {}::_start()",
        path.to_string_lossy()
    ))?;
    if let Some(spawn_ready) = args.spawn_ready {
        let _ = spawn_ready.send(());
    }

    // Wait on the main process to finish
    task.await.map_err(|error| anyhow!(error.to_string()))??;
    Ok(())
}

#[cfg(feature = "prometheus")]
#[derive(Args, Debug)]
pub struct PrometheusArgs {
    /// Enables the prometheus metrics exporter
    #[arg(long)]
    pub prometheus: bool,

    /// Address to bind the prometheus http listener to
    #[arg(long, value_name = "PROMETHEUS_HTTP_ADDRESS", requires = "prometheus")]
    pub prometheus_http: Option<std::net::SocketAddr>,
}

#[cfg(feature = "prometheus")]
pub fn prometheus(http_socket: Option<std::net::SocketAddr>, node_id: Option<u64>) -> Result<()> {
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(http_socket.unwrap_or_else(|| "0.0.0.0:9927".parse().unwrap()))
        .add_global_label("node_id", node_id.unwrap_or(0).to_string())
        .install()?;
    Ok(())
}
