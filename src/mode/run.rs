use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use clap::Parser;
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironment, LunaticEnvironments},
    module_registry::ModuleRegistry,
    runtimes::{self},
};
use lunatic_runtime::DefaultProcessState;

use super::common::{run_wasm, RunWasm};

#[derive(Parser, Debug)]
#[command(version)]
pub struct Args {
    /// Grant access to the given host directories
    #[arg(long, value_name = "DIRECTORY")]
    pub dir: Vec<PathBuf>,

    /// Indicate that a benchmark is running
    #[arg(long)]
    pub bench: bool,

    /// Entry .wasm file
    #[arg(index = 1)]
    pub path: PathBuf,

    /// Arguments passed to the guest
    #[arg(index = 2)]
    pub wasm_args: Vec<String>,

    /// Watch for file changes and automatically reload
    #[arg(long)]
    pub watch: bool,

    #[cfg(feature = "prometheus")]
    #[command(flatten)]
    prometheus: super::common::PrometheusArgs,
}

pub(crate) async fn start(mut args: Args) -> Result<()> {
    #[cfg(feature = "prometheus")]
    if args.prometheus.prometheus {
        super::common::prometheus(args.prometheus.prometheus_http, None)?;
    }

    // Create wasmtime runtime
    let wasmtime_config = runtimes::wasmtime::default_config();
    let runtime = runtimes::wasmtime::WasmtimeRuntime::new(&wasmtime_config)?;
    let envs = Arc::new(LunaticEnvironments::default());

    let env = envs.create(1).await?;
    if args.bench {
        args.wasm_args.push("--bench".to_owned());
    }

    if args.watch {
        run_with_watch(args, runtime, envs, env).await
    } else {
        run_wasm(RunWasm {
            path: args.path,
            wasm_args: args.wasm_args,
            dir: args.dir,
            runtime,
            envs,
            env,
            distributed: None,
            initial_module_version: None,
        })
        .await
    }
}

async fn run_with_watch(
    args: Args,
    runtime: runtimes::wasmtime::WasmtimeRuntime,
    envs: Arc<LunaticEnvironments>,
    env: Arc<impl lunatic_process::env::Environment + 'static>,
) -> Result<()> {
    use crate::hot_reload::{register_module_update, FileChangeEvent, FileWatcher};
    use log::{error, info};
    use tokio::sync::mpsc;

    info!("Starting lunatic in watch mode with hot reload support...");
    info!("Watching file: {:?}", args.path);

    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::new());

    let initial_bytes = std::fs::read(&args.path)?;
    let initial_module = runtime.compile_module(initial_bytes.into())?;
    let module_id = 0u64;
    let initial_version = module_registry.add_version(module_id, initial_module);

    info!("Module registry initialized with version 0");

    drop(env);
    let env = envs
        .create_with_registry(
            1,
            module_registry.clone() as Arc<dyn std::any::Any + Send + Sync>,
        )
        .await?;

    let (tx, mut rx) = mpsc::unbounded_channel::<FileChangeEvent>();
    let mut watcher = FileWatcher::new(&args.path, tx)?;
    watcher.start()?;

    let path = args.path.clone();
    let wasm_args = args.wasm_args.clone();
    let dir = args.dir.clone();

    struct ProcessInfo {
        handle: tokio::task::JoinHandle<Result<()>>,
        #[allow(dead_code)]
        env_id: u64,
    }

    let mut last_reload_time = tokio::time::Instant::now();
    let reload_debounce = tokio::time::Duration::from_millis(500);

    async fn start_process(
        runtime: &runtimes::wasmtime::WasmtimeRuntime,
        path: &Path,
        wasm_args: &[String],
        dir: &[PathBuf],
        envs: Arc<LunaticEnvironments>,
        env: Arc<LunaticEnvironment>,
        initial_module_version: (u64, u32),
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let runtime_clone = runtime.clone();
        let path_clone = path.to_path_buf();
        let wasm_args_clone = wasm_args.to_vec();
        let dir_clone = dir.to_vec();

        let handle = tokio::spawn(async move {
            run_wasm(RunWasm {
                path: path_clone,
                wasm_args: wasm_args_clone,
                dir: dir_clone,
                runtime: runtime_clone,
                envs,
                env,
                distributed: None,
                initial_module_version: Some(initial_module_version),
            })
            .await
        });
        Ok(handle)
    }

    let mut launch_version = initial_version;
    let handle = start_process(
        &runtime,
        &path,
        &wasm_args,
        &dir,
        envs.clone(),
        env.clone(),
        (module_id, launch_version),
    )
    .await?;
    let mut process_info = Some(ProcessInfo { handle, env_id: 1 });
    info!("Initial process started with hot reload support");

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                let now = tokio::time::Instant::now();
                if now.duration_since(last_reload_time) < reload_debounce {
                    info!("Ignoring rapid file change (debounced)");
                    continue;
                }
                last_reload_time = now;

                info!("File change detected: {:?}", event.path);
                println!("\n🔄 Hot reloading...");

                match std::fs::read(&path) {
                    Ok(new_bytes) => {
                        let env_for_reload: Arc<dyn Environment> = env.clone();
                        match register_module_update::<DefaultProcessState>(
                            &runtime,
                            &module_registry,
                            &env_for_reload,
                            module_id,
                            new_bytes,
                        ) {
                            Ok(new_version) => {
                                launch_version = new_version;
                                info!("Compiled new module version: {}", new_version);
                                println!("✅ Hot reload signal sent (version {})\n", new_version);
                                info!("Hot reload signal queued for version {}", new_version);
                            }
                            Err(e) => {
                                error!("Failed to compile new module: {}", e);
                                println!("❌ Compilation failed: {}\n", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read file: {}", e);
                        println!("❌ Failed to read file: {}\n", e);
                    }
                }
            }
            result = async {
                match &mut process_info {
                    Some(info) => (&mut info.handle).await,
                    None => std::future::pending().await
                }
            } => {
                match result {
                    Ok(Ok(_)) => {
                        info!("Process finished successfully");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Process error: {}", e);
                        info!("Restarting process...");

                        let new_handle = start_process(
                            &runtime,
                            &path,
                            &wasm_args,
                            &dir,
                            envs.clone(),
                            env.clone(),
                            (module_id, launch_version),
                        ).await?;
                        process_info = Some(ProcessInfo { handle: new_handle, env_id: 1 });
                        info!("Process restarted");
                    }
                    Err(e) => {
                        error!("Process panicked: {}", e);
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}
