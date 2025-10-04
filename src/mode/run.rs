use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use clap::Parser;
use lunatic_process::{
    env::{Environments, LunaticEnvironments},
    runtimes::{self},
};

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
        })
        .await
    }
}

async fn run_with_watch(
    args: Args,
    runtime: runtimes::wasmtime::WasmtimeRuntime,
    envs: Arc<LunaticEnvironments>,
    _env: Arc<impl lunatic_process::env::Environment + 'static>,
) -> Result<()> {
    use crate::hot_reload::{FileChangeEvent, FileWatcher};
    use log::{error, info};
    use tokio::sync::mpsc;

    info!("Starting lunatic in watch mode...");
    info!("Watching file: {:?}", args.path);

    let (tx, mut rx) = mpsc::unbounded_channel::<FileChangeEvent>();
    let mut watcher = FileWatcher::new(&args.path, tx)?;
    watcher.start()?;

    let path = args.path.clone();
    let wasm_args = args.wasm_args.clone();
    let dir = args.dir.clone();

    struct ProcessInfo {
        handle: tokio::task::JoinHandle<Result<()>>,
        env_id: u64,
    }

    let mut process_info: Option<ProcessInfo> = None;
    let mut next_env_id = 1u64;
    let mut last_reload_time = tokio::time::Instant::now();
    let reload_debounce = tokio::time::Duration::from_millis(500);

    async fn start_process(
        envs: &Arc<LunaticEnvironments>,
        runtime: &runtimes::wasmtime::WasmtimeRuntime,
        path: &PathBuf,
        wasm_args: &[String],
        dir: &[PathBuf],
        env_id: u64,
    ) -> Result<ProcessInfo> {
        let new_env = envs.create(env_id).await?;
        let runtime_clone = runtime.clone();
        let path_clone = path.clone();
        let wasm_args_clone = wasm_args.to_vec();
        let dir_clone = dir.to_vec();
        let envs_clone = envs.clone();

        let handle = tokio::spawn(async move {
            run_wasm(RunWasm {
                path: path_clone,
                wasm_args: wasm_args_clone,
                dir: dir_clone,
                runtime: runtime_clone,
                envs: envs_clone,
                env: new_env,
                distributed: None,
            })
            .await
        });
        Ok(ProcessInfo { handle, env_id })
    }

    process_info = Some(start_process(&envs, &runtime, &path, &wasm_args, &dir, next_env_id).await?);
    next_env_id += 1;
    info!("Initial process started");

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
                info!("Restarting process...");

                if let Some(info) = process_info.take() {
                    if let Some(env) = envs.get(info.env_id).await {
                        env.kill_all_processes();
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                    info.handle.abort();
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                
                println!("✅ Reloaded successfully\n");

                match start_process(&envs, &runtime, &path, &wasm_args, &dir, next_env_id).await {
                    Ok(info) => {
                        next_env_id += 1;
                        process_info = Some(info);
                        info!("Process restarted successfully");
                    }
                    Err(e) => {
                        error!("Failed to restart process: {}", e);
                    }
                }
            }
            result = async {
                match &mut process_info {
                    Some(info) => (&mut info.handle).await,
                    None => std::future::pending().await
                }
            } => {
                process_info = None;
                match result {
                    Ok(Ok(_)) => {
                        info!("Process finished successfully");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Process error: {}", e);
                        info!("Waiting for file changes...");
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
