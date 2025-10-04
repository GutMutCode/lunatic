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

    let mut process_handle = None;

    async fn start_process(
        envs: &Arc<LunaticEnvironments>,
        runtime: &runtimes::wasmtime::WasmtimeRuntime,
        path: &PathBuf,
        wasm_args: &[String],
        dir: &[PathBuf],
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let new_env = envs.create(1).await?;
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
        Ok(handle)
    }

    process_handle = Some(start_process(&envs, &runtime, &path, &wasm_args, &dir).await?);
    info!("Initial process started");

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                info!("File change detected: {:?}", event.path);
                info!("Restarting process...");

                if let Some(handle) = process_handle.take() {
                    handle.abort();
                }

                match start_process(&envs, &runtime, &path, &wasm_args, &dir).await {
                    Ok(handle) => {
                        process_handle = Some(handle);
                        info!("Process restarted successfully");
                    }
                    Err(e) => {
                        error!("Failed to restart process: {}", e);
                    }
                }
            }
            result = async {
                if let Some(ref mut handle) = process_handle {
                    handle.await
                } else {
                    std::future::pending().await
                }
            } => {
                match result {
                    Ok(Ok(_)) => {
                        info!("Process finished successfully");
                        break;
                    }
                    Ok(Err(e)) => {
                        error!("Process error: {}", e);
                        info!("Waiting for file changes...");
                        process_handle = None;
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
