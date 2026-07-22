use std::{
    collections::HashMap,
    env, fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{bail, Context, Result};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::Message,
    runtimes::{
        wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
        RawWasm,
    },
    state::SignalSendError,
    wasm::spawn_wasm,
    Process, Signal,
};
use lunatic_process_api::ProcessConfigCtx;
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::{sync::RwLock, time::timeout};
use wasmtime::Val;

const REQUIRED_ENV: &str = "LUNATIC_MULTILANGUAGE_GUESTS_REQUIRED";
const TEST_TIMEOUT: Duration = Duration::from_secs(20);
static NEXT_ENVIRONMENT_ID: AtomicU64 = AtomicU64::new(10_000);

struct TagObserver {
    id: u64,
    tags: tokio::sync::mpsc::UnboundedSender<i64>,
}

impl Process for TagObserver {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) -> std::result::Result<(), SignalSendError> {
        if let Signal::Message(Message::Data(message)) = signal {
            let _ = self.tags.send(message.tag.unwrap_or(0));
        }
        Ok(())
    }
}

fn artifact(relative: &str, language: &str) -> Result<Option<PathBuf>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
    if path.is_file() {
        return Ok(Some(path));
    }

    if env::var_os(REQUIRED_ENV).is_some() {
        bail!(
            "{language} guest artifact is required but missing: {}",
            path.display()
        );
    }

    eprintln!(
        "skipping {language} guest E2E; build {} or set {REQUIRED_ENV}=1 to require it",
        path.display()
    );
    Ok(None)
}

async fn run_guest(
    language: &str,
    artifact: &Path,
    entry: &str,
    observer_tag: i64,
    observer_as_wasi_arg: bool,
) -> Result<()> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let raw = RawWasm::new(
        None,
        fs::read(artifact)
            .with_context(|| format!("failed to read {language} guest {}", artifact.display()))?,
    );
    let module: Arc<WasmtimeCompiledModule<DefaultProcessState>> =
        Arc::new(runtime.compile_module(raw).with_context(|| {
            format!(
                "failed to compile {language} guest artifact {}",
                artifact.display()
            )
        })?);
    let environment = Arc::new(LunaticEnvironment::new(
        NEXT_ENVIRONMENT_ID.fetch_add(1, Ordering::Relaxed),
    ));
    let (tag_sender, mut tag_receiver) = tokio::sync::mpsc::unbounded_channel();
    let observer_id = environment.get_next_process_id();
    let observer: Arc<dyn Process> = Arc::new(TagObserver {
        id: observer_id,
        tags: tag_sender,
    });
    environment.add_process(observer_id, observer)?;

    let mut config = DefaultProcessConfig::default();
    config.set_can_create_configs(true);
    config.set_can_spawn_processes(true);
    if observer_as_wasi_arg {
        config.set_command_line_arguments(vec![observer_id.to_string()]);
    }
    let state = DefaultProcessState::new(
        environment.clone(),
        None,
        runtime.clone(),
        module.clone(),
        Arc::new(config),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    let params = if observer_as_wasi_arg {
        Vec::new()
    } else {
        vec![Val::I64(observer_id as i64)]
    };
    let (join, process) = spawn_wasm(
        environment.clone(),
        runtime,
        &module,
        state,
        entry,
        params,
        None,
    )
    .await
    .with_context(|| format!("failed to spawn {language} guest entry {entry}"))?;
    let process_id = process.id();

    timeout(TEST_TIMEOUT, join)
        .await
        .with_context(|| format!("{language} guest timed out"))?
        .with_context(|| format!("{language} guest task panicked"))?
        .with_context(|| format!("{language} guest trapped"))?;
    let observed = timeout(TEST_TIMEOUT, tag_receiver.recv())
        .await
        .with_context(|| format!("{language} guest did not report completion"))?
        .with_context(|| format!("{language} observer channel closed"))?;
    assert_eq!(observed, observer_tag, "{language} completion tag drifted");
    assert!(environment.get_process(process_id).is_none());
    timeout(TEST_TIMEOUT, async {
        while environment.process_count() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .with_context(|| format!("{language} child process did not terminate"))?;
    assert!(environment.remove_process(observer_id));
    assert_eq!(environment.process_count(), 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rust_guest_process_message_timeout_and_permission_e2e() -> Result<()> {
    let Some(path) = artifact(
        "examples/rust/guest-e2e/target/wasm32-unknown-unknown/release/lunatic_rust_guest_e2e.wasm",
        "Rust",
    )?
    else {
        return Ok(());
    };
    run_guest("Rust", &path, "parent", 4_201, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn go_guest_process_message_timeout_and_permission_e2e() -> Result<()> {
    let Some(path) = artifact("examples/go/build/guest_e2e.wasm", "Go/TinyGo")? else {
        return Ok(());
    };
    run_guest("Go/TinyGo", &path, "_start", 4_202, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn assemblyscript_guest_process_message_timeout_and_permission_e2e() -> Result<()> {
    let Some(path) = artifact(
        "examples/assemblyscript/build/guest_e2e.wasm",
        "AssemblyScript",
    )?
    else {
        return Ok(());
    };
    run_guest("AssemblyScript", &path, "parent", 4_203, false).await
}
