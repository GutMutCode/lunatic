use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironment, LunaticEnvironments},
    hot_reload::{ReloadCoordinator, ReloadStatus},
    message::{DataMessage, Message},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
    wasm::{spawn_wasm_with_options, WasmSpawnOptions},
    Process, Signal,
};
use lunatic_runtime::{
    hot_reload::register_module_update, DefaultProcessConfig, DefaultProcessState,
};
use tokio::{
    sync::{mpsc, RwLock},
    task::JoinHandle,
    time::{timeout, timeout_at, Instant},
};
use wasmtime::Val;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const MODULE_ID: u64 = 0;
const PROCESS_ID_TAG_BASE: i64 = 900_000;
const ALL_ACK_DELAY_MS: u64 = 1_000;
const APPLY_TIMEOUT: Duration = Duration::from_millis(1_000);
const LATE_APPLY_DELAY_MS: u64 = 2_500;
const ROLLBACK_TIMEOUT: Duration = Duration::from_secs(5);

const GUEST_V1: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))

    (memory (export "memory") 1)

    (func $report (param $observer i64) (param $tag i64)
        (call $create_data (local.get $tag) (i64.const 0))
        (drop (call $send (local.get $observer))))

    (func $add (param $delta i32) (result i32)
        (i32.const 0)
        (i32.const 0)
        (i32.load)
        (local.get $delta)
        (i32.add)
        (i32.store)
        (i32.const 0)
        (i32.load))

    (func (export "run") (param $observer i64)
        (local $tag i64)
        (local $count i32)
        (if (i32.eqz (i32.load (i32.const 0)))
            (then
                (i32.store (i32.const 0) (i32.const 41))))
        (call $report
            (local.get $observer)
            (i64.add (i64.const 900000) (call $process_id)))
        (call $report
            (local.get $observer)
            (i64.add
                (i64.const 1000)
                (i64.extend_i32_u (i32.load (i32.const 0)))))
        ;; The initial generation remains inside a real async host call until a
        ;; reload cancels its Wasmtime fiber. The flag survives a rollback, so
        ;; V1 does not repeat the long sleep when re-entered.
        (if (i32.eqz (i32.load (i32.const 4)))
            (then
                (i32.store (i32.const 4) (i32.const 1))
                (call $sleep_ms (i64.const 60000))))
        (loop $messages
            (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))
            (local.set $tag (call $get_tag))
            (local.set $count (call $add (i32.const 1)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const 10000) (local.get $tag)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const 30000) (i64.extend_i32_u (local.get $count))))
            (br $messages)))
)
"#;

const RELOAD_GUEST_TEMPLATE: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))

    (memory (export "memory") 1)

    __START_FUNCTION__

    (func $report (param $observer i64) (param $tag i64)
        (call $create_data (local.get $tag) (i64.const 0))
        (drop (call $send (local.get $observer))))

    (func $add (param $delta i32) (result i32)
        (i32.const 0)
        (i32.const 0)
        (i32.load)
        (local.get $delta)
        (i32.add)
        (i32.store)
        (i32.const 0)
        (i32.load))

    (func (export "run") (param $observer i64)
        (local $tag i64)
        (local $count i32)
        (call $report
            (local.get $observer)
            (i64.add (i64.const 900000) (call $process_id)))
        (call $report
            (local.get $observer)
            (i64.add
                (i64.const __GENERATION_BASE__)
                (i64.extend_i32_u (i32.load (i32.const 0)))))
        (loop $messages
            (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))
            (local.set $tag (call $get_tag))
            (local.set $count (call $add (i32.const __DELTA__)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const __RESPONSE_BASE__) (local.get $tag)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const 30000) (i64.extend_i32_u (local.get $count))))
            (br $messages)))
)
"#;

struct RecordingProcess {
    id: u64,
    tags: mpsc::UnboundedSender<i64>,
}

impl Process for RecordingProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        if let Signal::Message(Message::Data(message)) = signal {
            if let Some(tag) = message.tag {
                let _ = self.tags.send(tag);
            }
        }
    }
}

struct LiveProcess {
    join: JoinHandle<Result<DefaultProcessState>>,
    process: Arc<dyn Process>,
}

fn reload_guest(delta: i32, start_function: &str) -> String {
    RELOAD_GUEST_TEMPLATE
        .replace("__START_FUNCTION__", start_function)
        .replace("__GENERATION_BASE__", &(delta * 1_000).to_string())
        .replace("__DELTA__", &delta.to_string())
        .replace("__RESPONSE_BASE__", &(delta * 10_000).to_string())
}

fn trapping_start(process_id: u64) -> String {
    format!(
        r#"
        (func $reload_start
            (if (i64.eq (call $process_id) (i64.const {process_id}))
                (then unreachable)))
        (start $reload_start)
        "#
    )
}

fn sleeping_start(process_id: u64, millis: u64) -> String {
    format!(
        r#"
        (func $reload_start
            (if (i64.eq (call $process_id) (i64.const {process_id}))
                (then (call $sleep_ms (i64.const {millis})))))
        (start $reload_start)
        "#
    )
}

async fn spawn_live_process(
    environment: Arc<LunaticEnvironment>,
    runtime: &WasmtimeRuntime,
    module: &Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    observer_id: u64,
    version: u32,
) -> Result<LiveProcess> {
    let state = DefaultProcessState::new(
        environment.clone(),
        None,
        runtime.clone(),
        module.clone(),
        Arc::new(DefaultProcessConfig::default()),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    let (join, process) = spawn_wasm_with_options(
        environment,
        runtime.clone(),
        module,
        state,
        "run",
        vec![Val::I64(observer_id as i64)],
        WasmSpawnOptions {
            link: None,
            initial_module_version: Some((MODULE_ID, version)),
        },
    )
    .await?;
    Ok(LiveProcess { join, process })
}

fn spawn_module_update(
    runtime: WasmtimeRuntime,
    registry: Arc<ModuleRegistry<DefaultProcessState>>,
    environment: Arc<dyn Environment>,
    coordinator: Arc<ReloadCoordinator<DefaultProcessState>>,
    bytes: Vec<u8>,
) -> JoinHandle<Result<u32>> {
    tokio::spawn(async move {
        register_module_update(
            &runtime,
            registry.as_ref(),
            &environment,
            coordinator.as_ref(),
            MODULE_ID,
            bytes,
        )
        .await
    })
}

async fn await_reload_task(handle: JoinHandle<Result<u32>>) -> Result<Result<u32>> {
    let result = timeout(TEST_TIMEOUT, handle)
        .await
        .context("timed out waiting for the coordinated reload")?
        .context("coordinated reload task panicked")?;
    Ok(result)
}

async fn expect_tags(receiver: &mut mpsc::UnboundedReceiver<i64>, expected: &[i64]) -> Result<()> {
    for expected_tag in expected {
        let actual = timeout(TEST_TIMEOUT, receiver.recv())
            .await
            .context("timed out waiting for the live guest report")?
            .context("live guest report channel closed")?;
        assert_eq!(actual, *expected_tag);
    }
    Ok(())
}

async fn expect_tags_unordered(
    receiver: &mut mpsc::UnboundedReceiver<i64>,
    expected: &[i64],
    forbidden: &[i64],
) -> Result<()> {
    let mut remaining = HashMap::<i64, usize>::new();
    for tag in expected {
        *remaining.entry(*tag).or_default() += 1;
    }
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut observed = Vec::new();

    while !remaining.is_empty() {
        let tag = match timeout_at(deadline, receiver.recv()).await {
            Ok(Some(tag)) => tag,
            Ok(None) => bail!(
                "live guest report channel closed; remaining={remaining:?}, observed={observed:?}"
            ),
            Err(_) => bail!(
                "timed out waiting for live guest reports; remaining={remaining:?}, observed={observed:?}"
            ),
        };
        observed.push(tag);
        if forbidden.contains(&tag) {
            bail!("observed forbidden live guest tag {tag}; observed={observed:?}");
        }
        if let Some(count) = remaining.get_mut(&tag) {
            *count -= 1;
            if *count == 0 {
                remaining.remove(&tag);
            }
        }
    }
    Ok(())
}

fn tagged_message(tag: i64) -> Signal {
    Signal::Message(Message::Data(DataMessage::new(Some(tag), 0)))
}

fn process_id_tag(process: &Arc<dyn Process>) -> i64 {
    PROCESS_ID_TAG_BASE + process.id() as i64
}

fn acknowledgement_ids(status: &ReloadStatus) -> Vec<u64> {
    let acknowledgements = match status {
        ReloadStatus::Committed { acknowledgements }
        | ReloadStatus::RolledBack {
            acknowledgements, ..
        }
        | ReloadStatus::InDoubt {
            acknowledgements, ..
        } => acknowledgements,
        ReloadStatus::Applying | ReloadStatus::RollingBack { .. } => return Vec::new(),
    };
    acknowledgements
        .iter()
        .map(|acknowledgement| acknowledgement.process_id)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_atomic_reload_waits_for_acks_and_restores_every_process() -> Result<()> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::with_max_versions(8));
    let initial_module = runtime.compile_module(wat::parse_str(GUEST_V1)?.into())?;
    let initial_version = module_registry.add_version(MODULE_ID, initial_module)?;
    assert_eq!(initial_version, 0);
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(initial_version)
    );
    let initial_module = module_registry
        .get_version(MODULE_ID, initial_version)
        .context("initial module version was not registered")?;

    let environments = Arc::new(LunaticEnvironments::default());
    let environment = environments
        .create_with_registry(
            7,
            module_registry.clone() as Arc<dyn std::any::Any + Send + Sync>,
        )
        .await?;
    let environment_for_reload: Arc<dyn Environment> = environment.clone();

    let (tag_sender, mut tag_receiver) = mpsc::unbounded_channel();
    let observer = Arc::new(RecordingProcess {
        id: environment.get_next_process_id(),
        tags: tag_sender,
    });
    environment.add_process(observer.id(), observer.clone());

    let first = spawn_live_process(
        environment.clone(),
        &runtime,
        &initial_module,
        observer.id(),
        initial_version,
    )
    .await?;
    let second = spawn_live_process(
        environment.clone(),
        &runtime,
        &initial_module,
        observer.id(),
        initial_version,
    )
    .await?;
    let process_ids = vec![first.process.id(), second.process.id()];

    expect_tags_unordered(
        &mut tag_receiver,
        &[
            process_id_tag(&first.process),
            1_041,
            process_id_tag(&second.process),
            1_041,
        ],
        &[],
    )
    .await?;
    assert_eq!(
        module_registry.process_count(MODULE_ID, initial_version),
        Some(2)
    );

    // The second candidate pauses in an actual async guest import. The first
    // process applies and reports V2, but the public operation and committed
    // pointer must remain pending until the second process acknowledges.
    let coordinator = Arc::new(ReloadCoordinator::new());
    let gated_guest = reload_guest(2, &sleeping_start(second.process.id(), ALL_ACK_DELAY_MS));
    let committed_reload = spawn_module_update(
        runtime.clone(),
        module_registry.clone(),
        environment_for_reload.clone(),
        coordinator.clone(),
        wat::parse_str(&gated_guest)?,
    );
    expect_tags_unordered(
        &mut tag_receiver,
        &[process_id_tag(&first.process), 2_041],
        &[],
    )
    .await?;
    assert!(!committed_reload.is_finished());
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(initial_version)
    );
    assert_eq!(module_registry.process_count(MODULE_ID, 0), Some(1));
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(1));

    let committed_version = await_reload_task(committed_reload).await??;
    assert_eq!(committed_version, 1);
    expect_tags_unordered(
        &mut tag_receiver,
        &[process_id_tag(&second.process), 2_041],
        &[],
    )
    .await?;
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(committed_version)
    );
    assert_eq!(module_registry.process_count(MODULE_ID, 0), Some(0));
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(2));
    let committed_status = coordinator
        .status(MODULE_ID)
        .await
        .context("coordinator did not retain committed status")?;
    assert!(matches!(committed_status, ReloadStatus::Committed { .. }));
    assert_eq!(acknowledgement_ids(&committed_status), process_ids);

    // Preserve the original live-state/FIFO assertion on each actual process.
    first.process.send(tagged_message(7));
    first.process.send(tagged_message(9));
    expect_tags(&mut tag_receiver, &[20_007, 30_043, 20_009, 30_045]).await?;
    second.process.send(tagged_message(8));
    second.process.send(tagged_message(10));
    expect_tags(&mut tag_receiver, &[20_008, 30_043, 20_010, 30_045]).await?;

    // One candidate traps in its real Wasm start function while the peer can
    // apply it. The top-level update must fail, keep the committed pointer, and
    // confirm both processes back on V2.
    let partial_guest = reload_guest(3, &trapping_start(second.process.id()));
    let partial_reload = spawn_module_update(
        runtime.clone(),
        module_registry.clone(),
        environment_for_reload.clone(),
        coordinator.clone(),
        wat::parse_str(&partial_guest)?,
    );
    let partial_result = await_reload_task(partial_reload).await?;
    assert!(partial_result.is_err());
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(committed_version)
    );
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(2));
    assert_eq!(module_registry.process_count(MODULE_ID, 2), Some(0));
    let partial_status = coordinator
        .status(MODULE_ID)
        .await
        .context("coordinator did not retain rolled-back status")?;
    match &partial_status {
        ReloadStatus::RolledBack { apply_errors, .. } => {
            assert_eq!(apply_errors.len(), 1);
            assert_eq!(apply_errors[0].process_id, second.process.id());
            assert_eq!(apply_errors[0].known_version, Some(committed_version));
        }
        status => bail!("partial reload ended in unexpected status: {status:?}"),
    }
    assert_eq!(acknowledgement_ids(&partial_status), process_ids);

    first.process.send(tagged_message(11));
    expect_tags_unordered(&mut tag_receiver, &[20_011, 30_047], &[30_011]).await?;
    second.process.send(tagged_message(12));
    expect_tags_unordered(&mut tag_receiver, &[20_012, 30_047], &[30_012]).await?;

    // The slow process finishes applying after the coordinator's apply
    // timeout. A sufficiently long rollback timeout must wait for that late
    // apply and obtain a real rollback acknowledgement rather than accepting
    // an early "already old" observation.
    let timeout_coordinator = Arc::new(ReloadCoordinator::with_timeouts(
        APPLY_TIMEOUT,
        ROLLBACK_TIMEOUT,
    ));
    let delayed_guest = reload_guest(4, &sleeping_start(second.process.id(), LATE_APPLY_DELAY_MS));
    let delayed_reload = spawn_module_update(
        runtime.clone(),
        module_registry.clone(),
        environment_for_reload,
        timeout_coordinator.clone(),
        wat::parse_str(&delayed_guest)?,
    );
    let delayed_result = await_reload_task(delayed_reload).await?;
    assert!(delayed_result.is_err());
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(committed_version)
    );
    let delayed_status = timeout_coordinator
        .status(MODULE_ID)
        .await
        .context("coordinator did not retain timeout rollback status")?;
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(2));
    assert_eq!(module_registry.process_count(MODULE_ID, 3), Some(0));
    match &delayed_status {
        ReloadStatus::RolledBack { apply_errors, .. } => {
            assert_eq!(apply_errors.len(), 1);
            assert_eq!(apply_errors[0].process_id, second.process.id());
            assert_eq!(apply_errors[0].known_version, None);
        }
        status => bail!("timed-out reload ended in unexpected status: {status:?}"),
    }
    assert_eq!(acknowledgement_ids(&delayed_status), process_ids);

    first.process.send(tagged_message(13));
    expect_tags_unordered(&mut tag_receiver, &[20_013, 30_049], &[40_013]).await?;
    second.process.send(tagged_message(14));
    expect_tags_unordered(&mut tag_receiver, &[20_014, 30_049], &[40_014]).await?;

    // Force a real rollback timeout. The first candidate traps, while the
    // second applies V5. Rolling the second process back reinstantiates the
    // committed V2 whose PID-conditional start sleeps for one second; a 100ms
    // rollback deadline must therefore surface InDoubt rather than success.
    let in_doubt_coordinator = Arc::new(ReloadCoordinator::with_timeouts(
        Duration::from_secs(3),
        Duration::from_millis(100),
    ));
    let rollback_timeout_guest = reload_guest(5, &trapping_start(first.process.id()));
    let rollback_timeout = spawn_module_update(
        runtime.clone(),
        module_registry.clone(),
        environment.clone() as Arc<dyn Environment>,
        in_doubt_coordinator.clone(),
        wat::parse_str(&rollback_timeout_guest)?,
    );
    let rollback_timeout_result = await_reload_task(rollback_timeout).await?;
    assert!(rollback_timeout_result.is_err());
    assert_eq!(
        module_registry.get_committed_version_number(MODULE_ID),
        Some(committed_version)
    );
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(1));
    assert_eq!(module_registry.process_count(MODULE_ID, 4), Some(1));

    let in_doubt_status = in_doubt_coordinator
        .status(MODULE_ID)
        .await
        .context("coordinator did not retain in-doubt status")?;
    match &in_doubt_status {
        ReloadStatus::InDoubt {
            apply_errors,
            rollback_errors,
            ..
        } => {
            assert_eq!(apply_errors.len(), 1);
            assert_eq!(apply_errors[0].process_id, first.process.id());
            assert_eq!(rollback_errors.len(), 1);
            assert_eq!(rollback_errors[0].process_id, second.process.id());
            assert_eq!(rollback_errors[0].known_version, None);
        }
        status => bail!("rollback timeout ended in unexpected status: {status:?}"),
    }

    // InDoubt is a hard preflight barrier at the public API. In particular, a
    // rejected follow-up must not even allocate another registry version.
    let latest_before_blocked_reload = module_registry.get_latest_version_number(MODULE_ID);
    let version_count_before_blocked_reload = module_registry.version_count(MODULE_ID);
    let blocked_guest = reload_guest(6, "(func $reload_start) (start $reload_start)");
    let environment_for_blocked_reload: Arc<dyn Environment> = environment.clone();
    let blocked_result = register_module_update(
        &runtime,
        module_registry.as_ref(),
        &environment_for_blocked_reload,
        in_doubt_coordinator.as_ref(),
        MODULE_ID,
        wat::parse_str(&blocked_guest)?,
    )
    .await;
    assert!(blocked_result.is_err());
    assert_eq!(
        module_registry.get_latest_version_number(MODULE_ID),
        latest_before_blocked_reload
    );
    assert_eq!(
        module_registry.version_count(MODULE_ID),
        version_count_before_blocked_reload
    );

    // The timed-out rollback continues in the real execution driver. Wait on
    // registry lifecycle state instead of sleeping for a guessed wall time,
    // then verify both guests actually execute the committed code again.
    timeout(TEST_TIMEOUT, async {
        loop {
            if module_registry.process_count(MODULE_ID, 1) == Some(2)
                && module_registry.process_count(MODULE_ID, 4) == Some(0)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("late rollback did not restore registry process counts")?;

    first.process.send(tagged_message(15));
    expect_tags_unordered(&mut tag_receiver, &[20_015, 30_051], &[50_015]).await?;
    second.process.send(tagged_message(16));
    expect_tags_unordered(&mut tag_receiver, &[20_016, 30_051], &[50_016]).await?;

    let first_id = first.process.id();
    first.process.send(Signal::Kill);
    let first_result = timeout(TEST_TIMEOUT, first.join)
        .await
        .context("first live guest did not stop")?
        .context("first live guest task panicked")?;
    assert!(first_result.is_err(), "kill must fail the live guest");
    assert!(environment.get_process(first_id).is_none());
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(1));

    let second_id = second.process.id();
    second.process.send(Signal::Kill);
    let second_result = timeout(TEST_TIMEOUT, second.join)
        .await
        .context("second live guest did not stop")?
        .context("second live guest task panicked")?;
    assert!(second_result.is_err(), "kill must fail the live guest");
    assert!(environment.get_process(second_id).is_none());
    assert_eq!(module_registry.process_count(MODULE_ID, 1), Some(0));

    environment.remove_process(observer.id());
    Ok(())
}
