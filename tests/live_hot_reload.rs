use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use lunatic_process::{
    env::{Environment, Environments, LunaticEnvironments},
    message::{DataMessage, Message},
    module_registry::ModuleRegistry,
    runtimes::wasmtime::{default_config, WasmtimeRuntime},
    wasm::{spawn_wasm_with_options, WasmSpawnOptions},
    Process, Signal,
};
use lunatic_runtime::{
    hot_reload::register_module_update, DefaultProcessConfig, DefaultProcessState,
};
use tokio::{
    sync::{mpsc, RwLock},
    time::timeout,
};
use wasmtime::Val;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const MODULE_ID: u64 = 0;
const PROCESS_ID_TAG_BASE: i64 = 900_000;

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
        ;; The first generation remains inside a real async host call until a
        ;; reload cancels its Wasmtime fiber. The flag is preserved in linear
        ;; memory so an explicit rollback can re-enter V1 without sleeping.
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

const GUEST_V2: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::process" "process_id" (func $process_id (result i64)))

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
        (call $report
            (local.get $observer)
            (i64.add (i64.const 900000) (call $process_id)))
        (call $report
            (local.get $observer)
            (i64.add
                (i64.const 2000)
                (i64.extend_i32_u (i32.load (i32.const 0)))))
        (loop $messages
            (drop (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))
            (local.set $tag (call $get_tag))
            (local.set $count (call $add (i32.const 2)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const 20000) (local.get $tag)))
            (call $report
                (local.get $observer)
                (i64.add (i64.const 30000) (i64.extend_i32_u (local.get $count))))
            (br $messages)))
)
"#;

const INCOMPATIBLE_GUEST: &str = r#"
(module
    (memory (export "memory") 1)
    (func (export "run") (param i32)))
"#;

const TRAPPING_START_GUEST: &str = r#"
(module
    (memory (export "memory") 1)
    (func $fail unreachable)
    (start $fail)
    (func (export "run") (param i64)))
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

fn tagged_message(tag: i64) -> Signal {
    Signal::Message(Message::Data(DataMessage::new(Some(tag), 0)))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_reload_preserves_live_process_state_and_rolls_back_failures() -> Result<()> {
    let runtime = WasmtimeRuntime::new(&default_config())?;
    let module_registry = Arc::new(ModuleRegistry::<DefaultProcessState>::with_max_versions(8));
    let initial_module = runtime.compile_module(wat::parse_str(GUEST_V1)?.into())?;
    assert_eq!(module_registry.add_version(MODULE_ID, initial_module), 0);
    let initial_module = module_registry
        .get_version(MODULE_ID, 0)
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

    let state = DefaultProcessState::new(
        environment.clone(),
        None,
        runtime.clone(),
        initial_module.clone(),
        Arc::new(DefaultProcessConfig::default()),
        Arc::new(RwLock::new(HashMap::new())),
    )?;
    let (join, process) = spawn_wasm_with_options(
        environment.clone(),
        runtime.clone(),
        &initial_module,
        state,
        "run",
        vec![Val::I64(observer.id() as i64)],
        WasmSpawnOptions {
            link: None,
            initial_module_version: Some((MODULE_ID, 0)),
        },
    )
    .await?;
    let process_id_tag = PROCESS_ID_TAG_BASE + process.id() as i64;

    expect_tags(&mut tag_receiver, &[process_id_tag, 1_041]).await?;

    // The signal loop is biased and FIFO: both messages enter the shared
    // mailbox before the reload command cancels the blocked receive.
    let version_1 = register_module_update::<DefaultProcessState>(
        &runtime,
        &module_registry,
        &environment_for_reload,
        MODULE_ID,
        wat::parse_str(GUEST_V2)?,
    )?;
    assert_eq!(version_1, 1);
    process.send(tagged_message(7));
    process.send(tagged_message(8));
    expect_tags(
        &mut tag_receiver,
        &[process_id_tag, 2_041, 20_007, 30_043, 20_008, 30_045],
    )
    .await?;

    process.send(tagged_message(9));
    expect_tags(&mut tag_receiver, &[20_009, 30_047]).await?;

    // Signature preflight rejects this version without cancelling V2.
    let incompatible_version = register_module_update::<DefaultProcessState>(
        &runtime,
        &module_registry,
        &environment_for_reload,
        MODULE_ID,
        wat::parse_str(INCOMPATIBLE_GUEST)?,
    )?;
    assert_eq!(incompatible_version, 2);
    process.send(tagged_message(10));
    expect_tags(&mut tag_receiver, &[20_010, 30_049]).await?;

    // This candidate passes signature validation but traps during
    // instantiation. V2 is already blocked in `receive`; queueing two messages
    // behind the reload forces cancellation to restore the first awakened
    // message at its original FIFO position before V2 is re-entered.
    let trapping_version = register_module_update::<DefaultProcessState>(
        &runtime,
        &module_registry,
        &environment_for_reload,
        MODULE_ID,
        wat::parse_str(TRAPPING_START_GUEST)?,
    )?;
    assert_eq!(trapping_version, 3);
    process.send(tagged_message(11));
    process.send(tagged_message(12));
    expect_tags(
        &mut tag_receiver,
        &[process_id_tag, 2_049, 20_011, 30_051, 20_012, 30_053],
    )
    .await?;

    // Exercise the production Rollback signal as another committed
    // transaction. V1 observes V2's counter and changes behavior back to +1.
    process.send(Signal::Rollback {
        module_id: MODULE_ID,
        target_version: 0,
    });
    expect_tags(&mut tag_receiver, &[process_id_tag, 1_053]).await?;
    process.send(tagged_message(13));
    expect_tags(&mut tag_receiver, &[10_013, 30_054]).await?;

    // A second forward reload proves committed version tracking is not reset
    // to zero after the first successful transaction.
    process.send(Signal::HotReload {
        module_id: MODULE_ID,
        new_version: 1,
    });
    expect_tags(&mut tag_receiver, &[process_id_tag, 2_054]).await?;

    process.send(Signal::Kill);
    let result = timeout(TEST_TIMEOUT, join)
        .await
        .context("live guest did not stop")?
        .context("live guest task panicked")?;
    assert!(
        result.is_err(),
        "kill should stop the live guest with an error"
    );
    assert!(environment.get_process(process.id()).is_none());

    Ok(())
}
