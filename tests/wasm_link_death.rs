use std::{
    collections::HashMap,
    future::pending,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use anyhow::{anyhow, bail, Context, Result};
use lunatic_process::{
    env::{Environment, LunaticEnvironment},
    message::{DataMessage, Message},
    runtimes::{
        wasmtime::{default_config, WasmtimeCompiledModule, WasmtimeRuntime},
        RawWasm,
    },
    state::ProcessState,
    wasm::spawn_wasm,
    DeathReason, Process, Signal,
};
use lunatic_runtime::{state::DefaultProcessState, DefaultProcessConfig};
use tokio::{
    sync::{oneshot, Notify, RwLock},
    task::JoinHandle,
    time::timeout,
};
use wasmtime::{Linker, Val};

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const MISSING_PROCESS_ID: u64 = 9_999_999;

const LIFECYCLE_GUEST: &str = r#"
(module
    (import "lunatic::message" "create_data" (func $create_data (param i64 i64)))
    (import "lunatic::message" "send" (func $send (param i64) (result i32)))
    (import "lunatic::message" "receive" (func $receive (param i32 i32 i64) (result i32)))
    (import "lunatic::message" "get_tag" (func $get_tag (result i64)))
    (import "lunatic::message" "get_process_id" (func $get_process_id (result i64)))
    (import "lunatic::process" "die_when_link_dies" (func $die_when_link_dies (param i32)))
    (import "lunatic::process" "link" (func $link (param i64 i64)))
    (import "lunatic::process" "monitor" (func $monitor (param i64)))
    (import "lunatic::process" "sleep_ms" (func $sleep_ms (param i64)))
    (import "test" "panic" (func $host_panic))

    (func $ready (param $observer i64)
        (call $create_data (i64.const 0) (i64.const 0))
        (drop (call $send (local.get $observer))))

    (func $wait (result i32)
        (call $receive (i32.const 0) (i32.const 0) (i64.const -1)))

    (func (export "normal") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "guest_trap") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        unreachable)

    (func (export "host_panic") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $host_panic))

    (func (export "wait_for_kill") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "sleep_for_kill") (param $observer i64)
        (call $ready (local.get $observer))
        (call $sleep_ms (i64.const 60000)))

    (func (export "no_process") (param $observer i64) (param $missing i64)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $link (i64.const 404) (local.get $missing))
        (drop (call $wait))
        unreachable)

    (func (export "trap_no_process") (param $observer i64) (param $missing i64)
        (local $kind i32)
        (call $die_when_link_dies (i32.const 0))
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $link (i64.const 404) (local.get $missing))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 1)) (then unreachable))
        (if (i64.ne (call $get_tag) (i64.const 404)) (then unreachable)))

    (func (export "missing_monitor") (param $observer i64) (param $missing i64)
        (local $kind i32)
        (call $ready (local.get $observer))
        (drop (call $wait))
        (call $monitor (local.get $missing))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 2)) (then unreachable))
        (if (i64.ne (call $get_process_id) (local.get $missing)) (then unreachable)))

    (func (export "default_peer") (param $observer i64)
        (call $ready (local.get $observer))
        (drop (call $wait)))

    (func (export "trap_peer") (param $observer i64) (param $expected_tag i64)
        (local $kind i32)
        (call $die_when_link_dies (i32.const 0))
        (call $ready (local.get $observer))
        (local.set $kind (call $wait))
        (if (i32.ne (local.get $kind) (i32.const 1)) (then unreachable))
        (if (i64.ne (call $get_tag) (local.get $expected_tag)) (then unreachable)))

    (func (export "fail_now")
        unreachable)
)
"#;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Observed {
    Ready,
    LinkDied {
        process_id: u64,
        tag: Option<i64>,
        reason: DeathReason,
        removed_before_notification: bool,
    },
    ProcessDied {
        process_id: u64,
        removed_before_notification: bool,
    },
}

struct RecordingProcess {
    id: u64,
    environment: Weak<LunaticEnvironment>,
    events: Mutex<Vec<Observed>>,
    notify: Notify,
}

impl RecordingProcess {
    fn new(id: u64, environment: &Arc<LunaticEnvironment>) -> Self {
        Self {
            id,
            environment: Arc::downgrade(environment),
            events: Mutex::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    async fn wait_until_ready(&self) -> Result<()> {
        timeout(TEST_TIMEOUT, async {
            loop {
                let notified = self.notify.notified();
                if self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| matches!(event, Observed::Ready))
                {
                    return;
                }
                notified.await;
            }
        })
        .await
        .context("Wasm guest did not reach the ready barrier")?;
        Ok(())
    }

    fn assert_lifecycle(
        &self,
        process_id: u64,
        expected_tag: Option<i64>,
        expected_reason: DeathReason,
    ) {
        let events = self.events.lock().unwrap();
        let link_deaths: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Observed::LinkDied {
                    process_id: observed_id,
                    tag,
                    reason,
                    removed_before_notification,
                } if *observed_id == process_id => {
                    Some((*tag, *reason, *removed_before_notification))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            link_deaths,
            vec![(expected_tag, expected_reason, true)],
            "link notification must preserve the exit reason and be sent once after removal"
        );

        let monitor_deaths: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Observed::ProcessDied {
                    process_id: observed_id,
                    removed_before_notification,
                } if *observed_id == process_id => Some(*removed_before_notification),
                _ => None,
            })
            .collect();
        assert_eq!(
            monitor_deaths,
            vec![true],
            "duplicate monitor registration must still notify exactly once after removal"
        );
    }

    fn removed(&self, process_id: u64) -> bool {
        self.environment
            .upgrade()
            .and_then(|environment| environment.get_process(process_id))
            .is_none()
    }
}

impl Process for RecordingProcess {
    fn id(&self) -> u64 {
        self.id
    }

    fn send(&self, signal: Signal) {
        let event = match signal {
            Signal::Message(Message::Data(_)) => Some(Observed::Ready),
            Signal::LinkDied(process_id, tag, reason) => Some(Observed::LinkDied {
                process_id,
                tag,
                reason,
                removed_before_notification: self.removed(process_id),
            }),
            Signal::ProcessDied(process_id) => Some(Observed::ProcessDied {
                process_id,
                removed_before_notification: self.removed(process_id),
            }),
            _ => None,
        };

        if let Some(event) = event {
            self.events.lock().unwrap().push(event);
            self.notify.notify_one();
        }
    }
}

struct WasmHarness {
    environment: Arc<LunaticEnvironment>,
    runtime: WasmtimeRuntime,
    module: Arc<WasmtimeCompiledModule<DefaultProcessState>>,
    registry: Arc<RwLock<HashMap<String, (u64, u64)>>>,
}

impl WasmHarness {
    fn new() -> Result<Self> {
        let runtime = WasmtimeRuntime::new(&default_config())?;
        let raw = RawWasm::new(None, wat::parse_str(LIFECYCLE_GUEST)?);
        let module = wasmtime::Module::new(runtime.engine(), raw.as_slice())?;
        let mut linker = Linker::<DefaultProcessState>::new(runtime.engine());
        <DefaultProcessState as ProcessState>::register(&mut linker)?;
        linker.func_wrap::<_, ()>("test", "panic", || {
            panic!("intentional host panic for lifecycle verification")
        })?;
        let instance_pre = linker.instantiate_pre(&module)?;
        let module = Arc::new(WasmtimeCompiledModule::new(raw, module, instance_pre));

        Ok(Self {
            environment: Arc::new(LunaticEnvironment::new(7)),
            runtime,
            module,
            registry: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn recorder(&self) -> Arc<RecordingProcess> {
        let id = self.environment.get_next_process_id();
        let recorder = Arc::new(RecordingProcess::new(id, &self.environment));
        self.environment.add_process(id, recorder.clone());
        recorder
    }

    async fn spawn(
        &self,
        function: &str,
        params: Vec<Val>,
        link: Option<(Option<i64>, Arc<dyn Process>)>,
    ) -> Result<(JoinHandle<Result<DefaultProcessState>>, Arc<dyn Process>)> {
        let state = DefaultProcessState::new(
            self.environment.clone(),
            None,
            self.runtime.clone(),
            self.module.clone(),
            Arc::new(DefaultProcessConfig::default()),
            self.registry.clone(),
        )?;
        spawn_wasm(
            self.environment.clone(),
            self.runtime.clone(),
            &self.module,
            state,
            function,
            params,
            link,
        )
        .await
    }
}

#[derive(Clone, Copy)]
enum Trigger {
    Release,
    Kill,
}

#[derive(Clone, Copy)]
enum ExpectedOutcome {
    Success,
    Failure(&'static str),
}

async fn wait_for_join<T>(join: JoinHandle<Result<T>>) -> Result<Result<T>> {
    timeout(TEST_TIMEOUT, join)
        .await
        .context("process did not terminate before the lifecycle timeout")?
        .context("process task panicked outside the lifecycle runner")
}

async fn run_wasm_case(
    harness: &WasmHarness,
    function: &str,
    extra_params: Vec<Val>,
    trigger: Trigger,
    expected_outcome: ExpectedOutcome,
    expected_reason: DeathReason,
    link_tag: i64,
) -> Result<Option<DefaultProcessState>> {
    eprintln!("wasm lifecycle case `{function}`: starting");
    let recorder = harness.recorder();
    let observer: Arc<dyn Process> = recorder.clone();
    let mut params = vec![Val::I64(recorder.id() as i64)];
    params.extend(extra_params);
    let (join, process) = harness
        .spawn(function, params, Some((Some(link_tag), observer.clone())))
        .await?;
    let process_id = process.id();

    recorder.wait_until_ready().await?;
    process.send(Signal::Monitor(observer.clone()));
    process.send(Signal::Monitor(observer));

    match trigger {
        Trigger::Release => process.send(Signal::Message(Message::Data(DataMessage::default()))),
        Trigger::Kill => process.send(Signal::Kill),
    }

    let outcome = wait_for_join(join).await?;
    let state = match (expected_outcome, outcome) {
        (ExpectedOutcome::Success, Ok(state)) => Some(state),
        (ExpectedOutcome::Success, Err(error)) => {
            bail!("{function} should succeed, but failed: {error:#}")
        }
        (ExpectedOutcome::Failure(expected), Err(error)) => {
            let rendered = format!("{error:#}");
            assert!(
                rendered.contains(expected),
                "{} error `{}` did not contain `{}`",
                function,
                rendered,
                expected
            );
            None
        }
        (ExpectedOutcome::Failure(_), Ok(_)) => {
            bail!("{function} should fail, but returned successfully")
        }
    };

    recorder.assert_lifecycle(process_id, Some(link_tag), expected_reason);
    assert!(
        harness.environment.get_process(process_id).is_none(),
        "terminated process must be removed from the environment"
    );
    eprintln!("wasm lifecycle case `{function}`: completed");
    Ok(state)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_exit_matrix_preserves_link_and_monitor_semantics() -> Result<()> {
    let harness = WasmHarness::new()?;

    run_wasm_case(
        &harness,
        "normal",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        101,
    )
    .await?;
    run_wasm_case(
        &harness,
        "guest_trap",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Failure("wasm backtrace"),
        DeathReason::Failure,
        102,
    )
    .await?;
    run_wasm_case(
        &harness,
        "host_panic",
        Vec::new(),
        Trigger::Release,
        ExpectedOutcome::Failure("Process panicked"),
        DeathReason::Failure,
        103,
    )
    .await?;
    run_wasm_case(
        &harness,
        "wait_for_kill",
        Vec::new(),
        Trigger::Kill,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        104,
    )
    .await?;
    run_wasm_case(
        &harness,
        "sleep_for_kill",
        Vec::new(),
        Trigger::Kill,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        105,
    )
    .await?;
    run_wasm_case(
        &harness,
        "no_process",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Failure("Process killed"),
        DeathReason::Failure,
        106,
    )
    .await?;

    let trap_state = run_wasm_case(
        &harness,
        "trap_no_process",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        107,
    )
    .await?
    .context("trap-exit guest should return its state")?;
    assert!(
        trap_state.message_mailbox().is_empty(),
        "the tagged LinkDied message must be consumed exactly once"
    );

    let monitor_state = run_wasm_case(
        &harness,
        "missing_monitor",
        vec![Val::I64(MISSING_PROCESS_ID as i64)],
        Trigger::Release,
        ExpectedOutcome::Success,
        DeathReason::Normal,
        108,
    )
    .await?
    .context("missing-monitor guest should return its state")?;
    assert!(
        monitor_state.message_mailbox().is_empty(),
        "monitoring a missing process must enqueue exactly one ProcessDied message"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_wasm_peer_honors_default_and_trap_exit_link_modes() -> Result<()> {
    let harness = WasmHarness::new()?;

    let default_observer = harness.recorder();
    let default_observer_process: Arc<dyn Process> = default_observer.clone();
    let (default_join, default_peer) = harness
        .spawn(
            "default_peer",
            vec![Val::I64(default_observer.id() as i64)],
            Some((Some(201), default_observer_process.clone())),
        )
        .await?;
    let default_peer_id = default_peer.id();
    default_observer.wait_until_ready().await?;
    default_peer.send(Signal::Monitor(default_observer_process.clone()));
    default_peer.send(Signal::Monitor(default_observer_process));

    let (failing_join, _) = harness
        .spawn(
            "fail_now",
            Vec::new(),
            Some((Some(9001), default_peer.clone())),
        )
        .await?;
    assert!(wait_for_join(failing_join).await?.is_err());
    let default_result = wait_for_join(default_join).await?;
    let default_error = match default_result {
        Ok(_) => bail!("default linked Wasm peer should terminate after child failure"),
        Err(error) => error,
    };
    assert!(format!("{default_error:#}").contains("Process killed"));
    default_observer.assert_lifecycle(default_peer_id, Some(201), DeathReason::Failure);

    let trap_observer = harness.recorder();
    let trap_observer_process: Arc<dyn Process> = trap_observer.clone();
    let (trap_join, trap_peer) = harness
        .spawn(
            "trap_peer",
            vec![Val::I64(trap_observer.id() as i64), Val::I64(9002)],
            Some((Some(202), trap_observer_process.clone())),
        )
        .await?;
    let trap_peer_id = trap_peer.id();
    trap_observer.wait_until_ready().await?;
    trap_peer.send(Signal::Monitor(trap_observer_process.clone()));
    trap_peer.send(Signal::Monitor(trap_observer_process));

    let (failing_join, _) = harness
        .spawn(
            "fail_now",
            Vec::new(),
            Some((Some(9002), trap_peer.clone())),
        )
        .await?;
    assert!(wait_for_join(failing_join).await?.is_err());
    let trap_state = wait_for_join(trap_join).await??;
    assert!(
        trap_state.message_mailbox().is_empty(),
        "trap-exit peer must consume the tagged LinkDied message"
    );
    trap_observer.assert_lifecycle(trap_peer_id, Some(202), DeathReason::Normal);

    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum NativeOutcome {
    Normal,
    Error,
    Panic,
    Kill,
    NoProcess,
}

async fn run_native_case(outcome: NativeOutcome, link_tag: i64) -> Result<()> {
    let environment = Arc::new(LunaticEnvironment::new(8));
    let observer_id = environment.get_next_process_id();
    let recorder = Arc::new(RecordingProcess::new(observer_id, &environment));
    environment.add_process(observer_id, recorder.clone());
    let observer: Arc<dyn Process> = recorder.clone();
    let (release_sender, release_receiver) = oneshot::channel::<()>();

    let (join, process) =
        lunatic_process::spawn_native(environment.clone(), move |_, _| async move {
            match outcome {
                NativeOutcome::Normal => {
                    release_receiver
                        .await
                        .map_err(|_| anyhow!("native release sender disappeared"))?;
                    Ok(())
                }
                NativeOutcome::Error => {
                    release_receiver
                        .await
                        .map_err(|_| anyhow!("native release sender disappeared"))?;
                    Err(anyhow!("intentional native failure"))
                }
                NativeOutcome::Panic => {
                    let _ = release_receiver.await;
                    panic!("intentional native panic")
                }
                NativeOutcome::Kill | NativeOutcome::NoProcess => pending::<Result<()>>().await,
            }
        });
    let process_id = process.id();
    process.send(Signal::Link(Some(link_tag), observer.clone()));
    process.send(Signal::Monitor(observer.clone()));
    process.send(Signal::Monitor(observer));

    match outcome {
        NativeOutcome::Normal | NativeOutcome::Error | NativeOutcome::Panic => release_sender
            .send(())
            .map_err(|_| anyhow!("native process exited before release"))?,
        NativeOutcome::Kill => process.send(Signal::Kill),
        NativeOutcome::NoProcess => process.send(Signal::LinkDied(
            MISSING_PROCESS_ID,
            Some(404),
            DeathReason::NoProcess,
        )),
    }

    let result = wait_for_join(join).await?;
    let expected_reason = match outcome {
        NativeOutcome::Normal => {
            if let Err(error) = result {
                bail!("native normal process failed: {error:#}");
            }
            DeathReason::Normal
        }
        NativeOutcome::Error => {
            let error = result.err().context("native error process should fail")?;
            assert!(format!("{error:#}").contains("intentional native failure"));
            DeathReason::Failure
        }
        NativeOutcome::Panic => {
            let error = result.err().context("native panic should be captured")?;
            assert!(format!("{error:#}").contains("Process panicked"));
            DeathReason::Failure
        }
        NativeOutcome::Kill | NativeOutcome::NoProcess => {
            let error = result
                .err()
                .context("native terminated process should fail")?;
            assert!(format!("{error:#}").contains("Process killed"));
            DeathReason::Failure
        }
    };

    recorder.assert_lifecycle(process_id, Some(link_tag), expected_reason);
    assert!(environment.get_process(process_id).is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_and_wasm_runners_share_the_same_exit_contract() -> Result<()> {
    for (index, outcome) in [
        NativeOutcome::Normal,
        NativeOutcome::Error,
        NativeOutcome::Panic,
        NativeOutcome::Kill,
        NativeOutcome::NoProcess,
    ]
    .iter()
    .copied()
    .enumerate()
    {
        run_native_case(outcome, 300 + index as i64).await?;
    }
    Ok(())
}
